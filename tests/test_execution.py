import asyncio, base64, hashlib, hmac, json, time
from pathlib import Path

import httpx
import pytest

from shorteth.exchange.bitget import Bitget, ExchangeError, _rows
from shorteth.exchange.store import StoreBridge
from shorteth.exchange.signals import decimal as D, floor_step
from shorteth.execution import Execution, Ledger, OrderBlocked, TradeIntent, covers, filled, oid
from shorteth.strategy import HOUR_MS, Signal, evaluate, target_notional
from shorteth.runner import Runner


def credentials(account_type='classic'):
    return StoreBridge(account_type,{'SHORTETH_DEMO_KEY':'key123',
        'SHORTETH_DEMO_SECRET':'secret123','SHORTETH_DEMO_PASSPHRASE':'phrase123'})


def client(handler):
    return httpx.AsyncClient(base_url='https://api.bitget.com',transport=httpx.MockTransport(handler))


def response(data):return httpx.Response(200,json={'code':'00000','data':data})


@pytest.mark.asyncio
async def test_signature_sorted_query_and_demo_header():
    seen=[]
    def handler(req):
        seen.append(req)
        ts=req.headers['ACCESS-TIMESTAMP'];target=req.url.raw_path.decode();body=req.content.decode()
        sig=base64.b64encode(hmac.new(b'secret123',(ts+req.method+target+body).encode(),hashlib.sha256).digest()).decode()
        assert req.headers['ACCESS-SIGN']==sig
        assert req.headers['ACCESS-KEY']=='key123'
        assert req.headers['ACCESS-PASSPHRASE']=='phrase123'
        assert req.headers['paptrading']=='1'
        return response({'ok':True})
    b=Bitget(credentials(),'demo',client(handler));b.synced=time.time()
    await b.request('GET','/test',{'z':'x','a':'hello world'})
    assert b'a=hello+world&z=x' in seen[0].url.raw_path
    await b.close()


@pytest.mark.asyncio
async def test_live_omits_demo_header():
    env={'SHORTETH_BITGET_KEY':'k','SHORTETH_BITGET_SECRET':'s','SHORTETH_BITGET_PASSPHRASE':'p'}
    def handler(req):assert 'paptrading' not in req.headers;return response({})
    b=Bitget(StoreBridge('classic',env),'live',client(handler));b.synced=time.time()
    await b.request('GET','/private');await b.close()


@pytest.mark.asyncio
async def test_classic_account_detection_40084():
    def handler(req):return httpx.Response(200,json={'code':'40084','msg':'Classic Account Unified Account API not supported'})
    b=Bitget(StoreBridge('auto',credentials().env),'demo',client(handler));b.synced=time.time()
    assert await b.account_type()=='classic';await b.close()


@pytest.mark.asyncio
async def test_uta_account_detection():
    b=Bitget(StoreBridge('auto',credentials().env),'demo',client(lambda req:response({'accountMode':'unified'})));b.synced=time.time()
    assert await b.account_type()=='uta';await b.close()


@pytest.mark.asyncio
async def test_unknown_account_fails_closed():
    b=Bitget(StoreBridge('auto',credentials().env),'demo',client(lambda req:response({'accountMode':'mystery'})));b.synced=time.time()
    with pytest.raises(ValueError,match='無法確認'):await b.account_type()
    await b.close()


def market_data_handler(req):
    p=req.url.path
    if p.endswith('/contracts'):
        return response([{'symbol':'ETHUSDT','sizeMultiplier':'0.01','pricePlace':'2',
          'priceEndStep':'1','minTradeNum':'0.01','minTradeUSDT':'5','maxOrderQty':'9900',
          'maxLever':'150','symbolStatus':'normal'}])
    if p.endswith('/instruments'):
        return response([{'symbol':'ETHUSDT','symbolType':'crypto'}])
    if p.endswith('/query-position-lever'):
        return response([{'startUnit':'0','endUnit':'1000','leverage':'150'},
                         {'startUnit':'1000.01','endUnit':'100000','leverage':'100'}])
    if p.endswith('/ticker'):
        return response([{'symbol':'ETHUSDT','lastPr':'2000','markPrice':'2000',
                          'bidPr':'1999.9','askPr':'2000.1','ts':str(int(time.time()*1000))}])
    raise AssertionError(p)


@pytest.mark.asyncio
async def test_instrument_parsing_tick_qty_and_status():
    b=Bitget(credentials(),'demo',client(market_data_handler))
    c=await b.instrument('ETHUSDT','classic')
    assert c['step']=='0.01' and c['tick']=='0.01' and c['min_qty']=='0.01'
    assert c['min_value']=='5' and c['max_qty']=='9900' and c['max_leverage']=='150'
    await b.close()


@pytest.mark.asyncio
async def test_position_tier_low_and_high():
    b=Bitget(credentials(),'demo',client(market_data_handler))
    assert await b.tier('ETHUSDT',200,'classic')==150
    assert await b.tier('ETHUSDT',2000,'classic')==100
    await b.close()


@pytest.mark.asyncio
async def test_ticker_valid_and_stale_blocked():
    b=Bitget(credentials(),'demo',client(market_data_handler))
    assert (await b.ticker('ETHUSDT'))['ask']=='2000.1'
    b.offset=60_000
    with pytest.raises(ValueError,match='過期'):await b.ticker('ETHUSDT')
    await b.close()


@pytest.mark.asyncio
async def test_invalid_exchange_schema_fails_closed():
    b=Bitget(credentials(),'demo',client(lambda req:response({'unknown':'shape'})))
    with pytest.raises(ValueError):await b.positions('ETHUSDT','classic')
    await b.close()


@pytest.mark.asyncio
async def test_financial_post_timeout_once_uncertain():
    seen=[]
    def handler(req):seen.append(req);raise httpx.ReadTimeout('timeout')
    b=Bitget(credentials(),'demo',client(handler));b.synced=time.time()
    with pytest.raises(ExchangeError) as exc:await b.request('POST','/api/v2/mix/order/place-order',body={'x':1})
    assert exc.value.uncertain and len(seen)==1
    await b.close()


def test_s300_limit_payload_classic_and_uta():
    order={'id':'e1x','symbol':'ETHUSDT','side':'short','kind':'classic','entry':'2000',
           'qty':'0.1','sl':'1900','tp':'2100'}
    p=Bitget.entry_payload(order)
    assert p['orderType']=='limit' and p['price']=='2000' and p['force']=='gtc'
    assert p['presetStopLossPrice']=='1900' and p['presetStopSurplusPrice']=='2100'
    order['kind']='uta';p=Bitget.entry_payload(order)
    assert p['qty']=='0.1' and p['stopLoss']=='1900' and p['takeProfit']=='2100'


@pytest.mark.parametrize('kind',('classic','uta'))
def test_market_payload_uses_s300_route_fields_without_price(kind):
    order={'id':'e1x','symbol':'ETHUSDT','side':'short','kind':kind,
           'order_type':'market','entry':'2000','qty':'0.1','sl':None,'tp':None}
    p=Bitget.entry_payload(order)
    assert p['orderType']=='market' and 'price' not in p and 'force' not in p
    assert p['size' if kind=='classic' else 'qty']=='0.1'
    assert 'presetStopLossPrice' not in p and 'stopLoss' not in p


def test_client_oid_stable_unique():
    assert oid('strategy',1)!=oid('strategy',2)
    assert oid('strategy',1)==oid('strategy',1)
    assert len(oid('strategy',1))<=32


def test_filled_missing_quantity_fails_closed():
    with pytest.raises(ValueError):filled({'status':'filled'})
    assert filled({'baseVolume':'0.2'})==D('0.2')


def test_native_plan_coverage():
    assert covers([{'planType':'pos_loss','triggerPrice':'1800'}],'sl','0.1')
    assert covers([{'planType':'profit_plan','triggerPrice':'2200','size':'0.2'}],'tp','0.1')
    assert not covers([{'planType':'loss_plan','triggerPrice':'1800','size':'0.05'}],'sl','0.1')


@pytest.mark.parametrize('bad',('NaN','Infinity','-Infinity'))
def test_nonfinite_numeric_rejected(bad):
    with pytest.raises(ValueError):D(bad)


def test_quantity_floor_and_min_step():
    assert floor_step('0.059','0.01')==D('0.05')
    assert target_notional('100')==D('292.50000')


def test_ledger_client_oid_and_bar_kind_unique(tmp_path):
    ledger=Ledger(tmp_path/'x.sqlite')
    ledger.create('id1',1,'entry',{'x':1})
    with pytest.raises(Exception):ledger.create('id1',1,'entry',{'x':2})
    with pytest.raises(Exception):ledger.create('id2',1,'entry',{'x':2})
    assert ledger.by_oid('id1')['state']=='submitting'


class FakeBitget:
    def __init__(self,max_leverage=150,tier=150):
        self.mode='demo';self.max_leverage=max_leverage;self.tier_max=tier
        self.prepared=[];self.placed=[];self.reduced=[];self.cancelled=[]
        self.detail_data={'orderStatus':'filled','baseVolume':'0.1','priceAvg':'2000','orderId':'o1'}
        self.position_data=[{'symbol':'ETHUSDT','holdSide':'short','total':'0.1','openPriceAvg':'2000'}]
        self.plans_data=[]
    async def account_type(self):return 'classic'
    async def instrument(self,*args):return {'step':'0.01','tick':'0.01','min_qty':'0.01','min_value':'5','max_qty':'9900','max_leverage':str(self.max_leverage),'status':'normal'}
    async def ticker(self,*args):return {'ask':'2000','bid':'2000','mark':'2000','last':'2000','ts':int(time.time()*1000)}
    async def tier(self,*args):return self.tier_max
    async def account_snapshot(self,*args):return {'balance':{'accountEquity':'100','available':'100','crossedMaxAvailable':'100'},'positions':[],'orders':[],'plans':[]}
    async def prepare(self,o):self.prepared.append(o)
    async def place(self,o):self.placed.append(o);return {'orderId':'o1','clientOid':o['id']}
    async def detail(self,*args,**kwargs):return self.detail_data
    async def positions(self,*args):return self.position_data
    async def plans(self,*args):return self.plans_data
    async def reduce(self,o,qty,cid):self.reduced.append((o,qty,cid));return {'orderId':'c1','clientOid':cid}
    async def cancel(self,o):self.cancelled.append(o);return {'orderId':'o1'}


def intent(bar=1,notional='200',entry='2000'):
    return TradeIntent('ETHUSDT','short','market',notional,oid('test',bar),bar,entry=entry)


@pytest.mark.asyncio
async def test_preview_150_available_and_no_double_multiply(tmp_path):
    b=FakeBitget();e=Execution(b,Ledger(tmp_path/'x.sqlite'))
    p=await e.preview(intent())
    assert p['qty']=='0.1' and p['actual_notional']=='200.0'
    assert p['leverage']==150 and p['estimated_initial_margin']==str(D('200')/150)
    assert not b.placed


@pytest.mark.asyncio
@pytest.mark.parametrize('instrument,tier,maximum',[(100,150,100),(150,100,100)])
async def test_150_unavailable_blocks_before_prepare_or_order(tmp_path,instrument,tier,maximum):
    b=FakeBitget(instrument,tier);e=Execution(b,Ledger(tmp_path/'x.sqlite'))
    with pytest.raises(OrderBlocked) as exc:await e.submit(intent(),allow_post=True)
    assert exc.value.reason=='TARGET_LEVERAGE_NOT_SUPPORTED'
    assert exc.value.details=={'requested_leverage':150,'exchange_max_leverage':maximum}
    assert not b.prepared and not b.placed


@pytest.mark.asyncio
async def test_wrong_leverage_intent_rejected(tmp_path):
    b=FakeBitget();e=Execution(b,Ledger(tmp_path/'x.sqlite'))
    with pytest.raises(OrderBlocked):await e.preview(TradeIntent('ETHUSDT','short','market','200','a',1,100))


@pytest.mark.asyncio
async def test_too_small_notional_rejected(tmp_path):
    e=Execution(FakeBitget(),Ledger(tmp_path/'x.sqlite'))
    with pytest.raises(OrderBlocked) as exc:await e.preview(intent(notional='1'))
    assert exc.value.reason=='BELOW_EXCHANGE_MINIMUM'


@pytest.mark.asyncio
async def test_quote_drift_rejected(tmp_path):
    e=Execution(FakeBitget(),Ledger(tmp_path/'x.sqlite'))
    with pytest.raises(OrderBlocked) as exc:await e.preview(intent(entry='1900'))
    assert exc.value.reason=='MARKET_REFERENCE_PRICE_DRIFT'


@pytest.mark.asyncio
async def test_preview_never_posts(tmp_path):
    b=FakeBitget();e=Execution(b,Ledger(tmp_path/'x.sqlite'))
    assert (await e.submit(intent()))['state']=='preview_only'
    assert not b.placed and not e.ledger.rows()


@pytest.mark.asyncio
async def test_submit_accepted_then_confirmed_position(tmp_path):
    b=FakeBitget();e=Execution(b,Ledger(tmp_path/'x.sqlite'))
    result=await e.submit(intent(),allow_post=True)
    assert result['state']=='open' and len(b.placed)==1 and b.prepared[0]['leverage']==150
    assert result['order']['exchange_order_id']=='o1'


@pytest.mark.asyncio
async def test_submit_same_oid_never_reposts(tmp_path):
    b=FakeBitget();e=Execution(b,Ledger(tmp_path/'x.sqlite'))
    await e.submit(intent(),allow_post=True)
    with pytest.raises(OrderBlocked):await e.submit(intent(),allow_post=True)
    assert len(b.placed)==1


@pytest.mark.asyncio
@pytest.mark.parametrize('status,fill,pos,expected',[
    ('live','0',[], 'pending'),('partially_filled','0.05',[{'symbol':'ETHUSDT','holdSide':'short','total':'0.05'}],'partially_filled'),
    ('filled','0.1',[{'symbol':'ETHUSDT','holdSide':'short','total':'0.1'}],'open'),
    ('canceled','0',[],'canceled')])
async def test_order_reconciliation_states(tmp_path,status,fill,pos,expected):
    b=FakeBitget();b.detail_data={'orderStatus':status,'baseVolume':fill,'orderId':'o1'};b.position_data=pos
    e=Execution(b,Ledger(tmp_path/'x.sqlite'))
    assert (await e.submit(intent(),allow_post=True))['state']==expected


@pytest.mark.asyncio
async def test_position_exceeds_fill_fails_closed(tmp_path):
    b=FakeBitget();b.detail_data['baseVolume']='0.05';e=Execution(b,Ledger(tmp_path/'x.sqlite'))
    with pytest.raises(ValueError,match='POSITION_EXCEEDS_TRACKED_FILL'):
        await e.submit(intent(),allow_post=True)
    assert e.ledger.rows()[0]['state']=='needs_reconcile'


@pytest.mark.asyncio
async def test_native_sl_unverified_is_visible(tmp_path):
    b=FakeBitget();e=Execution(b,Ledger(tmp_path/'x.sqlite'))
    i=TradeIntent('ETHUSDT','short','market','200','sl1',1,entry='2000',stop_loss='2100')
    assert (await e.submit(i,allow_post=True))['state']=='protection_unverified'


@pytest.mark.asyncio
async def test_full_and_partial_close_use_reduce_only_path(tmp_path):
    b=FakeBitget();e=Execution(b,Ledger(tmp_path/'x.sqlite'))
    await e.submit(intent(),allow_post=True)
    partial=await e.close(intent().client_order_id,2,fraction='0.5',allow_post=True)
    assert partial['order']['qty']=='0.05'
    b.position_data=[{'symbol':'ETHUSDT','holdSide':'short','total':'0.05'}]
    await e.reconcile(partial['order']['id'])
    full=await e.close(intent().client_order_id,3,allow_post=True)
    assert full['order']['qty']=='0.05'
    assert len(b.reduced)==2 and not any(x['side']=='long' for x in b.placed)


@pytest.mark.asyncio
async def test_close_no_owned_entry_blocked(tmp_path):
    e=Execution(FakeBitget(),Ledger(tmp_path/'x.sqlite'))
    with pytest.raises(OrderBlocked):await e.close('unknown',2,allow_post=True)


@pytest.mark.asyncio
async def test_explicit_adoption_of_oneway_cross_150_existing_short(tmp_path):
    b=FakeBitget()
    async def snapshot(*args):return {'balance':{'accountEquity':'100'},
        'account':{'posMode':'one_way_mode','marginMode':'crossed','crossedMarginLeverage':'150'},
        'positions':[{'symbol':'ETHUSDT','holdSide':'short','total':'0.1',
                      'openPriceAvg':'2000','leverage':'150','posId':'p1','cTime':'1600000000000'}],
        'orders':[],'plans':[]}
    b.account_snapshot=snapshot
    e=Execution(b,Ledger(tmp_path/'x.sqlite'))
    with pytest.raises(OrderBlocked,match='EXPLICIT_ADOPTION_REQUIRED'):
        await e.adopt_position()
    adopted=await e.adopt_position(allow_adopt=True)
    assert adopted['state']=='open' and adopted['order']['adopted']
    assert (await e.reconcile(adopted['order']['id']))['state']=='open'
    assert not b.placed


@pytest.mark.asyncio
async def test_adoption_refuses_wrong_leverage(tmp_path):
    b=FakeBitget()
    async def snapshot(*args):return {'balance':{'accountEquity':'100'},
        'account':{'posMode':'one_way_mode','marginMode':'crossed','crossedMarginLeverage':'100'},
        'positions':[{'symbol':'ETHUSDT','holdSide':'short','total':'0.1',
                      'openPriceAvg':'2000','leverage':'100'}],
        'orders':[],'plans':[]}
    b.account_snapshot=snapshot
    e=Execution(b,Ledger(tmp_path/'x.sqlite'))
    with pytest.raises(OrderBlocked) as exc:await e.adopt_position(allow_adopt=True)
    assert exc.value.reason=='ADOPTION_REQUIRES_ONEWAY_CROSS_150'
