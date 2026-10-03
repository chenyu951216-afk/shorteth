import json, time
import httpx, pytest
from shorteth.exchange.bitget import Bitget, ExchangeError
from shorteth.exchange.store import StoreBridge

def bridge(account='classic'):
    return StoreBridge(account,{'SHORTETH_DEMO_KEY':'k','SHORTETH_DEMO_SECRET':'s','SHORTETH_DEMO_PASSPHRASE':'p'})
def ok(data):return httpx.Response(200,json={'code':'00000','data':data})
def create(handler):
    b=Bitget(bridge(),'demo',httpx.AsyncClient(base_url='https://api.bitget.com',transport=httpx.MockTransport(handler)))
    b.synced=time.time();return b

@pytest.mark.asyncio
async def test_classic_prepare_oneway_cross_150_and_readback():
    state={'posMode':'hedge_mode','marginMode':'isolated','crossedMarginLeverage':'20'}
    seen=[]
    def handler(req):
        path=req.url.path;seen.append(path)
        if path.endswith('/account'):return ok(state.copy())
        if path.endswith('/all-position'):return ok([])
        if path.endswith('/orders-pending'):return ok({'entrustedList':[]})
        if path.endswith('/orders-plan-pending'):return ok({'entrustedList':[]})
        if path.endswith('/set-position-mode'):
            state['posMode']='one_way_mode';return ok({})
        if path.endswith('/set-margin-mode'):
            state['marginMode']='crossed';return ok({})
        if path.endswith('/set-leverage'):
            assert json.loads(req.content)['leverage']=='150'
            state['crossedMarginLeverage']='150';return ok({})
        raise AssertionError(path)
    b=create(handler)
    await b.prepare({'symbol':'ETHUSDT','kind':'classic','leverage':150})
    assert seen.index('/api/v2/mix/account/set-position-mode')<seen.index('/api/v2/mix/account/set-leverage')
    assert seen.index('/api/v2/mix/account/set-margin-mode')<seen.index('/api/v2/mix/account/set-leverage')
    await b.close()

@pytest.mark.asyncio
async def test_classic_readback_not_150_blocks():
    calls=[]
    def handler(req):
        calls.append(req.url.path)
        if req.url.path.endswith('/account'):
            return ok({'posMode':'one_way_mode','marginMode':'crossed','crossedMarginLeverage':'100'})
        if req.url.path.endswith('/set-leverage'):return ok({})
        raise AssertionError(req.url.path)
    b=create(handler)
    with pytest.raises(ValueError,match='回讀不一致'):
        await b.prepare({'symbol':'ETHUSDT','kind':'classic','leverage':150})
    assert '/api/v2/mix/order/place-order' not in calls
    await b.close()


@pytest.mark.asyncio
async def test_direct_place_cannot_bypass_verified_150():
    seen=[]
    def handler(req):seen.append(req);return ok({'orderId':'o1'})
    b=create(handler)
    order={'id':'cid','symbol':'ETHUSDT','side':'short','kind':'classic','order_type':'market',
           'qty':'0.1','leverage':150}
    with pytest.raises(ValueError,match='回讀尚未確認'):
        await b.place(order)
    assert not seen
    await b.close()

@pytest.mark.asyncio
async def test_uta_prepare_150_readback():
    state={'holdMode':'hedge_mode','symbolConfigList':[{'symbol':'ETHUSDT','category':'USDT-FUTURES',
            'marginMode':'isolated','leverage':'100'}]}
    calls=[]
    def handler(req):
        path=req.url.path;calls.append(path)
        if path.endswith('/settings'):return ok(state.copy())
        if path.endswith('/current-position'):return ok([])
        if path.endswith('/unfilled-orders'):return ok([])
        if path.endswith('/set-hold-mode'):
            state['holdMode']='one_way_mode';return ok({})
        if path.endswith('/set-leverage'):
            assert json.loads(req.content)['leverage']=='150'
            state['symbolConfigList'][0].update(marginMode='crossed',leverage='150');return ok({})
        raise AssertionError(path)
    b=create(handler)
    await b.prepare({'symbol':'ETHUSDT','kind':'uta','leverage':150})
    assert calls.index('/api/v3/account/set-hold-mode')<calls.index('/api/v3/account/set-leverage')
    await b.close()

@pytest.mark.asyncio
async def test_uta_wrong_readback_blocks():
    state={'holdMode':'one_way_mode','symbolConfigList':[{'symbol':'ETHUSDT',
            'category':'USDT-FUTURES','marginMode':'crossed','leverage':'100'}]}
    def handler(req):
        if req.url.path.endswith('/settings'):return ok(state)
        if req.url.path.endswith('/set-leverage'):return ok({})
        raise AssertionError(req.url.path)
    b=create(handler)
    with pytest.raises(ValueError,match='回讀不一致'):
        await b.prepare({'symbol':'ETHUSDT','kind':'uta','leverage':150})
    await b.close()

@pytest.mark.asyncio
@pytest.mark.parametrize('kind,expected_path,field',[
    ('classic','/api/v2/mix/order/place-order','size'),
    ('uta','/api/v3/trade/place-order','qty')])
async def test_reduce_only_market_close_payload(kind,expected_path,field):
    calls=[]
    def handler(req):calls.append((req.url.path,json.loads(req.content)));return ok({'orderId':'close1'})
    b=create(handler)
    await b.reduce({'symbol':'ETHUSDT','side':'short','kind':kind,'position_mode':'one_way_mode'},'.05','cid')
    path,body=calls[0]
    assert path==expected_path and body[field]=='0.05' and body['side']=='buy'
    assert body['reduceOnly'].lower()=='yes' and body['orderType']=='market'
    await b.close()

@pytest.mark.asyncio
async def test_classic_hedge_close_uses_trade_side_close():
    body=[]
    def handler(req):body.append(json.loads(req.content));return ok({'orderId':'x'})
    b=create(handler)
    await b.reduce({'symbol':'ETHUSDT','side':'short','kind':'classic','position_mode':'hedge_mode'},'.05','cid')
    assert body[0]['side']=='sell' and body[0]['tradeSide']=='close'
    await b.close()

@pytest.mark.asyncio
async def test_detail_by_client_oid_and_order_id():
    calls=[]
    def handler(req):calls.append(dict(req.url.params));return ok({'orderId':'o1','baseVolume':'0.1'})
    b=create(handler);o={'symbol':'ETHUSDT','kind':'classic','id':'cid'}
    await b.detail(o);o['exchange_order_id']='o1';await b.detail(o)
    assert calls[0]['clientOid']=='cid' and calls[1]['orderId']=='o1'
    await b.close()

@pytest.mark.asyncio
async def test_classic_snapshot_reads_positions_pending_plans_balance():
    def handler(req):
        p=req.url.path
        if p.endswith('/accounts'):return ok([{'marginCoin':'USDT','accountEquity':'100','available':'95'}])
        if p.endswith('/account'):return ok({'posMode':'one_way_mode','marginMode':'crossed','crossedMarginLeverage':'150'})
        if p.endswith('/all-position'):return ok([{'symbol':'ETHUSDT','total':'0.1','holdSide':'short'}])
        if p.endswith('/orders-pending'):return ok({'entrustedList':[{'symbol':'ETHUSDT','orderId':'o1'}]})
        if p.endswith('/orders-plan-pending'):return ok({'entrustedList':[{'symbol':'ETHUSDT','orderId':'s1'}]})
        raise AssertionError(p)
    b=create(handler);snapshot=await b.account_snapshot('ETHUSDT','classic')
    assert snapshot['balance']['accountEquity']=='100' and len(snapshot['positions'])==1
    assert len(snapshot['orders'])==1 and len(snapshot['plans'])==1
    await b.close()

@pytest.mark.asyncio
async def test_plan_place_and_cancel_s300_endpoints():
    calls=[]
    def handler(req):calls.append((req.url.path,json.loads(req.content)));return ok({'orderId':'p1'})
    b=create(handler);order={'symbol':'ETHUSDT','side':'short','kind':'classic'}
    await b.add_plan(order,'sl','1800','0.1','plan1',full=True)
    await b.cancel_plan(order,{'orderId':'p1'})
    assert calls[0][0]=='/api/v2/mix/order/place-tpsl-order'
    assert calls[0][1]['planType']=='pos_loss'
    assert calls[1][0]=='/api/v2/mix/order/cancel-plan-order'
    await b.close()
