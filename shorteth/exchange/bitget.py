"""Bitget futures adapter.

Classic V2 is the primary path for Classic accounts; UTA V3 remains supported.
Financial POST requests are never automatically retried.
"""
from __future__ import annotations
import asyncio,base64,hashlib,hmac,time
from urllib.parse import urlencode
import httpx
from .signals import decimal as D,fmt,floor_step,validate_trade
from .store import dumps

PRODUCT='USDT-FUTURES'
MARGIN='USDT'

class ExchangeError(Exception):
    def __init__(self,phase,code,message,uncertain=False):
        self.phase,self.code,self.message,self.uncertain=phase,str(code),str(message),uncertain
        super().__init__(f'{phase}: [{code}] {message}')
    def info(self):
        return {'phase':self.phase,'code':self.code,'message':self.message,'uncertain':self.uncertain}

def _rows(data,*keys,allow_single=False,empty_ok=True):
    """Normalize only documented list containers; never invent a missing position."""
    if data is None:
        if empty_ok:return []
        raise ValueError('交易所回應 data 為空')
    if isinstance(data,list):
        return [x for x in data if isinstance(x,dict)]
    if isinstance(data,dict):
        for key in keys:
            if key not in data:continue
            value=data.get(key)
            if value is None:return []
            if isinstance(value,list):return [x for x in value if isinstance(x,dict)]
            if allow_single and isinstance(value,dict):return [value]
            raise ValueError(f'交易所欄位 {key} 結構不正確')
        if not data and empty_ok:return []
        if allow_single:return [data]
    raise ValueError('交易所清單回應結構未知；已停止，不把資料缺失當作沒有持倉')

def items(data):
    """Legacy strict helper kept for tests/callers."""
    return _rows(data,'list','entrustedList','orderList','positionList',empty_ok=False)

def quantity(p):
    for k in ('total','size','qty','positionAmt','available'):
        if k in p:
            try:return abs(D(p[k] or '0'))
            except Exception:pass
    raise ValueError('交易所未提供可辨識的持倉數量')

def _crossed(value):
    return str(value or '').lower() in {'cross','crossed'}

class Bitget:
    def __init__(self,store,mode='demo',client=None):
        self.s,self.mode=store,mode
        self.kind=None;self.offset=0.;self.synced=0.;self.lock=asyncio.Lock();self.last_request=0.
        self.http=client or httpx.AsyncClient(
            base_url='https://api.bitget.com',timeout=15,
            limits=httpx.Limits(max_connections=8,max_keepalive_connections=4),
            headers={'User-Agent':'S300/2.11.0'})
    async def close(self):await self.http.aclose()

    async def request(self,method,path,params=None,body=None,private=True):
        query=urlencode(sorted((k,str(v)) for k,v in (params or {}).items() if v is not None))
        target=path+('?' + query if query else '')
        text=dumps(body) if body is not None else ''
        headers={'Content-Type':'application/json','locale':'en-US'}
        if self.mode=='demo':headers['paptrading']='1'
        if private:
            if self.mode=='paper':raise ValueError('本機模擬不能呼叫 Bitget 私有 API')
            if time.time()-self.synced>240:await self.sync_time()
            prefix='demo' if self.mode=='demo' else 'bitget'
            key,secret,phrase=(self.s.get(prefix+'_'+k) for k in ('key','secret','passphrase'))
            if not all((key,secret,phrase)):raise ValueError('請先填寫此模式專用的 API Key、Secret 與 Passphrase')
            ts=str(int(time.time()*1000+self.offset))
            signature=base64.b64encode(hmac.new(secret.encode(),(ts+method.upper()+target+text).encode(),hashlib.sha256).digest()).decode()
            headers.update({'ACCESS-KEY':key,'ACCESS-SIGN':signature,'ACCESS-PASSPHRASE':phrase,'ACCESS-TIMESTAMP':ts})
        async with self.lock:
            await asyncio.sleep(max(0,.13-(time.monotonic()-self.last_request)));self.last_request=time.monotonic()
            try:r=await self.http.request(method.upper(),target,headers=headers,content=text.encode() if text else None)
            except httpx.HTTPError as e:raise ExchangeError(path,'NETWORK',type(e).__name__,method.upper()!='GET') from e
        try:payload=r.json()
        except ValueError:raise ExchangeError(path,str(r.status_code),'交易所回應不是 JSON',method.upper()!='GET')
        if not isinstance(payload,dict):raise ExchangeError(path,'SCHEMA','交易所回應不是物件',method.upper()!='GET')
        code=str(payload.get('code','MISSING'))
        if r.status_code>=400 or code not in {'00000','0'}:
            uncertain=method.upper()!='GET' and (r.status_code>=500 or code in {'40010','40725','45001','429'})
            raise ExchangeError(path,code if code!='MISSING' else r.status_code,self.s.redact(payload.get('msg','Bitget API 錯誤')),uncertain)
        return payload.get('data')

    async def sync_time(self):
        before=time.time()*1000
        data=await self.request('GET','/api/v2/public/time',private=False)
        if not isinstance(data,dict) or not data.get('serverTime'):raise ValueError('Bitget 沒有回傳伺服器時間')
        self.offset=float(data['serverTime'])-(before+time.time()*1000)/2;self.synced=time.time()
        return {'offset_ms':round(self.offset)}

    async def account_type(self):
        configured=self.s.get('account_type','auto')
        if configured in {'classic','uta'}:
            self.kind=configured;return self.kind
        if self.kind in {'classic','uta'}:return self.kind
        try:data=await self.request('GET','/api/v3/account/settings')
        except ExchangeError as e:
            msg=(e.message or '').lower()
            if e.code=='40084' or ('classic account' in msg and 'unified account api' in msg):
                self.kind='classic'
                self.s.event('bitget','已辨識 Classic Account，改用 V2 Futures API',data={'code':e.code})
                return self.kind
            raise
        if isinstance(data,dict) and data.get('accountMode') in {'unified','hybrid'}:
            self.kind='uta';return self.kind
        if isinstance(data,dict) and data.get('accountMode') in {'upgrading','switching'}:
            raise ValueError('Bitget 帳戶正在切換模式，完成後再交易')
        raise ValueError('Bitget 帳戶類型無法確認；請在設定指定 Classic 或 UTA')

    async def settings(self,symbol,kind=None):
        kind=kind or self.kind or await self.account_type()
        if kind=='uta':
            data=await self.request('GET','/api/v3/account/settings')
            if not isinstance(data,dict):raise ValueError('UTA 帳戶設定回應結構不正確')
            return data
        data=await self.request('GET','/api/v2/mix/account/account',
            {'symbol':symbol,'productType':PRODUCT,'marginCoin':MARGIN})
        if not isinstance(data,dict):raise ValueError('Classic 單幣帳戶設定回應結構不正確')
        return data

    async def balances(self,kind=None):
        kind=kind or self.kind or await self.account_type()
        if kind=='classic':
            data=await self.request('GET','/api/v2/mix/account/accounts',{'productType':PRODUCT})
            return _rows(data,'list','accounts')
        data=await self.request('GET','/api/v3/account/assets')
        return _rows(data,'list','assets','coinAssets')

    async def balance_usdt(self,kind=None):
        kind=kind or self.kind or await self.account_type()
        rows=await self.balances(kind)
        def coin(r):return str(r.get('marginCoin') or r.get('coin') or r.get('asset') or '').upper()
        row=next((r for r in rows if coin(r)==MARGIN),{})
        if kind=='classic':
            return {
                'marginCoin':MARGIN,
                'available':str(row.get('available') or '0'),
                'crossedMaxAvailable':str(row.get('crossedMaxAvailable') or row.get('available') or '0'),
                'accountEquity':str(row.get('accountEquity') or row.get('usdtEquity') or '0'),
                'unrealizedPL':str(row.get('unrealizedPL') or '0'),
                'crossedRiskRate':str(row.get('crossedRiskRate') or '0'),
                'locked':str(row.get('locked') or '0'),
                'raw':row,
            }
        return {
            'marginCoin':MARGIN,
            'available':str(row.get('available') or row.get('availableBalance') or row.get('availableToWithdraw') or '0'),
            'crossedMaxAvailable':str(row.get('available') or row.get('availableBalance') or '0'),
            'accountEquity':str(row.get('equity') or row.get('accountEquity') or row.get('balance') or '0'),
            'unrealizedPL':str(row.get('unrealizedPnl') or row.get('unrealizedPL') or '0'),
            'crossedRiskRate':str(row.get('marginRatio') or row.get('crossedRiskRate') or '0'),
            'locked':str(row.get('locked') or row.get('frozen') or '0'),
            'raw':row,
        }

    async def instrument(self,symbol,kind=None):
        kind=kind or self.kind or ('classic' if self.mode=='paper' else await self.account_type())
        if kind=='uta':
            data=await self.request('GET','/api/v3/market/instruments',{'category':PRODUCT,'symbol':symbol},private=False)
            raw=next((x for x in _rows(data,'list','instruments') if str(x.get('symbol','')).upper()==symbol),None)
            if not raw:raise ValueError('UTA 找不到此 USDT 合約')
            c={'step':raw.get('sizeMultiplier') or raw.get('quantityMultiplier') or fmt(D(10)**-int(raw['quantityPrecision'])),
               'tick':raw.get('priceMultiplier') or fmt(D(10)**-int(raw['pricePrecision'])),
               'min_qty':raw.get('minOrderQty','0'),'min_value':raw.get('minOrderAmount','0'),
               'max_qty':raw.get('maxOrderQty'),'max_leverage':raw.get('maxLeverage'),'status':raw.get('status')}
        else:
            data=await self.request('GET','/api/v2/mix/market/contracts',{'productType':PRODUCT,'symbol':symbol},private=False)
            raw=next((x for x in _rows(data,'list') if str(x.get('symbol','')).upper()==symbol),None)
            if not raw:raise ValueError('Classic 找不到此 USDT 合約')
            c={'step':raw['sizeMultiplier'],
               'tick':fmt(D(raw.get('priceEndStep','1'))*D(10)**-int(raw['pricePlace'])),
               'min_qty':raw['minTradeNum'],'min_value':raw.get('minTradeUSDT','0'),
               'max_qty':raw.get('maxOrderQty'),'max_leverage':raw.get('maxLever'),'status':raw.get('symbolStatus')}
        try:
            meta=await self.request('GET','/api/v3/market/instruments',{'category':PRODUCT,'symbol':symbol},private=False)
            m=next((x for x in _rows(meta,'list','instruments') if str(x.get('symbol','')).upper()==symbol),{})
            c['symbol_type']=str(m.get('symbolType') or '').lower()
        except Exception:c['symbol_type']=''
        if c['status'] not in {'normal','online'}:raise ValueError('此合約目前不可正常開倉：'+str(c['status']))
        if not c['max_leverage'] or D(c['max_leverage'])<1:raise ValueError('交易所沒有提供最大槓桿')
        return c

    async def tier(self,symbol,value,kind):
        if kind=='uta':
            data=await self.request('GET','/api/v3/market/position-tier',{'category':PRODUCT,'symbol':symbol},private=False)
            a,b,l='minTierValue','maxTierValue','leverage'
        else:
            data=await self.request('GET','/api/v2/mix/market/query-position-lever',
                                    {'productType':PRODUCT,'symbol':symbol},private=False)
            a,b,l='startUnit','endUnit','leverage'
        matches=[]
        for row in _rows(data,'list','tiers'):
            if a not in row or b not in row:continue
            if D(row[a])<=D(value)<=D(row[b]):
                lev=row.get(l) or row.get('maxLever')
                if lev and D(lev)>0:matches.append(int(D(lev)))
        if not matches:raise ValueError('沒有取得涵蓋此名目金額的槓桿階梯')
        return min(matches)

    async def ticker(self,symbol):
        data=await self.request('GET','/api/v2/mix/market/ticker',
                                {'productType':PRODUCT,'symbol':symbol},private=False)
        row=next((x for x in _rows(data,'list') if str(x.get('symbol','')).upper()==symbol),None)
        if not row:raise ValueError('沒有即時報價')
        q={k:fmt(row.get(key) or '0') for k,key in [('last','lastPr'),('mark','markPrice'),('bid','bidPr'),('ask','askPr')]}
        ts=int(row.get('ts') or 0)
        if min(D(q[k]) for k in q)<=0 or D(q['ask'])<D(q['bid']):raise ValueError('報價缺漏或買賣盤異常')
        if not ts or abs(time.time()*1000+self.offset-ts)>30000:raise ValueError('報價過期，暫停下單')
        return q|{'ts':ts}

    async def positions(self,symbol=None,kind=None):
        kind=kind or self.kind or await self.account_type()
        if kind=='classic':
            data=await self.request('GET','/api/v2/mix/position/all-position',
                                    {'productType':PRODUCT,'marginCoin':MARGIN})
            rows=_rows(data,'list','positionList')
        else:
            params={'category':PRODUCT}
            if symbol:params['symbol']=symbol
            data=await self.request('GET','/api/v3/position/current-position',params)
            rows=_rows(data,'list','positionList')
        result=[]
        for row in rows:
            sym=str(row.get('symbol','')).upper()
            if symbol and sym!=symbol:continue
            try:q=quantity(row)
            except ValueError:continue
            if q>0:result.append(row)
        return result

    async def pending(self,symbol=None,kind=None):
        kind=kind or self.kind or await self.account_type()
        if kind=='classic':
            params={'productType':PRODUCT}
            if symbol:params['symbol']=symbol
            data=await self.request('GET','/api/v2/mix/order/orders-pending',params)
            rows=_rows(data,'entrustedList','orderList','list')
        else:
            params={'category':PRODUCT}
            if symbol:params['symbol']=symbol
            data=await self.request('GET','/api/v3/trade/unfilled-orders',params)
            rows=_rows(data,'orderList','list','orders')
        return [x for x in rows if not symbol or str(x.get('symbol','')).upper()==symbol]

    async def plans(self,symbol=None,kind=None):
        kind=kind or self.kind or await self.account_type()
        if kind=='classic':
            params={'productType':PRODUCT,'planType':'profit_loss'}
            if symbol:params['symbol']=symbol
            data=await self.request('GET','/api/v2/mix/order/orders-plan-pending',params)
            rows=_rows(data,'entrustedList','list','orderList')
        else:
            data=await self.request('GET','/api/v3/trade/unfilled-strategy-orders',
                                    {'category':PRODUCT,'type':'tpsl'})
            rows=_rows(data,'list','orderList','strategyOrders')
        return [x for x in rows if not symbol or str(x.get('symbol','')).upper()==symbol]

    async def position_history(self,symbol=None,kind=None,limit=30):
        kind=kind or self.kind or await self.account_type();limit=str(max(1,min(int(limit),100)))
        if kind=='classic':
            params={'productType':PRODUCT,'limit':limit}
            if symbol:params['symbol']=symbol
            data=await self.request('GET','/api/v2/mix/position/history-position',params)
        else:
            params={'category':PRODUCT,'limit':limit}
            if symbol:params['symbol']=symbol
            data=await self.request('GET','/api/v3/position/history-position',params)
        rows=_rows(data,'list','positionList')
        return [x for x in rows if not symbol or str(x.get('symbol','')).upper()==str(symbol).upper()]

    async def recent_orders(self,symbol=None,kind=None,limit=100):
        kind=kind or self.kind or await self.account_type()
        if kind!='classic':
            raise ValueError('目前「接管既有交易所持倉」先支援 Classic Account；UTA 請由 S300 自己送單追蹤')
        params={'productType':PRODUCT,'limit':str(max(1,min(int(limit),100)))}
        if symbol:params['symbol']=symbol
        data=await self.request('GET','/api/v2/mix/order/orders-history',params)
        rows=_rows(data,'entrustedList','orderList','list')
        return [x for x in rows if not symbol or str(x.get('symbol','')).upper()==symbol]

    async def order_by_id(self,symbol,order_id,kind=None):
        kind=kind or self.kind or await self.account_type()
        if not order_id:raise ValueError('缺少 Bitget orderId')
        if kind=='classic':
            data=await self.request('GET','/api/v2/mix/order/detail',
                                    {'productType':PRODUCT,'symbol':symbol,'orderId':str(order_id)})
        else:
            data=await self.request('GET','/api/v3/trade/order-info',
                                    {'category':PRODUCT,'symbol':symbol,'orderId':str(order_id)})
        if not isinstance(data,dict):raise ValueError('交易所訂單明細回應不正確')
        return data

    async def account_snapshot(self,symbol='BTCUSDT',kind=None):
        kind=kind or self.kind or await self.account_type()
        if self.mode=='paper':
            return {'kind':'paper','balance':{'available':self.s.get('paper_balance'),
                    'crossedMaxAvailable':self.s.get('paper_balance'),'accountEquity':self.s.get('paper_balance'),
                    'unrealizedPL':'0','crossedRiskRate':'0'},'account':{},
                    'positions':[],'orders':[],'plans':[]}
        balance,account,positions,orders,plans=await asyncio.gather(
            self.balance_usdt(kind),self.settings(symbol,kind),self.positions(None,kind),
            self.pending(None,kind),self.plans(None,kind))
        return {'kind':kind,'symbol':symbol,'balance':balance,'account':account,
                'positions':positions,'orders':orders,'plans':plans}

    async def preview(self,req):
        e,s,p,n=validate_trade(req['symbol'],req['side'],req['entry'],req['sl'],req['tp'],req['notional'])
        if n>D(self.s.get('max_notional')):raise ValueError('名目金額超過網站設定的單筆上限')
        kind='classic' if self.mode=='paper' else await self.account_type()
        config=await self.instrument(req['symbol'],kind)
        if config.get('symbol_type')=='stock' and n<D(self.s.get('stock_floor_notional','3000')):
            raise ValueError('股票類標的名目金額低於後台設定的可調下限')
        for name,px in [('進場',e),('SL',s),('TP',p)]:
            if floor_step(px,config['tick'])!=px:raise ValueError(f'{name} 必須符合價格步進 {config["tick"]}')
        qty=floor_step(n/e,config['step'])
        if qty<=0 or qty<D(config['min_qty']) or qty*e<D(config['min_value']):raise ValueError('名目金額太小，不符合交易所最小下單量／金額')
        if config.get('max_qty') and D(config['max_qty'])>0 and qty>D(config['max_qty']):raise ValueError('下單數量超過單筆限制')
        leverage=min(int(D(config['max_leverage'])),await self.tier(req['symbol'],qty*e,kind))
        quote=await self.ticker(req['symbol'])
        snapshot=await self.account_snapshot(req['symbol'],kind) if self.mode!='paper' else await self.account_snapshot(req['symbol'],kind)
        same_positions=[x for x in snapshot['positions'] if str(x.get('symbol','')).upper()==req['symbol']]
        same_orders=[x for x in snapshot['orders'] if str(x.get('symbol','')).upper()==req['symbol']]
        same_plans=[x for x in snapshot['plans'] if str(x.get('symbol','')).upper()==req['symbol']]
        if self.mode!='paper':
            if same_positions:raise ValueError('交易所此幣種目前仍有實際持倉；控制權尚未釋放')
            if same_orders:raise ValueError('交易所此幣種仍有一般進場／掛單委託；等待成交或取消確認後才能建立新 lifecycle')
            if same_plans:raise ValueError('交易所此幣種沒有持倉但仍有未識別 TP/SL plan；S300 已清理可確認屬於舊 lifecycle 的 plan，剩餘項目不會自動刪除')
        initial=qty*e/D(leverage)
        available=D(snapshot['balance'].get('crossedMaxAvailable') or snapshot['balance'].get('available') or '0')
        if self.mode!='paper' and available<initial:
            raise ValueError(f'Bitget 全倉最大可用餘額 {fmt(available)} USDT，小於預估初始保證金 {fmt(initial)} USDT')
        return dict(req,entry=fmt(e),sl=fmt(s),tp=fmt(p),notional=fmt(n),mode=self.mode,kind=kind,
                    qty=fmt(qty),actual_notional=fmt(qty*e),leverage=leverage,
                    estimated_initial_margin=fmt(initial),estimated_stop_loss=fmt(abs(e-s)*qty),
                    margin_mode='crossed',position_mode='one_way_mode',config=config,quote=quote,
                    balance={k:v for k,v in snapshot['balance'].items() if k!='raw'})

    async def prepare(self,o):
        if D(o.get('leverage') or '0')!=D('150'):
            raise ValueError('TARGET_LEVERAGE_NOT_150')
        symbol,kind=o['symbol'],o['kind']
        if kind=='uta':
            current=await self.settings(symbol,kind)
            if current.get('holdMode')!='one_way_mode':
                if await self.positions(None,kind) or await self.pending(None,kind):
                    raise ValueError('UTA 目前不是單向持倉，且仍有持倉／掛單；不能安全切換 one-way mode')
                await self.request('POST','/api/v3/account/set-hold-mode',body={'holdMode':'one_way_mode'})
            await self.request('POST','/api/v3/account/set-leverage',
                               body={'category':PRODUCT,'symbol':symbol,'leverage':str(o['leverage']),'marginMode':'crossed'})
            check=await self.settings(symbol,kind)
            cfg=next((x for x in check.get('symbolConfigList',[]) if x.get('symbol')==symbol and x.get('category')==PRODUCT),{})
            if check.get('holdMode')!='one_way_mode' or not _crossed(cfg.get('marginMode')) or D(cfg.get('leverage','0'))!=D(o['leverage']):
                raise ValueError('UTA 單向／全倉／槓桿回讀不一致，委託未送出')
            if D(o['leverage'])!=D('150'):raise ValueError('TARGET_LEVERAGE_NOT_150')
            o['prepared_verified']=True
            return
        current=await self.settings(symbol,kind)
        if current.get('posMode')!='one_way_mode':
            all_positions,all_orders,all_plans=await asyncio.gather(
                self.positions(None,kind),self.pending(None,kind),self.plans(None,kind))
            if all_positions or all_orders or all_plans:
                raise ValueError('Classic 目前不是單向持倉，且產品線仍有持倉／掛單／TP-SL；Bitget 不允許安全切換，請先處理後重試')
            await self.request('POST','/api/v2/mix/account/set-position-mode',
                               body={'productType':PRODUCT,'posMode':'one_way_mode'})
            await asyncio.sleep(.15)
            current=await self.settings(symbol,kind)
        base={'symbol':symbol,'productType':PRODUCT,'marginCoin':MARGIN}
        if not _crossed(current.get('marginMode')):
            if await self.positions(symbol,kind) or await self.pending(symbol,kind):
                raise ValueError('此幣種仍有持倉／掛單，Bitget 不允許切換全倉')
            await self.request('POST','/api/v2/mix/account/set-margin-mode',body=base|{'marginMode':'crossed'})
            await asyncio.sleep(.15)
        # User requirement: always set the computed maximum permitted leverage before each new order.
        await self.request('POST','/api/v2/mix/account/set-leverage',
                           body=base|{'leverage':str(o['leverage'])})
        check=await self.settings(symbol,kind)
        if check.get('posMode')!='one_way_mode' or not _crossed(check.get('marginMode')) or D(check.get('crossedMarginLeverage','0'))!=D(o['leverage']):
            raise ValueError(f'Classic 設定回讀不一致：posMode={check.get("posMode")} marginMode={check.get("marginMode")} leverage={check.get("crossedMarginLeverage")}')
        if D(o['leverage'])!=D('150'):raise ValueError('TARGET_LEVERAGE_NOT_150')
        o['prepared_verified']=True

    @staticmethod
    def entry_payload(o):
        if o.get('order_type')=='market':
            common={'symbol':o['symbol'],'side':'buy' if o['side']=='long' else 'sell',
                    'orderType':'market','clientOid':o['id']}
            if o['kind']=='uta':
                body=common|{'category':PRODUCT,'qty':o['qty']}
                if o.get('tp'):body.update(takeProfit=o['tp'],tpTriggerBy='mark',tpOrderType='market')
                if o.get('sl'):body.update(stopLoss=o['sl'],slTriggerBy='mark',slOrderType='market')
                return body
            body=common|{'productType':PRODUCT,'marginMode':'crossed','marginCoin':MARGIN,
                         'size':o['qty']}
            if o.get('tp'):body['presetStopSurplusPrice']=o['tp']
            if o.get('sl'):body['presetStopLossPrice']=o['sl']
            return body
        common={'symbol':o['symbol'],'side':'buy' if o['side']=='long' else 'sell',
                'orderType':'limit','price':o['entry'],'clientOid':o['id']}
        if o['kind']=='uta':
            return common|{'category':PRODUCT,'qty':o['qty'],'timeInForce':'gtc',
                           'takeProfit':o['tp'],'stopLoss':o['sl'],
                           'tpTriggerBy':'mark','slTriggerBy':'mark',
                           'tpOrderType':'market','slOrderType':'market'}
        return common|{'productType':PRODUCT,'marginMode':'crossed','marginCoin':MARGIN,
                       'size':o['qty'],'force':'gtc',
                       'presetStopSurplusPrice':o['tp'],'presetStopLossPrice':o['sl']}

    async def place(self,o):
        if D(o.get('leverage') or '0')!=D('150') or o.get('prepared_verified') is not True:
            raise ValueError('150 倍與單向／全倉回讀尚未確認，禁止開倉')
        path='/api/v3/trade/place-order' if o['kind']=='uta' else '/api/v2/mix/order/place-order'
        return await self.request('POST',path,body=self.entry_payload(o))

    async def modify_entry(self,o,new_price,new_size,new_client_oid):
        if o['kind']!='classic':
            raise ValueError('目前 S300 追到現價功能先支援 Classic Account')
        if not new_client_oid:raise ValueError('缺少新的 clientOid')
        exchange_id=str(o.get('exchange_order_id') or '')
        client_id=str(o.get('exchange_client_oid') or o['id'])
        body={'symbol':o['symbol'],'productType':PRODUCT,'newClientOid':str(new_client_oid),
              'newSize':fmt(new_size),'newPrice':fmt(new_price)}
        body['orderId' if exchange_id else 'clientOid']=exchange_id or client_id
        return await self.request('POST','/api/v2/mix/order/modify-order',body=body)

    async def detail(self,o,client_oid=None,order_id=None):
        exchange_id=str(order_id or ('' if client_oid is not None else o.get('exchange_order_id')) or '')
        cid=str(client_oid or o.get('exchange_client_oid') or o['id'])
        if o['kind']=='uta':
            params={'category':PRODUCT,'symbol':o['symbol']}
            if exchange_id:params['orderId']=exchange_id
            else:params['clientOid']=cid
            data=await self.request('GET','/api/v3/trade/order-info',params)
        else:
            params={'productType':PRODUCT,'symbol':o['symbol']}
            if exchange_id:params['orderId']=exchange_id
            else:params['clientOid']=cid
            data=await self.request('GET','/api/v2/mix/order/detail',params)
        if not isinstance(data,dict):raise ValueError('交易所訂單明細回應不正確')
        return data

    async def cancel(self,o):
        exchange_id=str(o.get('exchange_order_id') or '')
        client_id=str(o.get('exchange_client_oid') or o['id'])
        if o['kind']=='uta':
            body={'category':PRODUCT,'symbol':o['symbol']}
            body['orderId' if exchange_id else 'clientOid']=exchange_id or client_id
            return await self.request('POST','/api/v3/trade/cancel-order',body=body)
        body={'productType':PRODUCT,'symbol':o['symbol'],'marginCoin':MARGIN}
        body['orderId' if exchange_id else 'clientOid']=exchange_id or client_id
        return await self.request('POST','/api/v2/mix/order/cancel-order',body=body)

    async def reduce(self,o,qty,cid):
        if o['kind']=='uta':
            body={'symbol':o['symbol'],'side':'sell' if o['side']=='long' else 'buy',
                  'orderType':'market','clientOid':cid,'category':PRODUCT,
                  'qty':fmt(qty),'reduceOnly':'yes'}
            return await self.request('POST','/api/v3/trade/place-order',body=body)
        pos_mode=str(o.get('position_mode') or o.get('exchange_pos_mode') or 'one_way_mode')
        if pos_mode=='hedge_mode':
            # Bitget Classic hedge mode uses position direction in side + tradeSide=close.
            body={'symbol':o['symbol'],'side':'buy' if o['side']=='long' else 'sell',
                  'tradeSide':'close','orderType':'market','clientOid':cid,
                  'productType':PRODUCT,'marginMode':o.get('margin_mode') or 'crossed',
                  'marginCoin':MARGIN,'size':fmt(qty)}
        else:
            body={'symbol':o['symbol'],'side':'sell' if o['side']=='long' else 'buy',
                  'orderType':'market','clientOid':cid,'productType':PRODUCT,
                  'marginMode':o.get('margin_mode') or 'crossed',
                  'marginCoin':MARGIN,'size':fmt(qty),'reduceOnly':'YES'}
        return await self.request('POST','/api/v2/mix/order/place-order',body=body)

    async def add_plan(self,o,kind,price,qty,cid,full=False):
        if kind not in {'tp','sl'}:raise ValueError('保護單類型不正確')
        if o['kind']=='uta':
            body={'category':PRODUCT,'symbol':o['symbol'],'clientOid':cid,'type':'tpsl',
                  'tpslMode':'full' if full else 'partial',
                  'side':'sell' if o['side']=='long' else 'buy','reduceOnly':'yes',
                  'takeProfit' if kind=='tp' else 'stopLoss':fmt(price),
                  'tpTriggerBy' if kind=='tp' else 'slTriggerBy':'mark',
                  'tpOrderType' if kind=='tp' else 'slOrderType':'market'}
            if not full:body['qty']=fmt(qty)
            return await self.request('POST','/api/v3/trade/place-strategy-order',body=body)
        body={'productType':PRODUCT,'symbol':o['symbol'],'marginCoin':MARGIN,'clientOid':cid,
              'planType':('pos_profit' if kind=='tp' else 'pos_loss') if full else ('profit_plan' if kind=='tp' else 'loss_plan'),
              'triggerPrice':fmt(price),'triggerType':'mark_price','executePrice':'0',
              'holdSide':'buy' if o['side']=='long' else 'sell'}
        if not full:body['size']=fmt(qty)
        return await self.request('POST','/api/v2/mix/order/place-tpsl-order',body=body)

    async def cancel_plan(self,o,p):
        ident={'orderId':str(p['orderId'])} if p.get('orderId') else {'clientOid':p.get('clientOid')}
        if o['kind']=='uta':return await self.request('POST','/api/v3/trade/cancel-strategy-order',body=ident)
        return await self.request('POST','/api/v2/mix/order/cancel-plan-order',
            body={'productType':PRODUCT,'symbol':o['symbol'],'marginCoin':MARGIN,'orderIdList':[ident]})

    async def diagnostics(self,symbol='BTCUSDT'):
        steps=[]
        try:
            steps.append({'step':'伺服器時間','ok':True,'data':await self.sync_time()})
            kind='classic' if self.mode=='paper' else await self.account_type()
            steps.append({'step':'帳戶路由','ok':True,'data':{'type':kind}})
            contract=await self.instrument(symbol,kind);steps.append({'step':'合約規格','ok':True,'data':contract})
            quote=await self.ticker(symbol);steps.append({'step':'即時報價','ok':True,'data':quote})
            snapshot=await self.account_snapshot(symbol,kind)
            balance={k:v for k,v in snapshot['balance'].items() if k!='raw'}
            steps.append({'step':'合約帳戶餘額','ok':True,'data':balance})
            account=snapshot['account']
            steps.append({'step':'帳戶模式','ok':True,'data':{
                'posMode':account.get('posMode') or account.get('holdMode'),
                'marginMode':account.get('marginMode'),
                'leverage':account.get('crossedMarginLeverage') or account.get('leverage'),
                'accountMode':account.get('accountMode')}})
            steps.append({'step':'持倉回讀','ok':True,'data':{'count':len(snapshot['positions']),'positions':snapshot['positions']}})
            steps.append({'step':'一般委託回讀','ok':True,'data':{'count':len(snapshot['orders']),'orders':snapshot['orders']}})
            steps.append({'step':'TP/SL 委託回讀','ok':True,'data':{'count':len(snapshot['plans']),'plans':snapshot['plans']}})
            return {'ok':True,'mode':self.mode,'kind':kind,'steps':steps,'snapshot':{
                'kind':kind,'balance':balance,'account':{
                    'posMode':account.get('posMode') or account.get('holdMode'),
                    'marginMode':account.get('marginMode'),
                    'leverage':account.get('crossedMarginLeverage') or account.get('leverage')},
                'positions':snapshot['positions'],'orders':snapshot['orders'],'plans':snapshot['plans']},
                'note':'診斷只讀取，不會改槓桿或送單。'}
        except Exception as e:
            detail=e.info() if isinstance(e,ExchangeError) else {'message':self.s.redact(str(e))}
            steps.append({'step':'失敗原因','ok':False,'data':detail})
            return {'ok':False,'mode':self.mode,'steps':steps}
