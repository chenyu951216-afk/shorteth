"""ETH strategy intents executed through the copied S300 Bitget adapter."""
from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from dataclasses import asdict, dataclass
from pathlib import Path

from .exchange.bitget import Bitget, ExchangeError, quantity
from .exchange.signals import decimal as D, floor_step, fmt

TARGET_LEVERAGE = 150
TERMINAL = {'filled', 'canceled', 'cancelled', 'rejected', 'expired'}


def oid(*parts: object) -> str:
    return 'se' + hashlib.sha256(':'.join(map(str, parts)).encode()).hexdigest()[:28]


def filled(detail: dict):
    for key in ('cumExecQty', 'baseVolume', 'filledQty'):
        if key in detail:
            return D(detail[key] or '0')
    raise ValueError('ORDER_DETAIL_MISSING_FILLED_QUANTITY')


def plan_price(plan: dict, kind: str):
    value = plan.get('takeProfit' if kind == 'tp' else 'stopLoss')
    if value and D(value) > 0:
        return D(value)
    types = {'tp': {'profit_plan', 'pos_profit'}, 'sl': {'loss_plan', 'pos_loss'}}
    return D(plan.get('triggerPrice') or '0') if plan.get('planType') in types[kind] else D(0)


def covers(plans: list[dict], kind: str, remaining: str):
    total = D(0)
    for plan in plans:
        if plan_price(plan, kind) <= 0:
            continue
        if plan.get('tpslMode') == 'full' or plan.get('planType') in {'pos_profit', 'pos_loss'}:
            return True
        total += D(plan.get('qty') or plan.get('size') or '0')
    return total >= D(remaining) > 0


class OrderBlocked(ValueError):
    def __init__(self, reason: str, **details):
        self.reason, self.details = reason, details
        super().__init__(json.dumps({'reason': reason, **details}, ensure_ascii=False))


@dataclass(frozen=True)
class TradeIntent:
    symbol: str
    side: str
    order_type: str
    notional_usdt: str
    client_order_id: str
    decision_bar_ms: int
    leverage: int = TARGET_LEVERAGE
    entry: str | None = None
    stop_loss: str | None = None
    take_profit: str | None = None
    strategy_id: str | None = None
    entry_hour_ms: int | None = None


class Ledger:
    """Durable clientOid lock. Existing submitting rows are never POSTed again."""
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.execute('CREATE TABLE IF NOT EXISTS operations (oid TEXT PRIMARY KEY, bar_ms INTEGER NOT NULL, kind TEXT NOT NULL, state TEXT NOT NULL, payload TEXT NOT NULL, updated REAL NOT NULL)')
            db.execute('DROP INDEX IF EXISTS one_op_per_bar_kind')
            db.execute("CREATE UNIQUE INDEX IF NOT EXISTS one_entry_per_bar ON operations(bar_ms) WHERE kind='entry'")

    def connect(self):
        db = sqlite3.connect(self.path, timeout=15)
        db.row_factory = sqlite3.Row
        return db

    def create(self, oid_value: str, bar_ms: int, kind: str, payload: dict):
        with self.connect() as db:
            db.execute('INSERT INTO operations VALUES (?,?,?,?,?,?)',
                       (oid_value, bar_ms, kind, 'submitting', json.dumps(payload), time.time()))

    def update(self, oid_value: str, state: str, payload: dict):
        with self.connect() as db:
            if db.execute('UPDATE operations SET state=?,payload=?,updated=? WHERE oid=?',
                          (state, json.dumps(payload, default=str), time.time(), oid_value)).rowcount != 1:
                raise ValueError('UNKNOWN_LEDGER_OPERATION')

    def rows(self):
        with self.connect() as db:
            return [dict(row) | {'payload': json.loads(row['payload'])}
                    for row in db.execute('SELECT * FROM operations ORDER BY updated')]

    def by_oid(self, value: str):
        return next((x for x in self.rows() if x['oid'] == value), None)


class Execution:
    def __init__(self, bitget: Bitget, ledger: Ledger):
        self.b, self.ledger = bitget, ledger
        self.post_guard = lambda: None

    async def preview(self, intent: TradeIntent) -> dict:
        if intent.symbol != 'ETHUSDT' or intent.side != 'short':
            raise OrderBlocked('ETH_SHORT_ONLY')
        if intent.leverage != TARGET_LEVERAGE:
            raise OrderBlocked('TARGET_LEVERAGE_NOT_150', requested_leverage=intent.leverage)
        if intent.order_type not in {'market', 'limit'}:
            raise OrderBlocked('UNSUPPORTED_ORDER_TYPE')
        if not intent.client_order_id:
            raise OrderBlocked('CLIENT_OID_REQUIRED')
        kind = await self.b.account_type()
        config = await self.b.instrument(intent.symbol, kind)
        quote = await self.b.ticker(intent.symbol)
        price = D(intent.entry if intent.entry is not None else quote['bid'])
        if price <= 0:
            raise OrderBlocked('INVALID_ENTRY_PRICE')
        if intent.order_type == 'market' and abs(D(quote['bid']) / price - 1) > D('0.005'):
            raise OrderBlocked('MARKET_REFERENCE_PRICE_DRIFT', reference=fmt(price), bid=quote['bid'])
        if intent.order_type == 'limit' and floor_step(price, config['tick']) != price:
            raise OrderBlocked('INVALID_TICK_SIZE', tick=config['tick'])
        notional = D(intent.notional_usdt)
        if notional > D('200000'):
            raise OrderBlocked('STRATEGY_NOTIONAL_CAP_EXCEEDED')
        if notional <= 0:
            raise OrderBlocked('INVALID_NOTIONAL')
        qty = floor_step(notional / price, config['step'])
        if qty <= 0 or qty < D(config['min_qty']) or qty * price < D(config['min_value']):
            raise OrderBlocked('BELOW_EXCHANGE_MINIMUM', target_notional=fmt(notional),
                               min_qty=config['min_qty'], min_value=config['min_value'])
        if config.get('max_qty') and D(config['max_qty']) > 0 and qty > D(config['max_qty']):
            raise OrderBlocked('ABOVE_EXCHANGE_MAXIMUM', max_qty=config['max_qty'])
        tier_max = await self.b.tier(intent.symbol, qty * price, kind)
        exchange_max = min(int(D(config['max_leverage'])), tier_max)
        if exchange_max < TARGET_LEVERAGE:
            raise OrderBlocked('TARGET_LEVERAGE_NOT_SUPPORTED', requested_leverage=150,
                               exchange_max_leverage=exchange_max)
        snapshot = await self.b.account_snapshot(intent.symbol, kind)
        if any(str(x.get('symbol', '')).upper() == intent.symbol for x in snapshot['positions']):
            raise OrderBlocked('EXISTING_EXCHANGE_POSITION')
        if any(str(x.get('symbol', '')).upper() == intent.symbol for x in snapshot['orders']):
            raise OrderBlocked('EXISTING_EXCHANGE_ORDER')
        if any(str(x.get('symbol', '')).upper() == intent.symbol for x in snapshot['plans']):
            raise OrderBlocked('UNOWNED_EXCHANGE_PLAN')
        if snapshot['positions'] or snapshot['orders'] or snapshot['plans']:
            raise OrderBlocked('DEDICATED_ACCOUNT_REQUIRED')
        available = D(snapshot['balance'].get('crossedMaxAvailable') or snapshot['balance'].get('available') or '0')
        margin = qty * price / D(TARGET_LEVERAGE)
        if margin + qty*price*D('0.001') > available:
            raise OrderBlocked('INSUFFICIENT_AVAILABLE_MARGIN', required=fmt(margin), available=fmt(available))
        for name, value in [('sl', intent.stop_loss), ('tp', intent.take_profit)]:
            if value is not None and (D(value) <= 0 or floor_step(value, config['tick']) != D(value)):
                raise OrderBlocked('INVALID_PROTECTION_PRICE', field=name, tick=config['tick'])
        if intent.stop_loss and D(intent.stop_loss) <= price:
            raise OrderBlocked('SHORT_STOP_MUST_BE_ABOVE_ENTRY')
        if intent.take_profit and D(intent.take_profit) >= price:
            raise OrderBlocked('SHORT_TP_MUST_BE_BELOW_ENTRY')
        return {'id': intent.client_order_id, 'symbol': intent.symbol, 'side': intent.side,
                'order_type': intent.order_type, 'kind': kind, 'mode':self.b.mode, 'entry': fmt(price),
                'notional': fmt(notional), 'qty': fmt(qty), 'actual_notional': fmt(qty * price),
                'leverage': TARGET_LEVERAGE, 'estimated_initial_margin': fmt(margin),
                'sl': intent.stop_loss, 'tp': intent.take_profit, 'config': config,
                'quote': quote, 'balance': {k:v for k,v in snapshot['balance'].items() if k != 'raw'},
                'decision_bar_ms': intent.decision_bar_ms, 'strategy_id': intent.strategy_id,
                'entry_hour_ms': intent.entry_hour_ms}

    async def submit(self, intent: TradeIntent, *, allow_post: bool = False) -> dict:
        order = await self.preview(intent)
        if not allow_post:
            return {'state': 'preview_only', 'order': order}
        if self.ledger.by_oid(intent.client_order_id):
            raise OrderBlocked('CLIENT_OID_ALREADY_USED')
        self.post_guard()
        self.ledger.create(intent.client_order_id, intent.decision_bar_ms, 'entry', order)
        try:
            await self.b.prepare(order)  # S300 verifies one-way/cross/actual 150x by read-back.
            self.post_guard()
            response = await self.b.place(order)
            if not isinstance(response, dict) or not (response.get('orderId') or response.get('clientOid')):
                raise ValueError('PLACE_RESPONSE_MISSING_ORDER_ID_AND_CLIENT_OID')
            order['exchange_order_id'] = str(response.get('orderId') or '')
            order['exchange_client_oid'] = str(response.get('clientOid') or order['id'])
            order['place_response'] = response
            self.ledger.update(order['id'], 'accepted', order)
        except OrderBlocked as exc:
            order['last_error']=str(exc)
            self.ledger.update(order['id'],'blocked',order)
            raise
        except ExchangeError as exc:
            order['last_error'] = exc.info()
            self.ledger.update(order['id'], 'uncertain' if exc.uncertain else 'blocked', order)
            raise
        except Exception as exc:
            order['last_error'] = str(exc)
            self.ledger.update(order['id'], 'uncertain', order)
            raise
        return await self.reconcile(order['id'])

    async def adopt_position(self, *, allow_adopt: bool = False):
        """Explicitly take over one existing ETH short only after exchange read-back.

        Adapted from S300 engine's controlled adoption, without Telegram ownership.
        """
        if not allow_adopt:raise OrderBlocked('EXPLICIT_ADOPTION_REQUIRED')
        if any(x['kind']=='entry' and x['state'] not in {'closed','canceled','cancelled','blocked','rejected'}
               for x in self.ledger.rows()):
            raise OrderBlocked('LOCAL_ENTRY_ALREADY_ACTIVE')
        kind=await self.b.account_type()
        snapshot=await self.b.account_snapshot('ETHUSDT',kind)
        positions=[p for p in snapshot['positions'] if str(p.get('symbol','')).upper()=='ETHUSDT']
        if len(positions)!=1:raise OrderBlocked('POSITION_NOT_UNIQUE')
        pos=positions[0]
        if str(pos.get('holdSide') or pos.get('posSide') or '').lower()!='short':
            raise OrderBlocked('POSITION_SIDE_MISMATCH')
        qty=quantity(pos)
        if qty<=0:raise OrderBlocked('POSITION_QUANTITY_INVALID')
        if any(str(x.get('symbol','')).upper()=='ETHUSDT' for x in snapshot['orders']):
            raise OrderBlocked('EXISTING_EXCHANGE_ORDER')
        if any(str(x.get('symbol','')).upper()=='ETHUSDT' for x in snapshot['plans']):
            raise OrderBlocked('UNOWNED_EXCHANGE_PLAN')
        account=snapshot['account']
        if kind=='classic':
            mode,margin,leverage=(account.get('posMode'),account.get('marginMode'),
                                  account.get('crossedMarginLeverage'))
        else:
            cfg=next((x for x in account.get('symbolConfigList',[])
                      if x.get('symbol')=='ETHUSDT' and x.get('category')=='USDT-FUTURES'),{})
            mode,margin,leverage=(account.get('holdMode'),cfg.get('marginMode'),cfg.get('leverage'))
        if mode!='one_way_mode' or str(margin).lower() not in {'cross','crossed'} or D(leverage or '0')!=150:
            raise OrderBlocked('ADOPTION_REQUIRES_ONEWAY_CROSS_150')
        if pos.get('leverage') and D(pos['leverage'])!=150:
            raise OrderBlocked('POSITION_LEVERAGE_NOT_150')
        config=await self.b.instrument('ETHUSDT',kind)
        avg=D(pos.get('openPriceAvg') or pos.get('avgPrice') or '0')
        if avg<=0:raise OrderBlocked('POSITION_ENTRY_PRICE_MISSING')
        created=int(pos.get('cTime') or pos.get('createdTime') or 0)
        if created<=0 or created>int(time.time()*1000):raise OrderBlocked('POSITION_ENTRY_TIME_MISSING')
        client_oid=oid('adopt',self.b.mode,pos.get('posId') or pos.get('positionId') or
                       f'{fmt(qty)}:{fmt(avg)}')
        order={'id':client_oid,'symbol':'ETHUSDT','side':'short','kind':kind,'mode':self.b.mode,
               'order_type':'adopted','adopted':True,'entry':fmt(avg),'avg_price':fmt(avg),
               'qty':fmt(qty),'filled_qty':fmt(qty),'remaining_qty':fmt(qty),
               'leverage':150,'position_mode':'one_way_mode','margin_mode':'crossed',
               'position_detail':pos,'config':config,'entry_status':'adopted',
               'entry_hour_ms':created//3600000*3600000,'strategy_id':'adopted-short'}
        self.ledger.create(client_oid,int(time.time()*1000),'entry',order)
        self.ledger.update(client_oid,'open',order)
        return {'state':'open','order':order}

    async def reconcile(self, client_oid: str) -> dict:
        row = self.ledger.by_oid(client_oid)
        if not row:
            raise ValueError('UNKNOWN_CLIENT_OID')
        order = row['payload']
        if order.get('mode') != self.b.mode:
            raise OrderBlocked('LEDGER_EXCHANGE_MODE_MISMATCH')
        try:
            detail = {'state':'adopted','baseVolume':order['filled_qty']} if order.get('adopted') else await self.b.detail(order)
            qty_filled = filled(detail)
            status = str(detail.get('orderStatus') or detail.get('state') or detail.get('status') or '').lower()
            if not status:
                raise ValueError('ORDER_DETAIL_MISSING_STATUS')
            if detail.get('orderId'):
                order['exchange_order_id'] = str(detail['orderId'])
            order['exchange_client_oid'] = str(detail.get('clientOid') or order.get('exchange_client_oid') or client_oid)
            positions = await self.b.positions(order['symbol'], order['kind'])
            if len(positions) > 1:
                raise ValueError('MULTIPLE_EXCHANGE_POSITIONS')
            pos = positions[0] if positions else None
            remaining = quantity(pos) if pos else D(0)
            if pos:
                side = str(pos.get('holdSide') or pos.get('posSide') or '').lower()
                if side != order['side']:
                    raise ValueError('POSITION_SIDE_MISMATCH')
                # A close may still have zero filled while the owned short remains.
                # Compare that position with the entry, never with close fills.
                owner = self.ledger.by_oid(order.get('entry_oid')) if row['kind'] == 'close' else None
                owned = D(owner['payload'].get('filled_qty') or '0') if owner else qty_filled
                if remaining > owned:
                    raise ValueError('POSITION_EXCEEDS_TRACKED_FILL')
            if row['kind'] == 'close':
                state = 'filled' if status == 'filled' and qty_filled >= D(order['qty']) else (
                    status if status in TERMINAL else 'pending')
            elif qty_filled > 0 and remaining == 0:
                closes = [x for x in self.ledger.rows() if x['kind'] == 'close'
                          and x['payload'].get('entry_oid') == client_oid and x['state'] == 'filled']
                state = 'closed' if closes else 'position_missing_after_fill'
            elif remaining > 0:
                state = 'partially_filled' if qty_filled < D(order['qty']) else 'open'
            elif status in {'canceled','cancelled','rejected','expired'}:
                state = status
            elif status == 'filled':
                state = 'filled_no_position'
            else:
                state = 'pending'
            plans = await self.b.plans(order['symbol'], order['kind']) if remaining > 0 else []
            if remaining > 0 and order.get('sl') and not covers(plans, 'sl', fmt(remaining)):
                state = 'protection_unverified'
            if remaining > 0 and order.get('tp') and not covers(plans, 'tp', fmt(remaining)):
                state = 'protection_unverified'
            avg=next((str(v) for v in (detail.get('avgPrice'),detail.get('priceAvg'),
                     (pos or {}).get('openPriceAvg'),(pos or {}).get('avgPrice'),order.get('avg_price'))
                     if v and D(v)>0),'0')
            order.update(entry_detail=detail, entry_status=status, filled_qty=fmt(qty_filled),
                         remaining_qty=fmt(remaining), position_detail=pos,
                         avg_price=avg, exchange_plans=plans)
            self.ledger.update(client_oid, state, order)
            return {'state': state, 'order': order}
        except Exception as exc:
            order['reconcile_error'] = str(exc)
            self.ledger.update(client_oid, 'needs_reconcile', order)
            raise

    async def close(self, entry_oid: str, bar_ms: int, *, fraction: str = '1', allow_post: bool = False, reason: str = 'manual'):
        row = self.ledger.by_oid(entry_oid)
        if not row or row['kind'] != 'entry':
            raise OrderBlocked('NO_OWNED_ENTRY')
        entry = row['payload']
        if row['state'] not in {'open', 'partially_filled', 'protection_unverified'}:
            raise OrderBlocked('ENTRY_NOT_CONFIRMED_OPEN', state=row['state'])
        if row['state']=='partially_filled' and entry.get('entry_status') not in {'canceled','cancelled','expired'}:
            raise OrderBlocked('ENTRY_REMAINDER_NOT_CANCELED')
        positions = await self.b.positions(entry['symbol'], entry['kind'])
        if len(positions) != 1:
            raise OrderBlocked('POSITION_NOT_UNIQUE')
        pos = positions[0]
        if str(pos.get('holdSide') or pos.get('posSide') or '').lower() != 'short':
            raise OrderBlocked('POSITION_SIDE_MISMATCH')
        current = quantity(pos)
        if current > D(entry.get('filled_qty') or '0'):
            raise OrderBlocked('POSITION_EXCEEDS_OWNED_FILL')
        share = D(fraction)
        if not 0 < share <= 1:
            raise OrderBlocked('INVALID_CLOSE_FRACTION')
        qty = current if share == 1 else floor_step(current * share, entry['config']['step'])
        if qty <= 0 or qty > current:
            raise OrderBlocked('CLOSE_QTY_INVALID')
        cid = oid(entry_oid, 'close', bar_ms, fmt(share))
        close_order = {'id': cid, 'entry_oid': entry_oid, 'kind': entry['kind'],
                       'symbol': entry['symbol'], 'side': 'short', 'qty': fmt(qty),
                       'position_mode': 'one_way_mode', 'margin_mode': 'crossed',
                       'mode':self.b.mode, 'exit_reason':reason, 'decision_bar_ms':bar_ms}
        if not allow_post:
            return {'state': 'preview_only', 'order': close_order}
        if any(x['kind']=='close' and x['payload'].get('entry_oid')==entry_oid and
               x['state'] not in {'filled','canceled','cancelled','blocked','rejected','expired'}
               for x in self.ledger.rows()):
            raise OrderBlocked('PREVIOUS_CLOSE_NOT_CONFIRMED')
        if self.ledger.by_oid(cid):
            raise OrderBlocked('CLOSE_OID_ALREADY_USED')
        self.post_guard()
        self.ledger.create(cid, bar_ms, 'close', close_order)
        try:
            response = await self.b.reduce(close_order, qty, cid)
            if not isinstance(response, dict) or not (response.get('orderId') or response.get('clientOid')):
                raise ValueError('REDUCE_RESPONSE_MISSING_ID')
            close_order['exchange_order_id'] = str(response.get('orderId') or '')
            close_order['exchange_client_oid'] = str(response.get('clientOid') or cid)
            self.ledger.update(cid, 'accepted', close_order)
        except ExchangeError as exc:
            close_order['last_error'] = exc.info()
            self.ledger.update(cid, 'uncertain' if exc.uncertain else 'blocked', close_order)
            raise
        except Exception as exc:
            close_order['last_error'] = str(exc)
            self.ledger.update(cid, 'uncertain', close_order)
            raise
        return {'state': 'accepted', 'order': close_order}

    async def cancel(self, entry_oid: str, bar_ms: int, *, allow_post: bool = False):
        row = self.ledger.by_oid(entry_oid)
        if not row or row['kind'] != 'entry' or row['state'] not in {'accepted','pending','partially_filled'}:
            raise OrderBlocked('NO_CANCELABLE_OWNED_ENTRY')
        entry = row['payload']
        op_oid = oid(entry_oid, 'cancel', bar_ms)
        operation = {'id':op_oid,'entry_oid':entry_oid,'kind':entry['kind'],
                     'symbol':entry['symbol'],'target_order_id':entry.get('exchange_order_id')}
        if not allow_post:return {'state':'preview_only','order':operation}
        if self.ledger.by_oid(op_oid):raise OrderBlocked('CANCEL_OID_ALREADY_USED')
        self.post_guard()
        self.ledger.create(op_oid, bar_ms, 'cancel', operation)
        try:
            response = await self.b.cancel(entry)
            operation['response'] = response
            self.ledger.update(op_oid,'accepted',operation)
            return {'state':'accepted','order':operation}
        except ExchangeError as exc:
            operation['last_error']=exc.info()
            self.ledger.update(op_oid,'uncertain' if exc.uncertain else 'blocked',operation)
            raise

    async def modify_limit(self, entry_oid: str, bar_ms: int, new_price: str,
                           new_qty: str, *, allow_post: bool = False):
        row = self.ledger.by_oid(entry_oid)
        if not row or row['kind'] != 'entry' or row['state'] not in {'accepted','pending'}:
            raise OrderBlocked('NO_MODIFIABLE_OWNED_ENTRY')
        entry = row['payload']
        if entry['order_type'] != 'limit' or entry['kind'] != 'classic':
            raise OrderBlocked('MODIFY_LIMIT_CLASSIC_ONLY')
        price, qty = D(new_price), D(new_qty)
        if price<=0 or floor_step(price,entry['config']['tick'])!=price or qty<=0 or floor_step(qty,entry['config']['step'])!=qty:
            raise OrderBlocked('INVALID_MODIFY_PRECISION')
        op_oid=oid(entry_oid,'modify',bar_ms,fmt(price),fmt(qty))
        operation={'id':op_oid,'entry_oid':entry_oid,'price':fmt(price),'qty':fmt(qty)}
        if not allow_post:return {'state':'preview_only','order':operation}
        if self.ledger.by_oid(op_oid):raise OrderBlocked('MODIFY_OID_ALREADY_USED')
        self.post_guard()
        self.ledger.create(op_oid,bar_ms,'modify',operation)
        try:
            response=await self.b.modify_entry(entry,price,qty,op_oid)
            operation['response']=response
            self.ledger.update(op_oid,'accepted',operation)
            return {'state':'accepted','order':operation}
        except ExchangeError as exc:
            operation['last_error']=exc.info()
            self.ledger.update(op_oid,'uncertain' if exc.uncertain else 'blocked',operation)
            raise

    async def add_protection(self, entry_oid: str, bar_ms: int, kind: str,
                             price: str, *, allow_post: bool = False):
        row=self.ledger.by_oid(entry_oid)
        if not row or row['kind']!='entry' or row['state'] not in {'open','partially_filled'}:
            raise OrderBlocked('NO_OWNED_OPEN_POSITION')
        if kind not in {'sl','tp'}:raise OrderBlocked('INVALID_PLAN_KIND')
        entry=row['payload'];px=D(price)
        if px<=0 or floor_step(px,entry['config']['tick'])!=px:
            raise OrderBlocked('INVALID_PLAN_PRICE')
        current=(await self.b.ticker(entry['symbol']))['mark']
        if kind=='sl' and px<=D(current):raise OrderBlocked('SHORT_SL_NOT_ABOVE_MARK')
        if kind=='tp' and px>=D(current):raise OrderBlocked('SHORT_TP_NOT_BELOW_MARK')
        positions=await self.b.positions(entry['symbol'],entry['kind'])
        if len(positions)!=1 or str(positions[0].get('holdSide') or positions[0].get('posSide') or '').lower()!='short' or quantity(positions[0])>D(entry.get('filled_qty') or '0'):
            raise OrderBlocked('OWNED_POSITION_MISMATCH')
        qty=quantity(positions[0]);cid=oid(entry_oid,kind,bar_ms,fmt(px))
        operation={'id':cid,'entry_oid':entry_oid,'kind':kind,'price':fmt(px),'qty':fmt(qty)}
        if not allow_post:return {'state':'preview_only','order':operation}
        if self.ledger.by_oid(cid):raise OrderBlocked('PLAN_OID_ALREADY_USED')
        self.post_guard()
        self.ledger.create(cid,bar_ms,'plan_'+kind,operation)
        try:
            response=await self.b.add_plan(entry,kind,px,qty,cid,full=True)
            operation['response']=response
            self.ledger.update(cid,'accepted',operation)
            plans=await self.b.plans(entry['symbol'],entry['kind'])
            if covers(plans,kind,fmt(qty)):
                operation['exchange_plans']=plans
                self.ledger.update(cid,'verified',operation)
                return {'state':'verified','order':operation}
            return {'state':'accepted_unverified','order':operation}
        except ExchangeError as exc:
            operation['last_error']=exc.info()
            self.ledger.update(cid,'uncertain' if exc.uncertain else 'blocked',operation)
            raise

    async def cancel_protection(self, plan_oid: str, bar_ms: int, *, allow_post: bool = False):
        row=self.ledger.by_oid(plan_oid)
        if not row or row['kind'] not in {'plan_sl','plan_tp'} or row['state'] not in {'accepted','verified'}:
            raise OrderBlocked('NO_OWNED_PROTECTION_PLAN')
        plan=row['payload']
        entry_row=self.ledger.by_oid(plan['entry_oid'])
        if not entry_row or entry_row['kind']!='entry':raise OrderBlocked('PLAN_ENTRY_NOT_OWNED')
        entry=entry_row['payload']
        plans=await self.b.plans(entry['symbol'],entry['kind'])
        matches=[p for p in plans if str(p.get('clientOid') or '')==plan_oid or
                 (plan.get('response',{}).get('orderId') and
                  str(p.get('orderId') or '')==str(plan['response']['orderId']))]
        if len(matches)!=1:raise OrderBlocked('PLAN_NOT_UNIQUELY_PRESENT_ON_EXCHANGE')
        cid=oid(plan_oid,'cancel',bar_ms)
        operation={'id':cid,'plan_oid':plan_oid,'entry_oid':plan['entry_oid'],
                   'target_plan':matches[0]}
        if not allow_post:return {'state':'preview_only','order':operation}
        if self.ledger.by_oid(cid):raise OrderBlocked('PLAN_CANCEL_OID_ALREADY_USED')
        self.post_guard()
        self.ledger.create(cid,bar_ms,'cancel_plan',operation)
        try:
            response=await self.b.cancel_plan(entry,matches[0])
            operation['response']=response
            self.ledger.update(cid,'accepted',operation)
            return {'state':'accepted','order':operation}
        except ExchangeError as exc:
            operation['last_error']=exc.info()
            self.ledger.update(cid,'uncertain' if exc.uncertain else 'blocked',operation)
            raise
