"""One current-hour strategy decision; no replay of missed historical orders."""
from __future__ import annotations

import time
from pathlib import Path

import httpx

from .execution import Execution, Ledger, OrderBlocked, TradeIntent, oid
from .strategy import evaluate, fetch_complete_closes, target_notional, exit_reason, STRATEGY_ID, HOUR_MS
from .exchange.signals import fmt, floor_step, decimal as D


class Runner:
    def __init__(self, execution: Execution, data_dir: str | Path):
        self.execution = execution
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

    async def signal(self):
        async with httpx.AsyncClient(base_url='https://fapi.binance.com', timeout=25) as client:
            response = await client.get('/fapi/v1/time')
            response.raise_for_status()
            now_ms = int(response.json()['serverTime'])
            closes = await fetch_complete_closes(client, now_ms,
                                                  self.data_dir / 'binance_eth_1h_closes.json')
        return evaluate(closes, now_ms), now_ms

    async def decide(self, *, allow_post: bool = False, signal=None, now_ms=None):
        if signal is None:
            signal, now_ms = await self.signal()
        now_ms = now_ms or int(time.time() * 1000)
        result = {'bar_open_ms': signal.bar_open_ms, 'decided_at_ms': signal.decided_at_ms,
                  'observation_lag_ms': now_ms - signal.decided_at_ms,
                  'momentum_60d': signal.momentum, 'ema720': signal.ema_slow,
                  'last_close':signal.last_close, 'entry_buffer_passed':signal.entry_allowed,
                  'desired': 'short' if signal.desired_short else 'flat',
                  'live_order_post_enabled_for_this_call': allow_post}
        if not 0 <= result['observation_lag_ms'] < 3_600_000:
            return result | {'action':'blocked','reason':'SIGNAL_HOUR_EXPIRED'}
        rows = self.execution.ledger.rows()
        for row in sorted(rows,key=lambda x: 0 if x['kind']=='close' else 1):
            if row['kind'] not in {'entry','close'}:
                continue
            if row['state'] in {'uncertain','needs_reconcile','submitting','accepted','pending',
                                'partially_filled','protection_unverified','open',
                                'position_missing_after_fill','filled_no_position'}:
                try:
                    await self.execution.reconcile(row['oid'])
                except Exception as exc:
                    return result | {'action':'blocked','reason':'RECONCILIATION_REQUIRED',
                                     'clientOid':row['oid'],'detail':str(exc)}
        rows = self.execution.ledger.rows()
        if any(x['kind'] in {'entry','close'} and x['state'] in {'uncertain','needs_reconcile','submitting','accepted','pending',
                               'position_missing_after_fill','filled_no_position','protection_unverified'} for x in rows):
            return result | {'action':'blocked','reason':'UNRESOLVED_EXCHANGE_STATE'}
        entry_rows = [x for x in rows if x['kind']=='entry' and x['state'] in {'open','partially_filled'}]
        if len(entry_rows) > 1:
            return result | {'action':'blocked','reason':'MULTIPLE_LOCAL_POSITIONS'}
        entry = entry_rows[0] if entry_rows else None
        if not entry:
            snapshot = await self.execution.b.account_snapshot('ETHUSDT')
            for key, reason in [('positions','EXISTING_EXCHANGE_POSITION'),
                                ('orders','EXISTING_EXCHANGE_ORDER'),
                                ('plans','UNOWNED_EXCHANGE_PLAN')]:
                if any(str(x.get('symbol','')).upper()=='ETHUSDT' for x in snapshot[key]):
                    return result | {'action':'blocked','reason':reason}
        if entry:
            payload=entry['payload']
            # Do not substitute the quote used for sizing for a confirmed fill.
            avg=payload.get('avg_price')
            if not avg or D(avg)<=0 or payload.get('entry_hour_ms') is None:
                return result | {'action':'blocked','reason':'ENTRY_PRICE_OR_TIME_UNCONFIRMED'}
            reason=exit_reason(signal,avg,int(payload['entry_hour_ms']))
            # A terminal partial/rejected close must finish reducing, even if the
            # next hour's signal recovers. Successful full closes were reconciled above.
            previous=[x for x in rows if x['kind']=='close' and x['payload'].get('entry_oid')==entry['oid']
                      and x['payload'].get('exit_reason') in {'signal_reset','close_stop_6pct','max_hold_168h'}]
            if previous:reason=previous[0]['payload']['exit_reason']
            if payload.get('strategy_exit_reason'):reason=payload['strategy_exit_reason']
            result.update(entry_oid=entry['oid'],entry_price=avg,
                          stop_close_price=fmt(D(avg)*D('1.06')),
                          timeout_at_ms=int(payload['entry_hour_ms'])+168*HOUR_MS,
                          holding_hours=(signal.decided_at_ms-int(payload['entry_hour_ms']))/HOUR_MS,
                          exit_reason=reason)
            if not reason:return result | {'action':'hold'}
            if allow_post and not payload.get('strategy_exit_reason'):
                # Latch before cancel OR close. A price recovery, process restart
                # or partial fill must not withdraw an already required exit.
                payload.update(strategy_exit_reason=reason,strategy_exit_bar_ms=signal.bar_open_ms)
                self.execution.ledger.update(entry['oid'],entry['state'],payload)
            try:
                if entry['state']=='partially_filled' and entry['payload'].get('entry_status') not in {'canceled','cancelled','expired'}:
                    existing_cancel=any(x['kind']=='cancel' and x['payload'].get('entry_oid')==entry['oid']
                                        for x in rows)
                    if existing_cancel:
                        return result | {'action':'blocked','reason':'WAITING_FOR_ENTRY_CANCEL'}
                    canceled=await self.execution.cancel(entry['oid'],signal.bar_open_ms,allow_post=allow_post)
                    return result | {'action':'cancel_unfilled_remainder','execution':canceled}
                close = await self.execution.close(entry['oid'], signal.bar_open_ms,
                                                   allow_post=allow_post,reason=reason)
                return result | {'action':'exit','execution':close}
            except OrderBlocked as exc:
                return result | {'action':'blocked','reason':exc.reason,'details':exc.details}
        if not signal.desired_short:
            return result | {'action':'flat'}
        # Stop/timeout re-entry lock is derived from the same durable close intent
        # recorded BEFORE the financial POST. Restart/timeout cannot lose it.
        stopped=[x for x in rows if x['kind']=='close' and
                 x['payload'].get('exit_reason') in {'close_stop_6pct','max_hold_168h'}]
        stopped_entries=[x for x in rows if x['kind']=='entry' and
                         x['payload'].get('strategy_exit_reason') in {'close_stop_6pct','max_hold_168h'}]
        if (any(signal.last_flat_bar_ms<=x['bar_ms'] for x in stopped) or
            any(signal.last_flat_bar_ms<=x['payload']['strategy_exit_bar_ms'] for x in stopped_entries)):
            return result | {'action':'wait_reset','reason':'WAIT_FOR_SIGNAL_RESET'}
        if not signal.entry_allowed:
            return result | {'action':'wait_buffer','reason':'ENTRY_BUFFER_NOT_REACHED'}
        # A confirmed closed/manual-test order in this hour must not be replayed.
        if any(x['kind']=='entry' and x['bar_ms']==signal.bar_open_ms for x in rows):
            return result | {'action':'flat','reason':'HOUR_ENTRY_ALREADY_HANDLED'}
        # Size only on a new entry. Deposits never top up an existing position.
        snapshot = await self.execution.b.account_snapshot('ETHUSDT')
        quote = await self.execution.b.ticker('ETHUSDT')
        bid = D(quote['bid'])
        raw_notional = target_notional(snapshot['balance']['accountEquity'])
        qty = floor_step(raw_notional / bid, '0.01')
        if qty <= 0:
            return result | {'action':'blocked','reason':'BELOW_BACKTEST_0P01_ETH_STEP'}
        notional = qty * bid
        intent = TradeIntent(symbol='ETHUSDT', side='short', order_type='market',
                             notional_usdt=fmt(notional),
                             client_order_id=oid(STRATEGY_ID,signal.bar_open_ms,'entry'),
                             decision_bar_ms=signal.bar_open_ms, entry=fmt(bid),
                             strategy_id=STRATEGY_ID,entry_hour_ms=signal.decided_at_ms)
        try:
            opened = await self.execution.submit(intent, allow_post=allow_post)
            return result | {'action':'enter','requested_notional_usdt':fmt(notional),
                             'execution':opened}
        except OrderBlocked as exc:
            return result | {'action':'blocked','reason':exc.reason,'details':exc.details}


def make_runner(bitget, data_dir: str | Path):
    data_dir = Path(data_dir)
    return Runner(Execution(bitget, Ledger(data_dir / 'orders.sqlite')), data_dir)
