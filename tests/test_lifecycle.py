"""Exercise the production Runner and Execution against a stateful fake exchange.

No network, API keys, or real orders. Ledger survives simulated process restarts.
"""
import asyncio
import json
from pathlib import Path

import httpx
import pytest

from shorteth import web
from shorteth.execution import Execution, Ledger, OrderBlocked, TradeIntent, oid
from shorteth.runner import Runner
from shorteth.strategy import Signal, HOUR_MS, evaluate
from test_execution import FakeBitget, D, intent


class Exchange(FakeBitget):
    def __init__(self):
        super().__init__()
        self.qty=D(0)
        self.details={}
        self.delay_close=False

    async def positions(self,*args):
        return [{'symbol':'ETHUSDT','holdSide':'short','total':str(self.qty)}] if self.qty else []

    async def account_snapshot(self,*args):
        snap=await super().account_snapshot(*args)
        snap['positions']=await self.positions()
        return snap

    async def place(self,o):
        assert self.qty==0, 'duplicate entry'
        self.placed.append(o)
        self.qty=D(o['qty'])
        self.details[o['id']]={'orderStatus':'filled','baseVolume':o['qty'],'orderId':o['id'],'priceAvg':o['entry']}
        return {'orderId':o['id'],'clientOid':o['id']}

    async def detail(self,o):return self.details[o['id']]

    async def reduce(self,o,qty,cid):
        assert self.qty>=qty>0
        self.reduced.append((o,qty,cid))
        if not self.delay_close:self.qty-=qty
        self.details[cid]={'orderStatus':'live' if self.delay_close else 'filled',
                           'baseVolume':'0' if self.delay_close else str(qty),'orderId':cid}
        return {'orderId':cid,'clientOid':cid}


def signal(i,short):return Signal(i*HOUR_MS,(i+1)*HOUR_MS,short,-.1 if short else .1,2100,2000,4000,short,0 if short else i*HOUR_MS)


async def decision(r,i,long,post=True):
    return await r.decide(allow_post=post,signal=signal(i,long),now_ms=(i+1)*HOUR_MS+1000)


@pytest.mark.asyncio
async def test_repeated_scans_restart_and_delayed_close_never_duplicate(tmp_path):
    b=Exchange();r=Runner(Execution(b,Ledger(tmp_path/'orders.sqlite')),tmp_path)
    for _ in range(20):await decision(r,10,True)
    assert len(b.placed)==1 and b.qty==D('.14')
    r=Runner(Execution(b,Ledger(tmp_path/'orders.sqlite')),tmp_path)
    for i in range(11,15):assert (await decision(r,i,True))['action']=='hold'
    b.delay_close=True
    assert (await decision(r,15,False))['action']=='exit'
    for _ in range(10):assert (await decision(r,15,False))['action']=='blocked'
    assert len(b.reduced)==1
    close_id=b.reduced[-1][2]
    b.details[close_id].update(orderStatus='filled',baseVolume='.14');b.qty=D(0)
    assert (await decision(r,16,False))['action']=='flat'
    b.delay_close=False
    assert (await decision(r,17,True))['action']=='enter'
    assert len(b.placed)==2
    assert all(x['symbol']=='ETHUSDT' and x['side']=='short' and
               x['order_type']=='market' and not x['sl'] and not x['tp'] for x in b.placed)


@pytest.mark.asyncio
async def test_pending_partial_close_compares_remaining_with_owned_entry(tmp_path):
    b=Exchange();e=Execution(b,Ledger(tmp_path/'orders.sqlite'))
    await e.submit(intent(),allow_post=True);b.delay_close=True
    c=await e.close(intent().client_order_id,2,allow_post=True)
    b.qty=D('.08');b.details[c['order']['id']].update(orderStatus='partially_filled',baseVolume='.02')
    assert (await e.reconcile(c['order']['id']))['state']=='pending'
    with pytest.raises(OrderBlocked,match='PREVIOUS_CLOSE_NOT_CONFIRMED'):
        await e.close(intent().client_order_id,3,allow_post=True)
    assert len(b.reduced)==1


@pytest.mark.asyncio
async def test_exchange_filled_but_response_lost_is_recovered_without_repost(tmp_path):
    from shorteth.exchange.bitget import ExchangeError
    b=Exchange();original=b.place
    async def lost(o):
        await original(o)
        raise ExchangeError('place','NETWORK','lost response',True)
    b.place=lost
    r=Runner(Execution(b,Ledger(tmp_path/'orders.sqlite')),tmp_path)
    with pytest.raises(ExchangeError):await decision(r,10,True)
    r=Runner(Execution(b,Ledger(tmp_path/'orders.sqlite')),tmp_path)
    assert (await decision(r,10,True))['action']=='hold'
    assert len(b.placed)==1


@pytest.mark.asyncio
async def test_partial_entry_cancel_confirm_then_exit(tmp_path):
    b=Exchange();original=b.place
    async def partial(o):
        response=await original(o);b.qty=D('.04')
        b.details[o['id']].update(orderStatus='partially_filled',baseVolume='.04')
        return response
    async def cancel(o):
        b.cancelled.append(o);b.details[o['id']]['orderStatus']='canceled'
        return {'orderId':o['id'],'priceAvg':o['entry']}
    b.place=partial;b.cancel=cancel
    r=Runner(Execution(b,Ledger(tmp_path/'orders.sqlite')),tmp_path)
    await decision(r,10,True)
    assert (await decision(r,11,False))['action']=='cancel_unfilled_remainder'
    assert (await decision(r,11,False))['action']=='exit'
    assert (await decision(r,11,False))['action']=='flat'
    assert len(b.placed)==len(b.cancelled)==len(b.reduced)==1
    assert b.reduced[0][1]==D('.04')


@pytest.mark.asyncio
async def test_foreign_position_is_visible_even_when_signal_flat(tmp_path):
    b=Exchange();b.qty=D('.1');r=Runner(Execution(b,Ledger(tmp_path/'orders.sqlite')),tmp_path)
    assert (await decision(r,10,False))['reason']=='EXISTING_EXCHANGE_POSITION'
    assert not b.placed and not b.reduced


@pytest.mark.asyncio
async def test_deposit_while_holding_only_changes_next_entry_size(tmp_path):
    b=Exchange();equity='100';original=b.account_snapshot
    async def snapshot(*args):
        s=await original(*args)
        s['balance'].update(accountEquity=equity,available=equity,crossedMaxAvailable=equity)
        return s
    b.account_snapshot=snapshot
    r=Runner(Execution(b,Ledger(tmp_path/'orders.sqlite')),tmp_path)
    await decision(r,10,True)
    assert b.qty==D('.14')
    equity='400'  # User adds 300; no rebalance or extra entry is allowed.
    for i in range(11,20):assert (await decision(r,i,True))['action']=='hold'
    assert b.qty==D('.14') and len(b.placed)==1
    await decision(r,20,False);await decision(r,20,False)
    await decision(r,21,True)
    assert b.qty==D('.58') and len(b.placed)==2  # 400*0.0135*150/2000 -> .40


@pytest.mark.asyncio
async def test_stop_arriving_during_prepare_prevents_actual_place(tmp_path):
    b=Exchange();e=Execution(b,Ledger(tmp_path/'orders.sqlite'));enabled=True
    async def prepare(o):
        nonlocal enabled
        enabled=False
    def guard():
        if not enabled:raise OrderBlocked('TRADING_DISABLED_BEFORE_SEND')
    b.prepare=prepare;e.post_guard=guard
    with pytest.raises(OrderBlocked):await e.submit(intent(),allow_post=True)
    assert not b.placed and e.ledger.rows()[0]['state']=='blocked'


@pytest.mark.asyncio
async def test_scanner_reads_while_disarmed_tracks_gaps_and_recovers_feed(tmp_path,monkeypatch):
    b=Exchange();r=Runner(Execution(b,Ledger(tmp_path/'orders.sqlite')),tmp_path)
    turn=10;fail=False
    async def current():
        if fail:raise httpx.ReadTimeout('public feed unavailable')
        return signal(turn,True),(turn+1)*HOUR_MS+1000
    r.signal=current
    monkeypatch.setattr(web,'DATA',tmp_path)
    monkeypatch.setattr(web,'runtime',{'lock':asyncio.Lock(),'runner':r,'automatic':False,
        'armed':False,'scanner':{},'store':None,'events':[], 'last_result':None})
    # Event implementation writes a deque; capture it to avoid unrelated persistence.
    events=[];monkeypatch.setattr(web,'_event',lambda *a,**k:events.append((a,k)))
    await web._scan_once();await web._scan_once()
    assert web.runtime['scanner']['scans']==2 and not b.placed
    assert web.runtime['last_result']['execution']['state']=='preview_only'
    fail=True;await web._scan_once()
    assert web.runtime['scanner']['error']
    fail=False;turn=13;await web._scan_once()
    assert web.runtime['scanner']['missed_hours']==2 and web.runtime['scanner']['error'] is None
    assert not b.placed and not r.execution.ledger.rows()
