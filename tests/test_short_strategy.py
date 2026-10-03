from dataclasses import replace
from pathlib import Path
import pytest
from shorteth.strategy import *
from shorteth.execution import Execution,Ledger,OrderBlocked,TradeIntent
from shorteth.runner import Runner
from test_lifecycle import Exchange,signal,decision
from test_execution import D


def test_sizing_cap_and_invalid_equity():
    assert target_notional('100')==Decimal('292.5000')
    assert target_notional('100000')==Decimal('200000')
    for v in ['NaN','Infinity','0','-1']:
        with pytest.raises(ValueError):target_notional(v)


def test_signal_validation_and_hysteresis():
    closes=[(i*HOUR_MS,2000.) for i in range(1441)]
    closes.append((1441*HOUR_MS,1800.))
    first=evaluate(closes,1442*HOUR_MS)
    assert first.desired_short and first.entry_allowed
    # Momentum no longer below -5%: retain short state until zero/EMA reset.
    closes.append((1442*HOUR_MS,1950.))
    assert evaluate(closes,1443*HOUR_MS).desired_short
    closes.append((1443*HOUR_MS,2100.))
    reset=evaluate(closes,1444*HOUR_MS)
    assert not reset.desired_short and reset.last_flat_bar_ms==1443*HOUR_MS
    with pytest.raises(ValueError):evaluate(closes,1445*HOUR_MS)
    with pytest.raises(ValueError):evaluate(closes,1443*HOUR_MS)
    with pytest.raises(ValueError):evaluate(closes[:-1]+[closes[-2]],1444*HOUR_MS)
    with pytest.raises(ValueError):evaluate(closes[:-1]+[(1443*HOUR_MS,float('nan'))],1444*HOUR_MS)


def test_exit_priority_close_stop_and_timeout_exact_boundaries():
    sig=signal(11,True)
    assert exit_reason(replace(sig,last_close=2119.99),'2000',11*HOUR_MS) is None
    assert exit_reason(replace(sig,last_close=2120),'2000',11*HOUR_MS)=='close_stop_6pct'
    assert exit_reason(signal(177,True),'2000',11*HOUR_MS) is None
    assert exit_reason(signal(178,True),'2000',11*HOUR_MS)=='max_hold_168h'
    assert exit_reason(replace(signal(178,False),last_close=2120),'2000',11*HOUR_MS)=='signal_reset'


@pytest.mark.asyncio
@pytest.mark.parametrize('kind',['stop','timeout'])
async def test_exit_lock_survives_restart_no_reentry_until_reset(tmp_path,kind):
    b=Exchange();r=Runner(Execution(b,Ledger(tmp_path/'orders.sqlite')),tmp_path)
    await decision(r,10,True)
    i=11 if kind=='stop' else 178
    sig=replace(signal(i,True),last_close=2120 if kind=='stop' else 2000)
    preview=await r.decide(signal=sig,now_ms=sig.decided_at_ms+1)
    assert preview['action']=='exit' and not b.reduced
    result=await r.decide(allow_post=True,signal=sig,now_ms=sig.decided_at_ms+1)
    assert result['action']=='exit'
    r=Runner(Execution(b,Ledger(tmp_path/'orders.sqlite')),tmp_path)
    for _ in range(3):assert (await decision(r,i,True))['action']=='wait_reset'
    assert len(b.reduced)==1 and len(b.placed)==1
    # Reset may occur while process is off: signal reconstruction knows last flat.
    sig=replace(signal(i+3,True),last_flat_bar_ms=(i+1)*HOUR_MS)
    assert (await r.decide(allow_post=True,signal=sig,now_ms=sig.decided_at_ms+1))['action']=='enter'
    assert len(b.placed)==2


@pytest.mark.asyncio
async def test_buffer_only_filters_entry_does_not_exit(tmp_path):
    b=Exchange();r=Runner(Execution(b,Ledger(tmp_path/'orders.sqlite')),tmp_path)
    sig=replace(signal(10,True),entry_allowed=False)
    assert (await r.decide(allow_post=True,signal=sig,now_ms=sig.decided_at_ms+1))['action']=='wait_buffer'
    await decision(r,11,True)
    sig=replace(signal(12,True),entry_allowed=False)
    assert (await r.decide(allow_post=True,signal=sig,now_ms=sig.decided_at_ms+1))['action']=='hold'
    assert len(b.placed)==1 and not b.reduced


@pytest.mark.asyncio
async def test_foreign_long_never_adopted_or_closed(tmp_path):
    b=Exchange();r=Runner(Execution(b,Ledger(tmp_path/'orders.sqlite')),tmp_path)
    async def positions(*a):return [{'symbol':'ETHUSDT','holdSide':'long','total':'0.1'}]
    b.positions=positions
    assert (await decision(r,10,True))['reason']=='EXISTING_EXCHANGE_POSITION'
    with pytest.raises(OrderBlocked,match='POSITION_SIDE_MISMATCH'):
        await r.execution.adopt_position(allow_adopt=True)
    assert not b.placed and not b.reduced and not b.prepared


def test_all_frozen_trades_and_every_hour_exit_parity():
    from research.replay_frozen import validate
    assert validate()['完整交易']==113


@pytest.mark.asyncio
async def test_stop_remains_required_after_partial_entry_cancel_price_recovery(tmp_path):
    b=Exchange();original=b.place
    async def partial(o):
        out=await original(o);b.qty=D('.04')
        b.details[o['id']].update(orderStatus='partially_filled',baseVolume='.04')
        return out
    async def cancel(o):
        b.details[o['id']]['orderStatus']='canceled';b.cancelled.append(o)
        return {'orderId':o['id']}
    b.place=partial;b.cancel=cancel
    r=Runner(Execution(b,Ledger(tmp_path/'orders.sqlite')),tmp_path)
    await decision(r,10,True)
    sig=replace(signal(11,True),last_close=2120)
    assert (await r.decide(allow_post=True,signal=sig,now_ms=sig.decided_at_ms+1))['action']=='cancel_unfilled_remainder'
    r=Runner(Execution(b,Ledger(tmp_path/'orders.sqlite')),tmp_path)
    assert (await decision(r,12,True))['action']=='exit'
    assert (await decision(r,12,True))['action']=='wait_reset'
    assert len(b.placed)==len(b.reduced)==len(b.cancelled)==1


@pytest.mark.asyncio
async def test_lost_stop_close_response_cannot_double_close_or_reenter(tmp_path):
    from shorteth.exchange.bitget import ExchangeError
    b=Exchange();r=Runner(Execution(b,Ledger(tmp_path/'orders.sqlite')),tmp_path)
    await decision(r,10,True)
    original=b.reduce
    async def lost(o,q,c):
        await original(o,q,c)
        raise ExchangeError('reduce','NETWORK','response lost',True)
    b.reduce=lost
    sig=replace(signal(11,True),last_close=2120)
    with pytest.raises(ExchangeError):await r.decide(allow_post=True,signal=sig,now_ms=sig.decided_at_ms+1)
    r=Runner(Execution(b,Ledger(tmp_path/'orders.sqlite')),tmp_path)
    for _ in range(3):assert (await decision(r,12,True))['action']=='wait_reset'
    assert len(b.reduced)==len(b.placed)==1


@pytest.mark.asyncio
@pytest.mark.parametrize('kind',['classic','uta'])
async def test_adapter_short_open_close_and_native_plans_direction(kind):
    import json,time,httpx
    from shorteth.exchange.bitget import Bitget
    from test_execution import credentials,response
    seen=[]
    def handler(req):seen.append(json.loads(req.content));return response({'orderId':'test'})
    b=Bitget(credentials(),'demo',httpx.AsyncClient(base_url='https://api.bitget.com',transport=httpx.MockTransport(handler)));b.synced=time.time()
    o={'id':'test','symbol':'ETHUSDT','side':'short','kind':kind,'order_type':'market','qty':'.1','leverage':150,'prepared_verified':True}
    await b.place(o);await b.reduce(o,'.1','close');await b.add_plan(o,'sl','2120','.1','stop',full=True)
    assert seen[0]['side']=='sell' and seen[1]['side']=='buy'
    assert seen[1]['reduceOnly'].lower()=='yes'
    assert seen[2]['holdSide' if kind=='classic' else 'side']==('sell' if kind=='classic' else 'buy')
    await b.close()


@pytest.mark.asyncio
async def test_historical_runner_sqlite_restarts_and_repeat_scans(tmp_path):
    """All recorded transitions plus neighbors/raw resets through real Runner.

    Quotes and equity are controlled frozen inputs; this verifies execution
    intents and persistence, not real exchange market liquidity.
    """
    import numpy as np
    from research.replay_frozen import inputs,replay
    b,m,f,s,g,months,tiers,signals=inputs()
    _,hist,tr=replay(b,m,f,s,g,months,tiers,2.925,.06,168,.0006,.0002,
        np.array([300.]),0,len(b),int(np.flatnonzero(s)[0]),capture=True,cap=200000.)
    changes=np.flatnonzero(s[1:]!=s[:-1])+1
    anchors=set(map(int,changes))|set(map(int,tr[:,0]))|set(map(int,tr[:,1]))
    points=sorted({j for i in anchors for j in (i-1,i,i+1) if 1442<=j<len(b)})
    exchange=Exchange();current=0
    original=exchange.account_snapshot
    async def snapshot(*args):
        snap=await original(*args)
        equity=float(hist[current-1,0]+hist[current,1])
        snap['balance'].update(accountEquity=str(equity),available=str(equity),crossedMaxAvailable=str(equity))
        return snap
    async def ticker(*args):
        return {'bid':str(b[current,1]*(1-.0002)),'ask':str(b[current,1]*(1+.0002)),
                'mark':str(m[current,1]),'last':str(b[current,1]),'ts':int(b[current,0])}
    exchange.account_snapshot=snapshot;exchange.ticker=ticker
    runner=Runner(Execution(exchange,Ledger(tmp_path/'orders.sqlite')),tmp_path)
    opens=[];exits=[]
    for current in points:
        prev=(len(exchange.placed),len(exchange.reduced))
        for _ in range(2):
            result=await runner.decide(allow_post=True,signal=signals[current-1],now_ms=int(b[current,0])+1000)
            assert result['action']!='blocked',(current,result)
        if len(exchange.placed)>prev[0]:opens.append((current,float(exchange.placed[-1]['qty'])))
        if len(exchange.reduced)>prev[1]:exits.append(current)
        if len(exchange.placed)>prev[0] or len(exchange.reduced)>prev[1]:
            runner=Runner(Execution(exchange,Ledger(tmp_path/'orders.sqlite')),tmp_path)
    np.testing.assert_allclose(np.array(opens),tr[:,[0,2]],rtol=1e-10,atol=1e-8)
    np.testing.assert_array_equal(exits,tr[:,1])
