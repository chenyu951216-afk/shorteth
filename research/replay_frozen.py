"""Reproduce the selected short only; no parameter selection, API keys or orders."""
from pathlib import Path
import sys,json
import numpy as np
import pandas as pd

ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT.parent))
from shorteth.strategy import signal_stream, exit_reason, target_notional
from research.frozen_accounting import replay


def inputs():
    with np.load(ROOT/'data/eth_hourly_2020_2026.npz') as z:b,r=z['bars'],z['funding']
    m=np.load(ROOT/'data/eth_binance_mark_hourly_2020_2026.npz')['marks']
    assert len(b)==59160 and np.array_equal(b[:,0],m[:,0])
    assert np.isfinite(b).all() and np.isfinite(m).all() and np.isfinite(r).all()
    assert np.all(np.diff(b[:,0])==3600000)
    signals=list(signal_stream([(int(x[0]),float(x[4])) for x in b]))
    s=np.r_[np.int8(0),np.array([x.desired_short for x in signals[:-1]],dtype=np.int8)]
    g=np.r_[np.int8(0),np.array([x.entry_allowed for x in signals[:-1]],dtype=np.int8)]
    months=np.array([x.year*12+x.month for x in pd.to_datetime(b[:,0],unit='ms',utc=True)],np.int32)
    tiers=np.array([[float(x['endUnit']),float(x['keepMarginRate'])] for x in json.loads((ROOT/'reference/tiers_snapshot.json').read_text())['response']['data']])
    return b,m,r,s,g,months,tiers,signals


def validate():
    b,m,r,s,g,months,tiers,signals=inputs()
    anchor=int(np.flatnonzero(s)[0])
    a,h,tr=replay(b,m,r,s,g,months,tiers,2.925,.06,168,.0006,.0002,np.array([300.]),0,len(b),anchor,capture=True,cap=200000.)
    expected=pd.read_csv(ROOT/'reference/qualified_champion_trades.csv')
    columns=['entry_index','exit_index','qty','entry','exit','net','funding','reason']
    assert a[0]==0 and len(tr)==113
    np.testing.assert_allclose(tr,expected[columns].to_numpy(),rtol=1e-10,atol=1e-6)
    assert abs(a[2]-146060.37820294476)<1e-6
    # Independently walk EVERY hour using the production exit function, actual
    # historical entry fills, and the durable-reset rule used by Runner.
    opened=None; blocked_at=-1; seen=[];n=0
    reasons={'signal_reset':1,'close_stop_6pct':2,'max_hold_168h':3}
    for i in range(1,len(b)):
        sig=signals[i-1]
        if opened is not None:
            entry_i,price=opened
            reason=exit_reason(sig,str(price),int(b[entry_i,0]))
            if reason:
                seen.append((entry_i,i,reasons[reason]));opened=None
                if reason in {'close_stop_6pct','max_hold_168h'}:blocked_at=sig.bar_open_ms
        if opened is None and sig.desired_short and sig.entry_allowed and sig.last_flat_bar_ms>blocked_at:
            assert i==int(tr[n,0]),('unexpected entry',i,n)
            opened=(i,float(tr[n,3]));n+=1
    if opened is not None:seen.append((opened[0],len(b)-1,4))
    np.testing.assert_array_equal(np.array(seen),tr[:,[0,1,7]])
    return {'通過':True,'資料根數':len(b),'完整交易':len(tr),'本金U':100+a[4],
            '交易淨利U':a[2],'期末權益U':a[3],'最大回撤百分比':a[8]*100,
            '進出場逐筆核對':True,'每小時正式出場函式核對':True,
            '限制':'Binance代理行情、歷史成本模型；不是Bitget真實成交或強平保證。'}

if __name__=='__main__':print(json.dumps(validate(),ensure_ascii=True))
