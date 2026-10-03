"""Frozen short accounting copy, adding entry-only gate/cooldown and common cash-flow anchor. No live access."""
import numpy as np
from numba import njit

@njit(cache=True)
def mm_value(notional,tiers):
    for j in range(len(tiers)):
        if notional<=tiers[j,0]:return tiers[j,1]
    return 1.

# metrics: status,stop_index,net,ending,deposits,fees,funding,gross,dd_adverse,
# dd_close,entries,exits,wins,losses,worst,best,hold_hours,max_notional,min_headroom,
# first_index,last_index,max_consecutive_losses
@njit(cache=True)
def replay(b,m,r,s,gate,months,tiers,size,stop,maxhold,fee,slip,monthly,start,end,deposit_anchor,cooldown=0,capture=False,funding_factor=1.,family=1,power=1.,cap=180000.):
    cash=100.;q=entry=0.;deps=fees=funding=gross=0.;navunits=100.;peak=1.;dd=ddc=0.
    entries=exits=wins=losses=0;worst=best=holdtotal=maxnot=0.;headroom=1e100
    active_start=-1;first=-1;blocked=False;cooldown_until=-1;status=0;at=end-1;tf=of=0.;losing=consecutive=0
    hist=np.full((end-start,6),np.nan) if capture else np.empty((0,6))
    trades=np.zeros((end-start,8)) if capture else np.empty((0,8))
    for t in range(start,end):
        op=b[t,1];mo=m[t,1];dep=0.
        if t>deposit_anchor and months[t]!=months[t-1]:
            dep=monthly[(months[t]-months[deposit_anchor]-1)%len(monthly)]
            before=cash+q*(entry-mo)
            if before<=0:status=1;at=t;break
            navunits+=dep/(before/navunits);cash+=dep;deps+=dep
        if q and r[t]:
            pay=q*mo*r[t]*funding_factor;cash+=pay;funding+=pay;tf+=pay
        if q:
            notional=q*mo;eq=cash+q*(entry-mo)
            if eq<=notional*(mm_value(notional,tiers)+fee):status=7;at=t;break
        if not s[t]:blocked=False
        close_now=False;reason=0
        if q:
            if not s[t]:close_now=True;reason=1
            elif stop>0 and t>active_start and b[t-1,4]>=entry*(1+stop):close_now=True;reason=2;blocked=True
            elif maxhold>0 and t-active_start>=maxhold:close_now=True;reason=3;blocked=True
        if close_now:
            fill=op*(1+slip);gp=q*(entry-fill);cf=q*fill*fee;net=gp-of-cf+tf
            cash+=gp-cf;gross+=gp;fees+=cf;exits+=1;wins+=net>0;losses+=net<=0
            losing=losing+1 if net<=0 else 0;consecutive=max(consecutive,losing)
            worst=min(worst,net);best=max(best,net);holdtotal+=t-active_start
            if capture:trades[exits-1]=np.array([active_start,t,q,entry,fill,net,tf,reason],np.float64)
            q=0.;cooldown_until=t+cooldown
        if not q and s[t] and not blocked and gate[t] and t>=cooldown_until:
            fill=op*(1-slip)
            if family==0: target=size
            elif family==1: target=cash*size
            elif family==2: target=100.*size*(max(cash,0.)/100.)**power
            else: target=(100.+deps)*size
            if cap>0: target=min(target,cap)
            units=np.floor(target/fill/.01+1e-10)*.01
            no=units*fill;cf=no*fee
            if units<.01 or no<5:status=2;at=t;break
            if no/150+cf>cash or units>1900 or no>200000:status=4;at=t;break
            q=units;entry=fill;cash-=cf;fees+=cf;of=cf;tf=0.;active_start=t;entries+=1
            if first<0:first=t
            maxnot=max(maxnot,no)
        eq=cash+q*(entry-m[t,4]);adverse=cash+q*(entry-m[t,2])
        if q:
            no=q*m[t,2];headroom=min(headroom,adverse-no*(mm_value(no,tiers)+fee))
            if adverse<=no*(mm_value(no,tiers)+fee):status=7;at=t;break
        dd=max(dd,1-adverse/navunits/peak)
        nav=eq/navunits;peak=max(peak,nav);ddc=max(ddc,1-nav/peak)
        if capture:hist[t-start]=np.array([eq,dep,nav,adverse/navunits,fees,funding])
    if status==0 and q:
        fill=b[end-1,4]*(1+slip);gp=q*(entry-fill);cf=q*fill*fee;net=gp-of-cf+tf
        cash+=gp-cf;gross+=gp;fees+=cf;exits+=1;wins+=net>0;losses+=net<=0
        losing=losing+1 if net<=0 else 0;consecutive=max(consecutive,losing)
        worst=min(worst,net);best=max(best,net);holdtotal+=end-active_start
        dd=max(dd,1-cash/navunits/peak);ddc=max(ddc,1-cash/navunits/peak)
        if capture:
            trades[exits-1]=np.array([active_start,end-1,q,entry,fill,net,tf,4],np.float64)
            hist[-1,0]=cash;hist[-1,2]=cash/navunits;hist[-1,3]=min(hist[-1,3],cash/navunits);hist[-1,4]=fees;hist[-1,5]=funding
    net=cash-100-deps if status==0 else np.nan
    ans=np.array([status,at,net,cash,deps,fees,funding,gross,dd,ddc,entries,exits,wins,losses,worst,best,holdtotal,maxnot,headroom,first,end-1,consecutive])
    return ans,hist,trades[:exits]
