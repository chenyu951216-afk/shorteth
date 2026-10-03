"""Only S300 numeric and trade-validation helpers; no signal parser.

Copied from chenyu951216-afk/s300 app/signals.py main 09656136.
"""
import re,unicodedata
from decimal import Decimal,InvalidOperation,ROUND_DOWN,ROUND_CEILING

def decimal(v):
    raw=unicodedata.normalize('NFKC',str(v if v is not None else '')).strip()
    raw=raw.replace(',','').replace('，','').replace('_','')
    if not raw:raise ValueError('數值不可留空')
    try:d=Decimal(raw)
    except (InvalidOperation,ValueError):raise ValueError('數值格式錯誤：'+raw[:48]) from None
    if not d.is_finite():raise ValueError('數值不得是 NaN 或無限大')
    return d
def fmt(v):return format(decimal(v),'f')
def floor_step(value,step):
    v,s=decimal(value),decimal(step)
    if s<=0:raise ValueError('交易所數量或價格步進無效')
    return (v/s).to_integral_value(rounding=ROUND_DOWN)*s
def ceil_step(value,step):
    v,s=decimal(value),decimal(step)
    if s<=0:raise ValueError('交易所數量或價格步進無效')
    return (v/s).to_integral_value(rounding=ROUND_CEILING)*s

def _zero_hint(value,entry,want_below):
    """Only suggest a likely extra/missing zero; never modify user input."""
    try:
        if want_below and value>=entry and value/10<entry and value/10>0:
            return f' 看起來可能多打一個 0：{fmt(value)} → {fmt(value/10)}；請自行確認，系統不會自動改價。'
        if not want_below and value<=entry and value*10>entry:
            return f' 看起來可能少打一個 0：{fmt(value)} → {fmt(value*10)}；請自行確認，系統不會自動改價。'
    except Exception:
        pass
    return ''

def validate_trade(symbol,side,entry,sl,tp,notional):
    if not re.fullmatch(r'[A-Z0-9]{2,24}USDT',symbol):raise ValueError('交易對格式錯誤：只支援明確的 USDT 永續，例如 BTCUSDT')
    if side not in {'long','short'}:raise ValueError('方向錯誤：只能是 long 或 short')
    try:
        e=decimal(entry)
    except ValueError as exc:raise ValueError('限價進場格式錯誤：'+str(exc)) from None
    try:
        s=decimal(sl)
    except ValueError as exc:raise ValueError('全部止損 SL 格式錯誤：'+str(exc)) from None
    try:
        p=decimal(tp)
    except ValueError as exc:raise ValueError('全部止盈 TP 格式錯誤：'+str(exc)) from None
    try:
        n=decimal(notional)
    except ValueError as exc:raise ValueError('名目金額格式錯誤：'+str(exc)) from None
    if e<=0:raise ValueError(f'限價進場必須大於 0，目前是 {fmt(e)}')
    if s<=0:raise ValueError(f'全部止損 SL 必須大於 0，目前是 {fmt(s)}')
    if p<=0:raise ValueError(f'全部止盈 TP 必須大於 0，目前是 {fmt(p)}')
    if n<=0:raise ValueError(f'名目金額必須大於 0，目前是 {fmt(n)}')
    if side=='long':
        if s>=e:
            raise ValueError(f'LONG 的全部止損 SL 必須低於進場價：目前 SL={fmt(s)}、進場={fmt(e)}。'+_zero_hint(s,e,True))
        if p<=e:
            raise ValueError(f'LONG 的全部止盈 TP 必須高於進場價：目前 TP={fmt(p)}、進場={fmt(e)}。'+_zero_hint(p,e,False))
    else:
        if s<=e:
            raise ValueError(f'SHORT 的全部止損 SL 必須高於進場價：目前 SL={fmt(s)}、進場={fmt(e)}。'+_zero_hint(s,e,False))
        if p>=e:
            raise ValueError(f'SHORT 的全部止盈 TP 必須低於進場價：目前 TP={fmt(p)}、進場={fmt(e)}。'+_zero_hint(p,e,True))
    return e,s,p,n
