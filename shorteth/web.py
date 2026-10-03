"""Local-first responsive control panel. No order POST before explicit web arming."""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import secrets
import sys
import time
import httpx
from collections import deque
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

from .exchange.bitget import Bitget, ExchangeError
from .exchange.store import StoreBridge
from .runner import make_runner
from .exchange.signals import decimal as D, fmt
from .execution import TradeIntent, oid, OrderBlocked
from .persistence import Journal, vault_key, save_credentials, load_credentials, atomic_write
from decimal import ROUND_CEILING

DATA = Path(os.environ.get('SHORTETH_DATA_DIR', str(Path.home() / '.shorteth'))).resolve()
DATA.mkdir(parents=True, exist_ok=True)
AUTH = DATA / 'auth.json'
STATIC = Path(__file__).parent / 'static'
PROJECT = Path(__file__).parent.parent
app = FastAPI(title='SHORTETH', docs_url=None, redoc_url=None)
runtime = {'mode':'demo','store':None,'bitget':None,'runner':None,'armed':False,
           'automatic':False,'task':None,'sessions':{},'last_result':None,
           'events':deque(maxlen=100),'lock':asyncio.Lock(),'vault_key':None,'scope':None,
           'history_task':None,'login_attempts':{},'scanner':{}}
ZH_ERRORS={
    'ETH_SHORT_ONLY':'此程式只允許 ETHUSDT 空單。',
    'DEDICATED_ACCOUNT_REQUIRED':'這套策略需要獨立交易帳戶；目前還有其他持倉或委託，未送新單。',
    'ENTRY_PRICE_OR_TIME_UNCONFIRMED':'尚未取得實際成交均價或進場小時，无法核對6%停損及168小時期限；請同步訂單。',
    'POSITION_ENTRY_TIME_MISSING':'交易所未提供可確認的持倉建立時間，無法接管168小時出場控制。',
    'POSITION_SIDE_MISMATCH':'交易所持倉方向不符空單，停止操作，避免影響多單。',
    'STRATEGY_NOTIONAL_CAP_EXCEEDED':'名目金額超過空頭策略20萬U上限。',
    'SHORT_STOP_MUST_BE_ABOVE_ENTRY':'空單止損價格必須高於進場價。',
    'SHORT_TP_MUST_BE_BELOW_ENTRY':'空單止盈價格必須低於進場價。',
    'SHORT_SL_NOT_ABOVE_MARK':'空單止損必須高於目前標記價。',
    'SHORT_TP_NOT_BELOW_MARK':'空單止盈必須低於目前標記價。',
    'TRADING_DISABLED_BEFORE_SEND':'送出前偵測到交易已關閉或帳戶已切換；本次未送單。',
    'TARGET_LEVERAGE_NOT_SUPPORTED':'交易所合約或這筆金額的倉位階梯不支援 150 倍槓桿；本次沒有下單。',
    'EXISTING_EXCHANGE_POSITION':'Bitget 已有 ETH 持倉；為避免重複開倉，本次沒有下單。',
    'EXISTING_EXCHANGE_ORDER':'Bitget 已有 ETH 委託；請先確認原單狀態。',
    'UNOWNED_EXCHANGE_PLAN':'Bitget 有未歸屬的止盈止損單；請先核對。',
    'INSUFFICIENT_AVAILABLE_MARGIN':'帳戶可用保證金不足；本次沒有下單。',
    'BELOW_EXCHANGE_MINIMUM':'依回測的 0.01 ETH 取整後，金額未達交易所最低下單量。',
    'BELOW_BACKTEST_0P01_ETH_STEP':'帳戶規模不足以按回測規則開出 0.01 ETH。',
    'MARKET_REFERENCE_PRICE_DRIFT':'報價在準備期間變動超過 0.5%；本次沒有下單。',
    'SIGNAL_HOUR_EXPIRED':'目前已不是剛完成的那根小時線；不回填過去價格下單。',
    'RECONCILIATION_REQUIRED':'前一筆委託尚未確認交易所狀態；暫停新單，避免重複下單。',
    'UNRESOLVED_EXCHANGE_STATE':'訂單或持倉狀態尚未確認；暫停新單。',
    'POSITION_EXCEEDS_TRACKED_FILL':'交易所持倉多於本程式確認的成交量；暫停自動操作。',
    'ORDER_DETAIL_MISSING_FILLED_QUANTITY':'交易所訂單明細沒有成交數量；無法安全確認。',
    'NEED_1442_COMPLETE_HOURLY_BARS':'歷史小時線不足 1442 根，暫停判斷。',
    'BINANCE_HOURLY_HISTORY_GAP':'Binance 歷史小時線有缺口；暫停判斷。',
    'BINANCE_HOURLY_HISTORY_INCOMPLETE':'Binance 歷史小時線不完整；暫停判斷。',
    'LATEST_COMPLETE_BINANCE_HOUR_UNAVAILABLE':'最新已完成小時線還取不到；稍後再試。',
    'NO_OWNED_ENTRY':'找不到本程式擁有的進場單；不會平掉未知持倉。',
    'ENTRY_NOT_CONFIRMED_OPEN':'進場單尚未確認成交與持倉，暫不平倉。',
    'POSITION_NOT_UNIQUE':'交易所有多筆或沒有可唯一辨識的 ETH 持倉；暫停操作。',
    'CLOSE_OID_ALREADY_USED':'這筆平倉指令已送出或結果待確認，不會重送。',
    'CLIENT_OID_ALREADY_USED':'這根小時線的開倉指令已存在，不會重複下單。',
    'ADOPTION_REQUIRES_ONEWAY_CROSS_150':'既有持倉不是單向、全倉、150 倍的組合，不能安全接管。',
    'POSITION_LEVERAGE_NOT_150':'既有持倉的實際槓桿不是 150 倍，不能接管。',
    'LOCAL_ENTRY_ALREADY_ACTIVE':'本程式已有未結束的進場單或持倉，不能重複接管。',
    'UNOWNED_EXCHANGE_PLAN':'交易所還有未知的止盈止損委託，請先處理再接管。',
    'WAITING_FOR_ENTRY_CANCEL':'正在等交易所確認未成交餘量已取消，暫不重送。',
    'PREVIOUS_CLOSE_NOT_CONFIRMED':'上一筆平倉尚未確認成交，暫不再送一筆。',
}

def human_error(exc):
    message=str(exc)
    for code,zh in ZH_ERRORS.items():
        if code in message:return zh
    if isinstance(exc,ExchangeError):
        prefix='交易所回應未確定，請核對原單，勿重送。' if exc.uncertain else 'Bitget 拒絕或無法完成操作。'
        return prefix+' 交易所代碼：'+str(exc.code)+'。原始回應：'+str(exc.message)
    if any(ord(char)>127 for char in message):return message
    return '操作未完成，請查看訂單與連線狀態。技術代碼：'+message[:160]


def present_result(value:dict):
    if not isinstance(value,dict):return value
    action=value.get('action')
    descriptions={'hold':'尚未觸發訊號解除、6%收盤確認停損或168小時到期；保持空單。',
                  'wait_reset':'上筆因停損或到期出場；等待原始空頭訊號解除後才能再進。',
                  'wait_buffer':'空頭狀態成立，但還沒低於 EMA720 至少 0.125%；繼續觀察。',
                  'flat':'訊號空手，目前沒有本程式持有的空單。',
                  'enter':'訊號要求開空；請查看下方委託與成交確認。',
                  'exit':'出場條件成立；以買入只減倉平空，請查看預覽／送出及成交狀態。',
                  'cancel_unfilled_remainder':'訊號反轉，先取消尚未成交的進場餘量。',
                  'blocked':ZH_ERRORS.get(value.get('reason'),
                       '條件未確認，這次沒有安全送出新單。')}
    return {'中文說明':descriptions.get(action,'決策已完成，請查看狀態。'),**value}


class Password(BaseModel):
    password: str
    setup_token: str = ''


class ChangePassword(BaseModel):
    current_password: str
    new_password: str


class Connect(BaseModel):
    mode: str
    account_type: str = 'auto'
    key: str
    secret: str
    passphrase: str
    save: bool = True


class Arm(BaseModel):
    phrase: str


class Toggle(BaseModel):
    enabled: bool


class Adopt(BaseModel):
    phrase: str


class DemoTest(BaseModel):
    phrase: str


class Manage(BaseModel):
    entry_oid: str
    plan_oid: str | None = None
    fraction: str = '1'
    kind: str | None = None
    price: str | None = None
    qty: str | None = None


def _local(request: Request):
    return request.client and request.client.host in {'127.0.0.1','::1'}


def _password_record(password: str):
    if len(password) < 12:raise HTTPException(400, '密碼至少 12 字元')
    salt = secrets.token_bytes(24)
    digest = hashlib.pbkdf2_hmac('sha256',password.encode(),salt,300_000)
    return {'salt':salt.hex(),'hash':digest.hex()}


def _check_password(password: str):
    if not AUTH.exists():return False
    record=json.loads(AUTH.read_text(encoding='utf-8'))
    digest=hashlib.pbkdf2_hmac('sha256',password.encode(),bytes.fromhex(record['salt']),300_000)
    return hmac.compare_digest(digest.hex(),record['hash'])


def _auth(request: Request):
    token=request.cookies.get('shorteth_session','')
    session=runtime['sessions'].get(token)
    if not session or session['expires'] < time.time():
        raise HTTPException(401,'請先登入')
    if request.method not in {'GET','HEAD'} and request.headers.get('x-csrf-token') != session['csrf']:
        raise HTTPException(403,'CSRF 驗證失敗')
    return session


def _event(message: str, **data):
    item={'at':time.time(),'message':message,'data':data}
    if runtime['store']:item=json.loads(runtime['store'].redact(json.dumps(item)))
    Journal(DATA).event(item)
    runtime['events'].appendleft(item)


def _connected():
    if runtime['bitget'] is None:raise HTTPException(409,'請先於網站連接 Bitget 帳戶')
    return runtime['bitget'],runtime['runner']


def _armed():
    if not runtime['armed']:raise HTTPException(409,'交易未啟用；請在網站輸入啟用確認文字')


@app.get('/')
async def home():
    return FileResponse(STATIC / 'index.html')


@app.get('/health')
async def health():
    return {'status':'ok','version':'1.0.0','program':'shorteth'}


@app.middleware('http')
async def security_headers(request, call_next):
    response=await call_next(request)
    response.headers['Cache-Control']='no-store'
    response.headers['X-Content-Type-Options']='nosniff'
    response.headers['X-Frame-Options']='DENY'
    response.headers['Referrer-Policy']='no-referrer'
    if os.environ.get('SHORTETH_CLOUD')=='1':
        response.headers['Strict-Transport-Security']='max-age=31536000'
    return response


@app.get('/daily.csv')
async def daily_csv(request:Request):
    _auth(request)
    return FileResponse(STATIC / 'short_1p95_daily.csv',media_type='text/csv',
                        filename='ETH空頭_1p95_歷史每日盈虧.csv')


@app.get('/api/bootstrap')
async def bootstrap(request:Request):
    return {'needs_setup':not AUTH.exists(),'local':bool(_local(request)),
            'setup_token_required':os.environ.get('SHORTETH_CLOUD')=='1',
            'authenticated':bool(request.cookies.get('shorteth_session') in runtime['sessions'])}


@app.get('/api/session')
async def session(request:Request):
    return {'csrf':_auth(request)['csrf']}


@app.post('/api/setup')
async def setup(request:Request,payload:Password):
    if AUTH.exists():raise HTTPException(403,'管理密碼已設定，請登入')
    if os.environ.get('SHORTETH_CLOUD')=='1':
        token=DATA/'setup-token'
        if not token.exists() or not hmac.compare_digest(token.read_text().strip(),payload.setup_token):
            raise HTTPException(403,'首次設定碼不正確；請使用部署完成時提供的設定碼')
    elif not _local(request):raise HTTPException(403,'首次設定只可在本機完成')
    record=_password_record(payload.password)
    fd=os.open(AUTH,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(fd,'w',encoding='utf-8') as file:json.dump(record,file)
    (DATA/'setup-token').unlink(missing_ok=True)
    return {'ok':True}


@app.post('/api/login')
async def login(request:Request,payload:Password):
    now=time.time(); attempts=runtime['login_attempts']
    recent=[x for x in attempts.get('global',[]) if now-x<60]
    if len(recent)>=10:raise HTTPException(429,'登入嘗試過於頻繁，請一分鐘後再試')
    attempts['global']=recent+[now]
    if not _check_password(payload.password):
        await asyncio.sleep(.5)
        raise HTTPException(401,'密碼錯誤')
    key=vault_key(payload.password,json.loads(AUTH.read_text())['salt'])
    async with runtime['lock']:
        runtime['vault_key']=key
        if runtime['runner'] is None:
            try:
                profile=load_credentials(DATA,key)
                if profile:await _connect_profile(Connect(**profile))
            except Exception:
                _event('已保存的連接資料解鎖失敗，請重新填入；交易保持關閉')
    token=secrets.token_urlsafe(32);csrf=secrets.token_urlsafe(24)
    runtime['sessions'][token]={'expires':time.time()+12*3600,'csrf':csrf}
    response=JSONResponse({'ok':True,'csrf':csrf})
    response.set_cookie('shorteth_session',token,httponly=True,samesite='strict',max_age=12*3600,
                        secure=os.environ.get('SHORTETH_CLOUD')=='1' or request.url.scheme=='https')
    return response


@app.post('/api/logout')
async def logout(request:Request):
    _auth(request)
    runtime['sessions'].pop(request.cookies.get('shorteth_session'),None)
    response=JSONResponse({'ok':True});response.delete_cookie('shorteth_session')
    return response


@app.get('/api/status')
async def status(request:Request):
    _auth(request)
    runner=runtime['runner']
    return {'mode':runtime['mode'],'connected':runner is not None,
            'armed':runtime['armed'],'automatic':runtime['automatic'],
            'last_result':present_result(runtime['last_result']) if runtime['last_result'] else None,
            'scanner':runtime['scanner'],
            'orders':runner.execution.ledger.rows() if runner else [],
            'events':Journal(DATA).events(),
            'credentials_saved':(DATA/'credentials.enc').exists(),
            'records':Journal(DATA).summary(runtime['scope']) if runtime['scope'] else None,
            'strategy':{'symbol':'ETHUSDT','signal':'60日動能低於-5%啟動；EMA720下方0.125%進空；動能回正或站回均線出場',
                        'allocation_pct':1.95,'leverage':150,'qty_floor_eth':'0.01',
                        'fixed_tp':None,'exchange_fixed_sl':None, 'close_confirmed_stop_pct':6,
                        'max_hold_hours':168,'notional_cap_usdt':200000,'reentry':'停損或到期後等原訊號解除',
                        'source':'Binance perpetual close; Bitget execution quote'}}


@app.post('/api/connect')
async def connect(request:Request,payload:Connect):
    _auth(request)
    if payload.mode not in {'demo','live'} or payload.account_type not in {'auto','classic','uta'}:
        raise HTTPException(400,'模式不正確')
    if not all([payload.key,payload.secret,payload.passphrase]):
        raise HTTPException(400,'請填完整 API 資料')
    async with runtime['lock']:
        if payload.save and not runtime['vault_key']:raise HTTPException(409,'請重新登入以解鎖密鑰保存')
        if payload.save:save_credentials(DATA,runtime['vault_key'],payload.model_dump())
        else:(DATA/'credentials.enc').unlink(missing_ok=True)
        await _connect_profile(payload)
        _event('帳戶已連接；'+('密鑰已加密保存' if payload.save else '僅本次使用')+'；交易仍關閉',mode=payload.mode)
    return {'ok':True,'mode':payload.mode,'armed':False}


async def _connect_profile(payload):
    runtime['automatic']=False;runtime['armed']=False
    if runtime['bitget']:await runtime['bitget'].close()
    prefix='DEMO' if payload.mode=='demo' else 'BITGET'
    env={f'SHORTETH_{prefix}_{key}':value for key,value in [
        ('KEY',payload.key),('SECRET',payload.secret),('PASSPHRASE',payload.passphrase)]}
    store=StoreBridge(payload.account_type,env);bitget=Bitget(store,payload.mode)
    scope=payload.mode+'-'+hashlib.sha256(payload.key.encode()).hexdigest()[:20]
    runtime.update(mode=payload.mode,store=store,bitget=bitget,scope=scope,
                   runner=make_runner(bitget,DATA/scope),last_result=None,
                   scanner={'scans':0,'missed_hours':0,'error':None})
    def guard():
        if not runtime['armed'] or runtime['bitget'] is not bitget or (
            getattr(runtime['runner'].execution,'automatic_call',False) and not runtime['automatic']):
            raise OrderBlocked('TRADING_DISABLED_BEFORE_SEND')
    runtime['runner'].execution.post_guard=guard
    if runtime['history_task'] is None or runtime['history_task'].done():
        runtime['history_task']=asyncio.create_task(_history_loop())
    if runtime['task'] is None or runtime['task'].done():
        runtime['task']=asyncio.create_task(_auto_loop())


async def _sync_history():
    bitget,runner=_connected();kind=await bitget.account_type();journal=Journal(DATA)
    # Reuse S300's read-only position history; never derive profit from deposits/equity.
    positions=await bitget.position_history('ETHUSDT',kind,100)
    for row in positions:
        identity=row.get('positionId')
        if identity is None:raise ValueError('歷史持倉缺少唯一編號，未保存不明紀錄')
        journal.put(runtime['scope'],'position',identity,row)
    if kind=='classic':
        for row in await bitget.recent_orders('ETHUSDT',kind,100):
            if row.get('orderId'):journal.put(runtime['scope'],'order',row['orderId'],row)
    snap=await bitget.account_snapshot('ETHUSDT',kind)
    snap['balance'].pop('raw',None)
    snap['observed_at_ms']=int(time.time()*1000)
    journal.put(runtime['scope'],'snapshot',snap['observed_at_ms'],snap)
    return journal.summary(runtime['scope'])


async def _history_loop():
    while True:
        await asyncio.sleep(60)
        if runtime['runner'] is None:continue
        try:
            async with runtime['lock']:await _sync_history()
        except Exception as exc:_event('損益紀錄同步失敗，已保留原紀錄',error=human_error(exc))


@app.post('/api/history-sync')
async def history_sync(request:Request):
    _auth(request)
    async with runtime['lock']:return await _sync_history()


@app.get('/api/records-export')
async def records_export(request:Request):
    _auth(request)
    data={'records':Journal(DATA).summary(runtime['scope']) if runtime['scope'] else {},
          'orders':runtime['runner'].execution.ledger.rows() if runtime['runner'] else [],
          'events':Journal(DATA).events()}
    return JSONResponse(data,headers={'Content-Disposition':'attachment; filename="shorteth-records.json"'})


@app.post('/api/credentials-forget')
async def credentials_forget(request:Request):
    _auth(request)
    async with runtime['lock']:
        runtime['armed']=False;runtime['automatic']=False
        if runtime['bitget']:await runtime['bitget'].close()
        runtime.update(bitget=None,runner=None,store=None,scope=None)
        (DATA/'credentials.enc').unlink(missing_ok=True)
        _event('已移除保存的密鑰並中斷連接；交易紀錄保留')
    return {'ok':True}


@app.get('/api/public-contract')
async def public_contract(request:Request):
    _auth(request)
    bitget=Bitget(StoreBridge(),'paper')
    try:
        instrument=await bitget.instrument('ETHUSDT','classic')
        tier=await bitget.tier('ETHUSDT',200,'classic')
        supported=min(int(instrument['max_leverage']),tier)>=150
        return {'中文說明':('目前公開商品與 200 U 名目階梯均顯示可用 150 倍；仍須以帳戶設定後回讀為準。'
                           if supported else '目前公開商品或 200 U 名目階梯不支援 150 倍，系統會擋單。'),
                'instrument':instrument,'tier_at_200_usdt':tier,
                'supports_150_at_200_usdt':supported}
    finally:await bitget.close()


@app.post('/api/self-test')
async def self_test(request:Request):
    _auth(request)
    proc=await asyncio.create_subprocess_exec(sys.executable,'-m','pytest','-q','tests',
        cwd=PROJECT,stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.STDOUT)
    try:output,_=await asyncio.wait_for(proc.communicate(),timeout=90)
    except TimeoutError:
        proc.kill();await proc.wait()
        raise HTTPException(504,'自動測試超時，沒有進行任何下單')
    return {'通過':proc.returncode==0,'中文說明':('本機模擬測試通過；尚非 Bitget 實際接單驗證。'
            if proc.returncode==0 else '本機模擬測試失敗；請勿啟用下單。'),
            '測試輸出':output.decode(errors='replace')[-6000:]}


@app.post('/api/replay-backtest')
async def replay_backtest(request:Request):
    _auth(request)
    proc=await asyncio.create_subprocess_exec(sys.executable,str(PROJECT/'research'/'replay_frozen.py'),
        cwd=PROJECT,stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.STDOUT)
    try:output,_=await asyncio.wait_for(proc.communicate(),timeout=60)
    except TimeoutError:
        proc.kill();await proc.wait()
        raise HTTPException(504,'凍結歷史重播超時；沒有進行任何下單')
    if proc.returncode:
        raise HTTPException(500,'凍結歷史重播未通過原結果核對，請勿將網站數字視為已驗證')
    return {'中文說明':'固定 1.95% 凍結歷史重播與原報告吻合；不是未來獲利預測。',
            '結果':json.loads(output)}


async def _minimum_intent(bitget):
    kind=await bitget.account_type()
    contract=await bitget.instrument('ETHUSDT',kind)
    quote=await bitget.ticker('ETHUSDT')
    price=D(quote['bid']);step=D(contract['step'])
    if price<=0 or step<=0:raise HTTPException(409,'交易所最低單規格或報價不正確')
    required=max(D(contract['min_qty']),D(contract['min_value'])/price,step)
    qty=(required/step).to_integral_value(rounding=ROUND_CEILING)*step
    return TradeIntent('ETHUSDT','short','market',fmt(qty*price),
        oid('minimum-test',secrets.token_hex(10)),int(time.time()*1000),entry=fmt(price),entry_hour_ms=int(time.time()*1000)//3600000*3600000, strategy_id='minimum-test')


@app.post('/api/minimum-preview')
async def minimum_preview(request:Request):
    _auth(request);bitget,runner=_connected()
    async with runtime['lock']:
        intent=await _minimum_intent(bitget)
        return {'中文說明':'只讀預覽，不送單。測試依交易所最小數量；不採策略 1.95% 部位尺寸。',
                'result':await runner.execution.preview(intent)}


@app.post('/api/demo-roundtrip')
async def demo_roundtrip(request:Request,payload:DemoTest):
    _auth(request)
    if runtime['mode']!='demo':raise HTTPException(409,'此入口只允許 Bitget 模擬帳戶')
    try:return await _minimum_roundtrip(request,payload,'執行模擬下單測試')
    finally:runtime['armed']=False;runtime['automatic']=False


@app.post('/api/minimum-roundtrip')
async def minimum_roundtrip(request:Request,payload:DemoTest):
    _auth(request)
    phrase='執行真實最小金額測試' if runtime['mode']=='live' else '執行模擬下單測試'
    try:return await _minimum_roundtrip(request,payload,phrase)
    finally:runtime['armed']=False;runtime['automatic']=False


async def _minimum_roundtrip(request,payload,expected):
    _auth(request);_armed()
    bitget,runner=_connected()
    if payload.phrase!=expected:raise HTTPException(400,'請輸入：'+expected)
    async with runtime['lock']:
        _armed();bitget,runner=_connected()
        current_phrase='執行真實最小金額測試' if runtime['mode']=='live' else '執行模擬下單測試'
        if expected!=current_phrase:raise HTTPException(409,'帳戶模式已變更，請重新確認測試')
        runtime['automatic']=False
        active=[x for x in runner.execution.ledger.rows() if x['kind'] in {'entry','close'}
                and x['state'] not in {'closed','filled','blocked','rejected','canceled','cancelled'}]
        if active:raise HTTPException(409,'尚有未結束或未確認的訂單，請先同步，不能重複測試')
        intent=await _minimum_intent(bitget)
        _event('使用者確認最小金額開平倉測試',mode=runtime['mode'],clientOid=intent.client_order_id)
        opened=await runner.execution.submit(intent,allow_post=True)
        for _ in range(12):
            if opened['state'] in {'open','partially_filled'}:break
            if opened['state'] in {'canceled','cancelled','blocked','needs_reconcile','position_missing_after_fill'}:
                return {'中文說明':'測試單未確認成為可管理持倉，請到訂單頁核對。','進場':opened}
            await asyncio.sleep(2)
            opened=await runner.execution.reconcile(intent.client_order_id)
        if opened['state']!='open':
            return {'中文說明':'測試進場尚未完全成交；未盲目送第二單，請到訂單頁取消或核對。',
                    '進場':opened}
        closed=await runner.execution.close(intent.client_order_id,int(time.time()*1000),
                                            allow_post=True)
        for _ in range(12):
            await asyncio.sleep(2)
            confirmed=await runner.execution.reconcile(closed['order']['id'])
            if confirmed['state']=='filled':
                entry_final=await runner.execution.reconcile(intent.client_order_id)
                if entry_final['state']!='closed':
                    return {'中文說明':'平倉委託已成交，但持倉尚未確認歸零；請立即核對，不算測試通過。',
                            '進場':opened,'平倉':confirmed,'最終持倉':entry_final}
                return {'中文說明':'最小單開倉、成交、只減倉平倉及交易所回讀已完成。',
                        '進場':opened,'平倉':confirmed,'最終持倉':entry_final}
        return {'中文說明':'測試平倉已送出但尚未確認完全成交，請立即到訂單頁核對。',
                '進場':opened,'平倉':closed}


@app.get('/api/diagnostics')
async def diagnostics(request:Request):
    _auth(request);bitget,_=_connected()
    return await bitget.diagnostics('ETHUSDT')


@app.get('/api/exchange-snapshot')
async def exchange_snapshot(request:Request):
    _auth(request);bitget,_=_connected()
    snapshot=await bitget.account_snapshot('ETHUSDT')
    snapshot['balance'].pop('raw',None)
    return snapshot


@app.post('/api/reconcile')
async def reconcile(request:Request):
    _auth(request);_,runner=_connected()
    output=[]
    async with runtime['lock']:
        for row in sorted(runner.execution.ledger.rows(),key=lambda x:0 if x['kind']=='close' else 1):
            if row['kind'] in {'entry','close'} and row['state'] not in {'blocked','rejected','canceled','cancelled','closed'} and not (row['kind']=='close' and row['state']=='filled'):
                try:output.append({'clientOid':row['oid'],'result':await runner.execution.reconcile(row['oid'])})
                except Exception as exc:output.append({'clientOid':row['oid'],'error':human_error(exc)})
    return {'results':output}


@app.post('/api/adopt')
async def adopt(request:Request,payload:Adopt):
    _auth(request);_connected()
    if payload.phrase!='接管既有 ETH 空單':
        raise HTTPException(400,'請輸入：接管既有 ETH 空單')
    async with runtime['lock']:
        runtime['automatic']=False
        value=await runtime['runner'].execution.adopt_position(allow_adopt=True)
        _event('已經核對並接管既有 ETH 空單；自動交易仍關閉',clientOid=value['order']['id'])
        return value


@app.get('/api/signal')
async def signal(request:Request):
    _auth(request);_,runner=_connected()
    data,now_ms=await runner.signal()
    return {'signal':data.__dict__,'observed_ms':now_ms,
            'lag_ms':now_ms-data.decided_at_ms}


@app.post('/api/preview')
async def preview(request:Request):
    _auth(request);_,runner=_connected()
    async with runtime['lock']:
        value=await runner.decide(allow_post=False)
        runtime['last_result']=value
        return present_result(value)


@app.post('/api/arm')
async def arm(request:Request,payload:Arm):
    _auth(request);_connected()
    expected='啟用模擬交易' if runtime['mode']=='demo' else '啟用真實交易'
    legacy='ENABLE DEMO TRADING' if runtime['mode']=='demo' else 'ENABLE LIVE TRADING'
    if payload.phrase not in {expected,legacy}:raise HTTPException(400,f'請輸入 {expected}')
    runtime['armed']=True
    _event('已在網站啟用下單',mode=runtime['mode'])
    return {'armed':True,'mode':runtime['mode']}


@app.post('/api/disarm')
async def disarm(request:Request):
    _auth(request);runtime['armed']=False;runtime['automatic']=False
    _event('已停止自動新單；既有持倉仍須管理')
    return {'armed':False,'automatic':False}


@app.post('/api/run')
async def run(request:Request):
    _auth(request);_armed();_,runner=_connected()
    async with runtime['lock']:
        _armed();_,runner=_connected()
        value=await runner.decide(allow_post=True)
        runtime['last_result']=value
        _event('手動執行一次策略決策',action=value.get('action'))
        return present_result(value)


async def _scan_once():
    async with runtime['lock']:
        if runtime['runner'] is None:return
        scan=runtime['scanner'];scan['started_at_ms']=int(time.time()*1000)
        scan['scans']=scan.get('scans',0)+1
        try:
            runner=runtime['runner']
            # Fetch signal before enabling any financial action; a feed outage is retriable.
            sig,now_ms=await runner.signal()
            previous=scan.get('decision_at_ms')
            missed=max(0,(sig.decided_at_ms-previous)//3_600_000-1) if previous else 0
            scan['missed_hours']=scan.get('missed_hours',0)+missed
            if missed:_event('偵測到漏掃小時；只處理目前訊號，不補下歷史訂單',hours=missed)
            scan['decision_at_ms']=sig.decided_at_ms
            scan['observation_lag_ms']=now_ms-sig.decided_at_ms
            # Recheck after awaits: an immediate stop may have arrived while fetching.
            enabled=bool(runtime['automatic'] and runtime['armed'])
            runner.execution.automatic_call=enabled
            try:value=await runner.decide(allow_post=enabled,signal=sig,now_ms=now_ms)
            finally:runner.execution.automatic_call=False
            runtime['last_result']=value
            scan.update(finished_at_ms=int(time.time()*1000),error=None,
                        execution_enabled=enabled,action=value.get('action'))
            if enabled and value.get('action') in {'enter','exit','blocked'}:
                _event('自動策略決策',action=value.get('action'),
                       reason=ZH_ERRORS.get(value.get('reason'),'已完成'))
        except Exception as exc:
            # Pure HTTP feed failures and failed exchange GETs can be retried.
            retry_read=isinstance(exc,httpx.HTTPError) or (
                isinstance(exc,ExchangeError) and not exc.uncertain and exc.code=='NETWORK')
            if not retry_read:runtime['automatic']=False
            message=human_error(exc)
            if scan.get('error')!=message:
                _event('掃描失敗；只讀網路錯誤稍後重試' if retry_read else
                       '掃描異常，自動下單已停用；持續只讀檢查',error=message)
            scan.update(error=message,failed_at_ms=int(time.time()*1000))


async def _auto_loop():
    while True:
        await asyncio.sleep(30)
        await _scan_once()


@app.post('/api/automatic')
async def automatic(request:Request,payload:Toggle):
    _auth(request)
    if payload.enabled:_armed();_connected()
    runtime['automatic']=payload.enabled
    if payload.enabled and (runtime['task'] is None or runtime['task'].done()):
        runtime['task']=asyncio.create_task(_auto_loop())
    _event('自動執行設定已更新',enabled=payload.enabled)
    return {'automatic':payload.enabled}


@app.post('/api/close')
async def close(request:Request,payload:Manage):
    _auth(request);_armed();_,runner=_connected()
    async with runtime['lock']:
        _armed();_,runner=_connected()
        result=await runner.execution.close(payload.entry_oid,int(time.time()*1000),
                                            fraction=payload.fraction,allow_post=True)
        runtime['automatic']=False
        _event('已送出只減倉平倉；等待交易所確認成交',clientOid=result['order']['id'])
        return result


@app.post('/api/cancel')
async def cancel(request:Request,payload:Manage):
    _auth(request);_armed();_,runner=_connected()
    async with runtime['lock']:
        _armed();_,runner=_connected()
        return await runner.execution.cancel(payload.entry_oid,int(time.time()*1000),allow_post=True)


@app.post('/api/modify')
async def modify(request:Request,payload:Manage):
    _auth(request);_armed();_,runner=_connected()
    if not payload.price or not payload.qty:raise HTTPException(400,'需填新價格及數量')
    async with runtime['lock']:
        _armed();_,runner=_connected()
        return await runner.execution.modify_limit(payload.entry_oid,int(time.time()*1000),
                                                   payload.price,payload.qty,allow_post=True)


@app.post('/api/protection')
async def protection(request:Request,payload:Manage):
    _auth(request);_armed();_,runner=_connected()
    if not payload.price or payload.kind not in {'sl','tp'}:raise HTTPException(400,'需填 SL/TP 與價格')
    async with runtime['lock']:
        _armed();_,runner=_connected()
        return await runner.execution.add_protection(payload.entry_oid,int(time.time()*1000),
                                                     payload.kind,payload.price,allow_post=True)


@app.post('/api/cancel-plan')
async def cancel_plan(request:Request,payload:Manage):
    _auth(request);_armed();_,runner=_connected()
    if not payload.plan_oid:raise HTTPException(400,'請填入本程式建立的止盈止損委託編號')
    async with runtime['lock']:
        _armed();_,runner=_connected()
        return await runner.execution.cancel_protection(payload.plan_oid,
                    int(time.time()*1000),allow_post=True)


@app.exception_handler(Exception)
async def errors(request:Request,exc:Exception):
    if isinstance(exc,HTTPException):
        return JSONResponse({'error':exc.detail},status_code=exc.status_code)
    message=human_error(exc)
    if runtime['store']:message=runtime['store'].redact(message)
    _event('操作失敗',error=message)
    return JSONResponse({'error':message},status_code=500)


@app.exception_handler(RequestValidationError)
async def validation_error(request:Request,exc:RequestValidationError):
    labels={'password':'密碼','mode':'交易模式','account_type':'帳戶類型',
            'key':'交易所金鑰','secret':'交易所密鑰','passphrase':'通行短語',
            'entry_oid':'進場委託編號','plan_oid':'止盈止損委託編號',
            'fraction':'平倉比例','price':'價格','qty':'數量','phrase':'確認文字'}
    missing=[]
    for item in exc.errors():
        field=str(item.get('loc',('欄位',))[-1]);missing.append(labels.get(field,field))
    return JSONResponse({'error':'表單資料不完整或格式錯誤：'+'、'.join(dict.fromkeys(missing))},
                        status_code=422)
