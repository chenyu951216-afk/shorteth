import pytest
from fastapi.testclient import TestClient
from shorteth import web
from shorteth.runner import make_runner


@pytest.fixture
def client(tmp_path,monkeypatch):
    monkeypatch.delenv('SHORTETH_CLOUD',raising=False)
    monkeypatch.setattr(web,'AUTH',tmp_path/'auth.json')
    monkeypatch.setattr(web,'DATA',tmp_path)
    monkeypatch.setattr(web,'_local',lambda request:True)
    web.runtime.update(mode='demo',store=None,bitget=None,runner=None,
                       armed=False,automatic=False,last_result=None,sessions={},vault_key=None,
                       scope=None,history_task=None,login_attempts={},task=None,scanner={})
    with TestClient(web.app) as c:yield c


def logged_in(client):
    assert client.post('/api/setup',json={'password':'long-testing-pass-123'}).status_code==200
    result=client.post('/api/login',json={'password':'long-testing-pass-123'})
    assert result.status_code==200
    return {'x-csrf-token':result.json()['csrf']}


def test_site_home_and_first_run_setup(client):
    assert client.get('/').status_code==200
    assert client.get('/api/bootstrap').json()['needs_setup']
    assert client.get('/api/status').status_code==401
    headers=logged_in(client)
    assert client.get('/api/status').json()['armed'] is False
    assert client.get('/api/session').json()['csrf']==headers['x-csrf-token']


def test_all_trade_endpoints_are_closed_by_default(client):
    headers=logged_in(client)
    for endpoint in ('/api/run','/api/close','/api/cancel','/api/modify','/api/protection'):
        assert client.post(endpoint,json={},headers=headers).status_code in {409,422}


def test_csrf_blocks_connection_and_live_arming(client):
    headers=logged_in(client)
    body={'mode':'live','account_type':'classic','key':'k','secret':'s','passphrase':'p'}
    assert client.post('/api/connect',json=body).status_code==403
    assert client.post('/api/connect',json=body,headers=headers).status_code==200
    assert not web.runtime['armed']
    assert client.post('/api/arm',json={'phrase':'ENABLE DEMO TRADING'},headers=headers).status_code==400
    assert client.post('/api/arm',json={'phrase':'ENABLE LIVE TRADING'},headers=headers).status_code==200
    assert web.runtime['armed']
    assert client.post('/api/demo-roundtrip',json={'phrase':'執行模擬下單測試'},
                       headers=headers).status_code==409
    assert client.post('/api/disarm',headers=headers).json()['armed'] is False


def test_daily_report_requires_login(client):
    assert client.get('/daily.csv').status_code==401
    logged_in(client)
    report=client.get('/daily.csv')
    assert report.status_code==200
    assert '當日交易盈虧' in report.text


def test_form_error_is_plain_chinese(client):
    headers=logged_in(client)
    response=client.post('/api/connect',json={'mode':'demo'},headers=headers)
    assert response.status_code==422
    assert '表單資料不完整' in response.json()['error']
    assert '交易所金鑰' in response.json()['error']


def test_demo_roundtrip_uses_only_demo_and_confirms_flat(client,tmp_path):
    headers=logged_in(client)
    class DemoExchange:
        mode='demo'
        def __init__(self):self.qty='0';self.calls=[]
        async def account_type(self):return 'classic'
        async def instrument(self,*args):return {'step':'0.01','tick':'0.01','min_qty':'0.01',
             'min_value':'5','max_qty':'100','max_leverage':'150','status':'normal'}
        async def ticker(self,*args):return {'ask':'2000','bid':'1999.99','mark':'2000','last':'2000','ts':1}
        async def tier(self,*args):return 150
        async def account_snapshot(self,*args):return {'balance':{'accountEquity':'100',
            'available':'100','crossedMaxAvailable':'100'},'account':{},
            'positions':[],'orders':[],'plans':[]}
        async def prepare(self,o):self.calls.append('prepare')
        async def place(self,o):self.calls.append('place');self.qty=o['qty'];return {'orderId':'o1','clientOid':o['id']}
        async def detail(self,o):
            if o.get('entry_oid'):return {'orderStatus':'filled','baseVolume':o['qty'],'orderId':'c1'}
            return {'orderStatus':'filled','baseVolume':o['qty'],'orderId':'o1','priceAvg':'2000'}
        async def positions(self,*args):
            return [{'symbol':'ETHUSDT','holdSide':'short','total':self.qty}] if self.qty!='0' else []
        async def plans(self,*args):return []
        async def reduce(self,o,qty,cid):
            self.calls.append('reduce');self.qty='0';return {'orderId':'c1','clientOid':cid}
    exchange=DemoExchange()
    web.runtime.update(mode='demo',bitget=exchange,runner=make_runner(exchange,tmp_path),
                       armed=True,automatic=False)
    response=client.post('/api/demo-roundtrip',json={'phrase':'執行模擬下單測試'},headers=headers)
    assert response.status_code==200
    assert exchange.calls==['prepare','place','reduce']
    assert response.json()['最終持倉']['state']=='closed'
