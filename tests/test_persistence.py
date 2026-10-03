import json
import pytest
from cryptography.fernet import InvalidToken
from shorteth.persistence import Journal, vault_key, save_credentials, load_credentials
from shorteth import web
from test_web import client, logged_in


def test_vault_encrypted_and_wrong_password_fails(tmp_path):
    key=vault_key('correct password', 'ab'*24)
    profile={'key':'private-api','secret':'top-secret','passphrase':'private-passphrase'}
    save_credentials(tmp_path,key,profile)
    assert b'top-secret' not in (tmp_path/'credentials.enc').read_bytes()
    assert load_credentials(tmp_path,key)==profile
    with pytest.raises(InvalidToken):load_credentials(tmp_path,vault_key('wrong password','ab'*24))


def test_restart_login_restores_encrypted_connection_but_not_arming(client):
    headers=logged_in(client)
    body={'mode':'live','key':'api-key-value','secret':'api-secret-value','passphrase':'api-pass-value'}
    assert client.post('/api/connect',json=body,headers=headers).status_code==200
    scope=web.runtime['scope']
    web.runtime.update(bitget=None,runner=None,store=None,vault_key=None,armed=False,automatic=False)
    assert client.post('/api/login',json={'password':'long-testing-pass-123'}).status_code==200
    assert web.runtime['scope']==scope and web.runtime['runner'] is not None
    status=client.get('/api/status')
    assert not status.json()['armed'] and not status.json()['automatic']
    assert 'api-secret-value' not in status.text
    assert 'api-secret-value' not in client.get('/api/records-export').text


def test_cloud_setup_rejects_local_spoof_and_uses_token(client,monkeypatch):
    monkeypatch.setenv('SHORTETH_CLOUD','1')
    (web.DATA/'setup-token').write_text('unpredictable-test-bootstrap')
    assert client.post('/api/setup',json={'password':'long-testing-pass-123'}).status_code==403
    assert client.post('/api/setup',json={'password':'long-testing-pass-123',
        'setup_token':'unpredictable-test-bootstrap'}).status_code==200
    assert not (web.DATA/'setup-token').exists()
    result=client.post('/api/login',json={'password':'long-testing-pass-123'})
    assert 'Secure' in result.headers['set-cookie']
    record=json.loads(web.AUTH.read_text())
    assert 'long-testing-pass-123' not in web.AUTH.read_text() and record['hash']


def test_journal_deduplicates_and_does_not_turn_missing_profit_into_zero(tmp_path):
    j=Journal(tmp_path)
    for _ in range(2):j.put('live-one','position','1',{'positionId':'1','netProfit':'-2.5'})
    j.put('live-one','position','2',{'positionId':'2'})
    j.put('live-two','position','1',{'positionId':'1','netProfit':'500'})
    j.event({'message':'persistent'})
    restored=Journal(tmp_path);s=restored.summary('live-one')
    assert s['已保存平倉紀錄']==2 and s['已知平倉淨利合計U']=='-2.5'
    assert s['缺少淨利的紀錄數']==1 and restored.events()[0]['message']=='persistent'


@pytest.mark.asyncio
async def test_minimum_test_size_uses_notional_and_step_without_strategy_floor():
    class Exchange:
        async def account_type(self):return 'classic'
        async def instrument(self,*a):return {'min_qty':'0.001','step':'0.001','min_value':'5'}
        async def ticker(self,*a):return {'ask':'2000','bid':'2000'}
    intent=await web._minimum_intent(Exchange())
    assert web.D(intent.notional_usdt)==web.D('6')  # 0.003 ETH, the smallest legal step above 5 USDT
    assert intent.leverage==150


def test_live_minimum_needs_separate_phrase_and_disarms(client):
    headers=logged_in(client)
    body={'mode':'live','key':'k','secret':'s','passphrase':'p'}
    client.post('/api/connect',json=body,headers=headers)
    web.runtime['armed']=True
    r=client.post('/api/minimum-roundtrip',json={'phrase':'執行模擬下單測試'},headers=headers)
    assert r.status_code==400 and not web.runtime['armed']
