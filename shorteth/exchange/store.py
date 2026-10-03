import json,os

def dumps(obj):return json.dumps(obj,ensure_ascii=False,separators=(',',':'),default=str)
class StoreBridge:
    def __init__(self,account_type='auto',env=None):self.account_type=account_type;self.env=env or os.environ
    def get(self,key,default=None):
        if key=='account_type':return self.account_type
        return self.env.get('SHORTETH_'+key.upper(),default)
    def redact(self,value):
        text=str(value)
        for key in ('BITGET_KEY','BITGET_SECRET','BITGET_PASSPHRASE','DEMO_KEY','DEMO_SECRET','DEMO_PASSPHRASE'):
            secret=self.env.get('SHORTETH_'+key)
            if secret:text=text.replace(secret,'[REDACTED]')
        return text
    def event(self,*args,**kwargs):pass
