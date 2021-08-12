import aerospike
import asyncio
import sys
from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder
from aerospike_helpers.awaitable import io
from pydantic import BaseModel

app = FastAPI()
config = {
      'hosts': [ ('127.0.0.1', 3000) ]
}

#Item class to hold post data and store the same
class Value(BaseModel):
    name: str
    desc: str

try:
    client = aerospike.client(config).connect()
except:
    print("Unable to connect")
    sys.exit(1)

@app.post('/')
async def post_data(k: str, val: Value):
    key = ("test", "demo", k)
    policy = {
        "total_timeout": 1000
    }
    v = jsonable_encoder(val)
    print("Value :::" , v)
    results = await io.put(client, key, v, None, policy)
    return results 

@app.get('/{k}')
async def get_data(k: str):
    key = ("test", "demo", k)
    key, meta, bin = await io.get(client, key)
    return bin 
