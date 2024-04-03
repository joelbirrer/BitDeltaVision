import asyncio
import aiohttp

from influxdb_client import Point
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from influxdb_client.client.exceptions import InfluxDBError

from dotenv import dotenv_values


async def fetch(session, url, api_key=""):
    headers = dict()
    if api_key:
        headers.update({
            "Accept-Encoding": "deflate",
            "Authorization": f"Bearer {api_key}"
        })

    async with session.get(url, headers=headers) as response:
        response_status = response.status
        response_content_type = response.headers.get("content-type")
        if response_status != 200:
            raise aiohttp.ClientError(f'Response status is not 200. Status: {response_status}.')
        elif "application/json" not in response_content_type:
            raise aiohttp.ClientError(f"Response Content-Type does not match.\n Expected: application/json. Received: {response_content_type}. Response: {await response.text()}.")
        return await response.json()

async def write_influx_db(influxdb_client, bucket, record, write_precision="s"):
        write_api = influxdb_client.write_api()
        ok_response = await write_api.write(bucket=bucket, record=record, write_precision=write_precision)
        
        if not ok_response:
            raise InfluxDBError(f"An error occurred during writing: {record}")


async def main():
    SLEEP_INTERVAL = 5 # seconds
    CONFIG = dotenv_values("bitdeltavision.env")
    
    COINCAP_BASE_API_URL = "https://api.coincap.io/v2/markets?exchangeId={}&baseSymbol={}&quoteSymbol={}" # https://docs.coincap.io/
    COINCAP_API_KEY = CONFIG.get("COINCAP_API_KEY")

    exchange_api_endpoints = {
        "gdax": {"name": "Coinbase Pro", "base_url": COINCAP_BASE_API_URL, "api_key": COINCAP_API_KEY, "pairs": ["BTC/EUR", "BTC/USD"]},
        "bitfinex": {"name": "Bitfinex", "base_url": COINCAP_BASE_API_URL, "api_key": COINCAP_API_KEY, "pairs": ["BTC/EUR", "BTC/USD"]},
        "kraken": {"name": "Kraken", "base_url": COINCAP_BASE_API_URL, "api_key": COINCAP_API_KEY, "pairs": ["BTC/EUR", "BTC/USD"]}
    }

    while True:
        try:
            async with aiohttp.ClientSession() as session:
                tasks = list()
                for exch, cred in exchange_api_endpoints.items():
                    base_url = cred.get("base_url")
                    api_key = cred.get("api_key")
                    pairs = cred.get("pairs", [])

                    for p in pairs:
                        if len(p.split("/")) != 2:
                            continue

                        crypto_symbol, currency_symbol = p.split("/")
                        url = base_url.format(exch, crypto_symbol.lower(), currency_symbol.lower())
                        tasks.append(fetch(session, url, api_key))

                responses = await asyncio.gather(*tasks)

            influxdb_url = CONFIG.get("INFLUXDB_URL")
            influxdb_token = CONFIG.get("INFLUXDB_TOKEN")
            influxdb_org = CONFIG.get("INFLUXDB_ORG")
            influxdb_bucket = CONFIG.get("INFLUXDB_BUCKET")
                
            async with InfluxDBClientAsync(url=influxdb_url, token=influxdb_token, org=influxdb_org) as influxdb_client:
                tasks = list()
                for r in responses:
                    for data in r.get("data", []):
                        exchangeId = data.get("exchangeId")
                        crypto_symbol = data.get("baseSymbol")
                        currency_symbol = data.get("quoteSymbol")
                        
                        exchange = exchange_api_endpoints.get(exchangeId).get("name")
                        price = float(data.get("priceQuote"))

                        price_point = Point(crypto_symbol).tag("Exchange", exchange).tag("Currency", currency_symbol).field("Price", price)
                        tasks.append(write_influx_db(influxdb_client, influxdb_bucket, price_point))
                
                await asyncio.gather(*tasks)

        except Exception as e:
            print(f"Exception: {e}")

        await asyncio.sleep(SLEEP_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())