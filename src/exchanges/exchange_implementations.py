import ccxt.async_support as ccxt
import time
import asyncio
import aiohttp
import json
from typing import Dict, Any
from .base_exchange import BaseExchange

class BinanceExchange(BaseExchange):
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.name = "Binance"
        self.ws_url = "wss://stream.binance.com:9443/ws"
        
    async def initialize(self):
        self.exchange = ccxt.binance({
            'enableRateLimit': True,
            'options': {
                'defaultType': 'spot'
            }
        })
        await self.connect_ws()

    async def connect_ws(self):
        """连接WebSocket"""
        try:
            connector = aiohttp.TCPConnector(force_close=True)
            self.session = aiohttp.ClientSession(connector=connector)
            self.ws = await self.session.ws_connect(
                self.ws_url,
                timeout=30,
                heartbeat=30
            )
            await self.subscribe_ticker("BTC/USDT")
            asyncio.create_task(self._handle_ws_messages())
        except Exception as e:
            self.logger.error(f"WebSocket连接失败: {str(e)}")
            await self.close_connections()
            raise

    async def subscribe_ticker(self, symbol: str):
        """订阅价格数据"""
        symbol = symbol.lower().replace('/', '')
        subscribe_message = {
            "method": "SUBSCRIBE",
            "params": [f"{symbol}@ticker"],
            "id": 1
        }
        await self.ws.send_json(subscribe_message)

    async def _handle_ws_messages(self):
        """处理WebSocket消息"""
        try:
            async for msg in self.ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    if 'E' in data:  # 事件时间戳
                        self.last_ws_message_time = data['E']
        except Exception as e:
            self.logger.error(f"处理WebSocket消息时出错: {str(e)}")
            await self.close_connections()
            await asyncio.sleep(5)  # 等待5秒后重试
            await self.connect_ws()

    async def fetch_ticker(self, symbol: str):
        """通过HTTP获取当前价格"""
        return await self.exchange.fetch_ticker(symbol)

class CoinexExchange(BaseExchange):
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.name = "Coinex"
        self.ws_url = "wss://socket.coinex.com/"
        
    async def initialize(self):
        self.exchange = ccxt.coinex({
            'enableRateLimit': True,
            'options': {
                'defaultType': 'spot'
            }
        })
        await self.connect_ws()

    async def connect_ws(self):
        """连接WebSocket"""
        try:
            connector = aiohttp.TCPConnector(force_close=True)
            self.session = aiohttp.ClientSession(connector=connector)
            self.ws = await self.session.ws_connect(
                self.ws_url,
                timeout=30,
                heartbeat=30
            )
            await self.subscribe_ticker("BTC/USDT")
            asyncio.create_task(self._handle_ws_messages())
            # 启动心跳任务
            asyncio.create_task(self._heartbeat())
        except Exception as e:
            self.logger.error(f"WebSocket连接失败: {str(e)}")
            await self.close_connections()
            raise

    async def _heartbeat(self):
        """发送心跳包"""
        try:
            while True:
                if self.ws and not self.ws.closed:
                    await self.ws.send_json({
                        "method": "server.ping",
                        "params": [],
                        "id": int(time.time())
                    })
                await asyncio.sleep(15)  # 每15秒发送一次心跳
        except Exception as e:
            self.logger.error(f"心跳发送失败: {str(e)}")

    async def subscribe_ticker(self, symbol: str):
        """订阅价格数据"""
        symbol = symbol.replace('/', '').lower()
        subscribe_message = {
            "method": "state.subscribe",
            "params": [symbol],
            "id": int(time.time())
        }
        await self.ws.send_json(subscribe_message)

    async def _handle_ws_messages(self):
        """处理WebSocket消息"""
        try:
            async for msg in self.ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    if 'method' in data:
                        if data['method'] == 'state.update':
                            # 更新时间戳
                            self.last_ws_message_time = int(time.time() * 1000)
                        elif data['method'] == 'server.pong':
                            continue  # 忽略心跳响应
        except Exception as e:
            self.logger.error(f"处理WebSocket消息时出错: {str(e)}")
            await self.close_connections()
            await asyncio.sleep(5)  # 等待5秒后重试
            await self.connect_ws()

    async def fetch_ticker(self, symbol: str):
        """通过HTTP获取当前价格"""
        return await self.exchange.fetch_ticker(symbol)

class GateExchange(BaseExchange):
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.name = "Gate.io"
        self.ws_url = "wss://api.gateio.ws/ws/v4/"
        
    async def initialize(self):
        self.exchange = ccxt.gateio({
            'enableRateLimit': True,
            'options': {
                'defaultType': 'spot'
            }
        })
        await self.connect_ws()

    async def connect_ws(self):
        """连接WebSocket"""
        try:
            connector = aiohttp.TCPConnector(force_close=True)
            self.session = aiohttp.ClientSession(connector=connector)
            self.ws = await self.session.ws_connect(
                self.ws_url,
                timeout=30,
                heartbeat=30
            )
            await self.subscribe_ticker("BTC/USDT")
            asyncio.create_task(self._handle_ws_messages())
        except Exception as e:
            self.logger.error(f"WebSocket连接失败: {str(e)}")
            await self.close_connections()
            raise

    async def subscribe_ticker(self, symbol: str):
        """订阅价格数据"""
        symbol = symbol.replace('/', '_')
        subscribe_message = {
            "time": int(time.time()),
            "channel": "spot.tickers",
            "event": "subscribe",
            "payload": [symbol]
        }
        await self.ws.send_json(subscribe_message)

    async def _handle_ws_messages(self):
        """处理WebSocket消息"""
        try:
            async for msg in self.ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    if 'time' in data:
                        self.last_ws_message_time = data['time'] * 1000
        except Exception as e:
            self.logger.error(f"处理WebSocket消息时出错: {str(e)}")
            await self.close_connections()
            await asyncio.sleep(5)
            await self.connect_ws()

    async def fetch_ticker(self, symbol: str):
        """通过HTTP获取当前价格"""
        return await self.exchange.fetch_ticker(symbol)

class BitgetExchange(BaseExchange):
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.name = "Bitget"
        
    async def initialize(self):
        self.exchange = ccxt.bitget({
            'enableRateLimit': True,
            'options': {
                'defaultType': 'spot'
            }
        })

    async def connect_ws(self):
        pass

    async def subscribe_ticker(self, symbol: str):
        pass

    async def fetch_ticker(self, symbol: str):
        """获取当前价格"""
        return await self.exchange.fetch_ticker(symbol)

class OkxExchange(BaseExchange):
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.name = "OKX"
        self.ws_url = "wss://ws.okx.com:8443/ws/v5/public"
        
    async def initialize(self):
        self.exchange = ccxt.okx({
            'enableRateLimit': True,
            'options': {
                'defaultType': 'spot'
            },
            'timeout': 30000  # 增加超时时间到 30 秒
        })
        await self.connect_ws()

    async def connect_ws(self):
        """连接WebSocket"""
        try:
            connector = aiohttp.TCPConnector(force_close=True)
            self.session = aiohttp.ClientSession(connector=connector)
            self.ws = await self.session.ws_connect(
                self.ws_url,
                timeout=30,
                heartbeat=30
            )
            await self.subscribe_ticker("BTC/USDT")
            asyncio.create_task(self._handle_ws_messages())
        except Exception as e:
            self.logger.error(f"WebSocket连接失败: {str(e)}")
            await self.close_connections()
            raise

    async def subscribe_ticker(self, symbol: str):
        """订阅价格数据"""
        symbol = symbol.replace('/', '-')
        subscribe_message = {
            "op": "subscribe",
            "args": [{
                "channel": "tickers",
                "instId": f"{symbol}"
            }]
        }
        await self.ws.send_json(subscribe_message)

    async def _handle_ws_messages(self):
        """处理WebSocket消息"""
        try:
            async for msg in self.ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    if 'data' in data and len(data['data']) > 0:
                        if 'ts' in data['data'][0]:  # OKX 使用 ts 作为时间戳
                            self.last_ws_message_time = int(data['data'][0]['ts'])
        except Exception as e:
            self.logger.error(f"处理WebSocket消息时出错: {str(e)}")
            await self.close_connections()
            await asyncio.sleep(5)  # 等待5秒后重试
            await self.connect_ws()

    async def fetch_ticker(self, symbol: str, max_retries: int = 3) -> dict:
        """获取当前价格，带重试机制"""
        for attempt in range(max_retries):
            try:
                return await self.exchange.fetch_ticker(symbol)
            except Exception as e:
                self.logger.error(f"OKX 获取价格失败 (尝试 {attempt + 1}/{max_retries}): {str(e)}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(1)  # 等待1秒后重试
                else:
                    raise

class HuobiExchange(BaseExchange):
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.name = "Huobi"
        self.ws_url = "wss://api.huobi.pro/ws"
        
    async def initialize(self):
        self.exchange = ccxt.huobi({
            'enableRateLimit': True,
            'options': {
                'defaultType': 'spot'
            }
        })
        await self.connect_ws()

    async def connect_ws(self):
        """连接WebSocket"""
        try:
            connector = aiohttp.TCPConnector(force_close=True)
            self.session = aiohttp.ClientSession(connector=connector)
            self.ws = await self.session.ws_connect(
                self.ws_url,
                timeout=30,
                heartbeat=30
            )
            await self.subscribe_ticker("BTC/USDT")
            asyncio.create_task(self._handle_ws_messages())
        except Exception as e:
            self.logger.error(f"WebSocket连接失败: {str(e)}")
            await self.close_connections()
            raise

    async def subscribe_ticker(self, symbol: str):
        """订阅价格数据"""
        symbol = symbol.lower().replace('/', '')
        subscribe_message = {
            "sub": f"market.{symbol}.ticker",
            "id": "id1"
        }
        await self.ws.send_json(subscribe_message)

    async def _handle_ws_messages(self):
        """处理WebSocket消息"""
        try:
            async for msg in self.ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    if 'ts' in data:  # Huobi 使用 ts 作为时间戳
                        self.last_ws_message_time = data['ts']
        except Exception as e:
            self.logger.error(f"处理WebSocket消息时出错: {str(e)}")
            await self.close_connections()
            await asyncio.sleep(5)  # 等待5秒后重试
            await self.connect_ws()

    async def fetch_ticker(self, symbol: str):
        """通过HTTP获取当前价格"""
        return await self.exchange.fetch_ticker(symbol)

class BybitExchange(BaseExchange):
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.name = "Bybit"
        self.ws_url = "wss://stream.bybit.com/v5/public/spot"
        
    async def initialize(self):
        self.exchange = ccxt.bybit({
            'enableRateLimit': True,
            'options': {
                'defaultType': 'spot'
            }
        })
        await self.connect_ws()

    async def connect_ws(self):
        """连接WebSocket"""
        try:
            connector = aiohttp.TCPConnector(force_close=True)
            self.session = aiohttp.ClientSession(connector=connector)
            self.ws = await self.session.ws_connect(
                self.ws_url,
                timeout=30,
                heartbeat=30
            )
            await self.subscribe_ticker("BTC/USDT")
            asyncio.create_task(self._handle_ws_messages())
        except Exception as e:
            self.logger.error(f"WebSocket连接失败: {str(e)}")
            await self.close_connections()
            raise

    async def subscribe_ticker(self, symbol: str):
        """订阅价格数据"""
        symbol = symbol.replace('/', '')
        subscribe_message = {
            "op": "subscribe",
            "args": [f"tickers.{symbol}"]
        }
        await self.ws.send_json(subscribe_message)

    async def _handle_ws_messages(self):
        """处理WebSocket消息"""
        try:
            async for msg in self.ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    if 'ts' in data:
                        self.last_ws_message_time = data['ts']
        except Exception as e:
            self.logger.error(f"处理WebSocket消息时出错: {str(e)}")
            await self.close_connections()
            await asyncio.sleep(5)
            await self.connect_ws()

    async def fetch_ticker(self, symbol: str):
        """通过HTTP获取当前价格"""
        return await self.exchange.fetch_ticker(symbol)

class KucoinExchange(BaseExchange):
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.name = "KuCoin"
        self.ws_url = None  # 将在连接时获取
        self.token = None
        
    async def initialize(self):
        self.exchange = ccxt.kucoin({
            'enableRateLimit': True,
            'options': {
                'defaultType': 'spot'
            }
        })
        await self.connect_ws()

    async def get_ws_token(self):
        """获取KuCoin WebSocket token"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post('https://api.kucoin.com/api/v1/bullet-public') as response:
                    data = await response.json()
                    if data['code'] == '200000':
                        token = data['data']['token']
                        server = data['data']['instanceServers'][0]
                        self.ws_url = f"{server['endpoint']}?token={token}"
                        return True
        except Exception as e:
            self.logger.error(f"获取KuCoin WebSocket token失败: {str(e)}")
            return False

    async def connect_ws(self):
        """连接WebSocket"""
        try:
            if not self.ws_url and not await self.get_ws_token():
                raise Exception("无法获取WebSocket连接信息")

            connector = aiohttp.TCPConnector(force_close=True)
            self.session = aiohttp.ClientSession(connector=connector)
            self.ws = await self.session.ws_connect(
                self.ws_url,
                timeout=30,
                heartbeat=30
            )
            await self.subscribe_ticker("BTC/USDT")
            asyncio.create_task(self._handle_ws_messages())
        except Exception as e:
            self.logger.error(f"WebSocket连接失败: {str(e)}")
            await self.close_connections()
            raise

    async def subscribe_ticker(self, symbol: str):
        """订阅价格数据"""
        symbol = symbol.replace('/', '-')
        subscribe_message = {
            "type": "subscribe",
            "topic": f"/market/ticker:{symbol}",
            "privateChannel": False,
            "response": True
        }
        await self.ws.send_json(subscribe_message)

    async def _handle_ws_messages(self):
        """处理WebSocket消息"""
        try:
            async for msg in self.ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    if 'data' in data and 'time' in data['data']:
                        self.last_ws_message_time = int(data['data']['time'])
        except Exception as e:
            self.logger.error(f"处理WebSocket消息时出错: {str(e)}")
            await self.close_connections()
            await asyncio.sleep(5)
            await self.connect_ws()

    async def fetch_ticker(self, symbol: str):
        """通过HTTP获取当前价格"""
        return await self.exchange.fetch_ticker(symbol)
