from abc import ABC, abstractmethod
import time
import asyncio
from typing import Dict, Any, Optional
from ..utils.log_manager import LogManager

class BaseExchange(ABC):
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.exchange = None
        self.name = ""
        self.ws = None
        self.session = None
        self.last_ws_message_time = 0  # 最后一次收到WS消息的时间
        self.logger = LogManager.get_logger(f"exchange.{self.name.lower()}")
    
    @abstractmethod
    async def initialize(self):
        """初始化交易所连接"""
        pass

    @abstractmethod
    async def connect_ws(self):
        """连接WebSocket"""
        pass

    @abstractmethod
    async def subscribe_ticker(self, symbol: str):
        """订阅价格数据"""
        pass

    @abstractmethod
    async def fetch_ticker(self, symbol: str):
        """通过HTTP获取当前价格"""
        pass

    async def close_connections(self):
        """关闭连接"""
        try:
            if self.ws:
                await self.ws.close()
                self.ws = None
            if self.session:
                await self.session.close()
                self.session = None
            if self.exchange:
                await self.exchange.close()
                self.exchange = None
        except Exception as e:
            self.logger.error(f"关闭连接时出错: {str(e)}")

    async def get_latency(self) -> Dict[str, float]:
        """同时测量HTTP和WebSocket延迟"""
        try:
            # 测量HTTP延迟
            start_time = time.time() * 1000
            await self.fetch_ticker("BTC/USDT")
            end_time = time.time() * 1000
            http_latency = end_time - start_time

            # 测量WebSocket延迟
            current_time = time.time() * 1000
            ws_latency = -1
            if self.last_ws_message_time > 0:
                ws_latency = current_time - self.last_ws_message_time
            
            return {
                "http_latency": http_latency,
                "ws_latency": ws_latency
            }
        except Exception as e:
            self.logger.error(f"{self.name} 延迟测量失败: {str(e)}")
            return {
                "http_latency": -1,
                "ws_latency": -1
            }
