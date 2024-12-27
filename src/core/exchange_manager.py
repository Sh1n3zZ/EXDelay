from typing import Dict, Any
from ..exchanges.base_exchange import BaseExchange
from ..exchanges.exchange_implementations import (
    BinanceExchange,
    CoinexExchange,
    GateExchange,
    BitgetExchange,
    OkxExchange,
    HuobiExchange,
    BybitExchange,
    KucoinExchange
)
from ..utils.log_manager import LogManager

class ExchangeManager:
    def __init__(self, config: Dict[str, Any]):
        self.exchanges = {}
        self.logger = LogManager.get_logger("exchange_manager")
        
    async def initialize_exchanges(self, exchange_configs: Dict[str, Any]):
        for name, config in exchange_configs.items():
            try:
                exchange_class = self.get_exchange_class(name)
                if exchange_class:
                    exchange = exchange_class(config)
                    await exchange.initialize()
                    self.exchanges[name] = exchange
                    self.logger.info(f"交易所 {name} 初始化成功")
            except Exception as e:
                self.logger.error(f"交易所 {name} 初始化失败: {str(e)}")
                continue  # 跳过失败的交易所,继续初始化下一个
    
    async def check_all_latencies(self) -> Dict[str, Dict[str, float]]:
        """检查所有交易所的延迟"""
        results = {}
        for name, exchange in self.exchanges.items():
            latencies = await exchange.get_latency()
            results[name] = latencies
        return results

    async def close_all(self):
        """关闭所有交易所连接"""
        for name, exchange in self.exchanges.items():
            try:
                await exchange.close_connections()
                self.logger.info(f"已关闭交易所 {name} 连接")
            except Exception as e:
                self.logger.error(f"关闭交易所 {name} 连接失败: {str(e)}") 

    def get_exchange_class(self, name: str) -> BaseExchange:
        exchange_classes = {
            'binance': BinanceExchange,
            'coinex': CoinexExchange,
            'gate': GateExchange,
            'bitget': BitgetExchange,
            'okx': OkxExchange,
            'huobi': HuobiExchange,
            'bybit': BybitExchange,
            'kucoin': KucoinExchange
        }
        return exchange_classes.get(name)
