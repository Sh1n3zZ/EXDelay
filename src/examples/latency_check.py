import asyncio
import yaml
from core.exchange_manager import ExchangeManager
from utils.log_manager import LogManager

async def check_latencies():
    logger = LogManager.get_logger("latency_checker")
    
    try:
        # 读取配置文件
        logger.info("开始读取配置文件")
        with open('config/config.yaml', 'r') as file:
            config = yaml.safe_load(file)
        
        manager = ExchangeManager()
        logger.info("初始化交易所连接")
        await manager.initialize_exchanges(config['exchanges'])
        
        # 检查延迟
        logger.info("开始延迟监控")
        while True:
            try:
                latencies = await manager.check_all_latencies()
                logger.info("=== 交易所延迟检测结果 ===")
                for exchange, latency in sorted(latencies.items(), key=lambda x: x[1]):
                    if latency == -1:
                        logger.error(f"{exchange}: 连接失败")
                    else:
                        logger.info(f"{exchange}: {latency:.2f}ms")
                await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"延迟检测过程出错: {str(e)}")
                await asyncio.sleep(5)
                
    except Exception as e:
        logger.critical(f"程序发生严重错误: {str(e)}")

if __name__ == "__main__":
    try:
        asyncio.run(check_latencies())
    except KeyboardInterrupt:
        LogManager.get_logger("latency_checker").info("程序已停止") 
