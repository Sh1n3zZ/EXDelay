import asyncio
from src.utils.config_loader import ConfigLoader
from src.utils.log_manager import LogManager
from src.core.exchange_manager import ExchangeManager
import signal
import sys
import time

class Application:
    def __init__(self):
        print("正在初始化应用...")
        self.logger = LogManager.get_logger("main")
        self.exchange_manager = None
        self.running = True
        self._task = None
    
    async def initialize(self):
        try:
            print("开始加载配置...")
            self.logger.info("正在初始化应用...")
            config = ConfigLoader.load_config()
            
            self.exchange_manager = ExchangeManager(config)
            await self.exchange_manager.initialize_exchanges(config['exchanges'])
            self.logger.info("应用初始化完成")
            return True
            
        except Exception as e:
            print(f"初始化过程中出现错误: {str(e)}")
            self.logger.error(f"应用初始化过程中出现错误: {str(e)}")
            return False
    
    def signal_handler(self, signum, frame):
        """处理退出信号"""
        if self._task:
            self._task.cancel()
        self.running = False
        self.logger.info("接收到停止信号，准备关闭应用...")
    
    async def monitor_latency(self):
        """监控延迟的主循环"""
        try:
            while self.running:
                try:
                    latencies = await self.exchange_manager.check_all_latencies()
                    
                    # 清屏并重新打印
                    print("\033[H\033[J")  # 清屏
                    print("="*50)
                    print(f"当前时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
                    print("="*50)
                    
                    for exchange, delays in latencies.items():
                        # 日志文件记录
                        self.logger.info(
                            f"{exchange} - HTTP延迟: {delays['http_latency']:.2f}ms, "
                            f"WebSocket延迟: {delays['ws_latency']:.2f}ms"
                        )
                        
                        # 控制台打印，带颜色
                        http_latency = delays['http_latency']
                        ws_latency = delays['ws_latency']
                        
                        # 根据HTTP延迟值设置颜色
                        if http_latency < 500:
                            color = "\033[92m"  # 绿色
                        elif http_latency < 1000:
                            color = "\033[93m"  # 黄色
                        else:
                            color = "\033[91m"  # 红色
                            
                        print(f"{color}{exchange}:")
                        print(f"  HTTP延迟: {http_latency:.2f}ms")
                        if ws_latency > 0:
                            print(f"  WebSocket延迟: {ws_latency:.2f}ms")
                        print("\033[0m")  # 重置颜色
                    
                    await asyncio.sleep(1)  # 每秒更新一次
                    
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self.logger.error(f"监控过程发生错误: {str(e)}")
                    print(f"\033[91m错误: {str(e)}\033[0m")
                    await asyncio.sleep(5)
                    
        except asyncio.CancelledError:
            self.logger.info("监控任务被取消")
        finally:
            self.running = False
    
    async def run(self):
        try:
            init_success = await self.initialize()
            if not init_success:
                self.logger.warning("部分初始化失败,但将继续运行可用的交易所")
            
            if not self.exchange_manager or not self.exchange_manager.exchanges:
                self.logger.error("没有可用的交易所,程序退出")
                return
            
            self.logger.info("开始监控交易所延迟...")
            print("\n" + "="*50)
            print("开始持续监控交易所延迟...")
            print("="*50)
            
            self._task = asyncio.create_task(self.monitor_latency())
            await self._task
            
        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"运行时错误: {str(e)}")
            self.logger.critical(f"应用运行时发生严重错误: {str(e)}")
        finally:
            self.logger.info("应用正在关闭...")
            if self.exchange_manager:
                await self.exchange_manager.close_all()
            
async def main():
    app = Application()
    try:
        # 注册信号处理
        signal.signal(signal.SIGINT, app.signal_handler)
        signal.signal(signal.SIGTERM, app.signal_handler)
        
        await app.run()
    except KeyboardInterrupt:
        print("\n程序被用户中断")
    finally:
        if app.exchange_manager:
            await app.exchange_manager.close_all()

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
