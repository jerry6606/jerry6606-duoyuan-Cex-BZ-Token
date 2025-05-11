import decimal
import requests
import json
import time
from decimal import Decimal
from typing import Dict, List, Tuple
from collections import defaultdict


class ExchangeDataFetcher:
    def __init__(self):
        self.exchange_apis = {
            'OKX': {
                'url': 'https://www.okx.com/api/v5/market/tickers?instType=SPOT',
                'parser': self._parse_okx_data
            },
            'Binance': {
                'url': 'https://api.binance.com/api/v3/ticker/24hr',
                'parser': self._parse_binance_data
            },
            'Bitget': {
                'url': 'https://api.bitget.com/api/spot/v1/market/tickers?limit=5000',
                'parser': self._parse_bitget_data
            },
            'Gate': {
                'url': 'https://api.gateio.ws/api/v4/spot/tickers',
                'parser': self._parse_gate_data
            },
            'MEXC': {
                'url': 'https://api.mexc.com/api/v3/ticker/24hr',
                'parser': self._parse_mexc_data
            },
            'HTX': {
                'url': 'https://api.huobi.pro/market/tickers',
                'parser': self._parse_htx_data
            }
        }
        self.ticker_data = defaultdict(dict)
        self.request_timeout = 10  # 请求超时时间(秒)

    def fetch_all_data(self) -> Dict[str, Dict]:
        for exchange, config in self.exchange_apis.items():
            try:
                print(f"正在获取 {exchange} 数据...")
                response = requests.get(config['url'], timeout=self.request_timeout)
                response.raise_for_status()
                raw_data = response.json()

                # 调用解析器并获取处理数量
                processed_count = config['parser'](exchange, raw_data)  # 修改解析器返回计数
                print(f"{exchange} 数据获取成功，共处理 {processed_count} 个交易对")  # 直接使用解析器的统计

            except Exception as e:
                print(f"获取 {exchange} 数据失败: {str(e)}")

        return self.ticker_data
    #下面获取各交易所交易对
    def _parse_okx_data(self, exchange: str, data: Dict):
        processed_count = 0
        if data.get('code') != '0':
            raise ValueError(f"OKX API返回错误: {data.get('msg', '未知错误')}")

        tickers = data.get('data', [])
        if not isinstance(tickers, list):
            raise ValueError(f"OKX API返回数据格式错误: data字段不是列表")

        processed_count = 0
        for ticker in tickers:
            try:
                inst_id = ticker.get('instId', '')
                if not inst_id or '-' not in inst_id:
                    continue  # 跳过无效的交易对格式

                # 统一转换为 BTC/USDT 格式
                symbol = inst_id.replace('-', '/')

                # 检查必要字段是否存在
                ask_px = ticker.get('askPx')
                bid_px = ticker.get('bidPx')
                vol_24h = ticker.get('vol24h')
                if None in (ask_px, bid_px, vol_24h):
                    continue  # 跳过字段缺失的交易对

                self.ticker_data[symbol][exchange] = {
                    'buy': Decimal(str(ask_px)),
                    'sell': Decimal(str(bid_px)),
                    'volume': Decimal(str(vol_24h)),
                    'timestamp': int(time.time())
                }
                processed_count += 1

            except Exception as e:
                print(f"跳过OKX交易对 {ticker.get('instId')}（解析错误: {str(e)}）")
                continue

        return processed_count

    def _parse_binance_data(self, exchange: str, data: List[Dict]) -> int:  # 添加返回类型
        processed_count = 0  # 初始化计数器
        for ticker in data:
            symbol = ticker['symbol']
            if symbol.endswith('USDT'):
                base = symbol[:-4]
                quote = 'USDT'
            elif symbol.endswith('BTC'):
                base = symbol[:-3]
                quote = 'BTC'
            else:
                continue

            formatted_symbol = f"{base}/{quote}"
            self.ticker_data[formatted_symbol][exchange] = {
                'buy': Decimal(ticker['askPrice']),
                'sell': Decimal(ticker['bidPrice']),
                'volume': Decimal(ticker['quoteVolume']),
                'timestamp': int(time.time())
            }
            processed_count += 1  # 成功处理时递增
        return processed_count  # 返回总数

    def _parse_bitget_data(self, exchange: str, data: Dict) -> int:
        processed_count = 0
        try:
            if data.get('code') != '00000':
                raise ValueError(f"Bitget API返回错误: {data.get('msg', '未知错误')}")

            tickers = data.get('data', [])
            if not isinstance(tickers, list):
                raise ValueError("Bitget API返回数据格式错误: data字段不是列表")

            for ticker in tickers:
                try:
                    symbol = ticker.get('symbol', '')
                    if not symbol.endswith('USDT'):
                        continue

                    # 使用正确的字段名
                    buy_price = ticker.get('buyOne')  # 买一价
                    sell_price = ticker.get('sellOne')  # 卖一价
                    volume = ticker.get('usdtVol') or ticker.get('quoteVol')  # USDT交易量

                    if None in (buy_price, sell_price, volume):
                        print(
                            f"Bitget 交易对 {symbol} 缺少必要字段: buyOne={buy_price}, sellOne={sell_price}, usdtVol={volume}")
                        continue

                    try:
                        formatted_symbol = f"{symbol[:-4]}/USDT"
                        self.ticker_data[formatted_symbol][exchange] = {
                            'buy': Decimal(str(buy_price)),
                            'sell': Decimal(str(sell_price)),
                            'volume': Decimal(str(volume)),
                            'timestamp': int(time.time())
                        }
                        processed_count += 1
                    except decimal.InvalidOperation as e:
                        print(f"跳过Bitget交易对 {symbol}（数字格式无效: {buy_price}/{sell_price}/{volume}）")
                        continue

                except Exception as e:
                    print(f"处理Bitget交易对 {symbol} 时出错: {str(e)}")
                    continue

            return processed_count

        except Exception as e:
            print(f"解析Bitget数据时发生严重错误: {str(e)}")
            return 0

    def _parse_gate_data(self, exchange: str, data: List[Dict]) -> int:
        processed_count = 0
        if not isinstance(data, list):
            print("Gate API返回数据格式错误: 期望列表")
            return 0

        skipped_pairs = set()  # 记录跳过的交易对

        for ticker in data:
            symbol = ticker.get('currency_pair', '')
            try:
                if not symbol or not symbol.endswith('_USDT'):
                    continue

                if any(x in symbol for x in ['3L', '3S', '5L', '5S']):
                    continue

                buy_price = ticker.get('lowest_ask')
                sell_price = ticker.get('highest_bid')
                volume = ticker.get('quote_volume')

                # 检查字段是否有效
                if None in (buy_price, sell_price, volume):
                    skipped_pairs.add(symbol)
                    continue

                try:
                    formatted_symbol = f"{symbol[:-5]}/USDT"
                    self.ticker_data[formatted_symbol][exchange] = {
                        'buy': Decimal(str(buy_price)),
                        'sell': Decimal(str(sell_price)),
                        'volume': Decimal(str(volume)),
                        'timestamp': int(time.time())
                    }
                    processed_count += 1
                except decimal.InvalidOperation:
                    skipped_pairs.add(symbol)
                    continue

            except Exception as e:
                print(f"处理Gate交易对 {symbol} 时出错: {str(e)}")
                skipped_pairs.add(symbol)

        return processed_count

    def _parse_mexc_data(self, exchange: str, data: List[Dict]) -> int:
        processed_count = 0
        if not isinstance(data, list):
            print("MEXC API返回数据格式错误: 期望列表")
            return 0

        for ticker in data:
            try:
                symbol = ticker.get('symbol', '')
                if not symbol.endswith('USDT'):
                    continue

                formatted_symbol = f"{symbol[:-4]}/USDT"
                self.ticker_data[formatted_symbol][exchange] = {
                    'buy': Decimal(str(ticker['askPrice'])),
                    'sell': Decimal(str(ticker['bidPrice'])),
                    'volume': Decimal(str(ticker['quoteVolume'])),
                    'timestamp': int(time.time())
                }
                processed_count += 1
            except Exception as e:
                print(f"处理MEXC交易对 {symbol} 时出错: {str(e)}")
        return processed_count

    def _parse_htx_data(self, exchange: str, data: Dict) -> int:
        processed_count = 0
        if data.get('status') != 'ok':
            print(f"HTX API返回错误: {data.get('err-msg', '未知错误')}")
            return 0

        tickers = data.get('data', [])
        if isinstance(tickers, dict):
            tickers = tickers.get('tickers', [])

        for ticker in tickers:
            try:
                symbol = ticker.get('symbol', '').upper()
                if not symbol.endswith('USDT'):
                    continue

                formatted_symbol = f"{symbol[:-4]}/USDT"
                self.ticker_data[formatted_symbol][exchange] = {
                    'buy': Decimal(str(ticker['ask'])),
                    'sell': Decimal(str(ticker['bid'])),
                    'volume': Decimal(str(ticker['vol'])),
                    'timestamp': int(time.time())
                }
                processed_count += 1
            except Exception as e:
                print(f"处理HTX交易对 {symbol} 时出错: {str(e)}")
        return processed_count

    def save_to_file(self, filename: str = 'exchange_data.json'):
        """将数据保存到JSON文件"""
        # 将Decimal转换为字符串以便JSON序列化
        serializable_data = {}
        for symbol, exchanges in self.ticker_data.items():
            serializable_data[symbol] = {}
            for exchange, values in exchanges.items():
                serializable_data[symbol][exchange] = {
                    'buy': str(values['buy']),
                    'sell': str(values['sell']),
                    'volume': str(values['volume']),
                    'timestamp': values['timestamp']
                }

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(serializable_data, f, indent=2, ensure_ascii=False)

        print(f"数据已保存到 {filename}")


if __name__ == "__main__":
    fetcher = ExchangeDataFetcher()

    # 获取所有交易所数据
    ticker_data = fetcher.fetch_all_data()

    print("\n获取到的交易对示例:")

    # 保存数据到文件
    fetcher.save_to_file()
