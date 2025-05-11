import json
from decimal import Decimal


def load_market_data(file_path):
    """加载市场数据文件"""
    with open(file_path, 'r') as f:
        return json.load(f)


def find_arbitrage_opportunities(data, min_profit=0.0005, min_volume=10, max_spread_pct=50):
    """
    完整的套利机会检测函数
    :param data: 市场数据字典
    :param min_profit: 最小净利润阈值（USDT）
    :param min_volume: 最小成交量阈值（USDT）
    :param max_spread_pct: 最大允许价差百分比（防止异常数据）
    :return: 套利机会列表（按净利润降序）
    """
    opportunities = []

    for pair, exchanges in data.items():
        # 跳过非字典数据
        if not isinstance(exchanges, dict):
            continue

        markets = []
        for exchange, values in exchanges.items():
            try:
                # 数据转换和验证
                buy_price = Decimal(str(float(values['buy'])))  # 处理科学计数法
                sell_price = Decimal(str(float(values['sell'])))
                volume = Decimal(str(values.get('volume', 0)))

                # 数据有效性检查
                min_valid_price = Decimal('0.000001')
                if (volume <= Decimal(str(min_volume)) or
                        buy_price <= min_valid_price or
                        sell_price <= min_valid_price or
                        buy_price > sell_price * Decimal('100')  # 防止价格异常
                ):
                    continue

                markets.append({
                    'exchange': exchange,
                    'buy': buy_price,
                    'sell': sell_price,
                    'volume': volume
                })

            except (KeyError, TypeError, ValueError) as e:
                print(f"[警告] 数据解析跳过 {pair}@{exchange}: {str(e)}")
                continue

        # 需要至少两个有效交易所
        if len(markets) < 2:
            continue

        # 找出最佳买卖价
        best_buy = max(markets, key=lambda x: x['buy'])
        best_sell = min(markets, key=lambda x: x['sell'])

        # 排除同一交易所
        if best_buy['exchange'] == best_sell['exchange']:
            continue

        # 计算利润（考虑0.2%手续费）
        spread_pct = ((best_buy['buy'] - best_sell['sell']) / best_sell['sell']) * 100
        if spread_pct > max_spread_pct:  # 过滤异常价差
            continue

        fee = (best_buy['buy'] + best_sell['sell']) * Decimal('0.002')
        net_profit = (best_buy['buy'] - best_sell['sell']) - fee

        # 最终筛选
        if net_profit >= Decimal(str(min_profit)):
            opportunities.append({
                'pair': pair,
                'buy_at': best_sell['exchange'],
                'sell_at': best_buy['exchange'],
                'buy_price': float(best_sell['sell']),
                'sell_price': float(best_buy['buy']),
                'volume': float(min(best_sell['volume'], best_buy['volume'])),
                'net_profit': float(net_profit),
                'profit_pct': float((net_profit / best_sell['sell']) * 100),
                'spread_pct': float(spread_pct)
            })

    # 按净利润降序排序
    return sorted(opportunities, key=lambda x: (-x['net_profit'], -x['volume']))


if __name__ == "__main__":
    data = load_market_data("exchange_data.json")

    opportunities = find_arbitrage_opportunities(data)

    print(f"发现 {len(opportunities)} 个套利机会：\n")
    for opp in opportunities:
        print(
            f"交易对: {opp['pair']}\n"
            f"操作: 在 {opp['buy_at']} 以 {opp['buy_price']} 买入 | "
            f"在 {opp['sell_at']} 以 {opp['sell_price']} 卖出\n"
            f"成交量: {opp['volume']:.2f} | "
            f"净利润: {opp['net_profit']:.6f} USDT ({opp['profit_pct']:.2f}%)\n"
            f"----------------------------------"
        )
