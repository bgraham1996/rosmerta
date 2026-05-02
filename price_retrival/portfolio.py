"""
Portfolio Fetcher — Interactive Brokers Position Viewer
=======================================================
Read-only retrieval of current IB portfolio positions.
No database interaction — output only.
"""

import logging
from ib_insync import IB, Stock


logger = logging.getLogger(__name__)


def fetch_portfolio(host='127.0.0.1', port=4001, client_id=10):
    """
    Connect to IB Gateway/TWS and retrieve all portfolio positions.

    Uses a distinct client_id (default 10) to avoid conflicts with
    the price fetcher's connection.

    Returns:
        list[dict]: Portfolio positions with fields:
            symbol, exchange, currency, position, avg_cost,
            market_price, market_value, unrealised_pnl, realised_pnl
        Returns empty list on connection failure or no positions.
    """
    ib = IB()

    try:
        ib.connect(host, port, clientId=client_id, timeout=10)
        logger.info(f"Connected to IB on {host}:{port}")
        ib.sleep(1)  # Allow position data to populate
    except Exception as e:
        logger.error(f"Failed to connect to IB: {e}")
        return []

    try:
        positions = ib.portfolio()

        results = []
        for item in positions:
            contract = item.contract
            results.append({
                'symbol': contract.symbol,
                'sec_type': contract.secType,
                'exchange': contract.exchange or contract.primaryExchange,
                'currency': contract.currency,
                'position': item.position,
                'avg_cost': item.averageCost,
                'market_price': item.marketPrice,
                'market_value': item.marketValue,
                'unrealised_pnl': item.unrealizedPNL,
                'realised_pnl': item.realizedPNL,
            })

        logger.info(f"Retrieved {len(results)} portfolio positions")
        return results

    except Exception as e:
        logger.error(f"Error fetching portfolio: {e}")
        return []

    finally:
        try:
            ib.disconnect()
            logger.info("Disconnected from IB")
        except Exception:
            pass
