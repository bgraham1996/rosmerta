"""
Rosmerta — Stock Analysis CLI
"""

import click
from rich.console import Console
from rich.table import Table
import os
from dotenv import load_dotenv

load_dotenv()
postgres_password = os.getenv('postgres_password')


console = Console()


@click.group()
@click.version_option(version='0.1.0', prog_name='rosmerta')
def cli():
    """Rosmerta — stock analysis and position management."""
    pass


@cli.command()
@click.argument('ticker')
@click.option('--start', '-s', required=True, help='Start date (YYYY-MM-DD)')
@click.option('--end', '-e', required=True, help='End date (YYYY-MM-DD)')
@click.option('--exchange', default='SMART', help='IB exchange routing (default: SMART)')
@click.option('--currency', default='USD', help='Contract currency (default: USD)')
@click.option('--no-db', is_flag=True, help='Skip database storage')
def fetch(ticker, start, end, exchange, currency, no_db):
    """Fetch hourly price bars for a stock.

    Example: rosmerta fetch AAPL --start 2024-01-01 --end 2024-03-31
    """
    from price_retrival.ib_api import IBStockDataFetcher

    ticker = ticker.upper()
    console.print(f"[cyan]Fetching hourly bars for {ticker}[/cyan] ({start} → {end})")

    # DB config — will move to a config file later
    db_config = None if no_db else {
        'host': '10.0.0.1',
        'port': 5432,
        'dbname': 'stocks',
        'user': 'stock_user',
        'password': postgres_password,
    }

    with IBStockDataFetcher(
        host='127.0.0.1',
        port=4001,
        db_config=db_config,
    ) as fetcher:
        df = fetcher.get_hourly_bars(
            ticker, start, end,
            exchange=exchange,
            currency=currency,
            save_to_db=not no_db,
        )

    if df is None or df.empty:
        console.print(f"[red]No data retrieved for {ticker}[/red]")
        return

    # Summary table
    table = Table(title=f"{ticker} — Hourly Bars")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")

    table.add_row("Bars fetched", str(len(df)))
    table.add_row("Date range", f"{df['date'].min()} → {df['date'].max()}")
    table.add_row("Open (first)", f"{df.iloc[0]['open']:.2f}")
    table.add_row("Close (last)", f"{df.iloc[-1]['close']:.2f}")
    table.add_row("High (max)", f"{df['high'].max():.2f}")
    table.add_row("Low (min)", f"{df['low'].min():.2f}")
    table.add_row("Total volume", f"{df['volume'].sum():,.0f}")

    console.print(table)

    if not no_db:
        console.print("[green]✓ Saved to database[/green]")
    else:
        console.print("[yellow]Database storage skipped (--no-db)[/yellow]")


if __name__ == '__main__':
    cli()
