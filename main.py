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
email = os.getenv('email')

console = Console()


# Shared DB config — used by both fetch subcommands
def get_db_config(no_db=False):
    if no_db:
        return None
    return {
        'host': '10.0.0.1',
        'port': 5432,
        'dbname': 'stocks',
        'user': 'stock_user',
        'password': postgres_password,
    }


@click.group()
@click.version_option(version='0.1.0', prog_name='rosmerta')
def cli():
    """Rosmerta — stock analysis and position management."""
    pass


# ─── Fetch command group ────────────────────────────────────
@cli.group()
def fetch():
    """Fetch data from external sources (IB, SEC EDGAR)."""
    pass


@fetch.command('price')
@click.argument('ticker')
@click.option('--start', '-s', required=True, help='Start date (YYYY-MM-DD)')
@click.option('--end', '-e', required=True, help='End date (YYYY-MM-DD)')
@click.option('--exchange', default='SMART', help='IB exchange routing (default: SMART)')
@click.option('--currency', default='USD', help='Contract currency (default: USD)')
@click.option('--no-db', is_flag=True, help='Skip database storage')
def fetch_price(ticker, start, end, exchange, currency, no_db):
    """Fetch hourly price bars from Interactive Brokers.

    Example: rosmerta fetch price AAPL --start 2024-01-01 --end 2024-03-31
    """
    from price_retrival.ib_api import IBStockDataFetcher

    ticker = ticker.upper()
    console.print(f"[cyan]Fetching hourly bars for {ticker}[/cyan] ({start} → {end})")

    db_config = get_db_config(no_db)

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


@fetch.command('fundamentals')
@click.argument('ticker')
@click.option('--start', '-s', default=None, help='Start date (YYYY-MM-DD). Omit for all available.')
@click.option('--end', '-e', default=None, help='End date (YYYY-MM-DD). Omit for up to present.')
@click.option('--no-db', is_flag=True, help='Skip database storage')
def fetch_fundamentals(ticker, start, end, no_db):
    """Fetch fundamental data from SEC EDGAR.

    Example: rosmerta fetch fundamentals PFE --start 2020-01-01 --end 2024-12-31
    """
    from price_retrival.edgar_api import EdgarFundamentalsFetcher

    ticker = ticker.upper()
    date_range = ""
    if start and end:
        date_range = f" ({start} → {end})"
    elif start:
        date_range = f" (from {start})"
    elif end:
        date_range = f" (up to {end})"

    console.print(f"[cyan]Fetching fundamentals for {ticker}[/cyan]{date_range}")

    db_config = get_db_config(no_db)

    with EdgarFundamentalsFetcher(
        db_config=db_config,
        user_agent=email,
    ) as fetcher:

        # Resolve and display CIK
        company_info = fetcher.resolve_ticker(ticker)
        if not company_info:
            console.print(f"[red]Ticker '{ticker}' not found in SEC company list[/red]")
            return

        console.print(
            f"  Resolved to [bold]{company_info['name']}[/bold] "
            f"(CIK: {company_info['cik']})"
        )

        records = fetcher.get_fundamentals(
            ticker,
            start_date=start,
            end_date=end,
            save_to_db=not no_db,
        )

    if not records:
        console.print(f"[red]No fundamental data found for {ticker}[/red]")
        return

    # Summary table
    table = Table(title=f"{ticker} — Fundamentals ({len(records)} periods)")
    table.add_column("Period", style="cyan")
    table.add_column("Revenue", justify="right", style="green")
    table.add_column("Net Income", justify="right", style="green")
    table.add_column("R&D", justify="right", style="green")
    table.add_column("Gross Margin", justify="right", style="yellow")
    table.add_column("Net Margin", justify="right", style="yellow")

    for r in records:
        period = f"{r['period_type']} {r['fiscal_year']} ({r['period_end']})"
        revenue = f"{r['revenue']:,.0f}" if r.get('revenue') else "—"
        net_inc = f"{r['net_income']:,.0f}" if r.get('net_income') else "—"
        rd = f"{r['research_and_development']:,.0f}" if r.get('research_and_development') else "—"
        gm = f"{r['gross_margin']:.1%}" if r.get('gross_margin') else "—"
        nm = f"{r['net_margin']:.1%}" if r.get('net_margin') else "—"
        table.add_row(period, revenue, net_inc, rd, gm, nm)

    console.print(table)

    if not no_db:
        console.print("[green]✓ Saved to database[/green]")
    else:
        console.print("[yellow]Database storage skipped (--no-db)[/yellow]")


if __name__ == '__main__':
    cli()
