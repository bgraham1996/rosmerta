"""
Rosmerta — Stock Analysis CLI
"""

import click
from rich.console import Console
from rich.table import Table
import os
from dotenv import load_dotenv
from db_config import get_db_config


load_dotenv()
email = os.getenv('email')



console = Console()



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


# ─── Bulk command group ─────────────────────────────────────
@cli.group()
def bulk():
    """Bulk operations across watchlists."""
    pass


@bulk.command('price')
@click.option('--list', '-l', 'list_name', required=True,
              help='Watchlist name to fetch (e.g. core, pharma_top20)')
@click.option('--start', '-s', required=True, help='Start date (YYYY-MM-DD)')
@click.option('--end', '-e', required=True, help='End date (YYYY-MM-DD)')
@click.option('--exchange', default='SMART', help='IB exchange routing (default: SMART)')
@click.option('--delay', default=2, type=int,
              help='Seconds between tickers to avoid IB rate limits (default: 2)')
@click.option('--dry-run', is_flag=True, default=False,
              help='Show what would be fetched without fetching')
def bulk_price(list_name, start, end, exchange, delay, dry_run):
    """Fetch hourly price bars for all stocks in a watchlist.

    Example: rosmerta bulk price --list core --start 2025-01-01 --end 2025-02-01
    """
    _run_bulk_fetch(list_name, start, end, dry_run, 'price',
                    exchange=exchange, delay=delay)


@bulk.command('fundamentals')
@click.option('--list', '-l', 'list_name', required=True,
              help='Watchlist name to fetch (e.g. core, pharma_top20)')
@click.option('--start', '-s', default=None, help='Start date (YYYY-MM-DD). Omit for all available.')
@click.option('--end', '-e', default=None, help='End date (YYYY-MM-DD). Omit for up to present.')
@click.option('--dry-run', is_flag=True, default=False,
              help='Show what would be fetched without fetching')
def bulk_fundamentals(list_name, start, end, dry_run):
    """Fetch fundamental data from SEC EDGAR for all stocks in a watchlist.

    Example: rosmerta bulk fundamentals --list core
    Example: rosmerta bulk fundamentals --list core --start 2020-01-01 --end 2024-12-31
    """
    _run_bulk_fetch(list_name, start, end, dry_run, 'fundamentals')


# ─── Shared bulk fetch logic ───────────────────────────────
def _resolve_watchlist(list_name):
    """Resolve a watchlist name to its tickers. Returns tickers or None."""
    from price_retrival.bulk_fetch import get_watchlist_tickers, get_available_lists
    import psycopg2

    db_config = get_db_config()
    conn = psycopg2.connect(**db_config)
    try:
        tickers = get_watchlist_tickers(conn, list_name)

        if tickers is None:
            console.print(f"[red]Watchlist '{list_name}' not found.[/red]")
            available = get_available_lists(conn)
            if available:
                console.print("\n[dim]Available watchlists:[/dim]")
                for name, count in available:
                    console.print(f"  [cyan]{name}[/cyan] ({count} stocks)")
            else:
                console.print("[dim]No watchlists found. Create one first.[/dim]")
            return None

        if not tickers:
            console.print(
                f"[yellow]Watchlist '{list_name}' exists but has no active stocks.[/yellow]"
            )
            return None

        return tickers
    finally:
        conn.close()


def _show_dry_run(list_name, tickers):
    """Display a dry-run preview table."""
    table = Table(title=f"Dry run — {list_name}")
    table.add_column("Symbol", style="cyan")
    table.add_column("Exchange", style="dim")
    table.add_column("Currency", style="dim")
    for t in tickers:
        table.add_row(t['symbol'], t['exchange'], t['currency'])
    console.print(table)
    console.print(f"\n[dim]Dry run complete. Remove --dry-run to fetch.[/dim]")


def _show_results(results, list_name, record_label='Records'):
    """Display the summary table and totals after a bulk fetch."""
    console.print()

    succeeded = [r for r in results if r.status == 'success']
    failed = [r for r in results if r.status == 'error']
    empty = [r for r in results if r.status == 'empty']

    summary = Table(title=f"Bulk fetch complete — {list_name}")
    summary.add_column("Symbol", style="cyan")
    summary.add_column("Status")
    summary.add_column(record_label, justify="right")
    summary.add_column("Notes", style="dim")

    for r in results:
        if r.status == 'success':
            status_str = "[green]✓[/green]"
            count_str = f"{r.records:,}"
        elif r.status == 'empty':
            status_str = "[yellow]—[/yellow]"
            count_str = "0"
        else:
            status_str = "[red]✗[/red]"
            count_str = "0"

        summary.add_row(r.symbol, status_str, count_str, r.error or "")

    console.print(summary)

    total = sum(r.records for r in results)
    console.print(
        f"\n[bold]Summary:[/bold] "
        f"[green]{len(succeeded)} succeeded[/green], "
        f"[yellow]{len(empty)} empty[/yellow], "
        f"[red]{len(failed)} failed[/red] — "
        f"{total:,} total {record_label.lower()} fetched"
    )


def _run_bulk_fetch(list_name, start, end, dry_run, fetch_type,
                    exchange='SMART', delay=2):
    """Shared implementation for bulk price and bulk fundamentals commands."""
    from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, MofNCompleteColumn
    from price_retrival.bulk_fetch import bulk_fetch_prices, bulk_fetch_fundamentals

    tickers = _resolve_watchlist(list_name)
    if tickers is None:
        return

    # ── Preview ─────────────────────────────────────────────
    date_range = ""
    if start and end:
        date_range = f"  {start} → {end}"
    elif start:
        date_range = f"  from {start}"
    elif end:
        date_range = f"  up to {end}"

    console.print(
        f"\n[bold]Bulk {fetch_type} fetch:[/bold] [cyan]{list_name}[/cyan] "
        f"({len(tickers)} stocks){date_range}"
    )

    if dry_run:
        _show_dry_run(list_name, tickers)
        return

    # ── Fetch with progress ─────────────────────────────────
    db_config = get_db_config()

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        console=console,
    ) as progress:
        task = progress.add_task(f"Fetching {list_name}...", total=len(tickers))

        def on_start(i, symbol):
            progress.update(task, description=f"Fetching {symbol}...")

        def on_complete(i, result):
            if result.status == 'error':
                console.print(f"  [red]✗ {result.symbol}: {result.error}[/red]")
            progress.update(task, advance=1)

        if fetch_type == 'price':
            results = bulk_fetch_prices(
                tickers=tickers,
                start=start,
                end=end,
                db_config=db_config,
                exchange=exchange,
                delay=delay,
                on_ticker_start=on_start,
                on_ticker_complete=on_complete,
            )
            record_label = 'Bars'
        else:
            results = bulk_fetch_fundamentals(
                tickers=tickers,
                db_config=db_config,
                user_agent=email,
                start=start,
                end=end,
                on_ticker_start=on_start,
                on_ticker_complete=on_complete,
            )
            record_label = 'Periods'

    _show_results(results, list_name, record_label)


if __name__ == '__main__':
    cli()
