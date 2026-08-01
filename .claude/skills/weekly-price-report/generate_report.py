#!/usr/bin/env python3
"""
Price-behaviour report generator — weekly (Fri->Fri) or biweekly (fortnightly).

Reads hourly bars from the rosmerta Postgres, rolls them to daily then to ISO
weeks (or consecutive ISO-week pairs), computes a battery of behaviour statistics,
and writes a single self-contained HTML dashboard (data embedded inline, ready to
publish as an Artifact). Templates live in ./templates/{weekly,biweekly}.html.

Usage (run from the project root so db_config / .env resolve):
    PYTHONPATH=. uv run .claude/skills/weekly-price-report/generate_report.py \
        [--timeframe weekly|biweekly] [--list core] \
        [--start 2020-01-01] [--end 2026-12-31] [--out report.html]

Options:
    --timeframe  weekly (default) or biweekly
    --list       watchlist name to restrict the universe (default: all stocks)
    --start/--end inclusive date window (YYYY-MM-DD)
    --out        output HTML path (default: <timeframe>-behaviour.html in cwd)

DB-read-only. Bars are regular-hours US sessions, so each session sits on one UTC
date and UTC weekday == trading weekday.
"""
import argparse, json, sys
from pathlib import Path
import numpy as np
import pandas as pd
from scipy import stats

MN = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
TPL_DIR = Path(__file__).resolve().parent / "templates"


def load_daily(conn, stock_list=None, start=None, end=None):
    """Return per-stock daily OHLCV built from hourly bars."""
    where, params = [], []
    if start:
        where.append("ph.timestamp >= %s"); params.append(start)
    if end:
        where.append("ph.timestamp < (%s::date + interval '1 day')"); params.append(end)
    clause = ("where " + " and ".join(where)) if where else ""
    q = f"""
        with d as (
            select ph.stock_id,
                   (ph.timestamp at time zone 'UTC')::date as d,
                   ph.timestamp, ph.open, ph.high, ph.low, ph.close, ph.volume
            from price_hourly ph
            {clause}
        )
        select s.symbol, d.stock_id, d.d,
               (array_agg(d.open  order by d.timestamp))[1]      as o,
               max(d.high) as h, min(d.low) as l,
               (array_agg(d.close order by d.timestamp desc))[1] as c,
               sum(d.volume) as v
        from d join stocks s on s.stock_id = d.stock_id
        {"where d.stock_id in (select stock_id from watchlist_members where list_name = %s)" if stock_list else ""}
        group by s.symbol, d.stock_id, d.d
    """
    if stock_list:
        params.append(stock_list)
    df = pd.read_sql(q, conn, params=params or None)
    if df.empty:
        sys.exit("No rows returned -- check the date window / watchlist name.")
    df['d'] = pd.to_datetime(df['d'])
    for col in ('o', 'h', 'l', 'c'):
        df[col] = df[col].astype(float)
    return df.sort_values(['stock_id', 'd']).reset_index(drop=True)


# ----------------------------------------------------------------------------- WEEKLY
def weekly_frame(df):
    iso = df['d'].dt.isocalendar()
    df = df.assign(dow=df['d'].dt.dayofweek,
                   yw=iso.year.astype(str) + '-' + iso.week.astype(str).str.zfill(2))
    wk = (df.groupby(['stock_id', 'symbol', 'yw'])
            .agg(n=('d', 'size'), wclose=('c', lambda s: s.iloc[-1]), wend=('d', 'last'),
                 whigh=('h', 'max'), wlow=('l', 'min')).reset_index())
    wk = wk[wk['n'] >= 3].sort_values(['stock_id', 'wend'])
    wk['prev_close'] = wk.groupby('stock_id')['wclose'].shift(1)
    wk['prev_end'] = wk.groupby('stock_id')['wend'].shift(1)
    gap = (wk['wend'] - wk['prev_end']).dt.days
    wk = wk[(gap >= 5) & (gap <= 10)].copy()
    wk['ret'] = (wk['wclose'] / wk['prev_close'] - 1) * 100
    wk['mfe'] = (wk['whigh'] / wk['prev_close'] - 1) * 100
    wk['mae'] = (wk['wlow'] / wk['prev_close'] - 1) * 100
    wk['range_pct'] = (wk['whigh'] - wk['wlow']) / wk['prev_close'] * 100
    wk['month'] = wk['wend'].dt.month
    wk['year'] = wk['wend'].dt.year
    return df, wk


def classify(net, mfe, mae, big=2.0):
    if net > 1 and mae > -1.5:  return 'Clean rally'
    if net < -1 and mfe < 1.5:  return 'Clean selloff'
    if net > 0 and mae <= -big: return 'Dip & recover'
    if net < 0 and mfe >= big:  return 'Pop & fade'
    return 'Quiet / range'


def _runs(sign):
    out, cur, prev = [], 0, None
    for s in sign:
        if s == prev:
            cur += 1
        else:
            if prev is not None:
                out.append((prev, cur))
            cur, prev = 1, s
    if prev is not None:
        out.append((prev, cur))
    return out


def build_data(df, wk):
    r = wk['ret']; n = len(wk)
    COL = {'Clean rally': 'var(--bull2)', 'Dip & recover': 'var(--bull1)',
           'Quiet / range': 'var(--flat)', 'Pop & fade': 'var(--bear1)', 'Clean selloff': 'var(--bear2)'}
    wk = wk.assign(type=[classify(a, b, c) for a, b, c in zip(wk['ret'], wk['mfe'], wk['mae'])])
    tt = wk.groupby('type').agg(share=('ret', 'size'), net=('ret', 'mean'), rng=('range_pct', 'mean')).reset_index()
    tt['share'] = tt['share'] / n * 100
    tt = tt[tt['share'] >= 0.5].sort_values('share', ascending=False)
    types = [{'name': x['type'], 'share': round(x['share'], 1), 'net': round(x['net'], 2),
              'rng': round(x['rng'], 1), 'c': COL.get(x['type'], 'var(--flat)')} for _, x in tt.iterrows()]

    dd = df.merge(wk[['stock_id', 'yw', 'prev_close']], on=['stock_id', 'yw'])
    dd['cum'] = (dd['c'] / dd['prev_close'] - 1) * 100
    DOW = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri']
    path = [{'d': DOW[i], 'v': round(dd[dd.dow == i]['cum'].mean(), 3)} for i in range(5)]
    hi = dd.groupby(['stock_id', 'yw']).apply(lambda s: s.loc[s['h'].idxmax(), 'dow'] if len(s) >= 3 else np.nan, include_groups=False).dropna().astype(int)
    lo = dd.groupby(['stock_id', 'yw']).apply(lambda s: s.loc[s['l'].idxmin(), 'dow'] if len(s) >= 3 else np.nan, include_groups=False).dropna().astype(int)
    hilo = [{'d': DOW[i], 'hi': round((hi == i).mean() * 100, 1), 'lo': round((lo == i).mean() * 100, 1)} for i in range(5)]

    wk['prev_ret'] = wk.groupby('stock_id')['ret'].shift(1)
    cc = wk.dropna(subset=['prev_ret'])
    ac_ret = cc['ret'].corr(cc['prev_ret'])
    crash = cc[cc['prev_ret'] <= cc['prev_ret'].quantile(.1)]['ret'].mean()
    allruns = []
    for _, g in wk.groupby('stock_id'):
        allruns += _runs(np.sign(g['ret']).values)
    rr = pd.DataFrame(allruns, columns=['sign', 'len'])
    ext = lambda sub: (sub['len'] >= 2).sum() / (sub['len'] >= 1).sum() * 100
    wk['prev_range'] = wk.groupby('stock_id')['range_pct'].shift(1)
    vc = wk.dropna(subset=['prev_range'])
    hi_q, lo_q = vc['prev_range'].quantile(.75), vc['prev_range'].quantile(.25)
    predict = {'ac_ret': round(ac_ret, 2), 'ac_range': round(vc['range_pct'].corr(vc['prev_range']), 2),
               'ext_up': round(ext(rr[rr['sign'] > 0])), 'ext_dn': round(ext(rr[rr['sign'] < 0])),
               'crash_bounce': round(crash, 1),
               'range_hi': round(vc[vc.prev_range >= hi_q]['range_pct'].mean(), 1),
               'range_lo': round(vc[vc.prev_range <= lo_q]['range_pct'].mean(), 1),
               'range_avg': round(vc['range_pct'].mean(), 2)}

    months = [{'m': MN[m - 1], 'v': round(wk[wk.month == m]['ret'].mean(), 2)} for m in range(1, 13) if (wk.month == m).any()]
    years = [{'y': str(int(y)), 'v': round(g['ret'].mean(), 2), 'up': round((g['ret'] > 0).mean() * 100)} for y, g in wk.groupby('year')]
    hh, edges = np.histogram(r.clip(-20, 20), bins=40, range=(-20, 20))
    dist = {'mean': round(r.mean(), 2), 'median': round(r.median(), 2), 'std': round(r.std(), 2),
            'skew': round(float(stats.skew(r)), 2), 'kurt': round(float(stats.kurtosis(r)), 1),
            'pct_pos': round((r > 0).mean() * 100, 1), 'gt5': round((r > 5).mean() * 100, 1),
            'lt5': round((r < -5).mean() * 100, 1), 'min': round(r.min(), 1), 'max': round(r.max(), 1),
            'range_mean': round(wk['range_pct'].mean(), 2), 'range_med': round(wk['range_pct'].median(), 1),
            'abs_move': round(r.abs().mean(), 2), 'mfe_avg': round(wk['mfe'].mean(), 1), 'mae_avg': round(wk['mae'].mean(), 1)}
    kpis = [
        {'lab': 'Avg weekly return', 'val': f"{dist['mean']:+.2f}%", 'cls': 'pos' if dist['mean'] > 0 else 'neg', 'sub': f"median {dist['median']:+.2f}%"},
        {'lab': 'Weeks positive', 'val': f"{dist['pct_pos']:.1f}%", 'cls': '', 'sub': 'directional tilt'},
        {'lab': 'Typical high–low range', 'val': f"{dist['range_mean']:.2f}%", 'cls': '', 'sub': f"median {dist['range_med']:.1f}%"},
        {'lab': 'Volatility (σ)', 'val': f"{dist['std']:.2f}%", 'cls': '', 'sub': 'per week, 1σ'},
        {'lab': 'Fat-tail (kurtosis)', 'val': f"{dist['kurt']:+.1f}", 'cls': '', 'sub': 'excess vs normal'},
    ]
    return {'kpis': kpis, 'dist': dist, 'types': types, 'path': path, 'hilo': hilo,
            'months': months, 'years': years, 'predict': predict,
            'hist': {'h': hh.tolist(), 'e': edges.tolist(), 'n': n, 'mean': dist['mean'], 'median': dist['median']}}


# --------------------------------------------------------------------------- BIWEEKLY
def biweekly_frame(df):
    """Fortnights = consecutive ISO-week pairs; measured block-end to block-end."""
    iso = df['d'].dt.isocalendar()
    iy, iw = iso.year.astype(int), iso.week.astype(int)
    df = df.assign(dow=df['d'].dt.dayofweek, iw=iw,
                   fk=iy.astype(str) + '-' + ((iw - 1) // 2).astype(str).str.zfill(2),
                   wif=(iw - 1) % 2)  # 0 = first week of the fortnight, 1 = second

    def w1c(s):
        sub = s[df.loc[s.index, 'wif'] == 0]
        return sub.iloc[-1] if len(sub) else np.nan
    fn = (df.groupby(['stock_id', 'symbol', 'fk'])
            .agg(n=('d', 'size'), fclose=('c', lambda s: s.iloc[-1]), fend=('d', 'last'),
                 fhigh=('h', 'max'), flow=('l', 'min'), w1close=('c', w1c)).reset_index())
    fn = fn[fn['n'] >= 6].sort_values(['stock_id', 'fend'])
    fn['prev_close'] = fn.groupby('stock_id')['fclose'].shift(1)
    fn['prev_end'] = fn.groupby('stock_id')['fend'].shift(1)
    gap = (fn['fend'] - fn['prev_end']).dt.days
    fn = fn[(gap >= 8) & (gap <= 18)].copy()
    fn['ret'] = (fn['fclose'] / fn['prev_close'] - 1) * 100
    fn['mfe'] = (fn['fhigh'] / fn['prev_close'] - 1) * 100
    fn['mae'] = (fn['flow'] / fn['prev_close'] - 1) * 100
    fn['range_pct'] = (fn['fhigh'] - fn['flow']) / fn['prev_close'] * 100
    fn['w1ret'] = (fn['w1close'] / fn['prev_close'] - 1) * 100
    fn['w2ret'] = (fn['fclose'] / fn['w1close'] - 1) * 100
    fn['month'] = fn['fend'].dt.month
    fn['year'] = fn['fend'].dt.year
    return df, fn


def classify_bw(net, mfe, mae, big=3.0):
    if net > 1.5 and mae > -2.2:  return 'Clean rally'
    if net < -1.5 and mfe < 2.2:  return 'Clean selloff'
    if net > 0 and mae <= -big:   return 'Dip & recover'
    if net < 0 and mfe >= big:    return 'Pop & fade'
    return 'Quiet / range'


def build_data_biweekly(df, fn):
    r = fn['ret']; n = len(fn)
    fn = fn.assign(type=[classify_bw(a, b, c) for a, b, c in zip(fn['ret'], fn['mfe'], fn['mae'])])
    tt = fn.groupby('type').agg(share=('ret', 'size'), net=('ret', 'mean'), rng=('range_pct', 'mean')).reset_index()
    tt['share'] = tt['share'] / n * 100
    tt = tt[tt['share'] >= 0.5].sort_values('share', ascending=False)
    types = [{'name': x['type'], 'share': round(x['share'], 1), 'net': round(x['net'], 2), 'rng': round(x['rng'], 1)} for _, x in tt.iterrows()]

    dd = df.merge(fn[['stock_id', 'fk', 'prev_close']], on=['stock_id', 'fk'])
    dd['cum'] = (dd['c'] / dd['prev_close'] - 1) * 100
    dd['slot'] = dd['wif'] * 5 + dd['dow']
    SLOT = ['W1·Mon', 'W1·Tue', 'W1·Wed', 'W1·Thu', 'W1·Fri', 'W2·Mon', 'W2·Tue', 'W2·Wed', 'W2·Thu', 'W2·Fri']
    path = [{'s': SLOT[i], 'v': round(dd[dd.slot == i]['cum'].mean(), 3)} for i in range(10)]
    hi = dd.groupby(['stock_id', 'fk']).apply(lambda s: s.loc[s['h'].idxmax(), 'slot'] if len(s) >= 6 else np.nan, include_groups=False).dropna().astype(int)
    lo = dd.groupby(['stock_id', 'fk']).apply(lambda s: s.loc[s['l'].idxmin(), 'slot'] if len(s) >= 6 else np.nan, include_groups=False).dropna().astype(int)
    hilo = [{'s': SLOT[i], 'hi': round((hi == i).mean() * 100, 1), 'lo': round((lo == i).mean() * 100, 1)} for i in range(10)]

    fn['pr'] = fn.groupby('stock_id')['ret'].shift(1)
    cc = fn.dropna(subset=['pr'])
    fn['prg'] = fn.groupby('stock_id')['range_pct'].shift(1)
    vc = fn.dropna(subset=['prg'])
    hq, lq = vc['prg'].quantile(.75), vc['prg'].quantile(.25)
    predict = {'ac': round(cc['ret'].corr(cc['pr']), 2), 'acr': round(vc['range_pct'].corr(vc['prg']), 2),
               'after_up': round(cc[cc.pr > 0]['ret'].mean(), 3), 'after_dn': round(cc[cc.pr < 0]['ret'].mean(), 3),
               'crash': round(cc[cc.pr <= cc['pr'].quantile(.1)]['ret'].mean(), 2),
               'rng_hi': round(vc[vc.prg >= hq]['range_pct'].mean(), 1), 'rng_lo': round(vc[vc.prg <= lq]['range_pct'].mean(), 1),
               'rng_avg': round(vc['range_pct'].mean(), 2)}
    weeks = {'w1': round(fn['w1ret'].mean(), 3), 'w2': round(fn['w2ret'].mean(), 3),
             'w1pos': round((fn['w1ret'] > 0).mean() * 100), 'w2pos': round((fn['w2ret'] > 0).mean() * 100)}

    months = [{'m': MN[m - 1], 'v': round(fn[fn.month == m]['ret'].mean(), 2)} for m in range(1, 13) if (fn.month == m).any()]
    years = [{'y': str(int(y)), 'v': round(g['ret'].mean(), 2), 'up': round((g['ret'] > 0).mean() * 100)} for y, g in fn.groupby('year')]
    hh, edges = np.histogram(r.clip(-30, 30), bins=40, range=(-30, 30))
    dist = {'mean': round(r.mean(), 2), 'median': round(r.median(), 2), 'std': round(r.std(), 2),
            'skew': round(float(stats.skew(r)), 2), 'kurt': round(float(stats.kurtosis(r)), 1),
            'pct_pos': round((r > 0).mean() * 100, 1), 'gt5': round((r > 5).mean() * 100, 1),
            'lt5': round((r < -5).mean() * 100, 1), 'gt10': round((r > 10).mean() * 100, 1), 'lt10': round((r < -10).mean() * 100, 1),
            'min': round(r.min(), 1), 'max': round(r.max(), 1), 'range_mean': round(fn['range_pct'].mean(), 2),
            'range_med': round(fn['range_pct'].median(), 1), 'abs': round(r.abs().mean(), 2),
            'mfe': round(fn['mfe'].mean(), 1), 'mae': round(fn['mae'].mean(), 1)}
    return {'dist': dist, 'types': types, 'path': path, 'hilo': hilo, 'weeks': weeks,
            'predict': predict, 'months': months, 'years': years,
            'hist': {'h': hh.tolist(), 'e': edges.tolist(), 'n': n, 'mean': dist['mean']}}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--timeframe', choices=['weekly', 'biweekly'], default='weekly')
    ap.add_argument('--list', dest='stock_list', default=None)
    ap.add_argument('--start', default=None)
    ap.add_argument('--end', default=None)
    ap.add_argument('--out', default=None)
    args = ap.parse_args()

    import psycopg2
    from db_config import get_db_config
    conn = psycopg2.connect(**get_db_config())
    df = load_daily(conn, args.stock_list, args.start, args.end)

    if args.timeframe == 'biweekly':
        df, fn = biweekly_frame(df); data = build_data_biweekly(df, fn); n = len(fn)
        tpl = TPL_DIR / "biweekly.html"
    else:
        df, wk = weekly_frame(df); data = build_data(df, wk); n = len(wk)
        tpl = TPL_DIR / "weekly.html"

    data['meta'] = {'n_stocks': int(df['stock_id'].nunique()), 'n': int(n), 'n_weeks': int(n),
                    'start': str(df['d'].min().date()), 'end': str(df['d'].max().date()),
                    'list': args.stock_list if args.stock_list else 'all stocks',
                    'generated': pd.Timestamp.utcnow().strftime('%Y-%m-%d')}
    html = tpl.read_text().replace('__DATA__', json.dumps(data))
    out = Path(args.out or f"{args.timeframe}-behaviour.html")
    out.write_text(html)
    m = data['meta']
    print(f"Wrote {out}  [{args.timeframe}]  ({m['n_stocks']} stocks, {n:,} periods, {m['start']}..{m['end']})")
    print("Publish it with the Artifact tool to view/share the dashboard.")


if __name__ == '__main__':
    main()
