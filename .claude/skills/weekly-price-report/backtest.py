#!/usr/bin/env python3
"""
Backtest: buy the red-Wednesday dip in an uptrend, hold N trading days.

Signal  : a Wednesday closing below Tuesday ("red") while the close is above its
          M-day SMA (uptrend = stand-in for "my models are bullish").
Entry   : that Wednesday's close.   Exit: close HOLD trading days later.
Outputs : a self-contained HTML report (equity curve vs buy-and-hold, execution
          edge vs random-day entry, cost sensitivity, stop-loss variants, drawdown
          distribution, by-year). Template: ./templates/backtest.html.

Usage (from the project root):
    PYTHONPATH=. uv run .claude/skills/weekly-price-report/backtest.py \
        [--hold 10] [--sma 50] [--cost-bps 10] [--list core] \
        [--start ...] [--end ...] [--out backtest-report.html]

DB-read-only. See generate_report.py for the shared daily-bar loader.
"""
import argparse, json, sys
from pathlib import Path
import numpy as np
import pandas as pd
from scipy import stats

sys.path.insert(0, str(Path(__file__).resolve().parent))
from generate_report import load_daily, TPL_DIR


def run(df, hold, sma, cost_bps):
    df = df.sort_values(['stock_id', 'd']).reset_index(drop=True)
    df['ret'] = df.groupby('stock_id')['c'].pct_change()
    df['dow'] = df['d'].dt.dayofweek
    df['sma'] = df.groupby('stock_id')['c'].transform(lambda s: s.rolling(sma).mean())
    df['f_hold'] = (df.groupby('stock_id')['c'].shift(-hold) / df['c'] - 1) * 100  # for matched baseline
    cost = cost_bps / 10000.0

    trades, daily = [], {}
    for sid, g in df.groupby('stock_id'):
        g = g.reset_index(drop=True)
        c, l, h, dd, ret, dow, sm = (g[k].values for k in ('c', 'l', 'h', 'd', 'ret', 'dow', 'sma'))
        sig = np.where((dow == 2) & (g['ret'].values < 0) & (c > sm))[0]
        for j in sig:
            if j + hold >= len(g):
                continue
            entry = c[j]
            fc, fl, fd, fr = c[j+1:j+1+hold], l[j+1:j+1+hold], dd[j+1:j+1+hold], ret[j+1:j+1+hold]
            gross = fc[-1] / entry - 1
            mae = fl.min() / entry - 1

            def stop(s):
                for k in range(hold):
                    if fl[k] / entry - 1 <= -s:
                        return -s
                return gross
            trades.append(dict(year=int(pd.Timestamp(dd[j]).year), gross=gross, mae=mae,
                               s5=stop(0.05), s8=stop(0.08)))
            for k in range(hold):
                daily.setdefault(pd.Timestamp(fd[k]), []).append(fr[k] - (cost if k == hold - 1 else 0.0))

    T = pd.DataFrame(trades)
    N = len(T)
    if N == 0:
        sys.exit("No trades generated -- check the window / filters.")

    # execution edge vs entering the same uptrend name on a random day
    up = df[df['c'] > df['sma']].dropna(subset=['f_hold'])
    redwed = up[(up.dow == 2) & (up.ret < 0)]['f_hold']
    rand = up['f_hold']
    green = up[(up.dow == 2) & (up.ret > 0)]['f_hold']
    _, p = stats.ttest_ind(redwed, rand, equal_var=False)
    t = stats.ttest_ind(redwed, rand, equal_var=False).statistic

    # equity curve vs equal-weight buy&hold
    dates = sorted(daily)
    port = pd.Series({d: np.mean(daily[d]) for d in dates}).sort_index()
    bmask = (df['d'] >= port.index.min()) & (df['d'] <= port.index.max())
    bench = df[bmask].groupby('d')['ret'].mean().sort_index()
    port = port.reindex(bench.index).fillna(0.0)

    def ann(r):
        r = r.dropna()
        yrs = (r.index.max() - r.index.min()).days / 365.25
        cagr = ((1 + r).prod()) ** (1 / yrs) - 1
        return cagr * 100, r.mean() / r.std() * np.sqrt(252)
    p_cagr, p_sh = ann(port)
    b_cagr, b_sh = ann(bench)
    eq = (1 + port).cumprod().resample('W').last()
    eb = (1 + bench).cumprod().resample('W').last().reindex(eq.index)

    g0 = T['gross']
    stops = [('No stop', g0 - cost, None), ('−5% stop', T['s5'] - cost, 5), ('−8% stop', T['s8'] - cost, 8)]
    return {
        'eq': {'dates': [d.strftime('%Y-%m-%d') for d in eq.index],
               'strategy': [round(float(v), 4) for v in eq.values],
               'bench': [round(float(v), 4) for v in eb.values]},
        'stats': {'n': int(N), 'win': round((g0 > 0).mean() * 100, 1), 'avg_trade': round(g0.mean() * 100, 3),
                  'net10': round((g0 - 0.001).mean() * 100, 3), 'mae': round(T['mae'].mean() * 100, 2),
                  'cagr': round(p_cagr, 1), 'cagr_b': round(b_cagr, 1), 'sharpe': round(p_sh, 2), 'sharpe_b': round(b_sh, 2),
                  'inv': round((port != 0).mean() * 100), 'avg_pos': round(np.mean([len(daily[d]) for d in dates]), 1)},
        'exec': {'redwed': round(redwed.mean(), 3), 'random': round(rand.mean(), 3), 'green': round(green.mean(), 3),
                 'edge': round(redwed.mean() - rand.mean(), 3), 'p': f"{p:.1e}", 't': round(float(t), 2)},
        'costs': [{'bps': b, 'avg': round((g0 - b / 10000).mean() * 100, 3), 'win': round((g0 - b / 10000 > 0).mean() * 100, 1)} for b in (0, 5, 10, 20)],
        'stops': [{'lab': lab, 'avg': round(x.mean() * 100, 3), 'win': round((x > 0).mean() * 100, 1),
                   'worst': round(x.min() * 100, 1), 'hit': (None if s is None else round((col == -s / 100).mean() * 100, 1))}
                  for (lab, x, s), col in zip(stops, [g0, T['s5'], T['s8']])],
        'mae': {'avg': round(T['mae'].mean() * 100, 2), 'med': round(T['mae'].median() * 100, 2),
                'd3': round((T['mae'] <= -.03).mean() * 100, 1), 'd5': round((T['mae'] <= -.05).mean() * 100, 1),
                'd8': round((T['mae'] <= -.08).mean() * 100, 1), 'd10': round((T['mae'] <= -.10).mean() * 100, 1)},
        'byyear': [{'y': int(y), 'avg': round(gg['gross'].mean() * 100, 2), 'win': round((gg['gross'] > 0).mean() * 100)} for y, gg in T.groupby('year')],
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--hold', type=int, default=10)
    ap.add_argument('--sma', type=int, default=50)
    ap.add_argument('--cost-bps', type=float, default=10)
    ap.add_argument('--list', dest='stock_list', default=None)
    ap.add_argument('--start', default=None)
    ap.add_argument('--end', default=None)
    ap.add_argument('--out', default='backtest-report.html')
    args = ap.parse_args()

    import psycopg2
    from db_config import get_db_config
    conn = psycopg2.connect(**get_db_config())
    df = load_daily(conn, args.stock_list, args.start, args.end)
    data = run(df, args.hold, args.sma, args.cost_bps)

    html = (TPL_DIR / "backtest.html").read_text().replace('__DATA__', json.dumps(data))
    Path(args.out).write_text(html)
    s = data['stats']
    print(f"Wrote {args.out}  ({s['n']:,} trades, hold {args.hold}d, sma {args.sma})")
    print(f"  per-trade net {s['net10']:+.3f}%  win {s['win']}%  execution edge {data['exec']['edge']:+.3f}% (p={data['exec']['p']})")
    print(f"  strategy CAGR {s['cagr']}% Sharpe {s['sharpe']}  vs  buy&hold {s['cagr_b']}% Sharpe {s['sharpe_b']}")
    print("Publish it with the Artifact tool to view/share the report.")


if __name__ == '__main__':
    main()
