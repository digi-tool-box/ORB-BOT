import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def calculate_metrics(trades_df, initial_capital=10000):
    if trades_df.empty:
        return {"total_trades": 0, "win_rate": 0, "profit_factor": 0, "net_pnl": 0,
                "max_drawdown": 0, "sharpe": 0}
    wins = trades_df[trades_df['pnl'] > 0]
    losses = trades_df[trades_df['pnl'] <= 0]
    gross_profit = wins['pnl'].sum() if not wins.empty else 0
    gross_loss = abs(losses['pnl'].sum()) if not losses.empty else 0
    net_pnl = trades_df['pnl'].sum()
    win_rate = len(wins) / len(trades_df) * 100
    profit_factor = gross_profit / gross_loss if gross_loss != 0 else np.inf
    equity = initial_capital + trades_df['pnl'].cumsum()
    rolling_max = equity.cummax()
    drawdown = (equity - rolling_max) / rolling_max
    max_drawdown = drawdown.min()
    returns = trades_df['pnl'] / initial_capital
    sharpe = returns.mean() / returns.std() * np.sqrt(252*24*12) if returns.std() != 0 else 0
    return {
        "total_trades": len(trades_df),
        "winning_trades": len(wins),
        "losing_trades": len(losses),
        "win_rate": round(win_rate, 2),
        "gross_profit": round(gross_profit, 2),
        "gross_loss": round(gross_loss, 2),
        "profit_factor": round(profit_factor, 2) if profit_factor != np.inf else "∞",
        "net_pnl": round(net_pnl, 2),
        "max_drawdown": round(max_drawdown * 100, 2),
        "sharpe_ratio": round(sharpe, 2),
        "equity_curve": equity,
    }

def plot_equity(equity_curve, trades_df, save_path="exports/equity_curve.png"):
    plt.figure(figsize=(12,6))
    plt.plot(equity_curve.index, equity_curve.values, label='Equity', color='blue')
    for _, trade in trades_df.iterrows():
        plt.axvline(trade['exit_time'], color='green' if trade['pnl']>0 else 'red', alpha=0.3)
    plt.title('Equity Curve')
    plt.xlabel('Date')
    plt.ylabel('Equity ($)')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(save_path)
    plt.close()

def export_trades(trades_df, path="exports/trades.csv"):
    trades_df.to_csv(path, index=False)
    print(f"Trades exported to {path}")

def export_metrics(metrics, path="exports/metrics.txt"):
    with open(path, 'w') as f:
        for key, value in metrics.items():
            if key != 'equity_curve':
                f.write(f"{key}: {value}\n")
    print(f"Metrics saved to {path}")