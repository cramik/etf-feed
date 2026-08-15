import pandas as pd
import yfinance as yf
from datetime import datetime
import time
import sqlite3
import json
import os
import re


DB_FILE = "etf_master_cache.db"

def init_db():
    """Initializes the SQLite database and creates the table if it doesn't exist."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS etf_meta (
            symbol TEXT PRIMARY KEY,
            name TEXT,
            inception_unix REAL,
            total_assets REAL,
            last_fetched TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    return conn

def fetch_master_ticker_list():
    """Pulls and merges the NASDAQ and Non-NASDAQ master lists."""
    print("Downloading exchange master lists via FTP...")
    df_nasdaq = pd.read_csv("ftp://ftp.nasdaqtrader.com/symboldirectory/nasdaqlisted.txt", sep="|")
    df_other = pd.read_csv("ftp://ftp.nasdaqtrader.com/symboldirectory/otherlisted.txt", sep="|")
    
    etfs_nasdaq = df_nasdaq[df_nasdaq['ETF'] == 'Y']['Symbol'].dropna()
    etfs_other = df_other[df_other['ETF'] == 'Y']['ACT Symbol'].dropna()
    
    all_symbols = pd.concat([etfs_nasdaq, etfs_other]).drop_duplicates().tolist()
    
    # Clean out test tickers
    return [sym for sym in all_symbols if isinstance(sym, str) and not sym.endswith('TEST') and '$' not in sym]

def build_cached_etf_feed():
    conn = init_db()
    cursor = conn.cursor()
    
    clean_symbols = fetch_master_ticker_list()
    print(f"Compiled master list of {len(clean_symbols)} total ETFs.")
    
    blacklist_regex = re.compile(r'\b(2x|3x|ultra|bull|bear|ProShares UltraShort)\b', re.IGNORECASE)
    feed_data = []
    
    print("Cross-referencing with SQLite cache and Yahoo Finance...")
    
    for i, symbol in enumerate(clean_symbols):
        # 1. Check local SQLite cache first
        cursor.execute("SELECT name, inception_unix, total_assets FROM etf_meta WHERE symbol = ?", (symbol,))
        row = cursor.fetchone()
        
        if row:
            # Cache hit
            name, inception_unix, total_assets = row
        else:
            # Cache miss: Fetch from yfinance
            try:
                ticker = yf.Ticker(symbol)
                info = ticker.info
                
                if not info or not info.get('longName'):
                    # Cache the failure to avoid querying Yahoo Finance for dead/test tickers on every run
                    cursor.execute('''
                        INSERT OR REPLACE INTO etf_meta (symbol, name, inception_unix, total_assets)
                        VALUES (?, ?, ?, ?)
                    ''', (symbol, 'Fetch Failed', -1.0, -1.0))
                    conn.commit()
                    time.sleep(0.3)
                    continue
                    
                name = info.get('longName', '')
                    
                inception_unix = info.get('fundInceptionDate') or info.get('firstTradeDateEpoch')
                
                # Fallback to Milliseconds
                if not inception_unix and info.get('firstTradeDateMilliseconds'):
                    inception_unix = info.get('firstTradeDateMilliseconds') / 1000.0
                    
                # Fallback to ipoExpectedDate
                if not inception_unix and info.get('ipoExpectedDate'):
                    try:
                        dt = datetime.strptime(info.get('ipoExpectedDate'), '%Y-%m-%d')
                        inception_unix = dt.timestamp()
                    except Exception:
                        pass
                
                # If still not found, store as -1 to prevent repeated query attempts
                if not inception_unix:
                    inception_unix = -1
                    
                total_assets = info.get('totalAssets')
                
                # Insert the newly fetched data into the SQLite database
                cursor.execute('''
                    INSERT OR REPLACE INTO etf_meta (symbol, name, inception_unix, total_assets)
                    VALUES (?, ?, ?, ?)
                ''', (symbol, name, inception_unix, total_assets))
                conn.commit()
                
                # Respect rate limits ONLY when making a live network call
                time.sleep(0.3)
                
            except Exception as e:
                print(f"Failed to process {symbol}: {e}")
                continue

        # Skip failed fetches
        if name == 'Fetch Failed':
            continue

        # 2. Apply Blacklist Logic
        # We apply this in memory so you can change the blacklist without rebuilding the cache
        if blacklist_regex.search(name):
            continue 
            
        # 3. Format Date and Append to Feed
        if inception_unix and inception_unix > 0:
            inception_date = datetime.fromtimestamp(inception_unix).strftime('%Y-%m-%d')
        else:
            inception_date = pd.NaT
            
        feed_data.append({
            'Symbol': symbol,
            'Name': name,
            'Inception_Date': inception_date,
            'Total_Assets': total_assets
        })
        
        # Simple progress tracker for the console
        if (i + 1) % 500 == 0:
            print(f"Processed {i + 1}/{len(clean_symbols)}...")

    conn.close()

    # 4. Format and sort the final DataFrame
    feed_df = pd.DataFrame(feed_data)
    
    if not feed_df.empty:
        feed_df['Inception_Date'] = pd.to_datetime(feed_df['Inception_Date'], errors='coerce')
        feed_df = feed_df.sort_values(by='Inception_Date', ascending=False)
        feed_df['Inception_Date'] = feed_df['Inception_Date'].dt.strftime('%Y-%m-%d').fillna("Unknown")
        
        # Batch fetch daily price change percentages
        symbols = feed_df['Symbol'].tolist()
        print(f"Fetching daily price change percentages for {len(symbols)} ETFs...")
        
        batch_size = 1000
        change_map = {}
        
        for idx in range(0, len(symbols), batch_size):
            batch = symbols[idx:idx+batch_size]
            try:
                data = yf.download(batch, period="2d", progress=False, threads=False)
                if 'Close' in data:
                    close_df = data['Close']
                    if isinstance(close_df, pd.Series):
                        close_df = close_df.to_frame()
                    if len(close_df) >= 2:
                        changes = (close_df.iloc[-1] - close_df.iloc[-2]) / close_df.iloc[-2] * 100
                        change_map.update(changes.to_dict())
                time.sleep(1.0)
            except Exception as e:
                print(f"Error fetching price changes for batch starting at {idx}: {e}")
                
        feed_df['Daily_Change'] = feed_df['Symbol'].map(change_map)
        
    return feed_df

def generate_dashboard(df, output_path="index.html"):
    """Generates a self-contained static HTML dashboard with search, pagination, and sorting."""
    records = []
    for _, row in df.iterrows():
        assets = row['Total_Assets']
        assets_val = float(assets) if pd.notna(assets) else None
        
        change = row.get('Daily_Change')
        change_val = float(change) if pd.notna(change) else None
        
        records.append({
            'Symbol': str(row['Symbol']),
            'Name': str(row['Name']),
            'Inception_Date': str(row['Inception_Date']),
            'Total_Assets': assets_val,
            'Daily_Change': change_val
        })
        
    json_data = json.dumps(records)
    sync_time_str = datetime.now().strftime('%Y-%m-%d %I:%M %p')
    
    html_template = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ETF Market Feed Dashboard</title>
    <style>
        :root {
            --bg-primary: #0a0e17;
            --bg-secondary: #121824;
            --bg-card: rgba(18, 24, 36, 0.7);
            --border: rgba(255, 255, 255, 0.08);
            --border-hover: rgba(255, 255, 255, 0.16);
            --text-primary: #f3f4f6;
            --text-secondary: #9ca3af;
            --accent: #3b82f6;
            --accent-hover: #60a5fa;
            --accent-bg: rgba(59, 130, 246, 0.1);
            --shadow: 0 4px 30px rgba(0, 0, 0, 0.4);
            --success: #10b981;
            --danger: #f87171;
            --glass-bg: rgba(18, 24, 36, 0.5);
        }
        [data-theme="light"] {
            --bg-primary: #f8fafc;
            --bg-secondary: #ffffff;
            --bg-card: rgba(255, 255, 255, 0.8);
            --border: rgba(0, 0, 0, 0.08);
            --border-hover: rgba(0, 0, 0, 0.16);
            --text-primary: #0f172a;
            --text-secondary: #64748b;
            --accent: #2563eb;
            --accent-hover: #1d4ed8;
            --accent-bg: rgba(37, 99, 235, 0.08);
            --shadow: 0 4px 20px rgba(0, 0, 0, 0.05);
            --success: #059669;
            --danger: #dc2626;
            --glass-bg: rgba(255, 255, 255, 0.5);
        }

        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }

        body {
            font-family: 'Inter', sans-serif;
            background-color: var(--bg-primary);
            color: var(--text-primary);
            padding: 2rem;
            min-height: 100vh;
            transition: background-color 0.3s, color 0.3s;
            line-height: 1.5;
        }

        .container {
            max-width: 1400px;
            margin: 0 auto;
            display: flex;
            flex-direction: column;
            gap: 2rem;
        }

        header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            border-bottom: 1px solid var(--border);
            padding-bottom: 1.5rem;
        }

        .logo-area h1 {
            font-size: 2rem;
            font-weight: 700;
            background: linear-gradient(135deg, var(--accent) 0%, var(--accent-hover) 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }

        .logo-area p {
            font-size: 0.95rem;
            color: var(--text-secondary);
            margin-top: 0.25rem;
        }

        .theme-toggle-btn {
            background: var(--bg-card);
            border: 1px solid var(--border);
            color: var(--text-primary);
            padding: 0.6rem;
            border-radius: 50%;
            cursor: pointer;
            display: flex;
            align-items: center;
            justify-content: center;
            transition: all 0.2s ease;
            box-shadow: var(--shadow);
        }

        .theme-toggle-btn:hover {
            border-color: var(--accent);
            transform: scale(1.05);
        }

        .theme-toggle-btn svg {
            transition: transform 0.3s ease;
        }

        .theme-toggle-btn:hover svg {
            transform: rotate(15deg);
        }

        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
            gap: 1.5rem;
        }

        .stat-card {
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 1.5rem;
            box-shadow: var(--shadow);
            backdrop-filter: blur(12px);
            -webkit-backdrop-filter: blur(12px);
            display: flex;
            flex-direction: column;
            gap: 0.5rem;
            transition: transform 0.2s ease, border-color 0.2s ease;
        }

        .stat-card:hover {
            transform: translateY(-2px);
            border-color: var(--border-hover);
        }

        .stat-label {
            font-size: 0.85rem;
            font-weight: 500;
            color: var(--text-secondary);
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }

        .stat-value {
            font-size: 2rem;
            font-weight: 700;
            color: var(--text-primary);
        }

        .stat-desc {
            font-size: 0.8rem;
            color: var(--text-secondary);
        }

        .controls-card {
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 1.5rem;
            box-shadow: var(--shadow);
            backdrop-filter: blur(12px);
            display: flex;
            flex-wrap: wrap;
            justify-content: space-between;
            align-items: center;
            gap: 1.5rem;
        }

        .search-wrapper {
            position: relative;
            flex: 1;
            min-width: 300px;
        }

        .search-input {
            width: 100%;
            background: var(--bg-primary);
            border: 1px solid var(--border);
            color: var(--text-primary);
            padding: 0.75rem 1rem 0.75rem 2.5rem;
            border-radius: 8px;
            font-size: 0.95rem;
            transition: all 0.2s ease;
        }

        .search-input:focus {
            outline: none;
            border-color: var(--accent);
            box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.15);
        }

        .search-icon {
            position: absolute;
            left: 0.85rem;
            top: 50%;
            transform: translateY(-50%);
            color: var(--text-secondary);
            pointer-events: none;
            width: 16px;
            height: 16px;
        }

        .page-size-selector {
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }

        .control-label {
            font-size: 0.9rem;
            color: var(--text-secondary);
            font-weight: 500;
        }

        .size-btn-group {
            display: flex;
            background: var(--bg-primary);
            border: 1px solid var(--border);
            border-radius: 8px;
            padding: 2px;
        }

        .size-btn {
            background: transparent;
            border: none;
            color: var(--text-secondary);
            padding: 0.5rem 1rem;
            font-size: 0.875rem;
            font-weight: 600;
            border-radius: 6px;
            cursor: pointer;
            transition: all 0.2s ease;
        }

        .size-btn:hover {
            color: var(--text-primary);
        }

        .size-btn.active {
            background: var(--accent);
            color: #ffffff;
            box-shadow: 0 2px 8px rgba(59, 130, 246, 0.3);
        }

        .table-card {
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: 12px;
            box-shadow: var(--shadow);
            backdrop-filter: blur(12px);
            overflow: hidden;
            display: flex;
            flex-direction: column;
        }

        .table-wrapper {
            overflow-x: auto;
            max-height: 600px;
        }

        table {
            width: 100%;
            border-collapse: collapse;
            text-align: left;
            font-size: 0.95rem;
        }

        th {
            background: var(--bg-secondary);
            padding: 1rem 1.5rem;
            font-weight: 600;
            font-size: 0.85rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            color: var(--text-secondary);
            border-bottom: 1px solid var(--border);
            position: sticky;
            top: 0;
            z-index: 10;
            cursor: pointer;
            user-select: none;
            transition: background-color 0.2s ease, color 0.2s ease;
        }

        th:hover {
            background: var(--border);
            color: var(--text-primary);
        }

        .sort-icon {
            display: inline-block;
            margin-left: 0.4rem;
            width: 12px;
            height: 12px;
            vertical-align: middle;
            transition: transform 0.2s ease;
        }

        td {
            padding: 1rem 1.5rem;
            border-bottom: 1px solid var(--border);
            transition: background-color 0.15s ease;
            vertical-align: middle;
        }

        tr:last-child td {
            border-bottom: none;
        }

        tr:hover td {
            background: rgba(59, 130, 246, 0.03);
        }

        .col-symbol {
            font-weight: 700;
        }

        .ticker-badge {
            display: inline-block;
            background: var(--accent-bg);
            color: var(--accent);
            padding: 0.25rem 0.6rem;
            border-radius: 6px;
            text-decoration: none;
            border: 1px solid rgba(59, 130, 246, 0.15);
            transition: all 0.2s ease;
        }

        .ticker-badge:hover {
            background: var(--accent);
            color: #ffffff;
            border-color: var(--accent);
            box-shadow: 0 2px 8px rgba(59, 130, 246, 0.2);
            transform: translateY(-1px);
        }

        .col-name {
            color: var(--text-primary);
            max-width: 400px;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }

        .col-date {
            text-align: center;
            font-family: monospace;
            color: var(--text-secondary);
        }

        .col-change {
            text-align: right;
            font-weight: 600;
            font-family: monospace;
        }

        .change-pos {
            color: var(--success);
        }

        .change-neg {
            color: var(--danger);
        }

        .change-zero {
            color: var(--text-secondary);
        }

        .col-assets {
            text-align: right;
            font-weight: 600;
        }

        .footer-controls {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 1.25rem 1.5rem;
            border-top: 1px solid var(--border);
            flex-wrap: wrap;
            gap: 1rem;
        }

        .info-text {
            font-size: 0.9rem;
            color: var(--text-secondary);
        }

        .pagination {
            display: flex;
            gap: 0.35rem;
            align-items: center;
        }

        .page-btn {
            background: var(--bg-primary);
            border: 1px solid var(--border);
            color: var(--text-secondary);
            min-width: 36px;
            height: 36px;
            padding: 0 0.5rem;
            border-radius: 8px;
            font-weight: 600;
            font-size: 0.875rem;
            cursor: pointer;
            display: flex;
            align-items: center;
            justify-content: center;
            transition: all 0.2s ease;
            user-select: none;
        }

        .page-btn:hover:not(.disabled):not(.active) {
            border-color: var(--accent);
            color: var(--text-primary);
        }

        .page-btn.active {
            background: var(--accent);
            color: #ffffff;
            border-color: var(--accent);
            box-shadow: 0 2px 8px rgba(59, 130, 246, 0.3);
        }

        .page-btn.disabled {
            opacity: 0.4;
            cursor: not-allowed;
        }

        .pagination-dots {
            padding: 0 0.25rem;
            color: var(--text-secondary);
            font-weight: 600;
        }

        .empty-state {
            padding: 4rem 2rem;
            text-align: center;
            color: var(--text-secondary);
        }

        .empty-state h3 {
            font-size: 1.25rem;
            font-weight: 600;
            color: var(--text-primary);
            margin-bottom: 0.5rem;
        }

        @media (max-width: 768px) {
            body {
                padding: 1rem;
            }
            .controls-card {
                flex-direction: column;
                align-items: stretch;
            }
            .search-wrapper {
                min-width: 100%;
            }
            .footer-controls {
                flex-direction: column;
                align-items: center;
                text-align: center;
            }
            th, td {
                padding: 0.75rem 1rem;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <div class="logo-area">
                <h1>ETF Market Insights</h1>
                <p>Real-time tracked and verified exchange traded funds</p>
            </div>
            <button class="theme-toggle-btn" id="theme-toggle" aria-label="Toggle dark/light theme">
            </button>
        </header>

        <div class="stats-grid">
            <div class="stat-card">
                <span class="stat-label">Total ETFs</span>
                <span class="stat-value" id="stat-total-etfs">-</span>
                <span class="stat-desc">Excluding 2x/3x, bull/bear & ProShares UltraShort funds</span>
            </div>
            <div class="stat-card">
                <span class="stat-label">Combined AUM</span>
                <span class="stat-value" id="stat-total-assets">-</span>
                <span class="stat-desc">Assets Under Management</span>
            </div>
            <div class="stat-card">
                <span class="stat-label">Average ETF Size</span>
                <span class="stat-value" id="stat-avg-assets">-</span>
                <span class="stat-desc">Per fund average</span>
            </div>
            <div class="stat-card">
                <span class="stat-label">Cache Sync Status</span>
                <span class="stat-value" id="stat-sync-time" style="font-size: 1.2rem; margin-top: 0.6rem; margin-bottom: 0.4rem;">-</span>
                <span class="stat-desc" style="display: flex; align-items: center; gap: 0.35rem;">
                    <span style="display: inline-block; width: 8px; height: 8px; background: var(--success); border-radius: 50%;"></span>
                    Database fully synced
                </span>
            </div>
        </div>

        <div class="controls-card">
            <div class="search-wrapper">
                <svg class="search-icon" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="8"></circle><line x1="21" y1="21" x2="16.65" y2="16.65"></line></svg>
                <input type="text" class="search-input" id="search-input" placeholder="Search by symbol or name..." aria-label="Search ETFs">
            </div>

            <div class="page-size-selector">
                <span class="control-label">Show:</span>
                <div class="size-btn-group" id="page-size-buttons">
                    <button class="size-btn active" data-size="25">25</button>
                    <button class="size-btn" data-size="50">50</button>
                    <button class="size-btn" data-size="100">100</button>
                    <button class="size-btn" data-size="all">All</button>
                </div>
            </div>
        </div>

        <div class="table-card">
            <div class="table-wrapper">
                <table id="etf-table">
                    <thead>
                        <tr>
                            <th style="width: 12%;" onclick="sortData('Symbol')">
                                Symbol<span class="sort-indicator" id="sort-Symbol"></span>
                            </th>
                            <th style="width: 43%;" onclick="sortData('Name')">
                                Name<span class="sort-indicator" id="sort-Name"></span>
                            </th>
                            <th style="width: 15%; text-align: center;" onclick="sortData('Inception_Date')">
                                Inception Date<span class="sort-indicator" id="sort-Inception_Date"></span>
                            </th>
                            <th style="width: 15%; text-align: right;" onclick="sortData('Daily_Change')">
                                Change %<span class="sort-indicator" id="sort-Daily_Change"></span>
                            </th>
                            <th style="width: 15%; text-align: right;" onclick="sortData('Total_Assets')">
                                Total Assets<span class="sort-indicator" id="sort-Total_Assets"></span>
                            </th>
                        </tr>
                    </thead>
                    <tbody id="table-body">
                    </tbody>
                </table>
                <div id="empty-state" class="empty-state" style="display: none;">
                    <h3>No ETFs Found</h3>
                    <p>Try refining your search terms.</p>
                </div>
            </div>

            <div class="footer-controls">
                <div class="info-text" id="info-text">
                    Showing 0 to 0 of 0 entries
                </div>
                <div class="pagination" id="pagination">
                </div>
            </div>
        </div>
    </div>

    <script>
        const etfData = {{ETF_DATA_JSON}};
        const syncTimeStr = "{{SYNC_TIME_STR}}";
    </script>

    <script>
        const themeToggle = document.getElementById("theme-toggle");
        
        const sunIcon = `<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="5"></circle><line x1="12" y1="1" x2="12" y2="3"></line><line x1="12" y1="21" x2="12" y2="23"></line><line x1="4.22" y1="4.22" x2="5.64" y2="5.64"></line><line x1="18.36" y1="18.36" x2="19.78" y2="19.78"></line><line x1="1" y1="12" x2="3" y2="12"></line><line x1="21" y1="12" x2="23" y2="12"></line><line x1="4.22" y1="19.78" x2="5.64" y2="18.36"></line><line x1="18.36" y1="5.64" x2="19.78" y2="4.22"></line></svg>`;
        const moonIcon = `<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"></path></svg>`;

        const savedTheme = localStorage.getItem("theme") || "dark";
        document.body.setAttribute("data-theme", savedTheme);
        themeToggle.innerHTML = savedTheme === "light" ? moonIcon : sunIcon;

        themeToggle.onclick = () => {
            const currentTheme = document.body.getAttribute("data-theme");
            const newTheme = currentTheme === "dark" ? "light" : "dark";
            document.body.setAttribute("data-theme", newTheme);
            themeToggle.innerHTML = newTheme === "light" ? moonIcon : sunIcon;
            localStorage.setItem("theme", newTheme);
        };

        function formatUSD(num) {
            if (num === null || num === undefined || isNaN(num) || num === 0) return '—';
            if (num >= 1e12) return '$' + (num / 1e12).toFixed(2) + 'T';
            if (num >= 1e9) return '$' + (num / 1e9).toFixed(2) + 'B';
            if (num >= 1e6) return '$' + (num / 1e6).toFixed(2) + 'M';
            return '$' + num.toLocaleString();
        }

        function formatChange(val) {
            if (val === null || val === undefined || isNaN(val)) return '<span class="change-zero">—</span>';
            const formatted = Math.abs(val).toFixed(2) + '%';
            if (val > 0) {
                return `<span class="change-pos">▲ +${formatted}</span>`;
            } else if (val < 0) {
                return `<span class="change-neg">▼ -${formatted}</span>`;
            } else {
                return `<span class="change-zero">0.00%</span>`;
            }
        }

        let filteredData = [...etfData];
        let currentPage = 1;
        let pageSize = 25;
        let sortColumn = "Inception_Date";
        let sortDirection = "desc";

        function initStats() {
            document.getElementById("stat-total-etfs").innerText = etfData.length.toLocaleString();
            
            const totalAUM = etfData.reduce((sum, item) => sum + (item.Total_Assets || 0), 0);
            document.getElementById("stat-total-assets").innerText = formatUSD(totalAUM);
            
            const validAssetsItems = etfData.filter(item => item.Total_Assets > 0);
            const avgAUM = validAssetsItems.length > 0 ? (totalAUM / validAssetsItems.length) : 0;
            document.getElementById("stat-avg-assets").innerText = formatUSD(avgAUM);
            
            document.getElementById("stat-sync-time").innerText = syncTimeStr;
        }

        function renderTable() {
            const tbody = document.getElementById("table-body");
            const emptyState = document.getElementById("empty-state");
            const etfTable = document.getElementById("etf-table");
            tbody.innerHTML = "";

            if (filteredData.length === 0) {
                emptyState.style.display = "block";
                etfTable.style.display = "none";
                document.getElementById("info-text").innerText = "Showing 0 to 0 of 0 entries";
                document.getElementById("pagination").innerHTML = "";
                return;
            }

            emptyState.style.display = "none";
            etfTable.style.display = "table";

            const totalEntries = filteredData.length;
            const startIdx = pageSize === "all" ? 0 : (currentPage - 1) * pageSize;
            const endIdx = pageSize === "all" ? totalEntries : Math.min(totalEntries, startIdx + pageSize);
            
            const pagedData = filteredData.slice(startIdx, endIdx);

            pagedData.forEach(item => {
                const tr = document.createElement("tr");
                
                const tdSymbol = document.createElement("td");
                tdSymbol.className = "col-symbol";
                tdSymbol.innerHTML = `<a href="https://finviz.com/quote.ashx?t=${item.Symbol}" target="_blank" class="ticker-badge" title="Analyze ${item.Symbol} on Finviz">${item.Symbol}</a>`;
                
                const tdName = document.createElement("td");
                tdName.className = "col-name";
                tdName.innerText = item.Name || "—";
                tdName.title = item.Name || "";

                const tdDate = document.createElement("td");
                tdDate.className = "col-date";
                tdDate.innerText = item.Inception_Date || "—";

                const tdChange = document.createElement("td");
                tdChange.className = "col-change";
                tdChange.innerHTML = formatChange(item.Daily_Change);

                const tdAssets = document.createElement("td");
                tdAssets.className = "col-assets";
                const formattedAssets = formatUSD(item.Total_Assets);
                tdAssets.innerText = formattedAssets;
                if (item.Total_Assets) {
                    tdAssets.title = `$${item.Total_Assets.toLocaleString()}`;
                }

                tr.appendChild(tdSymbol);
                tr.appendChild(tdName);
                tr.appendChild(tdDate);
                tr.appendChild(tdChange);
                tr.appendChild(tdAssets);
                tbody.appendChild(tr);
            });

            const displayStart = totalEntries === 0 ? 0 : startIdx + 1;
            const displayEnd = endIdx;
            document.getElementById("info-text").innerText = `Showing ${displayStart.toLocaleString()} to ${displayEnd.toLocaleString()} of ${totalEntries.toLocaleString()} entries`;

            renderPagination(totalEntries);
            updateSortHeaders();
        }

        function renderPagination(totalEntries) {
            const paginationContainer = document.getElementById("pagination");
            paginationContainer.innerHTML = "";

            const totalPages = pageSize === "all" ? 1 : Math.ceil(totalEntries / pageSize);
            if (totalPages <= 1) return;

            const addButton = (page, text, active = false, disabled = false) => {
                const btn = document.createElement("button");
                btn.className = `page-btn${active ? ' active' : ''}${disabled ? ' disabled' : ''}`;
                btn.innerText = text;
                if (!disabled && !active) {
                    btn.onclick = () => {
                        currentPage = page;
                        renderTable();
                        document.querySelector(".table-wrapper").scrollTop = 0;
                    };
                }
                paginationContainer.appendChild(btn);
            };

            addButton(1, "«", false, currentPage === 1);
            addButton(currentPage - 1, "‹", false, currentPage === 1);

            const maxVisible = 5;
            let startPage = Math.max(1, currentPage - 2);
            let endPage = Math.min(totalPages, currentPage + 2);

            if (startPage > 1) {
                addButton(1, "1");
                if (startPage > 2) {
                    const span = document.createElement("span");
                    span.className = "pagination-dots";
                    span.innerText = "...";
                    paginationContainer.appendChild(span);
                }
            }

            for (let i = startPage; i <= endPage; i++) {
                addButton(i, i.toString(), i === currentPage);
            }

            if (endPage < totalPages) {
                if (endPage < totalPages - 1) {
                    const span = document.createElement("span");
                    span.className = "pagination-dots";
                    span.innerText = "...";
                    paginationContainer.appendChild(span);
                }
                addButton(totalPages, totalPages.toString());
            }

            addButton(currentPage + 1, "›", false, currentPage === totalPages);
            addButton(totalPages, "»", false, currentPage === totalPages);
        }

        function sortData(column) {
            if (sortColumn === column) {
                sortDirection = sortDirection === "asc" ? "desc" : "asc";
            } else {
                sortColumn = column;
                sortDirection = (column === "Symbol" || column === "Name") ? "asc" : "desc";
            }
            applyFilterAndSort();
        }

        function updateSortHeaders() {
            const columns = ["Symbol", "Name", "Inception_Date", "Daily_Change", "Total_Assets"];
            columns.forEach(col => {
                const el = document.getElementById(`sort-${col}`);
                if (!el) return;
                
                if (sortColumn === col) {
                    el.innerHTML = sortDirection === "asc" 
                        ? ` <svg class="sort-icon" xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"><polyline points="18 15 12 9 6 15"></polyline></svg>`
                        : ` <svg class="sort-icon" xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"><polyline points="6 9 12 15 18 9"></polyline></svg>`;
                    el.style.opacity = 1;
                } else {
                    el.innerHTML = ` <svg class="sort-icon" style="opacity: 0.3;" xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="6 9 12 15 18 9"></polyline></svg>`;
                    el.style.opacity = 0.5;
                }
            });
        }

        function applyFilterAndSort() {
            const query = document.getElementById("search-input").value.trim().toLowerCase();

            if (query) {
                filteredData = etfData.filter(item => {
                    const sym = (item.Symbol || "").toLowerCase();
                    const name = (item.Name || "").toLowerCase();
                    return sym.includes(query) || name.includes(query);
                });
            } else {
                filteredData = [...etfData];
            }

            filteredData.sort((a, b) => {
                let valA = a[sortColumn];
                let valB = b[sortColumn];

                if (valA === null || valA === undefined || valA === "") {
                    return 1;
                }
                if (valB === null || valB === undefined || valB === "") {
                    return -1;
                }

                if (typeof valA === "string") {
                    return sortDirection === "asc"
                        ? valA.localeCompare(valB)
                        : valB.localeCompare(valA);
                } else {
                    return sortDirection === "asc"
                        ? valA - valB
                        : valB - valA;
                }
            });

            currentPage = 1;
            renderTable();
        }

        document.getElementById("search-input").oninput = () => {
            applyFilterAndSort();
        };

        const sizeButtons = document.querySelectorAll("#page-size-buttons .size-btn");
        sizeButtons.forEach(btn => {
            btn.onclick = () => {
                sizeButtons.forEach(b => b.classList.remove("active"));
                btn.classList.add("active");
                
                const val = btn.getAttribute("data-size");
                pageSize = val === "all" ? "all" : parseInt(val);
                currentPage = 1;
                renderTable();
            };
        });

        initStats();
        applyFilterAndSort();
    </script>
</body>
</html>"""
    
    html_content = html_template.replace("{{ETF_DATA_JSON}}", json_data).replace("{{SYNC_TIME_STR}}", sync_time_str)
    
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_content)

# Execute the pipeline
final_feed = build_cached_etf_feed()
print(f"\nFinal feed contains {len(final_feed)} non-blacklisted ETFs.")
if not final_feed.empty:
    print("Generating HTML Dashboard...")
    generate_dashboard(final_feed)
    print("Dashboard generated successfully as 'index.html'.")
else:
    print("Feed is empty. Dashboard not generated.")
print(final_feed.head())