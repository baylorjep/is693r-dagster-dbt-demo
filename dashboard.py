#!/usr/bin/env python3
"""
Bidi Contracting - Estimation Intelligence Dashboard

A simple dashboard that showcases the power of the cleaned/transformed data.
This dashboard queries LIVE data from the DuckDB warehouse created by the pipeline.

Run with: python dashboard.py
Opens at: http://localhost:8888
"""

import http.server
import socketserver
import webbrowser
from pathlib import Path

import duckdb

# Configuration
PORT = 8888
DB_PATH = Path(__file__).parent / "warehouse" / "analytics.duckdb"


def get_dashboard_data():
    """Query DuckDB and return dashboard metrics."""
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    
    data = {}
    
    # Portfolio Overview
    data["portfolio"] = conn.execute("""
        SELECT 
            COUNT(*) as total_projects,
            ROUND(SUM(estimate_total_mid) / 1000000, 1) as portfolio_value_millions,
            ROUND(AVG(avg_confidence), 3) as avg_ai_confidence,
            SUM(takeoff_count) as total_takeoffs
        FROM mart_estimation_dashboard
        WHERE estimate_total_mid > 0
    """).fetchone()
    
    # Projects needing attention
    data["attention_projects"] = conn.execute("""
        SELECT 
            project_name,
            ROUND(estimate_total_mid / 1000000, 2) as estimate_millions,
            ROUND(avg_confidence, 2) as confidence,
            open_qa_issues,
            readiness_status
        FROM mart_estimation_dashboard
        WHERE readiness_status = 'Needs Review' OR open_qa_issues > 2
        ORDER BY estimate_total_mid DESC
        LIMIT 5
    """).fetchall()
    
    # Top cost drivers
    data["cost_drivers"] = conn.execute("""
        SELECT 
            division_name,
            item_type,
            COUNT(*) as occurrences,
            ROUND(SUM(extended_cost_mid) / 1000000, 2) as total_cost_millions
        FROM fct_takeoffs
        GROUP BY division_name, item_type
        ORDER BY SUM(extended_cost_mid) DESC
        LIMIT 5
    """).fetchall()
    
    # AI performance by discipline
    data["ai_performance"] = conn.execute("""
        SELECT 
            discipline_name,
            COUNT(*) as takeoffs,
            ROUND(AVG(confidence), 3) as avg_confidence,
            ROUND(SUM(CASE WHEN is_low_confidence THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as pct_needs_review
        FROM fct_takeoffs
        GROUP BY discipline_name
        ORDER BY avg_confidence DESC
    """).fetchall()
    
    # Readiness breakdown
    data["readiness"] = conn.execute("""
        SELECT 
            readiness_status,
            COUNT(*) as count
        FROM mart_estimation_dashboard
        GROUP BY readiness_status
        ORDER BY count DESC
    """).fetchall()
    
    # Recent activity (simulated from estimates)
    data["recent_activity"] = conn.execute("""
        SELECT 
            p.project_name,
            e.estimation_method,
            e.generated_at::DATE as activity_date
        FROM fct_estimates e
        JOIN stg_projects p ON e.project_id = p.project_id
        ORDER BY e.generated_at DESC
        LIMIT 4
    """).fetchall()
    
    conn.close()
    return data


def generate_html(data):
    """Generate the dashboard HTML styled like the Bidi site."""
    
    portfolio = data["portfolio"]
    
    # Build attention projects table
    attention_rows = ""
    for p in data["attention_projects"]:
        status_badge = f'<span class="badge badge-warning">{p[4]}</span>' if p[4] == "Needs Review" else f'<span class="badge badge-default">{p[4]}</span>'
        attention_rows += f"""
            <tr>
                <td class="project-name">{p[0]}</td>
                <td>${p[1]}M</td>
                <td>{p[2]}</td>
                <td>{p[3]}</td>
                <td>{status_badge}</td>
            </tr>
        """
    
    # Build cost drivers table
    cost_rows = ""
    for c in data["cost_drivers"]:
        cost_rows += f"""
            <tr>
                <td>{c[0]}</td>
                <td>{c[1]}</td>
                <td>{c[2]:,}</td>
                <td class="cost-value">${c[3]}M</td>
            </tr>
        """
    
    # Build AI performance table
    ai_rows = ""
    for a in data["ai_performance"]:
        conf_class = "conf-high" if a[2] >= 0.85 else ("conf-med" if a[2] >= 0.75 else "conf-low")
        ai_rows += f"""
            <tr>
                <td>{a[0]}</td>
                <td>{a[1]:,}</td>
                <td class="{conf_class}">{a[2]}</td>
                <td>{a[3]}%</td>
            </tr>
        """
    
    # Build readiness breakdown
    readiness_items = ""
    icons = {"Ready": "✓", "Needs Review": "⚠", "No Estimate": "○", "In Progress": "◐"}
    colors = {"Ready": "#10b981", "Needs Review": "#f97316", "No Estimate": "#9ca3af", "In Progress": "#3b82f6"}
    for r in data["readiness"]:
        icon = icons.get(r[0], "●")
        color = colors.get(r[0], "#6b7280")
        readiness_items += f'<div class="stat-item"><span class="stat-value" style="color:{color}">{r[1]}</span><span class="stat-label">{r[0]}</span></div>'
    
    # Build activity feed
    activity_items = ""
    for a in data["recent_activity"]:
        method_badge = "AI" if a[1] == "ai" else ("Hybrid" if a[1] == "hybrid" else "Manual")
        activity_items += f"""
            <div class="activity-item">
                <div class="activity-icon">📋</div>
                <div class="activity-content">
                    <div class="activity-title">Estimate Generated</div>
                    <div class="activity-desc">{a[0]} ({method_badge})</div>
                </div>
            </div>
        """
    
    html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Bidi Contracting - Estimation Intelligence</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Saira+Stencil+One&display=swap" rel="stylesheet">
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        
        :root {{
            --orange-500: #f97316;
            --orange-600: #ea580c;
            --gray-50: #f9fafb;
            --gray-100: #f3f4f6;
            --gray-200: #e5e7eb;
            --gray-300: #d1d5db;
            --gray-400: #9ca3af;
            --gray-500: #6b7280;
            --gray-600: #4b5563;
            --gray-700: #374151;
            --gray-800: #1f2937;
            --gray-900: #111827;
            --green-500: #10b981;
            --red-500: #ef4444;
        }}
        
        body {{
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            background: var(--gray-50);
            color: var(--gray-900);
            min-height: 100vh;
        }}
        
        /* Header - Bidi style */
        .header {{
            background: white;
            border-bottom: 1px solid var(--gray-200);
            padding: 1rem 2rem;
            display: flex;
            align-items: center;
            justify-content: space-between;
            position: sticky;
            top: 0;
            z-index: 100;
        }}
        
        .logo {{
            display: flex;
            align-items: center;
            gap: 0.75rem;
        }}
        
        .logo-icon {{
            width: 36px;
            height: auto;
            display: flex;
            align-items: center;
            justify-content: center;
        }}
        
        .logo-icon svg {{
            width: 36px;
            height: auto;
        }}
        
        .logo-text {{
            font-family: 'Saira Stencil One', sans-serif;
            font-size: 1.75rem;
            font-weight: 400;
            color: var(--gray-900);
            letter-spacing: 0.02em;
        }}
        
        .logo-badge {{
            background: var(--orange-500);
            color: white;
            font-size: 0.65rem;
            font-weight: 600;
            padding: 0.2rem 0.5rem;
            border-radius: 4px;
            text-transform: uppercase;
        }}
        
        .header-nav {{
            display: flex;
            gap: 2rem;
        }}
        
        .header-nav a {{
            color: var(--gray-600);
            text-decoration: none;
            font-size: 0.95rem;
            font-weight: 500;
            transition: color 0.2s;
        }}
        
        .header-nav a:hover, .header-nav a.active {{
            color: var(--gray-900);
        }}
        
        /* Main content */
        .main {{
            max-width: 1400px;
            margin: 0 auto;
            padding: 2rem;
        }}
        
        /* Welcome section */
        .welcome {{
            margin-bottom: 2rem;
        }}
        
        .welcome h1 {{
            font-size: 1.75rem;
            font-weight: 600;
            color: var(--gray-900);
            margin-bottom: 0.5rem;
        }}
        
        .welcome p {{
            color: var(--gray-500);
        }}
        
        /* KPI Cards */
        .kpi-grid {{
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 1.5rem;
            margin-bottom: 2rem;
        }}
        
        .kpi-card {{
            background: white;
            border-radius: 12px;
            padding: 1.5rem;
            border: 1px solid var(--gray-200);
            transition: box-shadow 0.2s, transform 0.2s;
        }}
        
        .kpi-card:hover {{
            box-shadow: 0 4px 12px rgba(0,0,0,0.08);
            transform: translateY(-2px);
        }}
        
        .kpi-value {{
            font-size: 2.25rem;
            font-weight: 700;
            color: var(--orange-500);
            margin-bottom: 0.25rem;
        }}
        
        .kpi-label {{
            color: var(--gray-500);
            font-size: 0.875rem;
            font-weight: 500;
        }}
        
        /* Two column layout */
        .grid-2 {{
            display: grid;
            grid-template-columns: 2fr 1fr;
            gap: 1.5rem;
            margin-bottom: 1.5rem;
        }}
        
        .grid-equal {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 1.5rem;
        }}
        
        /* Cards */
        .card {{
            background: white;
            border-radius: 12px;
            border: 1px solid var(--gray-200);
            overflow: hidden;
        }}
        
        .card-header {{
            padding: 1.25rem 1.5rem;
            border-bottom: 1px solid var(--gray-100);
            display: flex;
            align-items: center;
            justify-content: space-between;
        }}
        
        .card-header h2 {{
            font-size: 1rem;
            font-weight: 600;
            color: var(--gray-900);
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }}
        
        .card-header h2 .icon {{
            font-size: 1.25rem;
        }}
        
        .card-body {{
            padding: 1rem 1.5rem;
        }}
        
        /* Tables */
        table {{
            width: 100%;
            border-collapse: collapse;
        }}
        
        th, td {{
            padding: 0.875rem 1rem;
            text-align: left;
        }}
        
        th {{
            color: var(--gray-500);
            font-weight: 500;
            font-size: 0.75rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            background: var(--gray-50);
        }}
        
        td {{
            color: var(--gray-700);
            font-size: 0.9rem;
            border-bottom: 1px solid var(--gray-100);
        }}
        
        tr:last-child td {{
            border-bottom: none;
        }}
        
        .project-name {{
            font-weight: 500;
            color: var(--gray-900);
        }}
        
        .cost-value {{
            font-weight: 600;
            color: var(--gray-900);
        }}
        
        /* Badges */
        .badge {{
            display: inline-block;
            padding: 0.25rem 0.75rem;
            border-radius: 9999px;
            font-size: 0.75rem;
            font-weight: 600;
        }}
        
        .badge-warning {{
            background: #fef3c7;
            color: #d97706;
        }}
        
        .badge-success {{
            background: #d1fae5;
            color: #059669;
        }}
        
        .badge-default {{
            background: var(--gray-100);
            color: var(--gray-600);
        }}
        
        /* Confidence colors */
        .conf-high {{ color: var(--green-500); font-weight: 600; }}
        .conf-med {{ color: #f59e0b; font-weight: 600; }}
        .conf-low {{ color: var(--red-500); font-weight: 600; }}
        
        /* Stats grid */
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 1rem;
            padding: 1rem;
        }}
        
        .stat-item {{
            text-align: center;
            padding: 1rem;
            background: var(--gray-50);
            border-radius: 8px;
        }}
        
        .stat-value {{
            font-size: 1.75rem;
            font-weight: 700;
            display: block;
        }}
        
        .stat-label {{
            font-size: 0.75rem;
            color: var(--gray-500);
            margin-top: 0.25rem;
            display: block;
        }}
        
        /* Activity feed */
        .activity-feed {{
            padding: 0;
        }}
        
        .activity-item {{
            display: flex;
            gap: 1rem;
            padding: 1rem 1.5rem;
            border-bottom: 1px solid var(--gray-100);
        }}
        
        .activity-item:last-child {{
            border-bottom: none;
        }}
        
        .activity-icon {{
            width: 36px;
            height: 36px;
            background: #fff7ed;
            border-radius: 8px;
            display: flex;
            align-items: center;
            justify-content: center;
            flex-shrink: 0;
        }}
        
        .activity-title {{
            font-weight: 500;
            color: var(--gray-900);
            font-size: 0.9rem;
        }}
        
        .activity-desc {{
            color: var(--gray-500);
            font-size: 0.8rem;
            margin-top: 0.125rem;
        }}
        
        /* Footer */
        .footer {{
            text-align: center;
            padding: 2rem;
            color: var(--gray-400);
            font-size: 0.875rem;
        }}
        
        .pipeline-badge {{
            display: inline-flex;
            align-items: center;
            gap: 0.5rem;
            background: var(--gray-900);
            color: white;
            padding: 0.5rem 1rem;
            border-radius: 9999px;
            font-size: 0.8rem;
            font-weight: 500;
            margin-top: 0.75rem;
        }}
        
        /* Responsive */
        @media (max-width: 900px) {{
            .kpi-grid {{ grid-template-columns: repeat(2, 1fr); }}
            .grid-2 {{ grid-template-columns: 1fr; }}
            .grid-equal {{ grid-template-columns: 1fr; }}
        }}
        
        @media (max-width: 600px) {{
            .kpi-grid {{ grid-template-columns: 1fr; }}
            .stats-grid {{ grid-template-columns: repeat(2, 1fr); }}
            .header-nav {{ display: none; }}
        }}
    </style>
</head>
<body>
    <header class="header">
        <div class="logo">
            <div class="logo-icon">
                <svg width="66" height="93" viewBox="0 0 66 93" fill="none" xmlns="http://www.w3.org/2000/svg">
                    <g filter="url(#filter0_d_24_3)">
                        <path d="M41.9067 28.8966L33.2871 34L22.9505 28.5L32.7129 23L41.9067 28.8966Z" fill="white"/>
                        <path d="M33.8614 0L62 17L41.9067 29.5L32.9915 23.9035L24.1349 29L4 18L33.8614 0Z" fill="#1E1D1E"/>
                        <path d="M33.2871 64.5V58.5L4.57426 42V47.5L33.2871 64.5Z" fill="#F1AD6F"/>
                        <path d="M33.2871 85V63.9459L4.57426 47V68.0541L33.2871 85Z" fill="#EB5023"/>
                        <path d="M4.57426 42L33.2871 58.5V34L4 18L4.57426 42Z" fill="#404042"/>
                        <path d="M33.2871 85V64L62 47V68.5L33.2871 85Z" fill="#F58D22"/>
                        <path d="M62 41.5L33.2871 58.3667V64L62 47.6333V41.5Z" fill="#F1CDA2"/>
                        <path d="M62 42L33.2871 58.5V34L62 17V42Z" fill="#777878"/>
                    </g>
                    <defs>
                        <filter id="filter0_d_24_3" x="0" y="0" width="66" height="93" filterUnits="userSpaceOnUse" color-interpolation-filters="sRGB">
                            <feFlood flood-opacity="0" result="BackgroundImageFix"/>
                            <feColorMatrix in="SourceAlpha" type="matrix" values="0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 127 0" result="hardAlpha"/>
                            <feOffset dy="4"/>
                            <feGaussianBlur stdDeviation="2"/>
                            <feComposite in2="hardAlpha" operator="out"/>
                            <feColorMatrix type="matrix" values="0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0.25 0"/>
                            <feBlend mode="normal" in2="BackgroundImageFix" result="effect1_dropShadow_24_3"/>
                            <feBlend mode="normal" in="SourceGraphic" in2="effect1_dropShadow_24_3" result="shape"/>
                        </filter>
                    </defs>
                </svg>
            </div>
            <span class="logo-text">BIDI</span>
            <span class="logo-badge">Analytics</span>
        </div>
        <nav class="header-nav">
            <a href="#" class="active">Dashboard</a>
            <a href="#">Projects</a>
            <a href="#">Estimates</a>
            <a href="#">Reports</a>
        </nav>
    </header>
    
    <main class="main">
        <div class="welcome">
            <h1>Estimation Intelligence Dashboard</h1>
            <p>Real-time insights from your blueprint takeoff pipeline</p>
        </div>
        
        <div class="kpi-grid">
            <div class="kpi-card">
                <div class="kpi-value">{portfolio[0]}</div>
                <div class="kpi-label">Active Projects</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-value">${portfolio[1]}M</div>
                <div class="kpi-label">Portfolio Value (Mid)</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-value">{portfolio[2]}</div>
                <div class="kpi-label">AI Confidence Score</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-value">{portfolio[3]:,}</div>
                <div class="kpi-label">Takeoff Items Processed</div>
            </div>
        </div>
        
        <div class="grid-2">
            <div class="card">
                <div class="card-header">
                    <h2><span class="icon">⚠️</span> Needs Attention</h2>
                </div>
                <table>
                    <thead>
                        <tr>
                            <th>Project</th>
                            <th>Estimate</th>
                            <th>Confidence</th>
                            <th>QA Issues</th>
                            <th>Status</th>
                        </tr>
                    </thead>
                    <tbody>
                        {attention_rows if attention_rows else '<tr><td colspan="5" style="text-align:center;color:var(--gray-400);padding:2rem;">All projects healthy! ✓</td></tr>'}
                    </tbody>
                </table>
            </div>
            
            <div class="card">
                <div class="card-header">
                    <h2><span class="icon">📊</span> Activity Feed</h2>
                </div>
                <div class="activity-feed">
                    {activity_items}
                </div>
            </div>
        </div>
        
        <div class="grid-equal">
            <div class="card">
                <div class="card-header">
                    <h2><span class="icon">💰</span> Top Cost Drivers</h2>
                </div>
                <table>
                    <thead>
                        <tr>
                            <th>Division</th>
                            <th>Item Type</th>
                            <th>Count</th>
                            <th>Total Cost</th>
                        </tr>
                    </thead>
                    <tbody>
                        {cost_rows}
                    </tbody>
                </table>
            </div>
            
            <div class="card">
                <div class="card-header">
                    <h2><span class="icon">🤖</span> AI Performance by Discipline</h2>
                </div>
                <table>
                    <thead>
                        <tr>
                            <th>Discipline</th>
                            <th>Takeoffs</th>
                            <th>Confidence</th>
                            <th>Review %</th>
                        </tr>
                    </thead>
                    <tbody>
                        {ai_rows}
                    </tbody>
                </table>
            </div>
        </div>
        
        <div class="card" style="margin-top: 1.5rem;">
            <div class="card-header">
                <h2><span class="icon">📈</span> Project Readiness Overview</h2>
            </div>
            <div class="stats-grid">
                {readiness_items}
            </div>
        </div>
    </main>
    
    <footer class="footer">
        <p>Powered by the Bidi Estimation Pipeline</p>
        <div class="pipeline-badge">
            ⚡ Dagster + dbt + DuckDB
        </div>
        <p style="margin-top: 1rem;">
            From raw blueprints to actionable intelligence — this is modern data engineering.
        </p>
    </footer>
</body>
</html>
"""
    return html


class DashboardHandler(http.server.SimpleHTTPRequestHandler):
    """Custom handler that serves the dashboard."""
    
    def do_GET(self):
        if self.path == "/" or self.path == "/index.html":
            try:
                data = get_dashboard_data()
                html = generate_html(data)
                
                self.send_response(200)
                self.send_header("Content-type", "text/html")
                self.send_header("Content-Length", len(html))
                self.end_headers()
                self.wfile.write(html.encode())
            except Exception as e:
                self.send_error(500, f"Error generating dashboard: {e}")
        else:
            self.send_error(404, "Not found")
    
    def log_message(self, format, *args):
        # Suppress logging for cleaner output
        pass


def main():
    print("\n" + "=" * 60)
    print("  BIDI CONTRACTING - ESTIMATION INTELLIGENCE DASHBOARD")
    print("=" * 60)
    print(f"\n  Starting dashboard server...")
    print(f"  Open in browser: http://localhost:{PORT}")
    print(f"\n  Press Ctrl+C to stop\n")
    
    # Open browser automatically
    webbrowser.open(f"http://localhost:{PORT}")
    
    with socketserver.TCPServer(("", PORT), DashboardHandler) as httpd:
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\n  Dashboard stopped.")


if __name__ == "__main__":
    main()
