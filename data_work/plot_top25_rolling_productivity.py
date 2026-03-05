import json
import pandas as pd
import matplotlib.pyplot as plt
import json as pyjson

WINDOWS = [
    (2016, 2020),
    (2017, 2021),
    (2018, 2022),
    (2019, 2023),
    (2020, 2024),
    (2021, 2025),
    (2022, 2026),
]

metrics = pd.read_csv('data_work/school_year_metrics_combined.csv')
with open('_data/rankings_data.json', 'r') as f:
    rankings = json.load(f)

faculty = {}
for s in rankings['schools']:
    total = int(s['faculty'].get('assistant', 0)) + int(s['faculty'].get('associate', 0)) + int(s['faculty'].get('full', 0))
    if total > 0:
        faculty[s['name']] = total

metric_cols = ['ms_personal_papers', 'or_personal_papers', 'msom_personal_papers']
rows = []
for start, end in WINDOWS:
    years = end - start + 1
    sub = metrics[(metrics['year'] >= start) & (metrics['year'] <= end)].copy()
    if sub.empty:
        continue
    grouped = sub.groupby('rankings_school')[metric_cols].sum().sum(axis=1)
    for school, personal_papers in grouped.items():
        fcount = faculty.get(school, 0)
        if fcount <= 0:
            continue
        ppfy = float(personal_papers) / (fcount * years)
        rows.append({
            'school': school,
            'window': f'{start}-{end}',
            'start': start,
            'end': end,
            'ppfy': ppfy,
        })

df = pd.DataFrame(rows)
if df.empty:
    raise SystemExit('No data available for rolling windows')

# Rank all schools each window (1 = best)
df['rank'] = df.groupby('window')['ppfy'].rank(ascending=False, method='min')

# Top 25 by average productivity across all rolling windows
top25 = (
    df.groupby('school', as_index=False)['ppfy']
      .mean()
      .sort_values('ppfy', ascending=False)
      .head(25)
)
top25_schools = top25['school'].tolist()
plot_df = df[df['school'].isin(top25_schools)].copy()
max_rank_display = int(max(25, plot_df['rank'].max()))

# For highlighting, top 8 by average productivity + JHU
JHU_LABEL = 'Johns Hopkins (Carey)'
highlight = set(top25_schools[:8])
if JHU_LABEL in top25_schools:
    highlight.add(JHU_LABEL)

window_order = [f'{s}-{e}' for s, e in WINDOWS]
window_to_x = {w: i for i, w in enumerate(window_order)}

fig, ax = plt.subplots(figsize=(15, 10), dpi=160)
fig.patch.set_facecolor('white')
ax.set_facecolor('#f7f9fc')

palette = ['#003f5c', '#2f4b7c', '#665191', '#a05195', '#d45087', '#f95d6a', '#ff7c43', '#ffa600']
color_map = {school: palette[i] for i, school in enumerate(top25_schools[:8])}
if JHU_LABEL in highlight:
    color_map[JHU_LABEL] = '#005EB8'

for school in top25_schools:
    s = plot_df[plot_df['school'] == school].copy()
    s['x'] = s['window'].map(window_to_x)
    s = s.sort_values('x')
    if school in highlight:
        ax.plot(s['x'], s['rank'], linewidth=2.4, marker='o', markersize=4.5,
                color=color_map[school], alpha=0.95, zorder=3)
    else:
        ax.plot(s['x'], s['rank'], linewidth=1.0, marker='o', markersize=2.5,
                color='#9aa4b2', alpha=0.45, zorder=1)

# Label highlighted schools at last point
for school in top25_schools:
    if school not in highlight:
        continue
    s = plot_df[(plot_df['school'] == school) & (plot_df['window'] == window_order[-1])]
    if s.empty:
        continue
    y = float(s['rank'].iloc[0])
    ax.text(len(window_order) - 1 + 0.07, y, school, va='center', ha='left',
            fontsize=9, color=color_map[school], fontweight='bold')

ax.set_xticks(range(len(window_order)))
ax.set_xticklabels(window_order, rotation=0, fontsize=9)
ax.set_xlim(-0.2, len(window_order) - 1 + 1.35)
ax.set_ylim(max_rank_display + 0.5, 0.5)  # rank 1 at top
ax.set_yticks(range(1, max_rank_display + 1, 2))
ax.set_ylabel('Rank (1 = best, among all schools)', fontsize=11)
ax.set_xlabel('Rolling 5-year horizon', fontsize=11)
ax.set_title('Top 25 Schools: Rolling 5-Year Faculty Productivity (Papers / Faculty / Year)',
             fontsize=14, fontweight='bold', pad=14)
ax.grid(axis='y', color='#d7dee9', linewidth=0.8)
ax.grid(axis='x', color='#e8edf5', linewidth=0.5)

for spine in ['top', 'right']:
    ax.spines[spine].set_visible(False)
ax.spines['left'].set_color('#9aa4b2')
ax.spines['bottom'].set_color('#9aa4b2')

plt.tight_layout()
out_png = 'data_work/top25_rolling_faculty_productivity_bump.png'
out_csv = 'data_work/top25_rolling_faculty_productivity_values.csv'
out_html = 'data_work/top25_rolling_faculty_productivity_interactive.html'
fig.savefig(out_png, bbox_inches='tight')

pivot = plot_df.pivot_table(index='school', columns='window', values='ppfy')
pivot = pivot.reindex(top25_schools)
pivot.to_csv(out_csv)

# Build interactive Plotly HTML for all schools (line hover by school/window/rank/productivity)
avg_ppfy_all = (
    df.groupby('school', as_index=False)['ppfy']
      .mean()
      .sort_values('ppfy', ascending=False)
)
all_schools = avg_ppfy_all['school'].tolist()
interactive_df = df[df['school'].isin(all_schools)].copy()
max_rank_all = int(interactive_df['rank'].max())

# Keep top-5 from latest 5-year window (2022-2026) + JHU highlighted
latest_window = f"{WINDOWS[-1][0]}-{WINDOWS[-1][1]}"
latest_top5 = (
    interactive_df[interactive_df['window'] == latest_window]
    .sort_values('ppfy', ascending=False)
    .head(5)['school']
    .tolist()
)
highlight_all = set(latest_top5)
if JHU_LABEL in all_schools:
    highlight_all.add(JHU_LABEL)
color_map_all = {school: palette[i] for i, school in enumerate(latest_top5)}
if JHU_LABEL in highlight_all:
    # Johns Hopkins primary blue (darker navy) for line, label, and endpoint marker.
    color_map_all[JHU_LABEL] = '#002D72'

plot_data = []
for school in all_schools:
    s = interactive_df[interactive_df['school'] == school].copy()
    s = s.sort_values(['start', 'end'])
    color = color_map_all.get(school, '#c7cfdb')
    width = 3.2 if school in highlight_all else 0.55
    opacity = 0.98 if school in highlight_all else 0.18
    marker_size = 7 if school in highlight_all else 0
    trace = {
        "type": "scatter",
        "mode": "lines+markers",
        "name": school,
        "x": s['window'].tolist(),
        "y": s['rank'].tolist(),
        "customdata": s['ppfy'].round(4).tolist(),
        "line": {"color": color, "width": width},
        "marker": {"size": marker_size, "color": color},
        "opacity": opacity,
        "hovertemplate": (
            "<b>%{fullData.name}</b><br>"
            "Window: %{x}<br>"
            "Rank: %{y}<br>"
            "Papers/Faculty/Year: %{customdata:.3f}<extra></extra>"
        ),
    }
    plot_data.append(trace)

# Add explicit endpoint markers (full circles) at the latest window.
endpoint_x = []
endpoint_y = []
endpoint_color = []
endpoint_size = []
for school in all_schools:
    s = interactive_df[
        (interactive_df['school'] == school) &
        (interactive_df['window'] == latest_window)
    ]
    if s.empty:
        continue
    endpoint_x.append(latest_window)
    endpoint_y.append(float(s['rank'].iloc[0]))
    c = color_map_all.get(school, '#c7cfdb')
    endpoint_color.append(c)
    endpoint_size.append(8 if school in highlight_all else 4)

plot_data.append({
    "type": "scatter",
    "mode": "markers",
    "x": endpoint_x,
    "y": endpoint_y,
    "marker": {
        "symbol": "circle",
        "size": endpoint_size,
        "color": endpoint_color,
        "line": {"width": 0}
    },
    "hoverinfo": "skip",
    "showlegend": False
})

# Right-side labels for highlighted schools at latest window (near line endpoints)
label_rows = (
    interactive_df[
        (interactive_df['window'] == latest_window) &
        (interactive_df['school'].isin(highlight_all))
    ][['school', 'rank']]
    .sort_values('rank')
)
label_points = []
min_sep = 1.0
last_y = None
for _, row in label_rows.iterrows():
    y = float(row['rank'])
    if last_y is not None and (y - last_y) < min_sep:
        y = last_y + min_sep
    label_points.append((row['school'], y))
    last_y = y

annotations = []
for school, y in label_points:
    annotations.append({
        "xref": "x",
        "yref": "y",
        "x": latest_window,
        "y": y,
        "text": school,
        "showarrow": False,
        "xanchor": "left",
        "yanchor": "middle",
        "xshift": 14,
        "font": {"family": "Arial, sans-serif", "size": 11, "color": color_map_all.get(school, "#2b2b2b")},
    })

layout = {
    "template": "plotly_white",
    "paper_bgcolor": "#f4f7fb",
    "plot_bgcolor": "#f8fafd",
    "xaxis": {
        "title": "Rolling windows",
        "fixedrange": True,
        "categoryorder": "array",
        "categoryarray": window_order,
        "range": [-0.2, len(window_order) - 1.0],
        "showgrid": True,
        "gridcolor": "#e2e8f2",
        "gridwidth": 1,
        "zeroline": False,
        "showline": False
    },
    "yaxis": {
        "title": "Rank",
        "autorange": "reversed",
        "tick0": 1,
        "dtick": 5,
        "range": [max_rank_all + 0.5, 0.8],
        "fixedrange": True,
        "showgrid": True,
        "gridcolor": "#e7edf6",
        "gridwidth": 1,
        "zeroline": False,
        "showline": False
    },
    "legend": {
        "orientation": "v",
        "x": 1.02,
        "y": 1.0,
        "font": {"size": 10},
    },
    "showlegend": False,
    "hovermode": "closest",
    "margin": {"l": 80, "r": 150, "t": 24, "b": 70},
    "annotations": annotations,
}

html = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Top 25 Rolling Faculty Productivity (Interactive)</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
</head>
<body style="margin:0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;">
  <div id="chart" style="width: 100vw; height: 100vh;"></div>
  <script>
    const data = {pyjson.dumps(plot_data)};
    const layout = {pyjson.dumps(layout)};
    const config = {{
      responsive: true,
      displaylogo: false,
      scrollZoom: false,
      doubleClick: false,
      modeBarButtonsToRemove: [
        'zoom2d', 'pan2d', 'select2d', 'lasso2d',
        'zoomIn2d', 'zoomOut2d', 'autoScale2d', 'resetScale2d'
      ]
    }};
    Plotly.newPlot('chart', data, layout, config).then(function() {{
      const chart = document.getElementById('chart');
      const traceCount = data.length;
      const lineTraceIdx = data
        .map((t, i) => (t.mode && t.mode.indexOf('lines') !== -1 ? i : -1))
        .filter(i => i >= 0);
      const lineData = lineTraceIdx.map(i => data[i]);
      const baseLineColors = lineData.map(t => (t.line ? t.line.color : null));
      const baseLineWidths = lineData.map(t => (t.line ? t.line.width : null));
      const baseMarkerColors = lineData.map(t => t.marker.color);
      const baseMarkerSizes = lineData.map(t => t.marker.size);
      const baseOpacities = lineData.map(t => t.opacity);

      function resetStyles() {{
        Plotly.restyle(chart, {{
          'line.color': baseLineColors,
          'line.width': baseLineWidths,
          'marker.color': baseMarkerColors,
          'marker.size': baseMarkerSizes,
          'opacity': baseOpacities
        }}, lineTraceIdx);
      }}

      chart.on('plotly_hover', function(evt) {{
        if (!evt || !evt.points || !evt.points.length) return;
        const idx = evt.points[0].curveNumber;
        if (idx < 0 || idx >= traceCount) return;
        if (lineTraceIdx.indexOf(idx) === -1) return;
        const lineColors = baseLineColors.slice();
        const lineWidths = baseLineWidths.slice();
        const markerColors = baseMarkerColors.slice();
        const markerSizes = baseMarkerSizes.slice();
        const opacities = baseOpacities.slice();
        const localIdx = lineTraceIdx.indexOf(idx);
        if (localIdx === -1) return;

        lineColors[localIdx] = '#1d8cf8';
        markerColors[localIdx] = '#1d8cf8';
        lineWidths[localIdx] = Math.max(lineWidths[localIdx], 4.0);
        markerSizes[localIdx] = Math.max(markerSizes[localIdx], 8);
        opacities[localIdx] = 1.0;

        Plotly.restyle(chart, {{
          'line.color': lineColors,
          'line.width': lineWidths,
          'marker.color': markerColors,
          'marker.size': markerSizes,
          'opacity': opacities
        }}, lineTraceIdx);
      }});

      chart.on('plotly_unhover', function() {{
        resetStyles();
      }});
    }});
  </script>
</body>
</html>
"""

with open(out_html, 'w', encoding='utf-8') as f:
    f.write(html)

print(out_png)
print(out_csv)
print(out_html)
print("Latest-window top 5 (2022-2026):")
for s in latest_top5:
    print(f" - {s}")
print('Top 25 schools (avg across rolling windows):')
for i, s in enumerate(top25_schools, 1):
    print(f'{i:2d}. {s}')
