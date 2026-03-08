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
with open('_data/rankings_data.json', 'r', encoding='utf-8') as f:
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

# Break ties in ppfy by faculty count (more faculty = better rank)
df['faculty_count'] = df['school'].map(faculty).fillna(0).astype(int)
df = df.sort_values(['window', 'ppfy', 'faculty_count'], ascending=[True, False, False])
df['rank'] = df.groupby('window').cumcount() + 1
window_order = [f'{s}-{e}' for s, e in WINDOWS]
latest_window = window_order[-1]

avg_ppfy_all = (
    df.groupby('school', as_index=False)['ppfy']
      .mean()
      .sort_values('ppfy', ascending=False)
)
all_schools = avg_ppfy_all['school'].tolist()
interactive_df = df[df['school'].isin(all_schools)].copy()
max_rank_all = int(interactive_df['rank'].max())

latest_top5 = (
    interactive_df[interactive_df['window'] == latest_window]
    .sort_values('ppfy', ascending=False)
    .head(5)['school']
    .tolist()
)
JHU_LABEL = 'Johns Hopkins (Carey)'
highlight_all = set(latest_top5)
if JHU_LABEL in all_schools:
    highlight_all.add(JHU_LABEL)

# Static PNG remains a simple bump chart for reference.
fig, ax = plt.subplots(figsize=(15, 10), dpi=160)
fig.patch.set_facecolor('white')
ax.set_facecolor('#f7f9fc')
plot_df = interactive_df.copy()
for school in all_schools:
    s = plot_df[plot_df['school'] == school].sort_values(['start', 'end'])
    if school in highlight_all:
        ax.plot(range(len(s)), s['rank'], linewidth=2.4, marker='o', markersize=4.5, alpha=0.95)
    else:
        ax.plot(range(len(s)), s['rank'], linewidth=0.9, marker='o', markersize=2.3, color='#c8d0dc', alpha=0.4)
ax.set_xticks(range(len(window_order)))
ax.set_xticklabels(window_order, fontsize=9)
ax.set_ylim(max_rank_all + 0.5, 0.5)
ax.set_ylabel('Rank', fontsize=11)
ax.set_xlabel('Rolling windows', fontsize=11)
ax.grid(axis='y', color='#d7dee9', linewidth=0.8)
ax.grid(axis='x', color='#e8edf5', linewidth=0.5)
for spine in ['top', 'right']:
    ax.spines[spine].set_visible(False)
out_png = 'data_work/top25_rolling_faculty_productivity_bump.png'
out_csv = 'data_work/top25_rolling_faculty_productivity_values.csv'
out_html = 'data_work/top25_rolling_faculty_productivity_interactive.html'
fig.savefig(out_png, bbox_inches='tight')
plt.close(fig)

pivot = interactive_df.pivot_table(index='school', columns='window', values='ppfy')
pivot = pivot.reindex(all_schools)
pivot.to_csv(out_csv)

# ── School-inspired palette (bright enough for dark bg) ─────
# UCLA gold, MIT cardinal red, WashU green, Duke blue, UTD orange
school_colors = {
    'UCLA (Anderson)':       '#FFD100',   # UCLA gold
    'MIT':                   '#E63946',   # MIT cardinal red
    'WashU (Olin)':          '#22B573',   # WashU green
    'Duke (Fuqua)':          '#4B8FE2',   # Duke blue
    'UT Dallas':             '#E87A1D',   # UTD comet orange
    'Johns Hopkins (Carey)': '#3AADCF',   # Hopkins heritage blue
}
color_map_all = {}
for i, school in enumerate(latest_top5):
    color_map_all[school] = school_colors.get(school, '#FFB400')
if JHU_LABEL in highlight_all:
    color_map_all[JHU_LABEL] = school_colors.get(JHU_LABEL, '#3AADCF')

plot_data = []
for school in all_schools:
    s = interactive_df[interactive_df['school'] == school].sort_values(['start', 'end'])
    color = color_map_all.get(school, '#3a4560')
    is_highlighted = school in highlight_all
    plot_data.append({
        'type': 'scatter',
        'mode': 'lines+markers',
        'name': school,
        'x': s['window'].tolist(),
        'y': s['rank'].tolist(),
        'customdata': s['ppfy'].round(4).tolist(),
        'line': {'color': color, 'width': 3.0 if is_highlighted else 0.8},
        'marker': {'size': 7 if is_highlighted else 3, 'color': color},
        'opacity': 1.0 if is_highlighted else 0.2,
        'hovertemplate': (
            '<b>%{fullData.name}</b><br>'
            'Window: %{x}<br>'
            'Rank: %{y}<br>'
            'Papers/Faculty/Year: %{customdata:.3f}<extra></extra>'
        ),
        'showlegend': False,
    })

# ── Chart layout constants ──────────────────────────────────
LABEL_H = 21
MARGIN = {'l': 55, 'r': 80, 't': 90, 'b': 45}
# Stretch vertically as the school list grows so sidebar labels remain aligned
# with the plotted rank positions instead of getting compressed.
CHART_H = max(1200, MARGIN['t'] + MARGIN['b'] + len(all_schools) * (LABEL_H + 3) + 40)

layout = {
    'template': 'plotly_dark',
    'paper_bgcolor': '#1a1f2e',
    'plot_bgcolor': '#1e2538',
    'title': {
        'text': 'OM Research Output (OR/MS/MSOM papers) per research-track faculty',
        'font': {'size': 15, 'color': '#e8edf5', 'family': 'Georgia, serif'},
        'x': 0.02, 'xanchor': 'left', 'y': 0.985, 'yanchor': 'top',
    },
    'xaxis': {
        'title': {'text': '', 'font': {'size': 1}},
        'fixedrange': True,
        'categoryorder': 'array',
        'categoryarray': window_order,
        'range': [-0.2, len(window_order) - 1 + 0.22],
        'showgrid': True,
        'gridcolor': 'rgba(255,255,255,0.06)',
        'gridwidth': 1,
        'zeroline': False,
        'showline': True,
        'linecolor': '#2d3654',
        'linewidth': 1,
        'tickfont': {'size': 11, 'color': '#7b8aaa'},
    },
    'yaxis': {
        'title': {'text': 'Rank', 'font': {'size': 12, 'color': '#7b8aaa'}},
        'autorange': False,
        'tick0': 1,
        'dtick': 5,
        'range': [max_rank_all + 0.5, 0.5],
        'fixedrange': True,
        'showgrid': True,
        'gridcolor': 'rgba(255,255,255,0.05)',
        'gridwidth': 1,
        'zeroline': False,
        'showline': False,
        'tickfont': {'size': 11, 'color': '#7b8aaa'},
    },
    'showlegend': False,
    'hovermode': 'closest',
    'margin': MARGIN,
}

# ── Position-aligned sidebar ────────────────────────────────
PLOT_H = CHART_H - MARGIN['t'] - MARGIN['b']
Y_TOP_VAL = 0.5                    # rank value at top of plot
Y_BOT_VAL = max_rank_all + 0.5     # rank value at bottom of plot (matches y-axis)

sidebar_schools = []
for school in all_schools:
    rec = interactive_df[(interactive_df['school'] == school) & (interactive_df['window'] == latest_window)]
    if rec.empty:
        continue
    latest_rank = float(rec['rank'].iloc[0])
    frac = (latest_rank - Y_TOP_VAL) / (Y_BOT_VAL - Y_TOP_VAL)
    ideal_y = MARGIN['t'] + frac * PLOT_H
    sidebar_schools.append({
        'school': school,
        'rank': latest_rank,
        'highlight': school in highlight_all,
        'color': color_map_all.get(school, '#5a6b88') if school in highlight_all else '#5a6b88',
        'ideal_y': round(ideal_y, 1),
    })

sidebar_schools.sort(key=lambda x: (x['ideal_y'], x['school']))

# Collision avoidance
positions = []
for i, s in enumerate(sidebar_schools):
    y = s['ideal_y'] - LABEL_H / 2  # center label on ideal position
    if i > 0 and y < positions[-1] + LABEL_H:
        y = positions[-1] + LABEL_H
    positions.append(y)

# Second pass: push up from bottom if overflow
max_allowed = CHART_H - 5
for i in range(len(positions) - 1, -1, -1):
    if positions[i] + LABEL_H > max_allowed:
        positions[i] = max_allowed - LABEL_H
    if i < len(positions) - 1 and positions[i] + LABEL_H > positions[i + 1]:
        positions[i] = positions[i + 1] - LABEL_H

# Clamp all positions
for i in range(len(positions)):
    positions[i] = max(2, min(positions[i], CHART_H - LABEL_H - 2))

for i, s in enumerate(sidebar_schools):
    s['y_pos'] = round(positions[i], 1)

# ── HTML generation ─────────────────────────────────────────
html = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>OM Research Output (OR/MS/MSOM papers) per research-track faculty</title>
  <link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
</head>
<body style="margin:0; font-family: 'DM Sans', -apple-system, sans-serif; background:#1a1f2e;">
  <div style="display:flex; width:100%; height:{CHART_H}px;">
    <div id="chart" style="flex:1 1 auto; min-width:0; height:{CHART_H}px;"></div>
    <div id="school-list" style="position:relative; width:240px; height:{CHART_H}px; overflow:hidden; border-left:1px solid #2a3350; background:#161b28; box-sizing:border-box;"></div>
  </div>
  <script>
    const data = {pyjson.dumps(plot_data)};
    const layout = {pyjson.dumps(layout)};
    const sidebarSchools = {pyjson.dumps(sidebar_schools)};
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
      const schoolList = document.getElementById('school-list');
      const lineTraceIdx = data
        .map((t, i) => (t.mode && t.mode.indexOf('lines') !== -1 ? i : -1))
        .filter(i => i >= 0);
      const lineData = lineTraceIdx.map(i => data[i]);
      const baseLineColors = lineData.map(t => (t.line ? t.line.color : null));
      const baseLineWidths = lineData.map(t => (t.line ? t.line.width : null));
      const baseMarkerColors = lineData.map(t => t.marker.color);
      const baseMarkerSizes = lineData.map(t => t.marker.size);
      const baseOpacities = lineData.map(t => t.opacity);
      const schoolToLineIdx = new Map(lineTraceIdx.map((traceIdx, i) => [data[traceIdx].name, i]));
      const labelNodes = new Map();

      function resetStyles() {{
        Plotly.restyle(chart, {{
          'line.color': baseLineColors,
          'line.width': baseLineWidths,
          'marker.color': baseMarkerColors,
          'marker.size': baseMarkerSizes,
          'opacity': baseOpacities
        }}, lineTraceIdx);
        labelNodes.forEach(function(node, school) {{
          const meta = sidebarSchools.find(x => x.school === school);
          const nameEl = node.querySelector('.school-name');
          const rankEl = node.querySelector('.rank-badge');
          if (nameEl) {{
            nameEl.style.color = meta.highlight ? meta.color : '#4a5578';
            nameEl.style.fontWeight = meta.highlight ? '600' : '400';
          }}
          if (rankEl) {{
            rankEl.style.color = meta.highlight ? meta.color : '#3a4560';
          }}
          node.style.background = 'transparent';
        }});
      }}

      function highlightSchool(schoolName) {{
        const localIdx = schoolToLineIdx.get(schoolName);
        if (localIdx === undefined) return;
        const lineColors = baseLineColors.slice();
        const lineWidths = baseLineWidths.slice();
        const markerColors = baseMarkerColors.slice();
        const markerSizes = baseMarkerSizes.slice();
        const opacities = baseOpacities.slice();

        lineColors[localIdx] = '#ffffff';
        markerColors[localIdx] = '#ffffff';
        lineWidths[localIdx] = Math.max(lineWidths[localIdx], 4.0);
        markerSizes[localIdx] = Math.max(markerSizes[localIdx], 9);
        opacities[localIdx] = 1.0;

        Plotly.restyle(chart, {{
          'line.color': lineColors,
          'line.width': lineWidths,
          'marker.color': markerColors,
          'marker.size': markerSizes,
          'opacity': opacities
        }}, lineTraceIdx);

        const node = labelNodes.get(schoolName);
        if (node) {{
          const nameEl = node.querySelector('.school-name');
          const rankEl = node.querySelector('.rank-badge');
          if (nameEl) {{ nameEl.style.color = '#ffffff'; nameEl.style.fontWeight = '700'; }}
          if (rankEl) {{ rankEl.style.color = '#ffffff'; }}
          node.style.background = 'rgba(255,255,255,0.08)';
        }}
      }}

      /* ── Build positioned sidebar labels ── */
      sidebarSchools.forEach(function(meta) {{
        const rankStr = '#' + Math.round(meta.rank);
        const dotColor = meta.highlight ? meta.color : '#3a4560';
        const item = document.createElement('div');
        item.style.position = 'absolute';
        item.style.top = meta.y_pos + 'px';
        item.style.left = '0';
        item.style.right = '0';
        item.style.height = '21px';
        item.style.display = 'flex';
        item.style.alignItems = 'center';
        item.style.padding = '0 8px';
        item.style.boxSizing = 'border-box';
        item.style.borderRadius = '4px';
        item.style.cursor = 'default';
        item.style.transition = 'background 100ms ease';
        item.style.overflow = 'hidden';
        item.innerHTML = (
          '<span style="display:inline-block;width:6px;height:6px;border-radius:50%;background:' + dotColor + ';margin-right:6px;flex-shrink:0;"></span>' +
          '<span class="school-name" style="font-size:' + (meta.highlight ? '12.5' : '11.5') + 'px;font-weight:' + (meta.highlight ? '600' : '400') + ';color:' + (meta.highlight ? meta.color : '#4a5578') + ';flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">' + meta.school + '</span>' +
          '<span class="rank-badge" style="font-size:10.5px;font-weight:600;color:' + (meta.highlight ? meta.color : '#3a4560') + ';margin-left:4px;flex-shrink:0;">' + rankStr + '</span>'
        );
        item.addEventListener('mouseenter', function() {{
          resetStyles();
          highlightSchool(meta.school);
        }});
        item.addEventListener('mouseleave', function() {{
          resetStyles();
        }});
        schoolList.appendChild(item);
        labelNodes.set(meta.school, item);
      }});

      chart.on('plotly_hover', function(evt) {{
        if (!evt || !evt.points || !evt.points.length) return;
        const idx = evt.points[0].curveNumber;
        const trace = data[idx];
        if (!trace || !trace.name) return;
        resetStyles();
        highlightSchool(trace.name);
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
print('Latest-window top 5 (2022-2026):')
for s in latest_top5:
    print(f' - {s}')
print('Top 25 schools (avg across rolling windows):')
for i, s in enumerate(avg_ppfy_all.head(25)['school'].tolist(), 1):
    print(f'{i:2d}. {s}')
