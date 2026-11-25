/*
 * Copyright the original author or authors.
 * Licensed under the Apache License, Version 2.0.
 *
 * Historical usage chart for the Citrus Test Distribution Dashboard.
 */

import { html } from './vendor/htm-mini.js';

export function renderUsageChart(id, heading, points, metrics, timeWindow) {
  const width = 820;
  const height = 200;
  const plot = { left: 48, right: width - 32, top: 12, bottom: height - 30 };
  const step = Math.max(1, Math.ceil(Math.max(0, ...points.flatMap(point => metrics.map(metric => point[metric.key]))) / 4));
  const x = index => plot.left + index / Math.max(points.length - 1, 1) * (plot.right - plot.left);
  const y = value => plot.bottom - value / (step * 4) * (plot.bottom - plot.top);
  const axisIndexes = [...new Set([0, 0.25, 0.5, 0.75, 1].map(fraction => Math.round(fraction * (points.length - 1))))];
  const axisTime = new Intl.DateTimeFormat(undefined, timeWindow === '7d'
    ? { month: 'short', day: 'numeric' }
    : { hour: 'numeric', minute: '2-digit' });
  let selected = -1;
  const describe = index => [
    new Date(points[index].timestamp).toLocaleString(),
    ...metrics.map(metric => `${metric.label} ${points[index][metric.key]}`)
  ].join(' · ');
  const marker = html`<g class="usage-marker" visibility="hidden" aria-hidden="true">
    <line class="usage-crosshair" x1="0" x2="0" y1=${plot.top} y2=${plot.bottom}></line>
    ${metrics.map(metric => html`<circle r="4" stroke=${metric.color}></circle>`)}
  </g>`;
  const markerDots = marker.querySelectorAll('circle');
  const tooltip = html`<div class="usage-tooltip" aria-live="polite"></div>`;
  const select = (index, announce = true) => {
    if (selected === index) return;
    selected = index;
    if (index < 0) {
      marker.setAttribute('visibility', 'hidden');
      tooltip.textContent = '';
      return;
    }
    marker.setAttribute('visibility', 'visible');
    marker.setAttribute('transform', `translate(${x(index)},0)`);
    markerDots.forEach((dot, i) => dot.setAttribute('cy', y(points[index][metrics[i].key])));
    tooltip.setAttribute('aria-live', announce ? 'polite' : 'off');
    tooltip.textContent = describe(index);
  };
  return html`
    <section class="card usage-card">
      <div class="card-header">
        <h2 class="card-title" id=${`${id}-heading`}>${heading}</h2>
        ${metrics.length > 1 ? html`<ul class="usage-legend" aria-label="Chart legend">
          ${metrics.map(metric => html`
            <li><svg width="22" height="10" aria-hidden="true">
              <line x1="0" x2="22" y1="5" y2="5" stroke=${metric.color}
                stroke-width="2.5" stroke-dasharray=${metric.dash}></line>
            </svg>${metric.label}</li>
          `)}
        </ul>` : null}
      </div>
      ${points.length ? html`
        <div class="usage-chart-scroll">
          <svg class="usage-chart" viewBox=${`0 0 ${width} ${height}`} tabindex="0" role="group"
            aria-labelledby=${`${id}-heading`} aria-describedby="usage-instructions"
            data-focus-id=${id}
            onFocus=${() => { if (selected < 0) select(points.length - 1); }}
            onBlur=${() => select(-1)}
            onKeyDown=${e => {
              if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(e.key)) return;
              e.preventDefault();
              const index = e.key === 'Home' ? 0 : e.key === 'End' ? points.length - 1
                : Math.max(0, Math.min(points.length - 1, selected + (e.key === 'ArrowLeft' ? -1 : 1)));
              select(index);
            }}>
            ${Array.from({ length: 5 }, (_, i) => html`
              <g>
                <line class="usage-grid-line" x1=${plot.left} x2=${plot.right} y1=${y(i * step)} y2=${y(i * step)}></line>
                <text class="usage-axis" x=${plot.left - 8} y=${y(i * step)} dy=".35em" text-anchor="end">${i * step}</text>
              </g>
            `)}
            ${axisIndexes.map(i => html`
              <text class="usage-axis" x=${x(i)} y=${plot.bottom + 20} text-anchor="middle">${axisTime.format(points[i].timestamp)}</text>
            `)}
            ${[...metrics].reverse().map(metric => html`
              <polyline class="usage-series" fill="none" stroke=${metric.color} stroke-dasharray=${metric.dash}
                points=${points.map((point, i) => `${x(i)},${y(point[metric.key])}`).join(' ')}></polyline>
            `)}
            ${marker}
            <rect x=${plot.left} y=${plot.top} width=${plot.right - plot.left} height=${plot.bottom - plot.top} fill="transparent"
              onPointerMove=${e => {
                const bounds = e.currentTarget.getBoundingClientRect();
                select(Math.max(0, Math.min(points.length - 1,
                  Math.round((e.clientX - bounds.left) / bounds.width * (points.length - 1)))), false);
              }}
              onPointerLeave=${e => { if (document.activeElement !== e.currentTarget.ownerSVGElement) select(-1); }}></rect>
          </svg>
        </div>
        ${tooltip}
      ` : html`<div class="empty-state">No samples in this window.</div>`}
    </section>
  `;
}
