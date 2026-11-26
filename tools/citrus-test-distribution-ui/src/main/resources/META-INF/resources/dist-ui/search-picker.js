/*
 * Copyright the original author or authors.
 * Licensed under the Apache License, Version 2.0.
 */

import { html } from './vendor/htm-mini.js';

// Keep the open dialog outside the dashboard render tree. Its option snapshot, input,
// caret and scroll position stay together while telemetry replaces the underlying view.
export function openPicker({ trigger, title, prompt, options, selected, recentLimit, onSelect }) {
  const dialog = html`<dialog class="search-picker" aria-labelledby="picker-title">
    <header><h2 id="picker-title">${title}</h2>
      <button type="button" class="btn" aria-label="Close picker" onClick=${() => dialog.close()}>×</button>
    </header>
    <input type="search" class="search-input" role="combobox" autocomplete="off" autofocus
        aria-label=${prompt} placeholder=${prompt} aria-expanded="true"
        aria-controls="picker-options" aria-autocomplete="list" />
    <div id="picker-options" role="listbox" aria-label=${title}></div>
    <p class="picker-empty" role="status"></p>
  </dialog>`;
  const input = dialog.querySelector('input');
  const list = dialog.querySelector('[role=listbox]');
  const message = dialog.querySelector('[role=status]');
  let matches = [];
  let active = -1;

  function highlight(index) {
    list.children[active]?.classList.remove('highlighted');
    active = index;
    const row = list.children[index];
    if (row) {
      row.classList.add('highlighted');
      input.setAttribute('aria-activedescendant', row.id);
      row.scrollIntoView({ block: 'nearest' });
    } else {
      input.removeAttribute('aria-activedescendant');
    }
  }

  function choose(option) {
    dialog.close();
    onSelect(option.key);
  }

  function filter() {
    const query = input.value.trim().toLowerCase();
    matches = options.filter(o => o.pinned || !query || [o.label, o.detail, o.commit]
      .some(value => value?.toLowerCase().includes(query)));
    if (!query && recentLimit) matches = matches.slice(0, recentLimit);
    active = -1;
    list.replaceChildren(...matches.map((o, i) => html`
      <div role="option" id=${'picker-option-' + i} aria-selected=${String(o.key === selected)}
           class=${o.pinned ? 'picker-option pinned' : 'picker-option'} onClick=${() => choose(o)}>
        <div><strong>${o.label}</strong> ${o.status}
          ${o.key === selected ? html`<span aria-hidden="true">✓</span>` : null}
        </div>
        ${o.detail ? html`<small>${o.detail}</small>` : null}
      </div>`));
    message.textContent = matches.every(o => o.pinned) ? 'No matches' : '';
    const first = query ? matches.findIndex(o => !o.pinned) : matches.findIndex(o => o.key === selected);
    highlight(matches.length ? Math.max(0, first) : -1);
  }

  input.addEventListener('input', filter);
  input.addEventListener('keydown', e => {
    if (e.isComposing) return;
    if (e.key === 'Escape') {
      e.preventDefault();
      e.stopPropagation();
      dialog.close();
      return;
    }
    if (!matches.length) return;
    if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
      e.preventDefault();
      highlight(Math.max(0, Math.min(active + (e.key === 'ArrowDown' ? 1 : -1), matches.length - 1)));
    } else if (e.key === 'Enter') {
      e.preventDefault();
      choose(matches[active]);
    }
  });
  const dismiss = () => dialog.close();
  window.addEventListener('hashchange', dismiss);
  dialog.addEventListener('close', () => {
    window.removeEventListener('hashchange', dismiss);
    dialog.remove();
    document.querySelector(`[data-focus-id="${trigger}"]`)?.focus({ preventScroll: true });
  }, { once: true });
  document.body.append(dialog);
  dialog.showModal();
  filter();
}
