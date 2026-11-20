/*
 * Copyright the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const SVG_NS = 'http://www.w3.org/2000/svg';
const SVG_TAGS = new Set([
  'svg', 'g', 'circle', 'ellipse', 'line', 'path', 'polygon',
  'polyline', 'rect', 'text', 'tspan', 'defs', 'linearGradient',
  'radialGradient', 'stop', 'clipPath', 'mask', 'pattern', 'title'
]);

const MODE_SLASH = 0;
const MODE_TEXT = 1;
const MODE_WHITESPACE = 2;
const MODE_TAGNAME = 3;
const MODE_COMMENT = 4;
const MODE_PROP_SET = 5;
const MODE_PROP_APPEND = 6;

const CHILD_APPEND = 0;
const CHILD_RECURSE = 2;
const TAG_SET = 3;
const PROPS_ASSIGN = 4;
const PROP_SET = 5;
const PROP_APPEND = 6;

const TEMPLATE_CACHE = new WeakMap();

function evaluate(h, built, fields, args) {
  built[0] = 0;
  for (let i = 1; i < built.length; i++) {
    const type = built[i++];
    // commit(i) stores 1-based field indices (1..fields.length) so 0 remains a literal sentinel
    const value = built[i] ? ((built[0] |= type ? 1 : 2), fields[built[i++] - 1]) : built[++i];
    if (type === TAG_SET) {
      args[0] = value;
    } else if (type === PROPS_ASSIGN) {
      args[1] = Object.assign(args[1] || {}, value);
    } else if (type === PROP_SET) {
      (args[1] = args[1] || {})[built[++i]] = value;
    } else if (type === PROP_APPEND) {
      args[1][built[++i]] += String(value ?? '');
    } else if (type) {
      args.push(h.apply(value, evaluate(h, value, fields, ['', null])));
    } else {
      args.push(value);
    }
  }
  return args;
}

function build(statics) {
  let mode = MODE_TEXT;
  let buffer = '';
  let quote = '';
  let current = [0];
  let char;
  let propName;

  const commit = field => {
    if (mode === MODE_TEXT && (field || (buffer = buffer.replace(/^\s*\n\s*|\s*\n\s*$/g, '')))) {
      current.push(CHILD_APPEND, field, buffer);
    } else if (mode === MODE_TAGNAME && (field || buffer)) {
      current.push(TAG_SET, field, buffer);
      mode = MODE_WHITESPACE;
    } else if (mode === MODE_WHITESPACE && buffer === '...' && field) {
      current.push(PROPS_ASSIGN, field, 0);
    } else if (mode === MODE_WHITESPACE && buffer && !field) {
      current.push(PROP_SET, 0, true, buffer);
    } else if (mode >= MODE_PROP_SET) {
      if (buffer || (!field && mode === MODE_PROP_SET)) {
        current.push(mode, 0, buffer, propName);
        mode = MODE_PROP_APPEND;
      }
      if (field) {
        current.push(mode, field, 0, propName);
        mode = MODE_PROP_APPEND;
      }
    }
    buffer = '';
  };

  for (let i = 0; i < statics.length; i++) {
    if (i) {
      if (mode === MODE_TEXT) {
        commit();
      }
      commit(i);
    }
    for (let j = 0; j < statics[i].length; j++) {
      char = statics[i][j];
      if (mode === MODE_TEXT) {
        if (char === '<') {
          commit();
          current = [current];
          mode = MODE_TAGNAME;
        } else {
          buffer += char;
        }
      } else if (mode === MODE_COMMENT) {
        if (buffer === '--' && char === '>') {
          mode = MODE_TEXT;
          buffer = '';
        } else {
          buffer = char + buffer[0];
        }
      } else if (quote) {
        if (char === quote) {
          quote = '';
        } else {
          buffer += char;
        }
      } else if (char === '"' || char === "'") {
        quote = char;
      } else if (char === '>') {
        commit();
        mode = MODE_TEXT;
      } else if (!mode) {
        // Ignore characters inside closing tag until '>'
      } else if (char === '=') {
        mode = MODE_PROP_SET;
        propName = buffer;
        buffer = '';
      } else if (char === '/' && (mode < MODE_PROP_SET || statics[i][j + 1] === '>')) {
        commit();
        if (mode === MODE_TAGNAME) {
          current = current[0];
        }
        const child = current;
        (current = current[0]).push(CHILD_RECURSE, 0, child);
        mode = MODE_SLASH;
      } else if (char === ' ' || char === '\t' || char === '\n' || char === '\r') {
        commit();
        mode = MODE_WHITESPACE;
      } else {
        buffer += char;
      }
      if (mode === MODE_TAGNAME && buffer === '!--') {
        mode = MODE_COMMENT;
        current = current[0];
      }
    }
  }
  commit();
  return current;
}

function appendChildValue(parent, child) {
  if (child === null || child === undefined || child === false || child === true) {
    return;
  }
  if (Array.isArray(child)) {
    for (const item of child) {
      appendChildValue(parent, item);
    }
    return;
  }
  if (child instanceof Node) {
    parent.appendChild(child);
    return;
  }
  parent.appendChild(document.createTextNode(String(child)));
}

export function h(tag, props, ...children) {
  if (!tag) {
    const fragment = document.createDocumentFragment();
    for (const child of children) {
      appendChildValue(fragment, child);
    }
    return fragment;
  }
  if (typeof tag === 'function') {
    return tag(Object.assign({}, props, { children }));
  }

  const isSvg = SVG_TAGS.has(tag);
  const el = isSvg ? document.createElementNS(SVG_NS, tag) : document.createElement(tag);

  let deferredValue;
  if (props) {
    for (const [key, val] of Object.entries(props)) {
      if (val === undefined || val === null || val === false) {
        continue;
      }
      if (key === 'key') {
        el.dataset.key = String(val);
      } else if (key.startsWith('on') && typeof val === 'function') {
        const eventName = key.slice(2).toLowerCase();
        el.addEventListener(eventName, val);
      } else if (key === 'className' || key === 'class') {
        if (isSvg) {
          el.setAttribute('class', String(val));
        } else {
          el.className = String(val);
        }
      } else if (key === 'style' && typeof val === 'object') {
        Object.assign(el.style, val);
      } else if (!isSvg && key === 'value') {
        deferredValue = String(val);
      } else if (!isSvg && (key === 'checked' || key === 'disabled' || key === 'selected' || key === 'autofocus')) {
        el[key] = Boolean(val);
      } else if (val === true) {
        el.setAttribute(key, '');
      } else {
        el.setAttribute(key, String(val));
      }
    }
  }

  for (const child of children) {
    appendChildValue(el, child);
  }

  if (deferredValue !== undefined) {
    el.value = deferredValue;
  }

  return el;
}

export function html(statics, ...fields) {
  let built = TEMPLATE_CACHE.get(statics);
  if (!built) {
    built = build(statics);
    TEMPLATE_CACHE.set(statics, built);
  }
  const evaluated = evaluate(h, built, fields, []);
  const fragment = document.createDocumentFragment();
  if (evaluated.length > 1) {
    for (const item of evaluated) {
      appendChildValue(fragment, item);
    }
    return fragment;
  }
  if (evaluated.length === 1) {
    const single = evaluated[0];
    if (single instanceof Node) {
      return single;
    }
    appendChildValue(fragment, single);
  }
  return fragment;
}

export function escapeHtml(value) {
  if (value === null || value === undefined) {
    return '';
  }
  return String(value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

export function formatMs(ms) {
  if (!ms || ms <= 0) {
    return '0ms';
  }
  if (ms < 1000) {
    return `${ms}ms`;
  }
  if (ms < 60000) {
    return `${(ms / 1000).toFixed(1)}s`;
  }
  const mins = Math.floor(ms / 60000);
  const secs = ((ms % 60000) / 1000).toFixed(0);
  return `${mins}m ${secs}s`;
}
