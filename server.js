const http = require('http');

const PORT = process.env.PORT || 3000;
const API_KEY = process.env.API_KEY || '';

// ── Helpers ──────────────────────────────────────────
function sendJson(res, status, data) {
  res.writeHead(status, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(data));
}

function getRequestBody(req) {
  return new Promise((resolve, reject) => {
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', () => resolve(body));
    req.on('error', reject);
  });
}

// ── Discover databases from environment variables ────
function discoverDatabases() {
  const dbs = [];
  for (const key of Object.keys(process.env)) {
    const match = key.match(/^TURSO_DB_(.+)_URL$/);
    if (match) {
      const upper = match[1];
      const name = upper.toLowerCase();
      const url = process.env[key];
      const token = process.env[`TURSO_DB_${upper}_TOKEN`] || '';
      dbs.push({ name, url, token });
    }
  }
  return dbs;
}

// ── Convert libsql:// to https:// ────────────────────
function getHttpUrl(libsqlUrl) {
  return libsqlUrl.replace(/^libsql:\/\//, 'https://').replace(/\/$/, '');
}

// ── Turso HTTP API caller ────────────────────────────
async function tursoExecute(dbUrl, dbToken, sql) {
  const endpoint = getHttpUrl(dbUrl) + '/v2/pipeline';
  const body = JSON.stringify({
    requests: [
      { type: 'execute', stmt: { sql } },
      { type: 'close' }
    ]
  });
  let response;
  try {
    response = await fetch(endpoint, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${dbToken}`,
        'Content-Type': 'application/json'
      },
      body
    });
  } catch (fetchErr) {
    throw new Error(`Turso fetch failed: ${fetchErr.message}`);
  }
  const data = await response.json();
  if (!response.ok) {
    throw new Error(data.error || data.message || `HTTP ${response.status}`);
  }
  const executeResponse = data.results?.[0];
  if (executeResponse?.type === 'error') {
    throw new Error(executeResponse.error?.message || 'Turso pipeline error');
  }
  const executeResult = executeResponse?.response?.result;
  if (!executeResult) {
    throw new Error('Unexpected Turso response: ' + JSON.stringify(data).substring(0, 300));
  }
  return executeResult;
}

// ── Smart SQL splitter (unchanged) ───────────────────
function splitSqlStatements(sql) {
  const statements = [];
  let current = '';
  let inSingleQuote = false;
  let inDoubleQuote = false;
  let inBlockComment = false;
  let inLineComment = false;
  let parenDepth = 0;
  let beginDepth = 0;

  let i = 0;
  while (i < sql.length) {
    const char = sql[i];

    // Block comments
    if (!inSingleQuote && !inDoubleQuote && !inLineComment && char === '/' && sql[i + 1] === '*') {
      inBlockComment = true;
      current += '/*';
      i += 2; continue;
    }
    if (inBlockComment && char === '*' && sql[i + 1] === '/') {
      inBlockComment = false;
      current += '*/';
      i += 2; continue;
    }
    if (inBlockComment) { current += char; i++; continue; }

    // Line comments
    if (!inSingleQuote && !inDoubleQuote && !inBlockComment && char === '-' && sql[i + 1] === '-') {
      inLineComment = true;
      current += '--';
      i += 2; continue;
    }
    if (inLineComment && char === '\n') {
      inLineComment = false;
      current += char; i++; continue;
    }
    if (inLineComment) { current += char; i++; continue; }

    // Quotes
    if (char === "'" && !inDoubleQuote && !inBlockComment && !inLineComment) {
      inSingleQuote = !inSingleQuote;
    } else if (char === '"' && !inSingleQuote && !inBlockComment && !inLineComment) {
      inDoubleQuote = !inDoubleQuote;
    }

    // Parentheses
    if (!inSingleQuote && !inDoubleQuote && !inBlockComment && !inLineComment) {
      if (char === '(') parenDepth++;
      else if (char === ')') { if (parenDepth > 0) parenDepth--; }
    }

    // BEGIN/END depth
    if (!inSingleQuote && !inDoubleQuote && !inBlockComment && !inLineComment) {
      if (char === 'B' || char === 'b') {
        const sub = sql.substring(i, i + 5);
        if (/^BEGIN\b/i.test(sub) && !/[A-Za-z0-9_]/.test(sql[i + 5] || '')) beginDepth++;
      }
      if (char === 'E' || char === 'e') {
        const sub = sql.substring(i, i + 3);
        if (/^END\b/i.test(sub) && !/[A-Za-z0-9_]/.test(sql[i + 3] || '')) {
          if (beginDepth > 0) beginDepth--;
        }
      }
    }

    // Semicolon split at top level
    if (char === ';' && !inSingleQuote && !inDoubleQuote && !inBlockComment && !inLineComment && parenDepth === 0 && beginDepth === 0) {
      const trimmed = current.trim();
      if (trimmed) statements.push(trimmed);
      current = '';
      i++;
      continue;
    }

    current += char;
    i++;
  }

  const remaining = current.trim();
  if (remaining) statements.push(remaining);
  return statements.filter(s => s.length > 0);
}

// ── Request handler ──────────────────────────────────
const server = http.createServer(async (req, res) => {
  // CORS headers
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, x-api-key');

  if (req.method === 'OPTIONS') {
    res.writeHead(200);
    res.end();
    return;
  }

  let parsedUrl;
  try {
    parsedUrl = new URL(req.url, `http://${req.headers.host}`);
  } catch (e) {
    sendJson(res, 400, { error: 'Invalid URL' });
    return;
  }

  // ══════ API KEY AUTHENTICATION ══════
  if (API_KEY) {
    const providedKey = req.headers['x-api-key'] || parsedUrl.searchParams.get('api_key');
    if (providedKey !== API_KEY) {
      return sendJson(res, 401, { error: 'Unauthorized – missing or incorrect API key' });
    }
  }

  const { pathname } = parsedUrl;
  const searchParams = parsedUrl.searchParams;
  const method = req.method;

  try {
    // Root
    if (method === 'GET' && pathname === '/') {
      return sendJson(res, 200, {
        status: 'ok',
        message: 'Multi‑database server (Turso) is running',
        endpoints: {
          listDatabases: 'GET /api/databases',
          createDb: 'POST /api/database/:name',
          deleteDb: 'DELETE /api/database/:name',
          query: 'GET /api/:database/query?sql=...',
          exec: 'POST /api/:database/exec  (body: { "sql": "..." })'
        }
      });
    }

    // List databases
    if (method === 'GET' && pathname === '/api/databases') {
      const dbs = discoverDatabases().map(d => d.name);
      return sendJson(res, 200, dbs);
    }

    // Create/Delete (informational)
    const dbMatch = pathname.match(/^\/api\/database\/([^\/]+)$/);
    if (dbMatch) {
      const dbName = dbMatch[1].toLowerCase();
      if (method === 'POST') {
        return sendJson(res, 200, {
          message: 'Create the database in the Turso dashboard, then add URL & token as environment variables.'
        });
      }
      if (method === 'DELETE') {
        return sendJson(res, 200, {
          message: 'Delete the database in the Turso dashboard. Remove its environment variables from Railway.'
        });
      }
      return sendJson(res, 405, { error: 'Method not allowed' });
    }

    // Read‑only query
    const queryMatch = pathname.match(/^\/api\/([^\/]+)\/query$/);
    if (queryMatch && method === 'GET') {
      const dbName = queryMatch[1].toLowerCase();
      const sql = searchParams.get('sql');
      if (!sql) return sendJson(res, 400, { error: 'Missing ?sql= parameter' });
      if (!/^\s*SELECT\b/i.test(sql)) {
        return sendJson(res, 400, { error: 'Only SELECT queries are allowed on this endpoint' });
      }

      const dbs = discoverDatabases();
      const db = dbs.find(d => d.name === dbName);
      if (!db) return sendJson(res, 404, { error: `Database '${dbName}' not found` });

      try {
        const result = await tursoExecute(db.url, db.token, sql);
        const cols = result.cols || [];
        const rawRows = result.rows || [];
        const rows = rawRows.map(row =>
          Object.fromEntries(
            row.map((cell, idx) => [cols[idx]?.name || `col${idx}`, cell.value])
          )
        );
        return sendJson(res, 200, rows);
      } catch (err) {
        return sendJson(res, 400, { error: err.message });
      }
    }

    // Write / multi‑statement execution
    const execMatch = pathname.match(/^\/api\/([^\/]+)\/exec$/);
    if (execMatch && method === 'POST') {
      const dbName = execMatch[1].toLowerCase();
      let body;
      try { body = await getRequestBody(req); } catch { return sendJson(res, 400, { error: 'Failed to read body' }); }
      let sql;
      try { sql = JSON.parse(body).sql; } catch { return sendJson(res, 400, { error: 'Invalid JSON' }); }
      if (!sql) return sendJson(res, 400, { error: 'Missing "sql" in body' });

      const dbs = discoverDatabases();
      const db = dbs.find(d => d.name === dbName);
      if (!db) return sendJson(res, 404, { error: `Database '${dbName}' not found` });

      try {
        const statements = splitSqlStatements(sql);
        let totalChanges = 0;
        for (const stmt of statements) {
          const result = await tursoExecute(db.url, db.token, stmt);
          totalChanges += result.rows_affected || 0;
        }
        return sendJson(res, 200, { changes: totalChanges });
      } catch (err) {
        return sendJson(res, 400, { error: err.message });
      }
    }

    // Fallback
    sendJson(res, 404, { error: 'Not found' });
  } catch (err) {
    console.error('Request error:', err.message);
    sendJson(res, 500, { error: err.message });
  }
});

server.listen(PORT, () => {
  console.log(`🚀 Turso server on port ${PORT}`);
  console.log(`Databases: ${discoverDatabases().map(d => d.name).join(', ') || 'none'}`);
});
