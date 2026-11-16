import React, { useEffect, useState } from 'react';
import { createRoot } from 'react-dom/client';
import './styles.css';

const base = '/ipaklink/v1/internal/analytics';

async function request(path, options) {
  const response = await fetch(base + path, { credentials: 'same-origin', ...options });
  const body = await response.json();
  return { status: response.status, data: body.data };
}

function AdminApp() {
  const [view, setView] = useState({ type: 'loading' });
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);

  async function loadDashboard(signal) {
    const result = await request('/metadata', { signal });
    if (result.status === 401) return { type: 'login' };
    if (result.status === 403) return { type: 'restricted' };
    if (result.status === 200) return { type: 'dashboard', data: result.data };
    throw new Error('后台暂时不可用，请稍后重试。');
  }

  useEffect(() => {
    const controller = new AbortController();
    loadDashboard(controller.signal).then(setView).catch(error => {
      if (error.name !== 'AbortError') setView({ type: 'unavailable' });
    });
    return () => controller.abort();
  }, []);

  async function login(event) {
    event.preventDefault();
    if (busy) return;
    setError('');
    setBusy(true);
    const form = event.currentTarget;
    const data = new FormData(form);
    const password = form.elements.namedItem('password');
    try {
      const result = await request('/sessions', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username: data.get('username').trim(), password: data.get('password') }),
      });
      if (result.status === 201) {
        password.value = '';
        setView(await loadDashboard());
      } else if (result.status === 401) {
        setError('账号或密码有误。');
      } else if (result.status === 429) {
        setError('尝试次数过多，请稍后再试。');
      } else if (result.status === 400) {
        setError('请检查账号格式后重试。');
      } else {
        setError('暂时无法登录，请稍后重试。');
      }
    } catch {
      setError('暂时无法连接后台，请稍后重试。');
    } finally {
      setBusy(false);
    }
  }

  async function logout() {
    setError('');
    setBusy(true);
    try {
      const result = await request('/sessions/current', { method: 'DELETE' });
      if (result.status !== 200) throw new Error();
      setView({ type: 'login' });
    } catch {
      setError('退出失败，请重试。');
    } finally {
      setBusy(false);
    }
  }

  return (
    <main className="shell">
      <header className="brand">ipaklink<span>埋点后台</span></header>
      <section className="panel" aria-labelledby="title">
        {view.type === 'loading' && <p role="status">正在连接后台…</p>}
        {view.type === 'unavailable' && (
          <>
            <h1 id="title">暂时无法连接后台</h1>
            <button type="button" onClick={() => window.location.reload()}>重新加载</button>
          </>
        )}
        {view.type === 'login' && (
          <>
            <h1 id="title">员工登录</h1>
            <form onSubmit={login}>
              <label htmlFor="username">员工账号</label>
              <input id="username" name="username" autoComplete="username" required maxLength={64} />
              <label htmlFor="password">密码</label>
              <input id="password" name="password" type="password" autoComplete="current-password" required maxLength={256}/>
              <button disabled={busy} type="submit">{busy ? '正在登录…' : '登录'}</button>
            </form>
          </>
        )}
        {view.type === 'restricted' && (
          <>
            <h1 id="title">当前账号暂无报表权限</h1>
            <p>请联系平台管理员开通所需的数据范围。</p>
            <button disabled={busy} type="button" onClick={logout}>退出登录</button>
          </>
        )}
        {view.type === 'dashboard' && (
          <>
            <h1 id="title">分析工作台</h1>
            <dl>
              <dt>统计时区</dt><dd>{view.data.timezone}</dd>
              <dt>当前快照</dt><dd>{view.data.snapshotId ?? '等待构建'}</dd>
            </dl>
            <button disabled={busy} type="button" onClick={logout}>退出登录</button>
          </>
        )}
        {error && <p className="error" role="alert">{error}</p>}
      </section>
    </main>
  );
}

createRoot(document.getElementById('root')).render(<AdminApp />);
