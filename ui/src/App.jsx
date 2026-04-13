import { Fragment, useEffect, useMemo, useState } from "react";

const API = "http://127.0.0.1:8000";

function scorePillClass(score) {
  if (score == null || score === 0) return "green";
  if (score > 0.3) return "red";
  return "amber";
}

function formatScore(score) {
  if (score == null) return "—";
  return score.toFixed(2);
}

export default function App() {
  const [view, setView] = useState("feed");
  const [executions, setExecutions] = useState([]);
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState(null);
  const [selectedId, setSelectedId] = useState(null);
  const [detail, setDetail] = useState(null);
  const [objects, setObjects] = useState([]);
  const [detailLoading, setDetailLoading] = useState(false);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    fetch(`${API}/executions`)
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((data) => {
        if (!cancelled) {
          setExecutions(data);
          setErr(null);
        }
      })
      .catch((e) => {
        if (!cancelled) setErr(String(e.message || e));
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const sortedFeed = useMemo(() => {
    const copy = [...executions];
    copy.sort((a, b) => {
      const fa = (a.divergence_score || 0) > 0 ? 0 : 1;
      const fb = (b.divergence_score || 0) > 0 ? 0 : 1;
      if (fa !== fb) return fa - fb;
      return String(b.timestamp).localeCompare(String(a.timestamp));
    });
    return copy;
  }, [executions]);

  function openDetail(id) {
    setSelectedId(id);
    setView("detail");
    setDetailLoading(true);
    setDetail(null);
    setObjects([]);
    Promise.all([
      fetch(`${API}/executions/${id}`).then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      }),
      fetch(`${API}/executions/${id}/objects`).then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      }),
    ])
      .then(([d, o]) => {
        setDetail(d);
        setObjects(o);
        setErr(null);
      })
      .catch((e) => setErr(String(e.message || e)))
      .finally(() => setDetailLoading(false));
  }

  function back() {
    setView("feed");
    setSelectedId(null);
    setDetail(null);
    setObjects([]);
  }

  if (view === "feed") {
    return (
      <div className="app">
        <h1>Markov — execution feed</h1>
        {err && <div className="error">{err}</div>}
        {loading ? (
          <p className="muted">Loading…</p>
        ) : (
          <table className="data">
            <thead>
              <tr>
                <th>Agent</th>
                <th>Task context</th>
                <th>Timestamp</th>
                <th>Objects</th>
                <th>Divergence</th>
              </tr>
            </thead>
            <tbody>
              {sortedFeed.map((e) => (
                <tr
                  key={e.execution_id}
                  className="clickable"
                  onClick={() => openDetail(e.execution_id)}
                >
                  <td>{e.agent_id}</td>
                  <td className="truncate" title={e.task_context}>
                    {e.task_context.length > 80
                      ? `${e.task_context.slice(0, 80)}…`
                      : e.task_context}
                  </td>
                  <td className="mono">{e.timestamp}</td>
                  <td>{e.object_count}</td>
                  <td>
                    <span
                      className={`pill ${scorePillClass(e.divergence_score)}`}
                    >
                      {formatScore(e.divergence_score)}
                    </span>
                  </td>
                </tr>
              ))}
              {sortedFeed.length === 0 && (
                <tr>
                  <td colSpan={5} className="muted">
                    No executions yet. Run <code>demo/seed.py</code> and point
                    the API at the same database.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        )}
      </div>
    );
  }

  const visibleObjects = objects.filter((o) => !String(o.key).startsWith("__markov__"));

  return (
    <div className="app">
      <div className="toolbar">
        <button type="button" className="back" onClick={back}>
          ← Back
        </button>
        <h1 style={{ margin: 0 }}>Execution detail</h1>
      </div>
      {err && <div className="error">{err}</div>}
      {detailLoading && <p className="muted">Loading…</p>}
      {detail && (
        <>
          <div className="task-block">
            <div className="task-label">Task context</div>
            <div className="task-text">{detail.task_context}</div>
          </div>
          <p className="muted">
            <span className="mono">{detail.execution_id}</span>
            {" · "}
            {detail.agent_id}
            {" · "}
            <span className={`pill ${scorePillClass(detail.divergence_score)}`}>
              score {formatScore(detail.divergence_score)}
            </span>
          </p>
          {(detail.divergence_flags || []).some((f) => f.type === "volume") && (
            <div className="task-block" style={{ marginTop: 12 }}>
              <div className="task-label">Execution-level flags</div>
              {(detail.divergence_flags || [])
                .filter((f) => f.type === "volume")
                .map((f, i) => (
                  <p key={i} className="reason" style={{ margin: 0 }}>
                    {f.reason}
                  </p>
                ))}
            </div>
          )}
          <table className="data">
            <thead>
              <tr>
                <th>Key</th>
                <th>Size</th>
                <th>Last modified</th>
                <th>Action</th>
              </tr>
            </thead>
            <tbody>
              {visibleObjects.map((o) => {
                const flags = o.divergence_flags || [];
                const flagged = flags.length > 0;
                return (
                  <ObjectRow key={o.id} obj={o} flagged={flagged} flags={flags} />
                );
              })}
              {visibleObjects.length === 0 && (
                <tr>
                  <td colSpan={4} className="muted">
                    No object rows.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </>
      )}
    </div>
  );
}

function ObjectRow({ obj, flagged, flags }) {
  return (
    <Fragment>
      <tr className={flagged ? "row-flagged" : ""}>
        <td className="mono">{obj.key}</td>
        <td>{obj.size_bytes}</td>
        <td className="mono">{obj.last_modified}</td>
        <td>{obj.action}</td>
      </tr>
      {flagged &&
        flags.map((f, i) => (
          <tr key={`${obj.id}-r-${i}`} className="row-flagged">
            <td colSpan={4}>
              <p className="reason">{f.reason}</p>
            </td>
          </tr>
        ))}
    </Fragment>
  );
}
