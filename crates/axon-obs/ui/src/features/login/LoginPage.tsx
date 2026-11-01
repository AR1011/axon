import { useEffect, useRef, useState, type FormEvent } from "react";
import { base } from "../../api";
import { Dot } from "../../components/atoms";
import { Logo } from "../../components/Icon";

/** Where the sign-in returns to: `?next=`, a path inside the console, never another site or the login page. */
function destination(): string {
  const home = new URL(base, location.origin);
  try {
    const next = (new URLSearchParams(location.search).get("next") ?? "").replace(/^[\s/\\]+/, "");
    const url = new URL(next, home);
    if (url.origin === location.origin && url.pathname.startsWith(home.pathname) && url.pathname !== `${home.pathname}login`) {
      return url.pathname + url.search + url.hash;
    }
  } catch {
    /* a malformed next goes home */
  }
  return home.pathname;
}

/** The dashboard's 401 reasons a browser that held a session can meet, in the words this page uses. */
const ENDED: Record<string, string> = {
  "signed out: the console restarted or signed everyone out": "The console signed you out. Sign in again.",
  "the session expired": "Your session expired. Sign in again.",
};

export function LoginPage() {
  const [checking, setChecking] = useState(true);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [ended, setEnded] = useState<string | null>(null);
  const password = useRef<HTMLInputElement>(null);

  useEffect(() => {
    // Already signed in, or a console without users: straight on, once. Else the refusal says why this browser's session ended.
    fetch(`${base}api/session`, { credentials: "same-origin" })
      .then(async (answer) => {
        if (answer.ok) return location.replace(destination());
        const why = ((await answer.json().catch(() => null)) as { error?: unknown } | null)?.error;
        setEnded(typeof why === "string" ? (ENDED[why] ?? null) : null);
        setChecking(false);
      })
      .catch(() => setChecking(false));
  }, []);

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const form = new FormData(event.currentTarget);
    setBusy(true);
    setError(null);
    try {
      const answer = await fetch(`${base}login`, {
        method: "POST",
        body: new URLSearchParams({ user: String(form.get("user") ?? ""), password: String(form.get("password") ?? "") }),
        credentials: "same-origin",
        referrerPolicy: "same-origin",
      });
      if (answer.ok) return location.assign(destination());
      const wait = answer.headers.get("retry-after");
      setError(
        answer.status === 401
          ? "Wrong user or password."
          : answer.status === 429
            ? `Too many attempts. Try again in ${wait ?? "a few"} s.`
            : `Sign-in failed (${answer.status}).`,
      );
    } catch {
      setError("The browser cannot connect to the console.");
    }
    setBusy(false);
    password.current?.select();
  }

  if (checking) return <main aria-busy="true" className="h-full bg-bg" />;
  const input = "h-9 rounded-md border border-border bg-panel px-3 text-[13px] text-text outline-none transition-[border-color,box-shadow] focus:border-accent focus:ring-3 focus:ring-accent/15 aria-[invalid=true]:border-danger";
  return (
    <main className="login flex h-full overflow-y-auto p-4 pb-[12vh]">
      <form onSubmit={submit} aria-labelledby="login-title" className="m-auto flex w-full max-w-[300px] flex-col">
        <Logo size={36} />
        <h1 id="login-title" className="mt-5 text-[20px] font-[650] tracking-[-0.5px]">
          Sign in to axon
        </h1>
        <p role="status" className="mt-1 flex items-center text-[12.5px] text-muted">
          {ended ? <><Dot tone="warning" />{ended}</> : "The console for this cluster."}
        </p>
        <label className="mt-7 flex flex-col gap-1.5 text-[11.5px] font-[500] text-muted">
          User
          <input name="user" autoComplete="username" required autoFocus aria-invalid={!!error} aria-describedby={error ? "login-error" : undefined} className={input} />
        </label>
        <label className="mt-4 flex flex-col gap-1.5 text-[11.5px] font-[500] text-muted">
          Password
          <input ref={password} name="password" type="password" autoComplete="current-password" required aria-invalid={!!error} aria-describedby={error ? "login-error" : undefined} className={input} />
        </label>
        <button type="submit" disabled={busy} className="mt-6 h-9 rounded-md bg-accent text-[13px] font-[600] text-bg transition-opacity hover:opacity-90 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent disabled:opacity-60">
          {busy ? "Wait…" : "Sign in"}
        </button>
        <p id="login-error" role="alert" className="mt-3 min-h-4 text-[12px] text-danger">
          {error}
        </p>
      </form>
    </main>
  );
}
