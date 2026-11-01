/** Theme and accent: two choices in localStorage, applied as `data-theme` and the `--accent*` variables everything else derives from. */
export type ThemeChoice = "system" | "dark" | "light";

export const DEFAULT_ACCENT = "#e0b252";
export const ACCENTS = [
  { name: "Amber", value: DEFAULT_ACCENT },
  { name: "Mint", value: "#65cda0" },
  { name: "Teal", value: "#4fc3c8" },
  { name: "Sky", value: "#6aa9f0" },
  { name: "Indigo", value: "#8f93f2" },
  { name: "Violet", value: "#b58cf0" },
  { name: "Rose", value: "#ec86a8" },
  { name: "Coral", value: "#f08c6c" },
];

const read = (key: string) => {
  try {
    return localStorage.getItem(key);
  } catch {
    return null;
  }
};

const write = (key: string, value: string | null) => {
  try {
    if (value == null) localStorage.removeItem(key);
    else localStorage.setItem(key, value);
  } catch {
    /* storage may be unavailable */
  }
  apply();
};

const mix = (hex: string, other: number, share: number) =>
  `#${[1, 3, 5].map((i) => Math.round(parseInt(hex.slice(i, i + 2), 16) * (1 - share) + other * share).toString(16).padStart(2, "0")).join("")}`;

export function themeChoice(): ThemeChoice {
  const stored = read("axon-theme");
  return stored === "light" || stored === "dark" ? stored : "system";
}

export function accentChoice(): string {
  const stored = read("axon-accent") ?? "";
  return /^#[0-9a-f]{6}$/i.test(stored) ? stored.toLowerCase() : DEFAULT_ACCENT;
}

export function apply() {
  const root = document.documentElement;
  const choice = themeChoice();
  const light = choice === "system" ? matchMedia("(prefers-color-scheme: light)").matches : choice === "light";
  root.dataset.theme = light ? "light" : "dark";
  // app.css holds the default's values, derived the same way, for the first paint.
  const accent = accentChoice();
  root.style.setProperty("--accent", light ? mix(accent, 0, 0.4) : accent);
  root.style.setProperty("--accent-soft", light ? mix(accent, 255, 0.88) : mix(accent, 18, 0.86));
}

export const setTheme = (choice: ThemeChoice) => write("axon-theme", choice === "system" ? null : choice);
export const setAccent = (hex: string | null) => write("axon-accent", hex && hex.toLowerCase() !== DEFAULT_ACCENT ? hex : null);

/** The accent as charts need it: a resolved colour, since a canvas cannot read a CSS variable. */
export const accentColor = () => (typeof document === "undefined" ? DEFAULT_ACCENT : getComputedStyle(document.documentElement).getPropertyValue("--accent").trim() || DEFAULT_ACCENT);

if (typeof matchMedia !== "undefined") matchMedia("(prefers-color-scheme: light)").addEventListener("change", apply);
