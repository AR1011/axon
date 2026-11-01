export function Segmented<T extends string>({ label, value, options, onChange, small }: { label: string; value: T; options: { value: T; label: string }[]; onChange: (value: T) => void; small?: boolean }) {
  return (
    <div role="group" aria-label={label} className="inline-flex overflow-hidden rounded-md border border-border">
      {options.map((option) => (
        <button
          key={option.value}
          type="button"
          aria-pressed={option.value === value}
          onClick={() => onChange(option.value)}
          className={`min-w-[24px] px-2 text-[10px] ${small ? "h-[24px]" : "h-[26px]"} ${option.value === value ? "bg-raised text-text" : "text-muted hover:text-text"}`}
        >
          {option.label}
        </button>
      ))}
    </div>
  );
}
