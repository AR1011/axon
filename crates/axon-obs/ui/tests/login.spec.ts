import { expect, test, type Page } from "@playwright/test";

/** The third playground: `admin` signs in with PASSWORD, and a session idles out after 5 s. */
const SIGNED = "http://127.0.0.1:8099/";
const PASSWORD = "correct horse";

// One client address shares one sign-in budget, so the lockout comes last.
test.describe.configure({ mode: "serial" });

/** Signs in as the login page's user would, and returns the answer's status. */
async function signIn(page: Page, password = PASSWORD) {
  await page.getByLabel("User").fill("admin");
  await page.getByLabel("Password").fill(password);
  const answer = page.waitForResponse((r) => r.url() === `${SIGNED}login` && r.request().method() === "POST");
  await page.getByRole("button", { name: "Sign in" }).click();
  return (await answer).status();
}

test("a deep link goes through the login page and comes back", async ({ page }) => {
  await page.goto(`${SIGNED}overview?node=relay-1`);
  await expect(page).toHaveURL(`${SIGNED}login?next=overview%3Fnode%3Drelay-1`);
  await expect(page.getByRole("heading", { name: "Sign in to axon" })).toBeVisible();
  await expect(page.getByLabel("User")).toBeFocused();
  await signIn(page);
  await expect(page).toHaveURL(`${SIGNED}overview?node=relay-1`);
  await expect(page.getByRole("button", { name: /^Connected, updated/ })).toBeVisible();
  // The account sits at the foot of the sidebar, beside Settings, not in the top bar.
  const sidebar = page.getByRole("complementary", { name: "Sidebar" });
  await expect(sidebar).toContainText("Signed in as admin");
  await expect(page.getByRole("banner")).not.toContainText("admin");
  await expect(sidebar.getByRole("button", { name: "Sign out" })).toBeVisible();
  await page.getByRole("button", { name: "Collapse sidebar" }).click();
  await expect(sidebar.getByRole("button", { name: "Sign out" }), "collapsed, it stays named").toBeVisible();
  await page.getByRole("button", { name: "Expand sidebar" }).click();
});

test("a wrong password is refused and says so", async ({ page }) => {
  await page.goto(`${SIGNED}login`);
  expect(await signIn(page, "wrong")).toBe(401);
  await expect(page.getByRole("alert")).toHaveText("Wrong user or password.");
  await expect(page.getByLabel("Password")).toHaveAttribute("aria-invalid", "true");
  await expect(page).toHaveURL(`${SIGNED}login`);
});

test("signing out ends the session", async ({ page }) => {
  await page.goto(`${SIGNED}login`);
  await signIn(page);
  await expect(page).toHaveURL(`${SIGNED}overview`);
  await page.getByRole("complementary", { name: "Sidebar" }).getByRole("button", { name: "Sign out" }).click();
  await expect(page).toHaveURL(`${SIGNED}login`);
  await expect(page.getByRole("button", { name: "Sign in" })).toBeVisible();
  expect((await page.request.get(`${SIGNED}api/overview`)).status()).toBe(401);
});

test("an idle session ends on the login page, once", async ({ page }) => {
  const logins: string[] = [];
  page.on("request", (r) => r.url().includes("/login") && logins.push(r.url()));
  await page.goto(`${SIGNED}login?next=settings`);
  await signIn(page);
  await expect(page).toHaveURL(`${SIGNED}settings`);
  // Settings asks for nothing more; the live stream ends with the session and the page notices.
  await expect(page).toHaveURL(`${SIGNED}login?next=settings`, { timeout: 15_000 });
  await expect(page.getByRole("button", { name: "Sign in" })).toBeVisible();
  const seen = logins.length;
  await page.waitForTimeout(3000);
  expect(logins.length, "no retry loop").toBe(seen);
});

test("repeated wrong passwords lock the sign-in, the right one too", async ({ page }) => {
  await page.goto(`${SIGNED}login`);
  let status = 0;
  for (let attempt = 0; attempt < 7 && status !== 429; attempt++) status = await signIn(page, "wrong");
  expect(status).toBe(429);
  await expect(page.getByRole("alert")).toHaveText(/^Too many attempts\. Try again in \d+ s\.$/);
  expect(await signIn(page)).toBe(429);
  await expect(page.getByRole("alert")).toHaveText(/^Too many attempts/);
  await expect(page).toHaveURL(`${SIGNED}login`);
});
