import { test, expect, chromium, Page } from "@playwright/test";
import { generateSessionToken } from "../helpers/auth";
import { waitForServices } from "../helpers/wait-for-services";

const COOKIE_NAME = process.env.COOKIE_NAME || "session";

const BROWSER_ARGS = [
  "--ignore-certificate-errors",
  "--origin-to-force-quic-on=127.0.0.1:4433",
  "--use-fake-device-for-media-stream",
  "--use-fake-ui-for-media-stream",
  "--disable-gpu",
];

// SFU publishes SpeakerUpdate on a 200ms tick. After flipping mute state we
// wait long enough for: (a) the next tick on the SFU, (b) the SpeakerUpdate
// to propagate over the WebTransport/WebSocket link, and (c) the dioxus tile
// to re-render its glow inline style. 2.5s is conservative for a local stack
// and matches the budget assumed by the server-side integration test in
// p3-11.
const SPEAKER_PROPAGATION_MS = 2500;

async function createAuthenticatedContext(
  browser: ReturnType<typeof chromium.launch> extends Promise<infer B> ? B : never,
  email: string,
  name: string,
  uiURL: string,
) {
  const context = await browser.newContext({
    baseURL: uiURL,
    ignoreHTTPSErrors: true,
  });
  const token = generateSessionToken(email, name);
  const url = new URL(uiURL);
  await context.addCookies([
    {
      name: COOKIE_NAME,
      value: token,
      domain: url.hostname,
      path: "/",
      httpOnly: true,
      secure: false,
      sameSite: "Lax",
    },
  ]);
  return context;
}

async function navigateToMeeting(page: Page, meetingId: string, username: string) {
  await page.goto("/");
  await page.waitForTimeout(1500);

  await page.locator("#meeting-id").click();
  await page.locator("#meeting-id").pressSequentially(meetingId, { delay: 50 });
  await page.locator("#username").click();
  await page.locator("#username").fill("");
  await page.locator("#username").pressSequentially(username, { delay: 50 });
  await page.waitForTimeout(500);
  await page.locator("#username").press("Enter");
  await expect(page).toHaveURL(new RegExp(`/meeting/${meetingId}`), {
    timeout: 10_000,
  });
  await page.waitForTimeout(1500);
}

async function joinMeetingFromPage(
  page: Page,
): Promise<"in-meeting" | "waiting" | "waiting-for-meeting"> {
  const joinButton = page.getByText(/Start Meeting|Join Meeting/);
  const waitingRoom = page.getByText("Waiting to be admitted");
  const waitingForMeeting = page.getByText("Waiting for meeting to start");

  const result = await Promise.race([
    joinButton.waitFor({ timeout: 20_000 }).then(() => "join" as const),
    waitingRoom.waitFor({ timeout: 20_000 }).then(() => "waiting" as const),
    waitingForMeeting.waitFor({ timeout: 20_000 }).then(() => "waiting-for-meeting" as const),
  ]);

  if (result === "waiting") {
    return "waiting";
  }

  if (result === "waiting-for-meeting") {
    return "waiting-for-meeting";
  }

  await page.waitForTimeout(1000);
  await joinButton.click();
  await page.waitForTimeout(3000);

  await expect(page.locator("#grid-container")).toBeVisible({ timeout: 15_000 });
  return "in-meeting";
}

async function admitGuestIfNeeded(
  hostPage: Page,
  guestPage: Page,
  guestResult: "in-meeting" | "waiting" | "waiting-for-meeting",
): Promise<void> {
  if (guestResult === "in-meeting") {
    return;
  }

  if (guestResult === "waiting") {
    const admitButton = hostPage.getByTitle("Admit").first();
    await expect(admitButton).toBeVisible({ timeout: 20_000 });
    await hostPage.waitForTimeout(1000);
    await admitButton.dispatchEvent("click");
    await hostPage.waitForTimeout(3000);

    const guestJoinButton = guestPage.getByText(/Join Meeting|Start Meeting/);
    const guestGrid = guestPage.locator("#grid-container");

    const postAdmit = await Promise.race([
      guestJoinButton.waitFor({ timeout: 20_000 }).then(() => "join-button" as const),
      guestGrid.waitFor({ timeout: 20_000 }).then(() => "grid" as const),
    ]);

    if (postAdmit === "join-button") {
      await guestPage.waitForTimeout(1000);
      await guestJoinButton.click();
      await guestPage.waitForTimeout(3000);
      await expect(guestGrid).toBeVisible({ timeout: 15_000 });
    }
  }
}

/**
 * Click the toolbar mic button. The button's tooltip span reads "Unmute" when
 * the mic is currently muted and "Mute" when it is currently live, so we
 * select by the expected pre-click tooltip text.
 */
async function setMic(page: Page, target: "on" | "off"): Promise<void> {
  const tooltip = target === "on" ? "Unmute" : "Mute";
  const button = page.locator("nav.video-controls-container button").filter({
    has: page.locator(`span.tooltip:text-is("${tooltip}")`),
  });
  await expect(button).toBeVisible({ timeout: 10_000 });
  await button.first().click();
  // Brief settle so the next state read isn't racing the toggle.
  await page.waitForTimeout(300);
}

/**
 * Read the inline `style` attribute of the peer tile's `.glow-overlay`.
 *
 * The dioxus UI (see `dioxus-ui/src/components/canvas_generator.rs::speak_style`)
 * renders the speaker glow as an inline style on this element. When the peer
 * is silent the style contains `border: 1.5px solid transparent` and
 * `box-shadow: none`. When the peer is the active speaker the style contains
 * `border: 1.5px solid rgba(0, 255, 65, ...)` with a non-zero `box-shadow`.
 */
async function readPeerGlowStyle(observerPage: Page): Promise<string> {
  const peerTile = observerPage.locator("#grid-container .canvas-container").first();
  await expect(peerTile).toBeVisible({ timeout: 30_000 });
  const glowOverlay = peerTile.locator(".glow-overlay");
  await expect(glowOverlay).toBeVisible({ timeout: 10_000 });
  const style = await glowOverlay.getAttribute("style");
  expect(style).toBeTruthy();
  return style ?? "";
}

function expectSpeakingStyle(style: string): void {
  // Active-speaker style has a coloured border and a non-empty box-shadow.
  expect(style).toMatch(/border:\s*1\.5px\s+solid\s+rgba\(0,\s*255,\s*65/);
  expect(style).not.toContain("box-shadow: none");
}

function expectSilentStyle(style: string): void {
  expect(style).toContain("border: 1.5px solid transparent");
  expect(style).toContain("box-shadow: none");
}

/**
 * Poll `readPeerGlowStyle` until the predicate matches or the budget expires.
 * Returns the last-read style so the caller can produce a useful assertion
 * message on failure.
 */
async function waitForGlowStyle(
  observerPage: Page,
  predicate: (style: string) => boolean,
  budgetMs: number,
): Promise<string> {
  const deadline = Date.now() + budgetMs;
  let last = "";
  while (Date.now() < deadline) {
    last = await readPeerGlowStyle(observerPage);
    if (predicate(last)) {
      return last;
    }
    await observerPage.waitForTimeout(200);
  }
  return last;
}

test.describe("SFU speaker rotation reflected in peer tile UI", () => {
  test.beforeAll(async () => {
    await waitForServices();
  });

  test("active speaker swaps when mic ownership swaps", async ({ baseURL }) => {
    test.setTimeout(180_000);
    const uiURL = baseURL || "http://localhost:80";
    const meetingId = `e2e_sfu_rotation_${Date.now()}`;

    const browserA = await chromium.launch({ args: BROWSER_ARGS });
    const browserB = await chromium.launch({ args: BROWSER_ARGS });

    try {
      const ctxA = await createAuthenticatedContext(
        browserA,
        "speaker-a@videocall.rs",
        "SpeakerA",
        uiURL,
      );
      const ctxB = await createAuthenticatedContext(
        browserB,
        "speaker-b@videocall.rs",
        "SpeakerB",
        uiURL,
      );

      const pageA = await ctxA.newPage();
      const pageB = await ctxB.newPage();

      // Host (page A) starts the meeting, guest (page B) joins and is admitted.
      await navigateToMeeting(pageA, meetingId, "SpeakerA");
      const hostResult = await joinMeetingFromPage(pageA);
      expect(hostResult).toBe("in-meeting");

      await navigateToMeeting(pageB, meetingId, "SpeakerB");
      const guestResult = await joinMeetingFromPage(pageB);
      await admitGuestIfNeeded(pageA, pageB, guestResult);

      // Both clients land in the grid with their mic off (UI default).
      // Confirm the silent baseline on both observer sides.
      const silentOnB = await readPeerGlowStyle(pageB);
      expectSilentStyle(silentOnB);
      const silentOnA = await readPeerGlowStyle(pageA);
      expectSilentStyle(silentOnA);

      // --- Phase 1: A speaks, B observes A as active speaker.
      await setMic(pageA, "on");
      const aSpeakingOnB = await waitForGlowStyle(
        pageB,
        (style) => /rgba\(0,\s*255,\s*65/.test(style),
        SPEAKER_PROPAGATION_MS,
      );
      expectSpeakingStyle(aSpeakingOnB);

      // --- Phase 2: swap. B speaks, A observes B as active speaker.
      await setMic(pageA, "off");
      await setMic(pageB, "on");
      const bSpeakingOnA = await waitForGlowStyle(
        pageA,
        (style) => /rgba\(0,\s*255,\s*65/.test(style),
        SPEAKER_PROPAGATION_MS,
      );
      expectSpeakingStyle(bSpeakingOnA);

      // And A's tile on B should no longer be glowing — the SpeakerUpdate
      // must clear the prior speaker, not just add a new one.
      const aSilentOnB = await waitForGlowStyle(
        pageB,
        (style) => style.includes("border: 1.5px solid transparent"),
        SPEAKER_PROPAGATION_MS,
      );
      expectSilentStyle(aSilentOnB);
    } finally {
      await browserA.close();
      await browserB.close();
    }
  });
});
