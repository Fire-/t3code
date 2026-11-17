import { expect, it } from "vite-plus/test";

import { render } from "../../../test/render";
import { useComposerMultilinePrompt } from "./useComposerMultilinePrompt";

function Composer(props: { text: string; collapsed?: boolean }) {
  const [multiline, observeBody] = useComposerMultilinePrompt();
  return (
    <div style={{ width: 200 }}>
      {/* The mobile collapsed composer display: none's its body while its parent stays laid out. */}
      <div ref={observeBody} style={props.collapsed ? { display: "none" } : undefined}>
        <div
          data-testid="composer-editor"
          contentEditable
          suppressContentEditableWarning
          style={{ lineHeight: "20px" }}
        >
          {props.text}
        </div>
      </div>
      <output>{multiline ? "multiline" : "single line"}</output>
    </div>
  );
}

it("notices when typing makes the prompt wrap", async () => {
  await using screen = await render(<Composer text="short" />, { router: false });
  await expect.element(screen.getByText("single line")).toBeVisible();
  await screen.rerender(<Composer text={"a long prompt that wraps ".repeat(6)} />);
  await expect.element(screen.getByText("multiline")).toBeVisible();
});

it("notices wrapping text that lands while the body is display: none", async () => {
  await using screen = await render(<Composer text="short" collapsed />, { router: false });
  await expect.element(screen.getByText("single line")).toBeVisible();
  await screen.rerender(<Composer text={"a long prompt that wraps ".repeat(6)} collapsed />);
  await expect.element(screen.getByText("multiline")).toBeVisible();
  await screen.rerender(<Composer text="short again" collapsed />);
  await expect.element(screen.getByText("single line")).toBeVisible();
});
