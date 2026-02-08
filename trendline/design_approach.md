## Updated definitions / assumptions (implementation-ready)

### Lookback window

- Only consider pivots whose pivot bar index is within the **last&#32;`lookbackBars`&#32;(default 100)** bars.
- Touch counting and third-touch detection are evaluated only within this rolling window.

### Pivot points (touch anchors)

Use TradingView pivots to avoid subjective/manual swing detection.

- **Pivot Low**: `ta.pivotlow(low, leftBars, rightBars)` returns a value on the bar `rightBars` bars *after* the pivot. Actual pivot bar index: `bar_index - rightBars`.
- **Pivot High**: `ta.pivothigh(high, leftBars, rightBars)` similarly.

Store pivots as tuples:

- `pivotIndex` (bar_index of the pivot bar)
- `pivotPrice` (low for pivot low, high for pivot high)

Recommended defaults (user inputs):

- `leftBars = 3`, `rightBars = 3`

### Trendline types (default behavior)

Maintain and draw **up to 2 lines at once**:

- **Best support trendline** (from two pivot lows)
- **Best resistance trendline** (from two pivot highs)

This matches “draw that trendline” more usefully than selecting only one overall.

### Minimum distance between pivot anchors

To avoid unreliable lines from pivots too close together:

- Require `x2 - x1 >= minPivotDistance` (default e.g. **10 bars**).

### Line equation

Given pivots `(x1, y1)` and `(x2, y2)`, where `x` is `bar_index`:

- slope `m = (y2 - y1) / (x2 - x1)`
- projected price at bar `x`: `y(x) = y1 + m * (x - x1)`

### Touch tolerance

A bar is considered a “touch” if price comes close enough to the line.

Define tolerance `tol` by one of two modes (user input):
1) **Ticks**: `tol = syminfo.mintick * tolTicks`
2) **ATR**: `tol = ta.atr(atrLen) * tolAtrMult`

Touch test per bar:

- **Support**: `abs(low - y(x)) <= tol`
- **Resistance**: `abs(high - y(x)) <= tol`

### Optional slope constraint (recommended)

To reduce false positives from extremely steep or nearly-flat lines, add an optional filter.

Two practical options:

- **ATR-normalized slope**: require `abs(m) <= ta.atr(atrLen) * maxSlopeAtrMult` (units: price per bar). Example default: `maxSlopeAtrMult = 1.0`.
- Or simply provide a boolean `useSlopeFilter` and document that disabling it may increase false detections.

### What qualifies as a “2-touch trendline”

A candidate line (formed by a pivot pair) is eligible if:

- Both pivots are inside the lookback window.
- `x2 > x1`.
- `x2 - x1 >= minPivotDistance`.
- (Optional) passes slope constraint.
- The line has **exactly 2 touches** within the lookback window.
- The two pivot bars must be counted as the two touches (the line goes through them).
- No third (or more) touch has occurred yet.

Optional cleanliness rule (good to include as `requireNoBreak`):

- **No break** from pivot1 to current bar:
- Support: reject if any bar has `close < y(x) - tol` (or stricter `low < y(x) - tol`).
- Resistance: reject if any bar has `close > y(x) + tol` (or stricter `high > y(x) + tol`).

### Candidate generation within last 100 bars

Bound computation for Pine:

- Keep arrays of recent pivots within lookback (e.g., up to `maxPivots=10` lows and 10 highs).
- For each family (lows/highs): generate candidates by pairing pivots `(i, j)` with `i < j`.
- With 10 pivots → 45 candidates per family.

### Counting touches in a historical scan (de-bounce clarification)

When scanning historical bars in a `for` loop, you cannot rely on Pine’s `inTouch[1]` shorthand inside the loop for prior loop-iteration state.

Instead, use a manual state variable during the scan:

- `prevTouch = false`
- For each bar offset `k` in the scan:
- `inTouch = abs(priceAtBar - y(xAtBar)) <= tol`
- `if inTouch and not prevTouch` → increment `touchCount`
- `prevTouch := inTouch`

This prevents a cluster of consecutive bars near the line from counting as multiple touches.

### Selecting the “best” line per family

Among candidates with exactly 2 touches (and meeting filters), pick deterministically:

- Prefer the candidate with the most recent second pivot (`max x2`).
- Tie-breakers (optional): smallest average distance to line within scan; or more moderate slope.

### 3rd touch alerting (prefer `alert()` for user experience)

Goal: “alert me on the 3rd touch.”

Preferred approach:

- Use `alert("3rd touch ...", alert.freq_once_per_bar)` when the current bar becomes the 3rd touch.
- This provides immediate script-driven alerting.
- Works with intra-bar detection if the script uses `calc_on_every_tick=true`.

Alternative (more traditional):

- `alertcondition(thirdTouch, "3rd touch", "...")` (requires user to create an alert in TradingView UI; typically bar-close evaluation).

De-duplication:

- Track a unique key per selected line (e.g., `x1` and `x2` indices) and store `lastAlertKeySupport` / `lastAlertKeyResistance`.
- Fire only if `thirdTouchNow` and `currentKey != lastAlertKey...`.

### Drawing behavior

- Draw/update both best support and best resistance.
- Each line drawn from `x1` to `bar_index` and typically extended right.
- Delete/replace the prior line object when the selected candidate changes.

## Notes / inherent limitations to mention in code comments

- “Trendline with only 2 touches” is subjective; pivot-based anchors are a pragmatic approximation.
- Pivots confirm only after `rightBars` bars, so detection is delayed by that amount.
- If slope filter or minPivotDistance are too strict, fewer/no lines will qualify; make them adjustable inputs.