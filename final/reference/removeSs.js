/**
 * Processes product data:
 * 1. **Initial Filtering**: Rows where the '商品管理番号（商品URL）' (Key) ends with "-ss"
 *    are removed entirely. The Key is trimmed before checking the suffix.
 * 2. **Catalog ID Processing (for remaining rows)**:
 *    For rows sharing the same Key:
 *    a. If at least one row has a 'カタログID', then all rows for that Key *without* a 'カタログID' are removed.
 *       Rows with a 'カタログID' are kept.
 *    b. If *all* rows for a given Key have no 'カタログID', then for each of those rows,
 *       the Key itself is used as the 'カタログID'.
 * This version is optimized for speed by removing internal logging.
 *
 * @param {string[][]} keyValues Range containing '商品管理番号（商品URL）' (e.g., A2:A).
 * @param {string[][]} catalogValues Range containing 'カタログID' (e.g., C2:C).
 * @return {string[][]} A 2D array with processed data: ['商品管理番号（商品URL）', 'カタログID'].
 * @customfunction
 */
function REMOVE_SS(keyValues, catalogValues) {
  const out = [["商品管理番号（商品URL）","カタログID"]];
  const n = keyValues.length;
  if (n === 0) return out;

  // phase 1: group & detect
  const data = Object.create(null);
  for (let i = 0; i < n; i++) {
    const kRaw = keyValues[i][0] || "";
    const k = kRaw.trim();
    if (!k || k.slice(-3) === "-ss") continue;

    const cRaw = catalogValues[i][0] || "";
    const c = cRaw.trim();
    let d = data[k];
    if (!d) {
      d = { withCat: [], withoutCatCount: 0 };
      data[k] = d;
    }
    if (c) {
      d.withCat.push(c);
    } else {
      d.withoutCatCount++;
    }
  }

  // phase 2: emit with dedupe
  const seen = Object.create(null);
  for (const k in data) {
    const { withCat, withoutCatCount } = data[k];

    if (withCat.length) {
      for (let i = 0; i < withCat.length; i++) {
        const cat = withCat[i];
        const tag = k + "|" + cat;
        if (!seen[tag]) {
          seen[tag] = true;
          out.push([k, cat]);
        }
      }
    } else {
      // only one [k,k] even if withoutCatCount > 1
      const tag = k + "|" + k;
      if (!seen[tag]) {
        seen[tag] = true;
        out.push([k, k]);
      }
    }
  }

  return out;
}

// --- Example usage for testing in Apps Script editor ---
// You can keep your test function, just ensure it calls the REMOVE_SS function.
// If you rename your test function, make sure to select the new name in the editor to run it.
function testRemoveSsFunction() { // Renamed for clarity, matching the main function
  // Using console.log for testing in Node.js or for clarity in Apps Script logs if preferred over Logger.log
  // In Apps Script, Logger.log is standard. Using console.log for broader compatibility if code is moved.
  console.log("--- testRemoveSsFunction ---");

  const testData = [
    // Key, Catalog
    ["item-is-ok", "CAT_OK"],
    ["item-is-ok", ""],                  // Should be removed by catalog logic
    ["item-ends-ss", "CAT_SS_1"],        // Should be removed by -ss filter
    ["item-ends-ss", ""],                // Should be removed by -ss filter
    ["another-ok", ""],                  // Becomes [another-ok, another-ok]
    ["another-ok", ""],                  // Becomes [another-ok, another-ok]
    ["  spaced-key-ss  ", "CAT_SPACED_SS"], // Should be removed by -ss filter (after trim)
    ["final-item", "FINAL_CAT"],
    ["item-with-internal-ss-but-ok", "CAT_INTERNAL_SS"], // Should be kept
    ["64-twlk-9rfx", ""],                // This one should be removed by catalog logic
    ["64-twlk-9rfx", "4560275310021"],
    ["7o-y6t4-zfjg-ss", "SHOULD_BE_REMOVED"], // -ss filter
    ["key-no-cat-at-all", ""],           // Becomes [key-no-cat-at-all, key-no-cat-at-all]
    ["", "emptyKeyCat"],                 // Skipped due to empty key
    ["itemOnlyKey", null]                // Catalog becomes empty, then logic applies
  ];

  const keyValues = testData.map(row => [row[0]]);
  const catalogValues = testData.map(row => [row[1]]);

  console.log("Input Data for Testing:");
  testData.forEach(row => console.log(JSON.stringify(row)));

  const result = REMOVE_SS(keyValues, catalogValues); // Calling the main function

  console.log("\nProcessed Output Data (from REMOVE_SS):");
  if (result.length > 1) {
    result.forEach(row => console.log(JSON.stringify(row)));
  } else {
    console.log("Result is empty (only header returned).");
  }
  console.log("--- End of testRemoveSsFunction ---");
}