/**
 * Fully processes product data:
 * 1. Filters out rows where '商品管理番号（商品URL）' (Key) ends with "-ss".
 * 2. Processes 'カタログID' for remaining rows:
 *    - If a Key has mixed Catalog ID presence, keeps only rows with a Catalog ID.
 *    - If a Key has no Catalog IDs, uses the Key itself as the Catalog ID.
 * 3. Derives a 'group key' from the processed item key by removing trailing '-\d+s' patterns.
 *
 * @param {string[][]} keyValues Range containing '商品管理番号（商品URL）' (e.g., A2:A).
 * @param {string[][]} catalogValues Range containing 'カタログID' (e.g., C2:C).
 * @return {string[][]} A 2D array with columns: ['group key', 'item key', 'catalog ID'].
 * @customfunction
 */
function PROCESS_AND_GROUP_PRODUCT_DATA(keyValues, catalogValues) {
  const out = [
    ["商品管理番号", "商品管理番号（商品URL）", "カタログID"]
  ];
  const n = keyValues.length;
  if (n === 0) return out;

  // phase 1: group & detect catalog
  const data = Object.create(null);
  for (let i = 0; i < n; i++) {
    const raw = keyValues[i][0];
    if (!raw) continue;
    const k = raw.trim();
    if (!k || k.slice(-3) === "-ss") continue;

    const cRaw = catalogValues[i][0];
    const c = cRaw ? cRaw.trim() : "";
    let d = data[k];
    if (!d) {
      d = { hasCat: false, cats: [], countNoCat: 0 };
      data[k] = d;
    }
    if (c) {
      d.hasCat = true;
      d.cats.push(c);
    } else {
      d.countNoCat++;
    }
  }

  // tiny helper to strip "-<digits>s" suffix
  function deriveGroup(itemKey) {
    const L = itemKey.length;
    if (itemKey.charAt(L - 1) !== "s") return itemKey;
    const dash = itemKey.lastIndexOf("-");
    if (dash < 0) return itemKey;
    // check that everything between dash+1 and L-1 are digits
    for (let j = dash + 1; j < L - 1; j++) {
      const code = itemKey.charCodeAt(j);
      if (code < 48 || code > 57) return itemKey;
    }
    return itemKey.slice(0, dash);
  }

  // phase 2: emit final rows
  for (const k in data) {
    const { hasCat, cats, countNoCat } = data[k];
    const group = deriveGroup(k);
    if (hasCat) {
      for (let i = 0; i < cats.length; i++) {
        out.push([group, k, cats[i]]);
      }
    } else {
      for (let i = 0; i < countNoCat; i++) {
        out.push([group, k, k]);
      }
    }
  }

  return out;
}

// --- Example usage for testing in Apps Script editor ---
function testProcessAndGroupProductData() {
  Logger.log("--- testProcessAndGroupProductData ---");

  const testData = [
    // Item Key, Catalog ID
    ["abecho-k200", "cat_k200_base"],
    ["abecho-k200-10s", "cat_k200_10s"],
    ["abecho-k200-2s", ""],                // Catalog rule: this one would be removed if k200_base exists
                                          // BUT, these are distinct item keys for map. Catalog logic applies PER itemKey.
                                          // THEN grouping logic applies. Let's adjust test data thinking.

    // New understanding: catalog processing happens on *original* item keys before grouping
    ["item-A", "CAT_A_1"],
    ["item-A", ""],                      // Removed by catalog logic (item-A has CAT_A_1)
    ["item-A-10s", "CAT_A_10s"],
    ["item-B", ""],                      // Becomes [item-B, item-B] by catalog logic
    ["item-B-2s", ""],                   // Becomes [item-B-2s, item-B-2s] by catalog logic
    ["item-C-ss", "CAT_C_SS"],           // Removed by -ss filter
    ["item-D-5s", "CAT_D_5S"],
    ["item-D", "CAT_D_BASE"],
    ["no-cat-item", ""],                 // Becomes [no-cat-item, no-cat-item]
    ["no-cat-item-3s", ""],              // Becomes [no-cat-item-3s, no-cat-item-3s]
    ["  spaced-key-ss  ", "CAT_SPACED_SS"], // Removed by -ss filter
    ["  final-item-2s  ", "FINAL_CAT_2S"],
    ["final-item", ""],                  // Becomes [final-item, final-item]
    ["key-no-suffix", "CAT_NO_SUFFIX"],
    ["abecho-k200", "ID_k200"],
    ["abecho-k200", ""],                // removed due to ID_k200
    ["abecho-k200-10s", "ID_k200_10s"],
    ["abecho-k200-2s", "ID_k200_2s"],
    ["only-one-no-cat-4s", ""]          // becomes [only-one-no-cat-4s, only-one-no-cat-4s]
  ];

  const keyValues = testData.map(row => [row[0]]);
  const catalogValues = testData.map(row => [row[1]]);

  Logger.log("Input Data for Testing (testProcessAndGroupProductData):");
  testData.forEach(row => Logger.log(row.map(c => `"${c}"`).join("\t")));

  const result = PROCESS_AND_GROUP_PRODUCT_DATA(keyValues, catalogValues);

  Logger.log("\nFinal Processed Output Data (from PROCESS_AND_GROUP_PRODUCT_DATA):");
  if (result.length > 1) {
    result.forEach(row => Logger.log(row.map(c => `"${c}"`).join("\t")));
  } else {
    Logger.log("Result is empty (only header returned).");
  }
  Logger.log("--- End of testProcessAndGroupProductData ---");
}