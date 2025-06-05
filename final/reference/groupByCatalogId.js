/**
 * Processes product data to refine Catalog IDs.
 * 1. For a given Key, if some rows have a Catalog ID and others don't,
 *    it keeps only the rows with a Catalog ID.
 * 2. If all rows for a given Key have no Catalog ID, it uses the Key
 *    itself as the Catalog ID for those rows.
 *
 * @param {string[][]} keyValues Range containing '商品管理番号（商品URL）' (e.g., A2:A).
 * @param {string[][]} catalogValues Range containing 'カタログID' (e.g., C2:C).
 * @return {string[][]} A 2D array with processed data: ['商品管理番号（商品URL）', 'カタログID'].
 * @customfunction
 */
function PROCESS_CATALOG_IDS(keyValues, catalogValues) {
  const outputHeader = ["商品管理番号（商品URL）", "カタログID"];
  const processedRowsOutput = [outputHeader];

  // Ensure inputs are arrays, even if empty
  const keys = Array.isArray(keyValues) ? keyValues : [];
  const catalogs = Array.isArray(catalogValues) ? catalogValues : [];

  const numRows = keys.length; // Process based on the length of the key column

  if (numRows === 0) {
    Logger.log("PROCESS_CATALOG_IDS: Input 'keyValues' is empty. Returning header only.");
    return processedRowsOutput;
  }

  // Phase 1: Group data by key and analyze catalog ID presence
  const keyDataMap = new Map();

  for (let i = 0; i < numRows; i++) {
    const originalKey = String(keys[i]?.[0] || "").trim();
    // Ensure catalogValues[i] exists before trying to access its first element
    const originalCatalog = String(catalogs[i]?.[0] || "").trim();

    if (!originalKey) {
      // Optional: Log or handle rows with empty keys if necessary
      // Logger.log(`Skipping row ${i + 2} due to empty key.`);
      continue; // Skip rows with no key
    }

    if (!keyDataMap.has(originalKey)) {
      keyDataMap.set(originalKey, {
        rowsWithCatalog: [],    // Stores [key, catalog] pairs
        rowsWithoutCatalog: [], // Stores [key, originalCatalog (empty)] pairs
        hasAnyCatalog: false
      });
    }

    const dataForKey = keyDataMap.get(originalKey);

    if (originalCatalog !== "") {
      dataForKey.rowsWithCatalog.push([originalKey, originalCatalog]);
      dataForKey.hasAnyCatalog = true;
    } else {
      dataForKey.rowsWithoutCatalog.push([originalKey, originalCatalog]); // Store original empty catalog
    }
  }

  // Phase 2: Build the output based on the analysis
  for (const [key, dataForKey] of keyDataMap) {
    if (dataForKey.hasAnyCatalog) {
      // If any row for this key had a catalog ID, only keep those rows
      dataForKey.rowsWithCatalog.forEach(row => {
        processedRowsOutput.push(row);
      });
    } else {
      // If NO row for this key had a catalog ID, use the key as the catalog ID
      // for all original rows that belonged to this key.
      dataForKey.rowsWithoutCatalog.forEach(row => {
        // row[0] is the originalKey
        processedRowsOutput.push([row[0], row[0]]); // Use key as catalog ID
      });
    }
  }
  
  if (processedRowsOutput.length === 1) { // Only header
      Logger.log("PROCESS_CATALOG_IDS: No data rows met the criteria to be included in the output.");
  }

  return processedRowsOutput;
}

// --- Example usage for testing in Apps Script editor ---
function testProcessCatalogIds() {
  Logger.log("--- testProcessCatalogIds ---");

  const testData = [
    // Key, Catalog
    ["64-twlk-9rfx", ""],                     // This one should be removed
    ["64-twlk-9rfx", "4560275310021"],
    ["7o-2c9e-zbxd", ""],                     // This one should be removed
    ["7o-2c9e-zbxd", "4525048014292"],
    ["7o-y6t4-zfjg", ""],                     // This one should be removed
    ["7o-y6t4-zfjg", ""],                     // This one should be removed
    ["7o-y6t4-zfjg", "4562382390149"],
    ["key-no-cat", ""],                       // Becomes [key-no-cat, key-no-cat]
    ["key-no-cat", ""],                       // Becomes [key-no-cat, key-no-cat]
    ["key-only-one-cat", "CAT123"],
    ["key-mixed-cats", "CAT_A"],
    ["key-mixed-cats", ""],                   // This one should be removed
    ["key-mixed-cats", "CAT_B"],
    ["another-no-cat", ""],                   // Becomes [another-no-cat, another-no-cat]
    ["  spaced-key  ", ""],                   // Becomes [spaced-key, spaced-key] (key is trimmed)
    ["  spaced-key-with-cat  ", "CAT_SPACED"],
    ["", "CatalogWithNoKey"],                 // Skipped due to empty key
    ["KeyWithEmptyCatalogThenValue", ""],     // Removed
    ["KeyWithEmptyCatalogThenValue", "VAL"],
  ];

  const keyValues = testData.map(row => [row[0]]);
  const catalogValues = testData.map(row => [row[1]]);

  Logger.log("Input Data for Testing:");
  testData.forEach(row => Logger.log(row.join("\t")));

  const result = PROCESS_CATALOG_IDS(keyValues, catalogValues);

  Logger.log("\nProcessed Output Data (from PROCESS_CATALOG_IDS):");
  if (result.length > 1) {
    result.forEach(row => Logger.log(row.join("\t")));
  } else {
    Logger.log("Result is empty (only header returned).");
  }
  Logger.log("--- End of testProcessCatalogIds ---");
}