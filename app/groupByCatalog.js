/**
 * GROUP_BY_CATALOG
 * Groups SKUs by their Catalog ID, then within each group finds the common leading segment-prefix
 * (split on '-') and emits rows of [Catalog ID, Group name, SKU].
 * - If only one SKU in a catalog: outputs that SKU as both group and value.
 * - If no shared segment-prefix among multiple SKUs: uses the shortest SKU as the group name.
 * @param {string[][]} skuRange  A single-column range of SKUs
 * @param {string[][]} idRange   A single-column range of Catalog IDs (same length)
 * @return {string[][]}          A 2D array: header row + data rows
 */
function GROUP_BY_CATALOG(skuRange, idRange) {
  // Flatten and clean inputs
  const skus = skuRange.flat().map(String);
  const ids  = idRange.flat().map(String);

  // Build a map of Catalog ID → list of SKUs
  const groups = {};
  for (let i = 0; i < skus.length; i++) {
    const id = ids[i];
    const sku = skus[i];
    if (!id || !sku) continue;
    if (!groups[id]) groups[id] = [];
    groups[id].push(sku);
  }

  // Helper: find common leading segments of two SKUs
  function commonSegmentPrefix(a, b) {
    const pa = a.split('-');
    const pb = b.split('-');
    const len = Math.min(pa.length, pb.length);
    const both = [];
    for (let i = 0; i < len; i++) {
      if (pa[i] === pb[i]) both.push(pa[i]);
      else break;
    }
    return both.join('-');
  }

  // Build output: header + rows
  const output = [["カタログID", "最終SKU", "各商品SKU"]];

  Object.entries(groups).forEach(([id, skuList]) => {
    if (skuList.length === 1) {
      // Single item: use SKU as group and value
      const only = skuList[0];
      output.push([id, only, only]);
      return;
    }
    // For multiple SKUs: find common prefix across all
    let prefix = skuList.reduce((prev, cur) => commonSegmentPrefix(prev, cur));
    prefix = prefix.replace(/-+$/, '');
    // If no shared prefix, pick the shortest SKU as representative
    if (!prefix) {
      prefix = skuList.reduce((shortest, s) => s.length < shortest.length ? s : shortest);
    }
    // Emit each SKU under the prefix, including the prefix itself
    skuList.forEach(sku => {
      output.push([id, prefix, sku]);
    });
  });

  return output;
}
