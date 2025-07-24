def group_by_catalog(sku_list, id_list):
    """
    Group SKUs by their Catalog ID and find common segment-prefix.
    
    Args:
        sku_list (list): List of SKU strings
        id_list (list): List of Catalog ID strings
        
    Returns:
        list: List of tuples (catalog_id, handler, sku)
    """
    # Build mapping of catalog_id -> list of skus
    catalog_map = {}
    for sku, id in zip(sku_list, id_list):
        if not id or not sku:
            continue
            
        if id not in catalog_map:
            catalog_map[id] = []
        catalog_map[id].append(sku)
    
    # Process each catalog group
    result = []
    for catalog_id, skus in catalog_map.items():
        if len(skus) == 1:
            # Single SKU case
            handler = skus[0]
            result.append((catalog_id, handler, skus[0]))
        else:
            # Multiple SKUs: find common prefix
            prefix = find_common_prefix(skus)
            if not prefix:
                # No common prefix: use shortest SKU
                prefix = min(skus, key=len)
            
            # Add all SKUs under this handler
            for sku in skus:
                result.append((catalog_id, prefix, sku))
    
    return result

def find_common_prefix(skus):
    """
    Find the longest common prefix among a list of SKUs.
    
    Args:
        skus (list): List of SKU strings
        
    Returns:
        str: Longest common prefix or empty string if none found
    """
    if not skus:
        return ""
        
    # Split SKUs into segments
    segments = [sku.split('-') for sku in skus]
    
    # Find common segments
    common = []
    for i in range(min(len(s) for s in segments)):
        # Get ith segment from each SKU
        current_segments = [s[i] for s in segments]
        
        # Check if all segments are the same
        if len(set(current_segments)) == 1:
            common.append(current_segments[0])
        else:
            break
    
    return '-'.join(common) if common else ""
