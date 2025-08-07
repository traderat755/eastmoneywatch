# 缓存concept_df和picked_df，避免频繁读取
_concept_df_cache = None
_picked_df_cache = None

def get_current_concept_df():
    """获取当前缓存中的concept_df"""
    global _concept_df_cache
    return _concept_df_cache

def update_concept_df_cache(concept_df):
    """更新concept_df缓存"""
    global _concept_df_cache
    _concept_df_cache = concept_df
    print(f"[cache_manager] 更新concept_df缓存，记录数: {len(concept_df) if concept_df is not None else 0}")

def get_current_picked_df():
    """获取当前缓存中的picked_df"""
    global _picked_df_cache
    return _picked_df_cache

def update_picked_df_cache(picked_df):
    """更新picked_df缓存"""
    global _picked_df_cache
    _picked_df_cache = picked_df
    print(f"[cache_manager] 更新picked_df缓存，记录数: {len(picked_df) if picked_df is not None else 0}")