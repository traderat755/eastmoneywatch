#!/usr/bin/env python3
"""
测试picked.csv功能的脚本
"""
import pandas as pd
import os
import sys
from concepts import uplimit10jqka

def test_picked_functionality():
    """测试picked.csv功能"""
    print("=== 测试picked.csv功能 ===")

    # 测试文件路径
    static_dir = "static"
    concepts_path = os.path.join(static_dir, "concepts.csv")
    picked_path = os.path.join(static_dir, "picked.csv")

    # 检查文件是否存在
    print(f"1. 检查文件存在性:")
    print(f"   concepts.csv: {os.path.exists(concepts_path)}")
    print(f"   picked.csv: {os.path.exists(picked_path)}")

    if not os.path.exists(concepts_path):
        print("   ❌ concepts.csv不存在，无法测试")
        return

    if not os.path.exists(picked_path):
        print("   ❌ picked.csv不存在，无法测试")
        return

    # 读取文件
    print(f"\n2. 读取文件:")
    try:
        concept_df = pd.read_csv(concepts_path)
        print(f"   concepts.csv: {len(concept_df)}行, 字段: {list(concept_df.columns)}")

        picked_df = pd.read_csv(picked_path)
        print(f"   picked.csv: {len(picked_df)}行, 字段: {list(picked_df.columns)}")
    except Exception as e:
        print(f"   ❌ 读取文件失败: {e}")
        return

    # 测试板块排序功能
    print(f"\n3. 测试板块排序功能:")
    if '板块代码' in concept_df.columns:
        picked_sector_codes = picked_df['板块代码'].unique().tolist()
        print(f"   选中的板块代码: {picked_sector_codes}")

        # 创建排序索引
        concept_df['_sort_order'] = concept_df['板块代码'].apply(
            lambda x: picked_sector_codes.index(x) if x in picked_sector_codes else len(picked_sector_codes)
        )

        # 按排序索引排序
        sorted_concept_df = concept_df.sort_values('_sort_order').drop('_sort_order', axis=1)

        # 检查前几个板块是否包含选中的板块
        print(f"   排序后前10个板块:")
        first_sectors = sorted_concept_df['板块代码'].head(10).unique()
        for i, sector_code in enumerate(first_sectors):
            is_picked = sector_code in picked_sector_codes
            sector_name = sorted_concept_df[sorted_concept_df['板块代码'] == sector_code]['板块名称'].iloc[0]
            print(f"     {sector_code} ({sector_name}) {'✅' if is_picked else '❌'}")

        # 统计选中的板块在排序后的位置
        picked_positions = []
        for i, sector_code in enumerate(sorted_concept_df['板块代码'].unique()):
            if sector_code in picked_sector_codes:
                picked_positions.append(i)

        print(f"   选中的板块在排序后的位置: {picked_positions}")

        if all(pos < len(picked_sector_codes) for pos in picked_positions):
            print("   ✅ 板块排序功能正常，选中的板块都在最前面")
        else:
            print("   ❌ 板块排序功能异常，选中的板块不在最前面")

        # 显示每个选中板块的股票数量
        print(f"\n4. 选中板块的股票统计:")
        for sector_code in picked_sector_codes:
            sector_stocks = sorted_concept_df[sorted_concept_df['板块代码'] == sector_code]
            sector_name = sector_stocks['板块名称'].iloc[0]
            print(f"   {sector_code} ({sector_name}): {len(sector_stocks)}只股票")

    else:
        print("   ❌ concepts.csv中没有板块代码字段")

    print(f"\n=== 测试完成 ===")

if __name__ == "__main__":
    df = uplimit10jqka()
    print(df.columns)