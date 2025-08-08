import React, { createContext, useState, useEffect, useContext, ReactNode } from 'react';
import { toast } from 'react-hot-toast';

export interface PickedStock {
  股票代码: string;
  股票名称?: string;  // 设为可选
  板块代码: string;
  板块名称: string;
}

interface PickedContextType {
  pickedStocks: PickedStock[];
  pickedSectorNames: string[];
  loading: boolean;
  refetch: () => Promise<void>;
  addStock: (stock: PickedStock) => Promise<void>;
  deleteStockByCode: (stockCode: string) => Promise<void>;
  deleteStockBySectorName: (sectorName: string) => Promise<void>;
  updateStock: (stockCode: string, stockData: PickedStock) => Promise<void>;
}

const PickedContext = createContext<PickedContextType | undefined>(undefined);

export const PickedProvider: React.FC<{ children: ReactNode }> = ({ children }) => {
  const [pickedStocks, setPickedStocks] = useState<PickedStock[]>([]);
  const [loading, setLoading] = useState(true);

  const fetchPicked = async () => {
    if (!loading) setLoading(true);
    try {
      const res = await fetch('http://localhost:61125/api/picked');
      const data = await res.json();
      if (data.status === 'success') {
        setPickedStocks(data.data || []);
      } else {
        setPickedStocks([]);
        toast.error('加载精选列表失败: ' + data.message);
      }
    } catch (e) {
      setPickedStocks([]);
      toast.error('加载精选列表异常: ' + String(e));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchPicked();
  }, []);

  const addStock = async (stock: PickedStock) => {
    try {
      console.log('[PickedContext] 准备发送的股票数据:', stock);
      console.log('[PickedContext] 数据类型检查:', {
        股票代码: typeof stock.股票代码,
        股票名称: typeof stock.股票名称,
        板块代码: typeof stock.板块代码,
        板块名称: typeof stock.板块名称
      });
      console.log('[PickedContext] 数据值检查:', {
        股票代码: stock.股票代码,
        股票名称: stock.股票名称,
        板块代码: stock.板块代码,
        板块名称: stock.板块名称
      });
      
      const response = await fetch('http://localhost:61125/api/picked', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(stock),
      });
      const result = await response.json();
      if (result.status === 'success') {
        toast.success(result.message || '添加成功');
        await fetchPicked();
      } else {
        toast.error(result.message || '添加失败');
      }
    } catch (error) {
      console.error('[PickedContext] 添加股票失败:', error);
      toast.error('网络错误，请稍后重试');
    }
  };

  const deleteStockByCode = async (stockCode: string) => {
    if (!window.confirm('确定要删除这只股票吗？')) return;
    try {
      const response = await fetch(`http://localhost:61125/api/picked/${stockCode}`, {
        method: 'DELETE',
      });
      const result = await response.json();
      if (result.status === 'success') {
        toast.success(result.message || '删除成功');
        await fetchPicked();
      } else {
        toast.error(result.message || '删除失败');
      }
    } catch (error) {
      toast.error('网络错误，请稍后重试');
    }
  };
  
  const deleteStockBySectorName = async (sectorName: string) => {
    try {
      const response = await fetch(`http://localhost:61125/api/picked/${encodeURIComponent(sectorName)}`, {
        method: 'DELETE',
      });
      const result = await response.json();
      if (result.status === 'success') {
        toast.success(result.message || '删除成功');
        await fetchPicked();
      } else {
        toast.error(result.message || '删除失败');
      }
    } catch (error) {
      toast.error('网络错误，请稍后重试');
    }
  };

  const updateStock = async (stockCode: string, stockData: PickedStock) => {
    try {
      const response = await fetch(`http://localhost:61125/api/picked/${stockCode}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(stockData),
      });
      const data = await response.json();
      if (data.status === 'success') {
        toast.success('更新成功');
        await fetchPicked();
      } else {
        toast.error('更新失败: ' + data.message);
      }
    } catch (error) {
      toast.error('更新失败: ' + String(error));
    }
  };

  const pickedSectorNames = Array.from(new Set((pickedStocks || []).map((item: any) => item["板块名称"]))).filter(Boolean) as string[];

  const value = {
    pickedStocks,
    pickedSectorNames,
    loading,
    refetch: fetchPicked,
    addStock,
    deleteStockByCode,
    deleteStockBySectorName,
    updateStock,
  };

  return <PickedContext.Provider value={value}>{children}</PickedContext.Provider>;
};

export const usePicked = () => {
  const context = useContext(PickedContext);
  if (context === undefined) {
    throw new Error('usePicked must be used within a PickedProvider');
  }
  return context;
};
