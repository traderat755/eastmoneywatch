import { useState, useEffect } from 'react';
import { Search, Plus, Edit, Trash2, Save, X } from 'lucide-react';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';

interface PickedStock {
  股票代码: string;
  股票名称: string; 
  板块代码: string;
  板块名称: string;
}

interface ConceptStock {
  股票代码: string;
  股票名称: string;
  板块代码: string;
  板块名称: string;
}

interface Sector {
  板块代码: string;
  板块名称: string;
}

export function PickedPage() {
  const [pickedStocks, setPickedStocks] = useState<PickedStock[]>([]);
  const [searchResults, setSearchResults] = useState<ConceptStock[]>([]);
  const [sectors, setSectors] = useState<Sector[]>([]);
  const [stockSectors, setStockSectors] = useState<Sector[]>([]);
  const [searchQuery, setSearchQuery] = useState('');
  const [isLoading, setIsLoading] = useState(true);
  const [editingStock, setEditingStock] = useState<string | null>(null);
  const [editData, setEditData] = useState<PickedStock | null>(null);
  const [showAddForm, setShowAddForm] = useState(false);
  const [message, setMessage] = useState<{type: 'success' | 'error', text: string} | null>(null);

  // 加载精选股票列表
  const loadPickedStocks = async () => {
    try {
      const response = await fetch('http://localhost:61125/api/picked');
      const data = await response.json();
      if (data.status === 'success') {
        setPickedStocks(data.data);
      } else {
        showMessage('error', '加载精选股票失败: ' + data.message);
      }
    } catch (error) {
      showMessage('error', '加载精选股票失败: ' + error);
    } finally {
      setIsLoading(false);
    }
  };

  // 加载板块列表
  const loadSectors = async () => {
    try {
      const response = await fetch('http://localhost:61125/api/concepts/sectors');
      const data = await response.json();
      if (data.status === 'success') {
        setSectors(data.data);
      }
    } catch (error) {
      console.error('加载板块列表失败:', error);
    }
  };

  // 加载指定股票的板块列表
  const loadStockSectors = async (stockCode: string) => {
    try {
      const response = await fetch(`http://localhost:61125/api/concepts/stock-sectors/${stockCode}`);
      const data = await response.json();
      if (data.status === 'success') {
        setStockSectors(data.data);
      } else {
        console.error('加载股票板块列表失败:', data.message);
        setStockSectors([]);
      }
    } catch (error) {
      console.error('加载股票板块列表失败:', error);
      setStockSectors([]);
    }
  };

  // 搜索股票
  const searchStocks = async (query: string) => {
    if (!query.trim()) {
      setSearchResults([]);
      return;
    }
    
    try {
      const response = await fetch(`http://localhost:61125/api/concepts/search?q=${encodeURIComponent(query)}`);
      const data = await response.json();
      if (data.status === 'success') {
        setSearchResults(data.data);
      } else {
        showMessage('error', '搜索失败: ' + data.message);
      }
    } catch (error) {
      showMessage('error', '搜索失败: ' + error);
    }
  };

  // 添加股票到精选列表
  const addStock = async (stock: ConceptStock) => {
    try {
      const response = await fetch('http://localhost:61125/api/picked', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(stock),
      });
      const data = await response.json();
      
      if (data.status === 'success') {
        showMessage('success', '添加成功');
        loadPickedStocks();
        setSearchQuery('');
        setSearchResults([]);
        setShowAddForm(false);
      } else {
        showMessage('error', '添加失败: ' + data.message);
      }
    } catch (error) {
      showMessage('error', '添加失败: ' + error);
    }
  };

  // 更新股票信息
  const updateStock = async (stockCode: string, stockData: PickedStock) => {
    try {
      const response = await fetch(`http://localhost:61125/api/picked/${stockCode}`, {
        method: 'PUT',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(stockData),
      });
      const data = await response.json();
      
      if (data.status === 'success') {
        showMessage('success', '更新成功');
        loadPickedStocks();
        setEditingStock(null);
        setEditData(null);
      } else {
        showMessage('error', '更新失败: ' + data.message);
      }
    } catch (error) {
      showMessage('error', '更新失败: ' + error);
    }
  };

  // 删除股票
  const deleteStock = async (stockCode: string) => {
    if (!confirm('确定要删除这只股票吗？')) {
      return;
    }
    
    try {
      const response = await fetch(`http://localhost:61125/api/picked/${stockCode}`, {
        method: 'DELETE',
      });
      const data = await response.json();
      
      if (data.status === 'success') {
        showMessage('success', '删除成功');
        loadPickedStocks();
      } else {
        showMessage('error', '删除失败: ' + data.message);
      }
    } catch (error) {
      showMessage('error', '删除失败: ' + error);
    }
  };

  // 显示消息
  const showMessage = (type: 'success' | 'error', text: string) => {
    setMessage({ type, text });
    setTimeout(() => setMessage(null), 3000);
  };

  // 开始编辑股票
  const startEdit = async (stock: PickedStock) => {
    setEditingStock(stock.股票代码);
    setEditData({ ...stock });
    // 加载该股票的可选板块
    await loadStockSectors(stock.股票代码);
  };

  // 取消编辑
  const cancelEdit = () => {
    setEditingStock(null);
    setEditData(null);
  };

  // 保存编辑
  const saveEdit = () => {
    if (editData && editingStock) {
      updateStock(editingStock, editData);
    }
  };

  useEffect(() => {
    loadPickedStocks();
    loadSectors();
  }, []);

  useEffect(() => {
    const timeoutId = setTimeout(() => {
      searchStocks(searchQuery);
    }, 300);
    
    return () => clearTimeout(timeoutId);
  }, [searchQuery]);

  if (isLoading) {
    return (
      <div className="p-6 bg-white dark:bg-gray-900 min-h-screen">
        <div className="text-center py-8">
          <div className="text-gray-600 dark:text-gray-400">加载中...</div>
        </div>
      </div>
    );
  }

  return (
    <div className="p-6 bg-white dark:bg-gray-900 min-h-screen">
      <div className="max-w-6xl mx-auto">
        <div className="flex justify-between items-center mb-6">
          <h1 className="text-2xl font-bold text-gray-900 dark:text-white">精选股票管理</h1>
          <button
            onClick={() => setShowAddForm(!showAddForm)}
            className="flex items-center px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
          >
            <Plus className="mr-2 h-4 w-4" />
            添加股票
          </button>
        </div>

        {/* 消息提示 */}
        {message && (
          <div className={`mb-4 p-3 rounded-lg ${
            message.type === 'success' 
              ? 'bg-green-100 text-green-700 dark:bg-green-900 dark:text-green-300' 
              : 'bg-red-100 text-red-700 dark:bg-red-900 dark:text-red-300'
          }`}>
            {message.text}
          </div>
        )}

        {/* 添加股票表单 */}
        {showAddForm && (
          <div className="mb-6 p-4 border border-gray-200 dark:border-gray-700 rounded-lg bg-gray-50 dark:bg-gray-800">
            <div className="flex items-center mb-4">
              <Search className="mr-2 h-4 w-4 text-gray-500" />
              <input
                type="text"
                placeholder="搜索股票名称或代码..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="flex-1 px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 bg-white dark:bg-gray-700 text-gray-900 dark:text-white"
              />
              <button
                onClick={() => setShowAddForm(false)}
                className="ml-2 p-2 text-gray-500 hover:text-gray-700 dark:hover:text-gray-300"
              >
                <X className="h-4 w-4" />
              </button>
            </div>

            {/* 搜索结果 */}
            {searchResults.length > 0 && (
              <div className="max-h-60 overflow-y-auto border border-gray-200 dark:border-gray-600 rounded-lg">
                {searchResults.map((stock) => (
                  <div
                    key={stock.股票代码}
                    className="flex justify-between items-center p-3 border-b border-gray-200 dark:border-gray-600 last:border-b-0 hover:bg-gray-100 dark:hover:bg-gray-700"
                  >
                    <div>
                      <span className="font-medium text-gray-900 dark:text-white">
                        {stock.股票名称}
                      </span>
                      <span className="ml-2 text-sm text-gray-500 dark:text-gray-400">
                        ({stock.股票代码})
                      </span>
                      <span className="ml-2 text-sm text-blue-600 dark:text-blue-400">
                        {stock.板块名称}
                      </span>
                    </div>
                    <button
                      onClick={() => addStock(stock)}
                      className="px-3 py-1 bg-green-600 text-white text-sm rounded hover:bg-green-700 transition-colors"
                    >
                      添加
                    </button>
                  </div>
                ))}
              </div>
            )}
          </div>
        )}

        {/* 精选股票列表 */}
        <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 overflow-hidden">
          <Table>
            <TableHeader>
              <TableRow className="bg-gray-50 dark:bg-gray-900">
                <TableHead className="w-[15%]">股票代码</TableHead>
                <TableHead className="w-[20%]">股票名称</TableHead>
                <TableHead className="w-[15%]">板块代码</TableHead>
                <TableHead className="w-[30%]">板块名称</TableHead>
                <TableHead className="w-[20%]">操作</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {pickedStocks.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={5} className="text-center py-8 text-gray-500 dark:text-gray-400">
                    暂无精选股票
                  </TableCell>
                </TableRow>
              ) : (
                pickedStocks.map((stock) => (
                  <TableRow key={stock.股票代码} className="hover:bg-gray-50 dark:hover:bg-gray-900">
                    <TableCell className="font-mono text-sm">
                      {editingStock === stock.股票代码 ? (
                        <input
                          type="text"
                          value={editData?.股票代码 || ''}
                          onChange={(e) => setEditData(prev => prev ? {...prev, 股票代码: e.target.value} : null)}
                          className="w-full px-2 py-1 border border-gray-300 dark:border-gray-600 rounded text-sm bg-white dark:bg-gray-700 text-gray-900 dark:text-white"
                        />
                      ) : (
                        stock.股票代码
                      )}
                    </TableCell>
                    <TableCell>
                      {editingStock === stock.股票代码 ? (
                        <input
                          type="text"
                          value={editData?.股票名称 || ''}
                          onChange={(e) => setEditData(prev => prev ? {...prev, 股票名称: e.target.value} : null)}
                          className="w-full px-2 py-1 border border-gray-300 dark:border-gray-600 rounded text-sm bg-white dark:bg-gray-700 text-gray-900 dark:text-white"
                        />
                      ) : (
                        stock.股票名称
                      )}
                    </TableCell>
                    <TableCell className="font-mono text-sm">
                      {editingStock === stock.股票代码 ? (
                        <input
                          type="text"
                          value={editData?.板块代码 || ''}
                          readOnly
                          className="w-full px-2 py-1 border border-gray-300 dark:border-gray-600 rounded text-sm bg-gray-100 dark:bg-gray-600 text-gray-900 dark:text-white cursor-not-allowed"
                        />
                      ) : (
                        stock.板块代码
                      )}
                    </TableCell>
                    <TableCell>
                      {editingStock === stock.股票代码 ? (
                        <select
                          value={editData?.板块名称 || ''}
                          onChange={(e) => {
                            const selectedSector = stockSectors.find(s => s.板块名称 === e.target.value);
                            if (selectedSector) {
                              setEditData(prev => prev ? {
                                ...prev, 
                                板块名称: selectedSector.板块名称,
                                板块代码: selectedSector.板块代码
                              } : null);
                            }
                          }}
                          className="w-full px-2 py-1 border border-gray-300 dark:border-gray-600 rounded text-sm bg-white dark:bg-gray-700 text-gray-900 dark:text-white"
                        >
                          <option value="">选择板块</option>
                          {stockSectors.map((sector) => (
                            <option key={sector.板块代码} value={sector.板块名称}>
                              {sector.板块名称}
                            </option>
                          ))}
                        </select>
                      ) : (
                        stock.板块名称
                      )}
                    </TableCell>
                    <TableCell>
                      {editingStock === stock.股票代码 ? (
                        <div className="flex space-x-2">
                          <button
                            onClick={saveEdit}
                            className="p-1 text-green-600 hover:text-green-700 dark:text-green-400 dark:hover:text-green-300"
                            title="保存"
                          >
                            <Save className="h-4 w-4" />
                          </button>
                          <button
                            onClick={cancelEdit}
                            className="p-1 text-gray-600 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-300"
                            title="取消"
                          >
                            <X className="h-4 w-4" />
                          </button>
                        </div>
                      ) : (
                        <div className="flex space-x-2">
                          <button
                            onClick={() => startEdit(stock)}
                            className="p-1 text-blue-600 hover:text-blue-700 dark:text-blue-400 dark:hover:text-blue-300"
                            title="编辑"
                          >
                            <Edit className="h-4 w-4" />
                          </button>
                          <button
                            onClick={() => deleteStock(stock.股票代码)}
                            className="p-1 text-red-600 hover:text-red-700 dark:text-red-400 dark:hover:text-red-300"
                            title="删除"
                          >
                            <Trash2 className="h-4 w-4" />
                          </button>
                        </div>
                      )}
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </div>

        <div className="mt-4 text-sm text-gray-500 dark:text-gray-400">
          共 {pickedStocks.length} 只精选股票
        </div>
      </div>
    </div>
  );
}