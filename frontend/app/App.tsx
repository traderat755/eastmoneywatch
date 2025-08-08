import React, { useState, useEffect } from 'react';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import MultiSelect from '@/components/MultiSelect';
import { Sidebar } from './components/Sidebar';
import { SettingsPage } from './components/SettingsPage';
import { PickedPage } from './components/PickedPage';
import StockItem from './components/StockItem';
import { UpdateConceptsButton } from './components/UpdateConceptsButton';
import  { Toaster, toast } from 'react-hot-toast';
import SectorButton from './components/SectorButton';
import { usePicked } from './lib/PickedContext';

interface StockInfo {
  name: string;
  code: string;
  value: string;
  isLimit: boolean;
  isNew?: boolean;
  type?: string;
  sign?: string;
}

type TimeGroup = Record<string, StockInfo[]>;

interface PeriodData {
  "上午": TimeGroup;
  "下午": TimeGroup;
}

type ConceptData = Record<string, PeriodData>;

interface StockDataItem {
  "板块名称": string;
  "时间": string;
  "名称": string;
  "股票代码": string;
  "四舍五入取整": number;
  "类型": string;
  "上下午": "上午" | "下午";
  "标识"?: string; // 添加sign字段（几天几板）
}

// 涨停板类型列表
const LIMIT_UP_TYPES = ['封涨停板', '打开涨停板'];

const StockMarketMonitor = () => {
  const [data, setData] = useState<ConceptData>({});
  const [conceptNames, setConceptNames] = useState<string[]>([]);
  const [loadingMessage, setLoadingMessage] = useState('Loading data...');
  const [hasData, setHasData] = useState(false);
  const [updateTime, setUpdateTime] = useState<string>('');
  const [selectedConcepts, setSelectedConcepts] = useState<string[]>([]);
  const [currentPage, setCurrentPage] = useState<'home' | 'settings' | 'picked'>('home');
  const { pickedSectorNames, loading: pickedLoading, addStock, deleteStockBySectorName } = usePicked();
  const [sectors, setSectors] = useState<{板块名称: string; 板块代码: string}[]>([]);

  // 移除 isLoadingConcepts 状态和 handleGetConcepts 函数，这些逻辑已经移到 UpdateConceptsButton 组件中

  useEffect(() => {
    // WebSocket连接
    let ws: WebSocket | null = null;
    let reconnectTimer: NodeJS.Timeout | null = null;
    let manuallyClosed = false;

    function connectWS() {
      ws = new window.WebSocket('ws://localhost:61125/ws/changes');
      ws.onopen = () => {
        setLoadingMessage('连接数据流启动中...');
      };
      ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
          setUpdateTime(new Date().toLocaleString('zh-CN', { hour12: false, timeZone: 'Asia/Shanghai' }));

          // 检查是否是新的数据格式（columns + values）
          if (data.columns && data.values) {
            // 将新格式转换为原来的对象数组格式
            const fetchedData: StockDataItem[] = data.values.map((row: any[]) => {
              const item: any = {};
              data.columns.forEach((col: string, index: number) => {
                item[col] = row[index];
              });
              return item;
            });

            if (fetchedData && fetchedData.length > 0) {
            // 处理数据逻辑，与原fetch一致
            const concepts: ConceptData = {};
            const conceptNameSet = new Set<string>();
            const orderedConceptNames: string[] = []; // 保持板块顺序的数组
            const currentStockKeys = new Set<string>();

            // 找出最后一个时间点
            let lastTime = '';
            let lastPeriod = '';
            for (const item of fetchedData) {
              if (!item["板块名称"] || !item["时间"] || !item["名称"]) continue;
              const currentTime = item["时间"];
              const currentPeriod = item["上下午"];
              if (currentTime > lastTime || (currentTime === lastTime && currentPeriod > lastPeriod)) {
                lastTime = currentTime;
                lastPeriod = currentPeriod;
              }
            }

            console.log(`最后一个时间点: ${lastPeriod} ${lastTime}`);

            for (const item of fetchedData) {
              if (!item["板块名称"] || !item["时间"] || !item["名称"]) continue;
              if (!concepts[item["板块名称"]]) {
                concepts[item["板块名称"]] = { "上午": {}, "下午": {} };
              }

              // 保持板块出现的顺序
              if (!conceptNameSet.has(item["板块名称"])) {
                conceptNameSet.add(item["板块名称"]);
                orderedConceptNames.push(item["板块名称"]);
              }

              const period = item["上下午"];
              const time = item["时间"];
              if (!concepts[item["板块名称"]][period][time]) {
                concepts[item["板块名称"]][period][time] = [];
              }
              let valueStr = '';
              if (typeof item["四舍五入取整"] === 'number') {
                valueStr = item["四舍五入取整"].toString();
              }

              // 修改股票key生成逻辑，按"名称+类型"生成，与去重逻辑保持一致
              const isLimit = LIMIT_UP_TYPES.includes(item["类型"]);
              const stockKey = `${item["名称"]}_${item["类型"]}`;
              currentStockKeys.add(stockKey);

              // 判断是否为最后一个时间点的股票
              const isLastTimeStock = (time === lastTime && period === lastPeriod);
              // 验证数据完整性
              if (!item["名称"] || !item["股票代码"]) {
                console.warn('[WebSocket] 跳过无效股票数据:', item);
                continue;
              }
              concepts[item["板块名称"]][period][time].push({
                name: item["名称"],
                code: item["股票代码"],
                value: valueStr,
                isLimit: isLimit,
                isNew: isLastTimeStock, // 使用isNew字段标记最后时间点的股票
                type: item["类型"], // 添加类型字段
                sign: item["标识"] // 添加sign字段
              });
            }

            setHasData(true);
            setData(concepts);
            setConceptNames(orderedConceptNames); // 使用有序的板块名称列表
          }
        } else {
          // 兼容旧格式（数组对象）
          const fetchedData: StockDataItem[] = data;
          if (fetchedData && fetchedData.length > 0) {
            // 处理数据逻辑，与原fetch一致
            const concepts: ConceptData = {};
            const conceptNameSet = new Set<string>();
            const orderedConceptNames: string[] = []; // 保持板块顺序的数组
            const currentStockKeys = new Set<string>();

            // 找出最后一个时间点
            let lastTime = '';
            let lastPeriod = '';
            for (const item of fetchedData) {
              if (!item["板块名称"] || !item["时间"] || !item["名称"]) continue;
              const currentTime = item["时间"];
              const currentPeriod = item["上下午"];
              if (currentTime > lastTime || (currentTime === lastTime && currentPeriod > lastPeriod)) {
                lastTime = currentTime;
                lastPeriod = currentPeriod;
              }
            }

            console.log(`最后一个时间点: ${lastPeriod} ${lastTime}`);

            for (const item of fetchedData) {
              if (!item["板块名称"] || !item["时间"] || !item["名称"]) continue;
              if (!concepts[item["板块名称"]]) {
                concepts[item["板块名称"]] = { "上午": {}, "下午": {} };
              }

              // 保持板块出现的顺序
              if (!conceptNameSet.has(item["板块名称"])) {
                conceptNameSet.add(item["板块名称"]);
                orderedConceptNames.push(item["板块名称"]);
              }

              const period = item["上下午"];
              const time = item["时间"];
              if (!concepts[item["板块名称"]][period][time]) {
                concepts[item["板块名称"]][period][time] = [];
              }
              let valueStr = '';
              if (typeof item["四舍五入取整"] === 'number') {
                valueStr = item["四舍五入取整"].toString();
              }

              // 修改股票key生成逻辑，按"名称+类型"生成，与去重逻辑保持一致
              const isLimit = LIMIT_UP_TYPES.includes(item["类型"]);
              const stockKey = `${item["名称"]}_${item["类型"]}`;
              currentStockKeys.add(stockKey);

              // 判断是否为最后一个时间点的股票
              const isLastTimeStock = (time === lastTime && period === lastPeriod);
              concepts[item["板块名称"]][period][time].push({
                name: item["名称"],
                code: item["股票代码"],
                value: valueStr,
                isLimit: isLimit,
                isNew: isLastTimeStock, // 使用isNew字段标记最后时间点的股票
                type: item["类型"], // 添加类型字段
                sign: item["标识"] // 添加sign字段
              });
            }

            setHasData(true);
            setData(concepts);
            setConceptNames(orderedConceptNames); // 使用有序的板块名称列表
          }
        }
        } catch (error) {
          setLoadingMessage('数据解析错误');
          setHasData(false);
        }
      };
      ws.onerror = () => {
        setLoadingMessage('WebSocket连接错误，尝试重连...');
      };
      ws.onclose = () => {
        if (!manuallyClosed) {
          setLoadingMessage('连接断开，正在重连...');
          reconnectTimer = setTimeout(connectWS, 2000);
        }
      };
    }
    connectWS();
    return () => {
      manuallyClosed = true;
      if (ws) ws.close();
      if (reconnectTimer) clearTimeout(reconnectTimer);
    };
  }, []);



  useEffect(() => {
    // 拉取板块映射
    const fetchSectors = async () => {
      try {
        console.log('[fetchSectors] 开始拉取板块映射...');
        const res = await fetch('http://localhost:61125/api/concepts/sectors');
        const data = await res.json();
        console.log('[fetchSectors] API响应:', data);
        if (data.status === 'success') {
          console.log('[fetchSectors] 成功获取板块数据:', data.data);
          console.log('[fetchSectors] 板块数据长度:', data.data?.length || 0);
          setSectors(data.data || []);
        } else {
          console.error('[fetchSectors] 拉取板块映射失败:', data.message);
          setSectors([]);
        }
      } catch (e) {
        console.error('[fetchSectors] 拉取板块映射异常:', e);
        setSectors([]);
      }
    };
    fetchSectors();
  }, []);

  const handleSectorClick = async (sectorName: string) => {
    const isPicked = pickedSectorNames.includes(sectorName);
    if (isPicked) {
      // 已精选，点击即删除
      await deleteStockBySectorName(sectorName);
      return;
    }
    try {
      console.log(`点击了板块: ${sectorName}`);
      console.log('[handleSectorClick] sectors数组:', sectors);
      console.log('[handleSectorClick] sectors数组长度:', sectors.length);
      
      const sector = sectors.find(s => s.板块名称 === sectorName);
      if (!sector) {
        toast.error('未找到板块代码');
        return;
      }
      console.log('[handleSectorClick] 找到的sector对象:', sector);
      console.log('[handleSectorClick] sector字段验证:', {
        板块代码: sector.板块代码,
        板块名称: sector.板块名称,
        板块代码类型: typeof sector.板块代码,
        板块名称类型: typeof sector.板块名称
      });
      
      // 聚合该板块所有股票
      const periodData = data[sectorName];
      if (!periodData) {
        toast.error('未找到板块数据');
        return;
      }
      let allStocks: StockInfo[] = [];
      for (const period of ['上午', '下午'] as const) {
        const timeGroup = periodData[period];
        for (const time in timeGroup) {
          allStocks = allStocks.concat(timeGroup[time]);
        }
      }
      if (allStocks.length === 0) {
        toast.error('该板块无股票数据');
        return;
      }
      // 找到涨幅最大的股票
      const maxStock = allStocks.reduce((max, cur) => {
        const v1 = Number(max.value) || -Infinity;
        const v2 = Number(cur.value) || -Infinity;
        return v2 > v1 ? cur : max;
      }, allStocks[0]);
      console.log('[handleSectorClick] 板块所有股票:', allStocks);
      console.log('[handleSectorClick] 最高涨幅股票:', maxStock);
      console.log('[handleSectorClick] maxStock字段验证:', {
        code: maxStock.code,
        name: maxStock.name,
        code类型: typeof maxStock.code,
        name类型: typeof maxStock.name
      });
      console.log('[handleSectorClick] sector对象:', sector);
      console.log('[handleSectorClick] 准备发送的数据:', {
        股票代码: maxStock.code,
        股票名称: maxStock.name,
        板块代码: sector.板块代码,
        板块名称: sector.板块名称,
      });
      
      // 验证数据完整性
      if (!maxStock.code || !sector.板块代码 || !sector.板块名称) {
        console.error('[handleSectorClick] 数据验证失败:', {
          股票代码: maxStock.code,
          股票名称: maxStock.name,
          板块代码: sector.板块代码,
          板块名称: sector.板块名称
        });
        toast.error('数据不完整，无法添加股票');
        return;
      }
      
      // POST到/api/picked
      await addStock({
        股票代码: maxStock.code,
        股票名称: maxStock.name || '',  // 确保有值，即使为空字符串
        板块代码: sector.板块代码,
        板块名称: sector.板块名称,
      });
    } catch (error) {
      console.error('添加板块失败:', error);
      toast.error('网络错误，请稍后重试');
    }
  };

  const renderStockInfo = (stocks: StockInfo[]): React.JSX.Element => {
    const uniqueStocksMap = new Map<string, StockInfo>();
    stocks.forEach((stock) => {
      // 用"名称+类型"拼接做唯一key，与后端drop_duplicates逻辑一致
      const typeStr = stock.type || '未知类型';
      const key = `${stock.name}__${typeStr}`;
      uniqueStocksMap.set(key, stock);
    });

    // 添加稳定的排序逻辑，按股票名称排序
    const uniqueStocks = Array.from(uniqueStocksMap.values()).sort((a, b) => {
      // 首先按股票名称排序
      const nameCompare = a.name.localeCompare(b.name, 'zh-CN');
      if (nameCompare !== 0) return nameCompare;

      // 如果名称相同，按类型排序
      const typeA = a.type || '未知类型';
      const typeB = b.type || '未知类型';
      return typeA.localeCompare(typeB, 'zh-CN');
    });

    return (
      <>
        {uniqueStocks.map((stock, index) => (
          <StockItem
            key={`${stock.name}__${stock.type || '未知类型'}`}
            stock={stock}
            showComma={index > 0}
          />
        ))}
      </>
    );
  };

  const renderPeriodContent = (conceptName: string, period: "上午" | "下午"): React.JSX.Element[] => {
    const timeGroups = data[conceptName]?.[period] || {};
    const sortedTimes = Object.keys(timeGroups).sort();

    return sortedTimes.map((time, timeIndex) => (
      <div key={timeIndex} className="mb-1 last:mb-0">
        <span className="font-bold text-xs">{time}</span>{' '}
        <span className="flex-wrap text-xs">{renderStockInfo(timeGroups[time])}</span>
      </div>
    ));
  };

  const renderMainContent = () => {
    if (currentPage === 'settings') {
      return <SettingsPage />;
    }

    if (currentPage === 'picked') {
      return <PickedPage />;
    }

    return (
      <div className='min-h-screen bg-gray-50 dark:bg-gray-900'>
        <div className="w-full p-4">
          <div className="relative z-20 items-center justify-between flex font-bold text-gray-800 dark:text-white mb-4">
            <MultiSelect
                  label="板块筛选"
                  options={Array.isArray(conceptNames) ? conceptNames.filter((name): name is string => typeof name === 'string').map((name) => ({ label: name, value: name })) : []}
                  value={selectedConcepts}
                  onChange={setSelectedConcepts}
                  placeholder="请选择板块"
                />
                <div className="flex items-center gap-4">
                  <h1 className="text-xl">盘口异动</h1>
                </div>
                <div>
                <span className="text-xs mr-2">更新时间：{updateTime}</span>
                <UpdateConceptsButton />
                  </div>
          </div>
          {!hasData && (
            <div className="rounded-md bg-blue-50 dark:bg-blue-900 p-4">
              <div className="text-center py-2 text-gray-700 dark:text-gray-300">
                {loadingMessage}
              </div>
            </div>
          )}
          {hasData && (
            <>
              <div className="w-full max-h-[90vh] overflow-auto rounded-lg border border-gray-200 dark:border-gray-700 shadow-sm">
                <Table noWrapper className="border-collapse w-full bg-white dark:bg-gray-900">
                  <TableHeader className="sticky top-0 z-10 bg-amber-50 dark:bg-blue-900 shadow-md">
                    <TableRow>
                      <TableHead className="w-[10%] p-3 text-left font-semibold text-gray-700 dark:text-gray-300">
                        板块
                      </TableHead>
                      <TableHead className="w-[45%] p-3 text-left font-semibold text-gray-700 dark:text-gray-300">
                        上午
                      </TableHead>
                      <TableHead className="w-[45%] p-3 text-left font-semibold text-gray-700 dark:text-gray-300">
                        下午
                      </TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {(selectedConcepts.length ? selectedConcepts : Object.keys(data)).map((conceptName, index) => (
                      data[conceptName] && (
                        <TableRow key={index} className="hover:bg-gray-50 dark:hover:bg-gray-800">
                          <TableCell className="font-semibold align-top text-gray-800 dark:text-gray-200 border-r dark:border-gray-600 p-3">
                            <SectorButton
                              sectorName={conceptName}
                              isPicked={pickedSectorNames.includes(conceptName)}
                              loading={pickedLoading}
                              onClick={handleSectorClick}
                            />
                          </TableCell>
                          <TableCell className="align-top border-r dark:border-gray-600 p-3 text-left">
                            {renderPeriodContent(conceptName, "上午")}
                          </TableCell>
                          <TableCell className="align-top p-3 text-left">
                            {renderPeriodContent(conceptName, "下午")}
                          </TableCell>
                        </TableRow>
                      )
                    ))}
                  </TableBody>
                </Table>
              </div>
            </>
          )}
        </div>
      </div>
    );
  };

  return (
    <div className="flex">
      <Sidebar currentPage={currentPage} onPageChange={setCurrentPage} />
      <div className="flex-1">
        {renderMainContent()}
      </div>
      <Toaster
        toastOptions={{
          duration: 3000,
          style: {
            background: '#363636',
            color: '#fff',
          },
        }}
      />
    </div>
  );
};

export default StockMarketMonitor;