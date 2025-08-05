import React, { useState, useEffect } from 'react';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import MultiSelect from '@/components/MultiSelect';
import { Sidebar } from './components/Sidebar';
import { SettingsPage } from './components/SettingsPage';
import StockItem from './components/StockItem';

interface StockInfo {
  name: string;
  value: string;
  isLimit: boolean;
  isNew?: boolean;
  type?: string; // 添加类型字段
  sign?: string; // 添加sign字段（几天几板）
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
  "四舍五入取整": number;
  "类型": string;
  "上下午": "上午" | "下午";
  "sign"?: string; // 添加sign字段（几天几板）
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
  const [currentPage, setCurrentPage] = useState<'home' | 'settings'>('home');

  useEffect(() => {
    // WebSocket连接
    let ws: WebSocket | null = null;
    let reconnectTimer: NodeJS.Timeout | null = null;
    let manuallyClosed = false;

    function connectWS() {
      ws = new window.WebSocket('ws://localhost:61125/ws/changes');
      ws.onopen = () => {
        setLoadingMessage('已连接数据流...');
      };
      ws.onmessage = (event) => {
        try {
          const fetchedData: StockDataItem[] = JSON.parse(event.data);
          setUpdateTime(new Date().toLocaleString('zh-CN', { hour12: false, timeZone: 'Asia/Shanghai' }));
          if (fetchedData && fetchedData.length > 0) {
            // 调试：检查数据结构
            console.log('接收到的数据示例:', fetchedData[0]);
            console.log('数据字段:', Object.keys(fetchedData[0]));
            
            // 处理数据逻辑，与原fetch一致
            const concepts: ConceptData = {};
            const conceptNameSet = new Set<string>();
            const currentStockKeys = new Set<string>();

            // 找出最后一个时间点
            let lastTime = '';
            let lastPeriod = '';
            for (const item of fetchedData) {
              if (!item["板块名称"] || !item["时间"] || !item["名称"]) continue;
              const currentTime = item["时间"];
              const currentPeriod = item["上下午"];
              console.log(`检查时间点: ${currentPeriod} ${currentTime}`);
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
              console.log(`生成股票key: ${stockKey}, 名称: ${item["名称"]}, 类型: ${item["类型"]}, 涨跌幅: ${item["四舍五入取整"]}, 是否涨停: ${isLimit}`);
              currentStockKeys.add(stockKey);

              // 判断是否为最后一个时间点的股票
              const isLastTimeStock = (time === lastTime && period === lastPeriod);
              console.log(`股票 ${item["名称"]}(${item["类型"]}) 是否为最后时间点股票: ${isLastTimeStock} (时间: ${period} ${time})`);

              concepts[item["板块名称"]][period][time].push({
                name: item["名称"],
                value: valueStr,
                isLimit: isLimit,
                isNew: isLastTimeStock, // 使用isNew字段标记最后时间点的股票
                type: item["类型"], // 添加类型字段
                sign: item["sign"] // 添加sign字段
              });
              if (item["板块名称"]) {
                conceptNameSet.add(item["板块名称"]);
              }
            }

            setHasData(true);
            setData(concepts);
            setConceptNames(Array.from(conceptNameSet));
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

  const renderStockInfo = (stocks: StockInfo[]): React.JSX.Element => {
    // 前端按"名称+类型"去重，与后端保持一致
    console.log('renderStockInfo 去重前数据量:', stocks.length);
    console.log('renderStockInfo 去重前数据:', stocks.map(s => `${s.name}(${s.value}, ${s.type || '未知类型'})`));

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

    console.log('renderStockInfo 去重排序后数据:', uniqueStocks.map(s => `${s.name}(${s.value}, ${s.type || '未知类型'})`));

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
        <span className="text-gray-600 dark:text-gray-400 text-xs">{time}</span>{' '}
        <span className="text-xs text-gray-800 dark:text-gray-200">{renderStockInfo(timeGroups[time])}</span>
      </div>
    ));
  };

  const renderMainContent = () => {
    if (currentPage === 'settings') {
      return <SettingsPage />;
    }

    return (
      <div className='min-h-screen bg-gray-50 dark:bg-gray-900'>
        <div className="mx-auto max-w-7xl p-4">
          <div className="relative z-20 items-center justify-between flex font-bold text-gray-800 dark:text-white mb-4">
            <MultiSelect
                  label="板块筛选"
                  options={Array.isArray(conceptNames) ? conceptNames.filter((name): name is string => typeof name === 'string').map((name) => ({ label: name, value: name })) : []}
                  value={selectedConcepts}
                  onChange={setSelectedConcepts}
                  placeholder="请选择板块"
                />
                <h1 className="text-xl">盘口异动</h1>
                <span className="text-xs">更新时间：{updateTime}</span>
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
                <Table noWrapper className="border-collapse w-full bg-white dark:bg-gray-800">
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
                        <TableRow key={index} className="hover:bg-gray-50 dark:hover:bg-gray-900">
                          <TableCell className="font-semibold align-top text-gray-800 dark:text-gray-200 border-r dark:border-gray-600 p-3">
                            {conceptName}
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
    </div>
  );
};

export default StockMarketMonitor;