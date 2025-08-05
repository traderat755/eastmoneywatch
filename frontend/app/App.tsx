import React, { useState, useEffect } from 'react';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import MultiSelect from './components/MultiSelect';

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
  // 板块多选筛选
  const [selectedConcepts, setSelectedConcepts] = useState<string[]>([]);

  // const isTradingHours = () => {
  //   const now = new Date();
  //   const beijingTime = new Date(now.toLocaleString('en-US', { timeZone: 'Asia/Shanghai' }));
  //   const day = beijingTime.getDay(); // 0 = Sunday, 1 = Monday, ..., 6 = Saturday
  //   const hours = beijingTime.getHours();
  //   const minutes = beijingTime.getMinutes();
  //   const currentMinutes = hours * 60 + minutes;

    // // 添加调试信息
    // console.log('Beijing Time:', beijingTime);
    // console.log('Day:', day, 'Time:', hours + ':' + minutes);

    // // Check if it's a weekday (1-5) and within market hours
    // const isTrading = day >= 1 && day <= 5 && // Monday to Friday
    //   ((currentMinutes >= 9 * 60 + 30 && currentMinutes < 11 * 60 + 30) || // 9:30-11:30
    //     (currentMinutes >= 13 * 60 && currentMinutes < 15 * 60)); // 13:00-15:00

  //   console.log('Is trading hours:', isTrading);
  //   return isTrading;
  // };

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
    const uniqueStocks = Array.from(uniqueStocksMap.values());

    return (
      <>
        {uniqueStocks.map((stock, index) => (
          <React.Fragment key={index}>
            {index > 0 && ', '}
            <span className={stock.isNew ? 'bg-yellow-300' : ''}>
              {stock.name}
              {stock.sign && (
                <span className="text-red-600 font-bold ml-1">[{stock.sign}]</span>
              )}
              {' '}
              {(
                stock.isLimit || Math.round(Number(stock.value)) >= 10
              ) ? (
                <span className="text-red-600 font-medium">
                  {parseFloat(stock.value) > 0 ? '+' + stock.value : stock.value}
                </span>
              ) : (
                <span className="font-medium">
                  {parseFloat(stock.value) > 0 ? '+' + stock.value : stock.value}
                </span>
              )}
            </span>
          </React.Fragment>
        ))}
      </>
    );
  };

  const renderPeriodContent = (conceptName: string, period: "上午" | "下午"): React.JSX.Element[] => {
    const timeGroups = data[conceptName]?.[period] || {};
    const sortedTimes = Object.keys(timeGroups).sort();

    return sortedTimes.map((time, timeIndex) => (
      <div key={timeIndex} className="mb-1 last:mb-0">
        <span className="text-gray-600 text-xs">{time}</span>{' '}
        <span className="text-xs">{renderStockInfo(timeGroups[time])}</span>
      </div>
    ));
  };

  return (
    <div className='min-h-screen bg-gray-50'>
      <div className="mx-auto max-w-7xl p-4">
        <div className="relative z-20 items-center justify-between flex font-bold text-gray-800 mb-4">
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
          <div className="rounded-md bg-blue-50 p-4">
            <div className="text-center py-2">
              {loadingMessage}
            </div>
          </div>
        )}
        {hasData && (
          <>
            <div className="w-full max-h-[90vh] overflow-auto rounded-lg border border-gray-200 shadow-sm">
              <Table noWrapper className="border-collapse w-full bg-white">
                <TableHeader className="sticky top-0 z-10 bg-amber-50 shadow-md">
                  <TableRow className="hover:bg-amber-50">
                    <TableHead className="w-[10%] p-3 text-left font-semibold text-gray-700">
                      板块
                    </TableHead>
                    <TableHead className="w-[45%] p-3 text-left font-semibold text-gray-700">
                      上午
                    </TableHead>
                    <TableHead className="w-[45%] p-3 text-left font-semibold text-gray-700">
                      下午
                    </TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {(selectedConcepts.length ? selectedConcepts : Object.keys(data)).map((conceptName, index) => (
                    data[conceptName] && (
                      <TableRow key={index} className="hover:bg-gray-50">
                        <TableCell className="font-semibold align-top text-gray-800 border-r p-3">
                          {conceptName}
                        </TableCell>
                        <TableCell className="align-top border-r p-3 text-left">
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

export default StockMarketMonitor;