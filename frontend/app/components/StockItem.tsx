import React from 'react';

interface StockInfo {
  name: string;
  value: string;
  isLimit: boolean;
  isNew?: boolean;
  type?: string;
  sign?: string;
}

interface StockItemProps {
  stock: StockInfo;
  showComma?: boolean;
}

const StockItem: React.FC<StockItemProps> = ({ stock, showComma = false }) => {
  // 复制股票名称到剪贴板的函数
  const copyToClipboard = async (stockName: string) => {
    try {
      await navigator.clipboard.writeText(stockName);
      console.log(`已复制股票名称: ${stockName}`);
    } catch (err) {
      console.error('复制失败:', err);
      // 降级方案：使用传统方法
      const textArea = document.createElement('textarea');
      textArea.value = stockName;
      document.body.appendChild(textArea);
      textArea.select();
      document.execCommand('copy');
      document.body.removeChild(textArea);
      console.log(`已复制股票名称: ${stockName} (降级方案)`);
    }
  };

  return (
    <>
      {showComma && ', '}
      <span 
        className={`${stock.isNew ? 'bg-yellow-300 dark:bg-purple-600' : ''} cursor-pointer hover:bg-blue-100 dark:hover:bg-blue-800 px-1 rounded transition-colors`}
        onClick={() => copyToClipboard(stock.name)}
        title={`点击复制 ${stock.name}`}
      >
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
    </>
  );
};

export default StockItem; 