import React from 'react';
import toast from 'react-hot-toast';

interface StockInfo {
  name: string;
  code: string;
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
  // 复制股票代码到剪贴板的函数
  const copyToClipboard = async (stockCode: string) => {
    try {
      await navigator.clipboard.writeText(stockCode);
      toast.success(`已复制“${stock.name}”代码: ${stockCode}`);
    } catch (err) {
      console.error('复制失败:', err);
      // 降级方案：使用传统方法
      const textArea = document.createElement('textarea');
      textArea.value = stockCode;
      document.body.appendChild(textArea);
      textArea.select();
      document.execCommand('copy');
      document.body.removeChild(textArea);
      toast.success(`已复制股票代码: ${stockCode}`);
    }
  };

  // 格式化股票代码用于雪球链接
  const formatStockCodeForXueqiu = (code: string) => {
    if (code.startsWith('6')) {
      return `SH${code}`;
    } else if (code.startsWith('8')) {
      return `BJ${code}`;
    } else {
      return `SZ${code}`;
    }
  };

  const formattedCode = formatStockCodeForXueqiu(stock.code);

  return (
    <>
      {showComma && ', '}
      <span>
        <span
          className={`${stock.isNew ? 'bg-yellow-100 dark:bg-gray-800' : ''} cursor-pointer hover:bg-blue-100 dark:hover:bg-blue-800 px-1 rounded transition-colors`}
          onClick={() => copyToClipboard(stock.code)}
          title={`点击复制股票代码 ${stock.code}`}
        >
        {stock.name}
        </span>

        {stock.sign && (
          <span className="text-pink-600 ml-1">[{stock.sign}]</span>
        )}
        <a href={`https://xueqiu.com/S/${formattedCode}`} target="_blank" rel="noopener noreferrer" className="hover:underline">
        {(
          stock.isLimit || Math.round(Number(stock.value)) >= 10
        ) ? (
            <span className="text-pink-600 font-medium">
            {parseFloat(stock.value) > 0 ? '+' + stock.value : stock.value}
            </span>
        ) : (
            <span className="font-medium">
            {parseFloat(stock.value) > 0 ? '+' + stock.value : stock.value}
            </span>
        )}
        </a>
      </span>
    </>
  );
};

export default StockItem;