import React from 'react';
import { useTheme } from 'next-themes';
import { Sun, Moon, Monitor } from 'lucide-react';

export function ThemeToggle() {
  const { theme, setTheme } = useTheme();

  return (
    <div className="space-y-4">
      <h3 className="text-lg font-medium text-gray-900 dark:text-white">主题设置</h3>
      <div className="space-y-2">
        <button
          onClick={() => setTheme('light')}
          className={`w-full flex items-center px-4 py-3 rounded-lg text-left transition-colors ${
            theme === 'light'
              ? 'bg-blue-100 dark:bg-blue-900 text-blue-700 dark:text-blue-300 ring-2 ring-blue-500'
              : 'bg-gray-50 dark:bg-gray-700 text-gray-700 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-600'
          }`}
        >
          <Sun className="mr-3 h-5 w-5" />
          浅色主题
        </button>
        
        <button
          onClick={() => setTheme('dark')}
          className={`w-full flex items-center px-4 py-3 rounded-lg text-left transition-colors ${
            theme === 'dark'
              ? 'bg-blue-100 dark:bg-blue-900 text-blue-700 dark:text-blue-300 ring-2 ring-blue-500'
              : 'bg-gray-50 dark:bg-gray-700 text-gray-700 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-600'
          }`}
        >
          <Moon className="mr-3 h-5 w-5" />
          深色主题
        </button>
        
        <button
          onClick={() => setTheme('system')}
          className={`w-full flex items-center px-4 py-3 rounded-lg text-left transition-colors ${
            theme === 'system'
              ? 'bg-blue-100 dark:bg-blue-900 text-blue-700 dark:text-blue-300 ring-2 ring-blue-500'
              : 'bg-gray-50 dark:bg-gray-700 text-gray-700 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-600'
          }`}
        >
          <Monitor className="mr-3 h-5 w-5" />
          跟随系统
        </button>
      </div>
    </div>
  );
}