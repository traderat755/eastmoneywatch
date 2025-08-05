import React from 'react';
import { ThemeToggle } from './ThemeToggle';

export function SettingsPage() {
  return (
    <div className="p-6 bg-white dark:bg-gray-900 min-h-screen">
      <div className="max-w-2xl">
        <h1 className="text-2xl font-bold text-gray-900 dark:text-white mb-6">设置</h1>
        
        <div className="space-y-8">
          <ThemeToggle />
          
          <div className="border-t border-gray-200 dark:border-gray-700 pt-6">
            <h3 className="text-lg font-medium text-gray-900 dark:text-white mb-4">关于</h3>
            <p className="text-gray-600 dark:text-gray-400">
              股票市场监控应用 - 实时监控股票异动数据
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}