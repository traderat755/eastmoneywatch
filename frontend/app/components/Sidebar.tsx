import { Settings, Home } from 'lucide-react';

interface SidebarProps {
  currentPage: 'home' | 'settings';
  onPageChange: (page: 'home' | 'settings') => void;
}

export function Sidebar({ currentPage, onPageChange }: SidebarProps) {
  return (
    <div className="w-32 h-screen bg-white dark:bg-gray-800 border-r border-gray-200 dark:border-gray-700 flex flex-col">
      
      <nav className="flex-1 p-4 space-y-2">
        <button
          onClick={() => onPageChange('home')}
          className={`w-full flex items-center px-3 py-2 rounded-lg text-left transition-colors ${
            currentPage === 'home'
              ? 'bg-blue-100 dark:bg-blue-900 text-blue-700 dark:text-blue-300'
              : 'bg-transparent dark:bg-transparent text-gray-500 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700'
          }`}
        >
          <Home className="mr-3 h-4 w-4" />
          主页
        </button>
        
        <button
          onClick={() => onPageChange('settings')}
          className={`w-full flex items-center px-3 py-2 rounded-lg text-left transition-colors ${
            currentPage === 'settings'
              ? 'bg-blue-100 dark:bg-blue-900 text-blue-700 dark:text-blue-300'
              : 'bg-transparent dark:bg-transparent text-gray-500 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700'
          }`}
        >
          <Settings className="mr-3 h-4 w-4" />
          设置
        </button>
      </nav>
    </div>
  );
}