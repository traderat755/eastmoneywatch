import { Settings, Home, Star } from 'lucide-react';
import { Button } from './ui/button';
interface SidebarProps {
  currentPage: 'home' | 'settings' | 'picked';
  onPageChange: (page: 'home' | 'settings' | 'picked') => void;
}

export function Sidebar({ currentPage, onPageChange }: SidebarProps) {
  return (
    <div className="w-18 h-screen bg-white dark:bg-gray-800 border-r border-gray-200 dark:border-gray-700 flex flex-col">

      <nav className="flex-1 p-4 space-y-2">
        <Button
          onClick={() => onPageChange('home')}
          className={`w-full flex items-center rounded-lg text-left transition-colors ${
            currentPage === 'home'
              ? 'bg-blue-100 dark:bg-blue-900 text-blue-700 dark:text-blue-300'
              : 'bg-transparent dark:bg-transparent text-gray-500 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700'
          }`}
        >
          <Home className="h-4 w-4" />
        </Button>

        <Button
          onClick={() => onPageChange('picked')}
          className={`w-full flex items-center rounded-lg text-left transition-colors ${
            currentPage === 'picked'
              ? 'bg-blue-100 dark:bg-blue-900 text-blue-700 dark:text-blue-300'
              : 'bg-transparent dark:bg-transparent text-gray-500 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700'
          }`}
        >
          <Star className="h-4 w-4" />
        </Button>

        <Button
          onClick={() => onPageChange('settings')}
          className={`w-full flex items-center rounded-lg text-left transition-colors ${
            currentPage === 'settings'
              ? 'bg-blue-100 dark:bg-blue-900 text-blue-700 dark:text-blue-300'
              : 'bg-transparent dark:bg-transparent text-gray-500 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700'
          }`}
        >
          <Settings className="h-4 w-4" />
        </Button>
      </nav>
    </div>
  );
}