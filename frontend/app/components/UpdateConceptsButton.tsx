import React, { useState } from 'react';
import { Button } from '@/components/ui/button';
import toast from 'react-hot-toast';

interface UpdateConceptsButtonProps {
  className?: string;
  variant?: 'default' | 'destructive' | 'outline' | 'secondary' | 'ghost' | 'link';
  size?: 'default' | 'sm' | 'lg' | 'icon';
}

export const UpdateConceptsButton: React.FC<UpdateConceptsButtonProps> = ({
  className,
  variant = 'outline',
  size = 'sm'
}) => {
  const [isLoadingConcepts, setIsLoadingConcepts] = useState(false);

  const handleGetConcepts = async () => {
    console.log('UpdateConceptsButton: handleGetConcepts 被调用');
    
    // 二次确认对话框
    const confirmed = window.confirm('更新概念会中断盘口异动监测，是否确认开始？');
    console.log('UpdateConceptsButton: 用户确认结果:', confirmed);
    
    if (!confirmed) {
      console.log('UpdateConceptsButton: 用户取消了更新概念操作');
      return;
    }
    
    console.log('UpdateConceptsButton: 用户确认更新概念，开始执行...');
    setIsLoadingConcepts(true);
    
    try {
      console.log('UpdateConceptsButton: 发送更新概念请求到后端...');
      const response = await fetch('http://localhost:61125/api/queue_get_concepts', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
      });

      const result = await response.json();
      console.log('UpdateConceptsButton: 后端响应:', result);

      if (response.ok) {
        console.log('UpdateConceptsButton: 更新概念任务启动成功');
        toast.success(result.message || 'getConcepts任务已启动');
      } else {
        console.log('UpdateConceptsButton: 更新概念任务启动失败:', result.message);
        toast.error(result.message || '启动getConcepts任务失败');
      }
    } catch (error) {
      console.error('UpdateConceptsButton: 调用getConcepts API失败:', error);
      toast.error('调用getConcepts API失败');
    } finally {
      setIsLoadingConcepts(false);
      console.log('UpdateConceptsButton: 更新概念操作完成，重置loading状态');
    }
  };

  return (
    <Button
      onClick={handleGetConcepts}
      disabled={isLoadingConcepts}
      variant={variant}
      size={size}
      className={className}
    >
      {isLoadingConcepts ? '更新中...' : '更新概念'}
    </Button>
  );
};

export default UpdateConceptsButton; 