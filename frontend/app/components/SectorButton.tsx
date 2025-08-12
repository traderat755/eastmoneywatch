import React from 'react';
import { Button } from './ui/button';
import { Heart, HeartPlus } from 'lucide-react';

interface SectorButtonProps {
  sectorName: string;
  sectorCode: string;
  isPicked: boolean;
  loading?: boolean;
  onClick: (sectorName: string) => void;
}

const SectorButton: React.FC<SectorButtonProps> = ({ sectorName, sectorCode, isPicked, loading, onClick }) => {
  return (
    <div className="flex items-center gap-2">
    <Button
      variant='outline'
      disabled={loading}
      onClick={() => onClick(sectorName)}
    >
      {isPicked ? <Heart className="text-orange-500" size={16} /> : <HeartPlus className="text-gray-400" size={16} />}
    </Button>
    <a href={`https://quote.eastmoney.com/changes/boards/${sectorCode}.html`} target='_blank'>{sectorName}</a>
    </div>
  );
};

export default SectorButton;