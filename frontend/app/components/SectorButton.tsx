import React from 'react';
import { Button } from './ui/button';
import { Heart, HeartPlus } from 'lucide-react';

interface SectorButtonProps {
  sectorName: string;
  isPicked: boolean;
  loading?: boolean;
  onClick: (sectorName: string) => void;
}

const SectorButton: React.FC<SectorButtonProps> = ({ sectorName, isPicked, loading, onClick }) => {
  return (
    <Button
      variant='outline'
      disabled={loading}
      className={`flex items-center gap-1 px-2 py-1 text-xs`}
      onClick={() => onClick(sectorName)}
    >
      {isPicked ? <Heart className="text-orange-500" size={16} /> : <HeartPlus className="text-gray-400" size={16} />}
      {sectorName}
    </Button>
  );
};

export default SectorButton;