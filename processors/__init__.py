"""
Processors Module Initialization
Provides Factory for getting specific processors
"""
from processors.sbrs_processor import SBRSProcessor
from processors.collection_processor import CollectionProcessor
from processors.mc_processor import MCProcessor
from processors.mb_processor import MBProcessor
from processors.ardebt_processor import ArdebtProcessor
from processors.mainbill_processor import MainBillProcessor

class ProcessorFactory:
    """Factory class to create appropriate processors"""
    
    @staticmethod
    def get_processor(file_type, db):
        """
        Returns an instance of the requested processor
        file_type: SBRS, COLLECTION, MC, MB, ARDEBT, MAINBILL
        """
        file_type = file_type.upper()
        
        processors = {
            'SBRS': SBRSProcessor,
            'COLLECTION': CollectionProcessor,
            'MC': MCProcessor,
            'MB': MBProcessor,
            'ARDEBT': ArdebtProcessor,
            'MAINBILL': MainBillProcessor
        }
        
        processor_class = processors.get(file_type)
        if processor_class:
            return processor_class(db)
        return None
