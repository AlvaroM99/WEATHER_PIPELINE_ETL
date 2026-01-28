from abc import ABC, abstractmethod
import logging

class BaseETL(ABC):
    """Base abstract class for all ETL components."""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def log_start(self, msg: str):
        self.logger.info(f"🚀 START: {msg}")

    def log_end(self, msg: str):
        self.logger.info(f"🏁 END: {msg}")

    def log_error(self, msg: str, error: Exception = None):
        if error:
            self.logger.error(f"❌ ERROR: {msg} - {str(error)}")
        else:
            self.logger.error(f"❌ ERROR: {msg}")


class BaseExtractor(BaseETL):
    """Base class for all Data Extractors."""

    @abstractmethod
    def extract(self, **kwargs):
        """
        Abstract method to extract data from a source.
        Must be implemented by subclasses.
        returns: The extracted data or metadata about extraction.
        """
        pass


class BaseTransformer(BaseETL):
    """Base class for all Data Transformers."""

    @abstractmethod
    def transform(self, **kwargs):
        """
        Abstract method to transform data.
        Must be implemented by subclasses.
        returns: The path to the transformed data or transformation stats.
        """
        pass


class BaseLoader(BaseETL):
    """Base class for all Data Loaders."""

    @abstractmethod
    def load(self, **kwargs):
        """
        Abstract method to load data into a destination.
        Must be implemented by subclasses.
        returns: The number of records loaded or other success metric.
        """
        pass
