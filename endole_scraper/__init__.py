__version__ = "0.0.1"

import logging
import os

MODULE_PATH = os.path.dirname(os.path.realpath(__file__))
REPO_PATH = os.path.dirname(MODULE_PATH)

DATABASE_PATH = f"{REPO_PATH}/databases"


# Create a custom filter to exclude specific loggers
class ExcludeSpecificLoggersFilter(logging.Filter):
    def filter(self, record):
        excluded_loggers = ['selenium', 'urllib3', 'WDM']
        return not any(record.name.startswith(excluded) for excluded in excluded_loggers)


# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logging.getLogger('selenium').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('WDM').setLevel(logging.WARNING)

# Get the root logger
log = logging.getLogger()

# Add the custom filter to the root logger
log.addFilter(ExcludeSpecificLoggersFilter())
