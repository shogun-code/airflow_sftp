import logging, os

from .common import *

def cleanup_temp_files():
    """Clean up temporary files and directories."""
    try:
        if os.path.exists(TEMP_LOCAL_PATH):
            for file in os.listdir(TEMP_LOCAL_PATH):
                file_path = os.path.join(TEMP_LOCAL_PATH, file)
                if os.path.isfile(file_path):
                    os.remove(file_path)
            logging.info("Cleaned up temporary files")
    except Exception as e:
        logging.warning(f"Error cleaning up temp files: {str(e)}")