import logging
from logging.handlers import RotatingFileHandler


log_file_path = "./logging/dx_backend.log"
max_log_size_bytes = 1 * 1024 * 1024 * 1024  # 1GB in bytes
backup_count = 5  # Number of backup log files to keep
file_handler = RotatingFileHandler(
    filename=log_file_path,
    maxBytes=max_log_size_bytes,
    backupCount=backup_count,
)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] [%(module)s:%(lineno)d]: %(message)s"))

# Get the root logger
root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)  # Set the desired log level for the root logger (DEBUG, INFO, etc.)

# Add the RotatingFileHandler to the root logger
root_logger.addHandler(file_handler)


def confirm_logger():
    logging.info("Logging configured correctly")
