import logging

def create_logger(logger_name: str, log_level: int = logging.INFO) -> logging.Logger:
    """
    Create a logger with a given name.
    
    :param logger_name: The name of the logger.
    
    :return: A logger.
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)

    # Only add handler if the logger doesn't already have any
    if not logger.handlers:
        console_handler = logging.StreamHandler()

        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        console_handler.setFormatter(formatter)

        logger.addHandler(console_handler)

    return logger