import logging

def init_logger_aux(logger_name, filelog_name, console_level=logging.WARNING, file_level=logging.DEBUG):    
    logger = logging.getLogger(logger_name) # type: logging.Logger
    logger.setLevel(file_level)
    fh = logging.FileHandler(filelog_name)
    fh.setLevel(file_level)
    ch = logging.StreamHandler()
    ch.setLevel(console_level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger