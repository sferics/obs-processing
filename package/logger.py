import logging
import os

#TODO is a seperate logging class really necessary? what could be the benefits over a function?

class LoggerClass:
    def __init__(self, config={"log_path":"log"}):
        
        self.log_levels = { "CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET" }
        self.log_path = config["log_path"]
        
        if not os.path.exists(self.log_path):
            os.makedirs(self.log_path)

#TODO integrate into above class
def get_logger(script_name, log_level="NOTSET", log_path="log", mode="w", formatter=""):
    """
    Parameter:
    ----------

    Notes:
    ------
    inspired by: https://stackoverflow.com/a/69693313

    Return:
    -------

    """
    from global_variables import log_levels
    assert(log_level in log_levels)
    import logging, logging.handlers

    if not os.path.exists(log_path): os.makedirs(log_path)

    logger = logging.getLogger(script_name)
    logger.setLevel( getattr(logging, log_level) )

    if formatter:
        #formatter = logging.Formatter('%(asctime)s:%(levelname)s : %(name)s : %(message)s')
        formatter = logging.Formatter(formatter)
        file_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(f"{log_path}/{script_name}.log", mode=mode)
    if logger.hasHandlers(): logger.handlers.clear()
    logger.addHandler(file_handler)

    return logger
