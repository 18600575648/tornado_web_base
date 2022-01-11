
class ExtraLog():
    def __init__(self, handler, logger) -> None:
        self.handler = handler
        self.logger = logger
        self.__extra__ = None
        return

    @property
    def extra(self):
        if self.__extra__:
            return self.__extra__

        self.__extra__ = {'request_trace': ''}
        self.__extra__.update(
            {k: v for k, v in self.handler.__dict__.items() if k in self.__extra__})
        return self.__extra__

    def debug(self, msg):
        self.logger.debug(msg, extra=self.extra)
        return

    def info(self, msg):
        self.logger.info(msg, extra=self.extra)
        return

    def warning(self, msg):
        self.logger.warning(msg, extra=self.extra)
        return

    def warn(self, msg):
        self.logger.warn(msg, extra=self.extra)
        return

    def error(self, msg):
        self.logger.error(msg, extra=self.extra)
        return

    def exception(self, msg):
        self.logger.exception(msg, extra=self.extra)
        return

    def critical(self, msg):
        self.logger.critical(msg, extra=self.extra)
        return

    def log(self, level, msg):
        self.logger.log(level, msg, extra=self.extra)
        return