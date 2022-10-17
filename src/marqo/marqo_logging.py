import logging

mq_logger = logging.getLogger("marqo")
mq_logger.setLevel(logging.INFO)

formatter = logging.Formatter(
    "{asctime} logger:'marqo' {levelname} {message}", style='{')
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)
mq_logger.addHandler(ch)
