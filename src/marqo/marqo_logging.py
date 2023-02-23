import logging

mq_logger = logging.getLogger("marqo")
mq_logger.setLevel(logging.INFO)

formatter = logging.Formatter(
    "{asctime} logger:'marqo' {levelname} {message}", style='{')
ch = logging.StreamHandler()

# the ch stream handler won't accept any logs from a higher level than the one set here:
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
mq_logger.addHandler(ch)
