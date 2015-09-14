import logging
import os
import yaml

has_a_tty = os.isatty(1) # test stdout

def load_run_params(run_params_file):
    with open(run_params_file) as fd:
        dt = yaml.load(fd)

    return dict(run_uuid=dt['run_uuid'],
                comment=dt.get('comment'))


def color_me(color):
    RESET_SEQ = "\033[0m"
    COLOR_SEQ = "\033[1;%dm"

    color_seq = COLOR_SEQ % (30 + color)

    def closure(msg):
        return color_seq + msg + RESET_SEQ
    return closure


class ColoredFormatter(logging.Formatter):
    BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE = range(8)

    colors = {
        'WARNING': color_me(YELLOW),
        'DEBUG': color_me(BLUE),
        'CRITICAL': color_me(RED),
        'ERROR': color_me(RED),
        'INFO': color_me(GREEN)
    }

    def __init__(self, msg, use_color=True, datefmt=None):
        logging.Formatter.__init__(self, msg, datefmt=datefmt)
        self.use_color = use_color

    def format(self, record):
        orig = record.__dict__
        record.__dict__ = record.__dict__.copy()
        levelname = record.levelname

        prn_name = levelname + ' ' * (8 - len(levelname))
        if (levelname in self.colors) and has_a_tty:
            record.levelname = self.colors[levelname](prn_name)
        else:
            record.levelname = prn_name

        # super doesn't work here in 2.6 O_o
        res = logging.Formatter.format(self, record)
        # res = super(ColoredFormatter, self).format(record)

        # restore record, as it will be used by other formatters
        record.__dict__ = orig
        return res


def setup_loggers(def_level=logging.DEBUG, log_fname=None):
    logger = logging.getLogger('cbt')
    logger.setLevel(logging.DEBUG)
    sh = logging.StreamHandler()
    sh.setLevel(def_level)

    log_format = '%(asctime)s - %(levelname)s - %(name)-8s - %(message)s'
    colored_formatter = ColoredFormatter(log_format, datefmt="%H:%M:%S")

    sh.setFormatter(colored_formatter)
    logger.addHandler(sh)

    if log_fname is not None:
        fh = logging.FileHandler(log_fname)
        formatter = logging.Formatter(log_format, datefmt="%H:%M:%S")
        fh.setFormatter(formatter)
        fh.setLevel(logging.DEBUG)
        logger.addHandler(fh)
    else:
        fh = None
