"""
    This module implements some logging functionality for CBT especially doing the coloring stuff
    For VT-100 compliant linux terminals, maps these colors to the logging severity and also stores
    these custom formatting log records to the disk using file handlers.
"""

import logging
import os
import yaml


# Determine if file descriptor 1 is attached to a terminal
# basically checking if stdout is available with a terminal

# this is to determine whether the coloring stuff can be done,
# so, if stdout of the system is attached to a terminal, we can apply coloring!
has_a_tty = os.isatty(1) # test stdout

def load_run_params(run_params_file):
    """
        Simple conversin of the given yaml file into a python object for easier manipulation.
        Consequently, retrieve the 'run_uuid' and 'comment' fields from the object.
    """
    with open(run_params_file) as fd:
        dt = yaml.load(fd)

    return dict(run_uuid=dt['run_uuid'],
                comment=dt.get('comment'))


def color_me(color):
    """This function takes an integer as an input (which is a color code)
    and returns a function which is able to convert any given message 
    (string) into one of the colors for which the function was created"""
    
    """Extended ASCII coloring codes for standard terminals
    to check these out, try 'echo -e "<code> linux"' in terminal to see what each of these does to the output!
    do 'man console_codes' for more details of these"""

    # \033 is the terminal 'escape sequence' ESC and will always be at the start of everything
    # 0 -> reset all attributes to their defaults
    RESET_SEQ = "\033[0m"
    # set %d as the color of the text to be printed, this is in effect unless a \033[0m is received, which clears all attrs
    COLOR_SEQ = "\033[1;%dm"

    #    30      set black foreground
    #    31      set red foreground
    #    32      set green foreground
    #    33      set brown foreground
    #    34      set blue foreground
    #    35      set magenta foreground
    #    36      set cyan foreground
    #    37      set white foreground
    color_seq = COLOR_SEQ % (30 + color) # using string formatting to insert the correct color code in appropriate place

    # take a message as input, change it into a form that allows for the given formatting on the linux terminal
    def closure(msg):
        return color_seq + msg + RESET_SEQ
    return closure


class ColoredFormatter(logging.Formatter):
    """A customized formatting class to do coloring of the given record as per severity level
    0 = black, 1 = red, 2 = green, 3 = yellow, 4 = blue, 5 = magenta, 6 =  cyan, 7 = white"""
    
    # creating simple 'macro-like' variables for easier color management
    BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE = range(8)

    # a dictionary of log severity as key, and a function ptr as value, which will convert to a color as mentioned
    colors = {
        'WARNING': color_me(YELLOW),
        'DEBUG': color_me(BLUE),
        'CRITICAL': color_me(RED),
        'ERROR': color_me(RED),
        'INFO': color_me(GREEN)
    }

    # initialize the parent class constructor for coloring
    def __init__(self, msg, use_color=True, datefmt=None):
        logging.Formatter.__init__(self, msg, datefmt=datefmt)
        self.use_color = use_color

    # apply our formatting to a log record
    def format(self, record):
        # backup the original record
        orig = record.__dict__
        # make a shallow copy of the record
        record.__dict__ = record.__dict__.copy()
        # determine the severity level or the record 
        levelname = record.levelname

        # Adding len(largest text) - len(current) amount of spaces at the end of levelname, to make
        # a 'print name' prn which allows for a sexier output on the terminal when being printed, cool!
        prn_name = levelname + ' ' * (8 - len(levelname))

        # if stdout is attached to a terminal, and coloring is possible
        if (levelname in self.colors) and has_a_tty:
            # call the specific coloring function with the message as parameter!
            record.levelname = self.colors[levelname](prn_name)
        else:
            # otherwise, don't do anything
            record.levelname = prn_name

        # result is the return value from the format function where we put in our desired record
        # super doesn't work here in 2.6 O_o
        res = logging.Formatter.format(self, record)
        # res = super(ColoredFormatter, self).format(record)

        # restore record, as it will be used by other formatters
        record.__dict__ = orig

        # return the result record, this has all the sexy coloring attached to it if applicable
        return res


def setup_loggers(def_level=logging.DEBUG, log_fname=None):
    """Setup logging with all the coloring and stuff"""

    # create a new logging object named 'cbt'
    logger = logging.getLogger('cbt')
    # set logging level to DEBUG, just a simple severity level
    logger.setLevel(logging.DEBUG)
    # create handler object, to format logging records and send to a stream
    sh = logging.StreamHandler()
    # set the severity level for the handler
    sh.setLevel(def_level)

    # the log record format to be used
    # asctime   -> time in ASCII format
    # levelname -> severity level of the log in textual form
    # name      -> name of the logging object
    # message   -> the actual log record message
    log_format = '%(asctime)s - %(levelname)s - %(name)-8s - %(message)s'
    
    # instantiating our special coloring formatter for the log
    colored_formatter = ColoredFormatter(log_format, datefmt="%H:%M:%S")
    
    # setting it to the handler to start coloring the logs
    sh.setFormatter(colored_formatter)

    # applying the handler to our logger to start formatting!
    logger.addHandler(sh)

    # if this is the first time code is being run
    if log_fname is not None:
        # file handler is a special handler to allow 'formatted data' writing to files on disk
        fh = logging.FileHandler(log_fname)
        # specifying a format for logging
        formatter = logging.Formatter(log_format, datefmt="%H:%M:%S")
        # setting the formatter for the handler
        fh.setFormatter(formatter)
        # setting severty level for the handler
        fh.setLevel(logging.DEBUG)
        # adding the handler to our logging object
        logger.addHandler(fh)
    else:
        fh = None
