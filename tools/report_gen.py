#!/usr/bin/python
"""
This script traverses the dir tree to select .JSON entries to
generate a report in .tex
"""

import argparse
import logging
import os
import sys
import json
import glob
import tempfile

__author__ = 'Jose J Palacios-Perez'

logger = logging.getLogger(__name__)

class Reporter(object):
    OSD_LIST = [1,3,8]
    REACTOR_LIST = [1,2,4]
    ALIEN_LIST = [7,14,21]
    TBL_HEAD = r"""
\begin{table}[h!]
\centering
\begin{tabular}[t]{|l*{6}{|c|}}
   \hline 
"""
    
    def __init__(self, jsonName:str=""):
        """
        This class expects a list of result files to process into a report
        """
        self.jsonName = jsonName
        self.entries = {}
        self.body = {}

    def traverse_dir(self):
        """
        Traverse the given list (.JSON) use .tex template to generate document
        """
        pass

    def find(name, path):
        """
        find a name file in path
        """
        for root, dirs, files in os.walk(path):
            if name in files:
                return os.path.join(root, name)

    def start_fig_table(self, header:list[str]):
        """
        Instantiates the table template for the path and caption
        """
        head_table="""
\\begin{table}\\sffamily
\\begin{tabular}{l*2{C}@{}}
\\toprule
""" +  " & ".join(header) + "\\\\" + """
\\midrule
"""
        #print(head_table)
        return head_table

    def end_fig_table(self, caption:str=""):
        end_table=f"""
\\bottomrule 
\\end{{tabular}}
\\caption{{{caption}}}
\\end{{table}}
"""
        return end_table
        #print(end_table)

    def instance_fig(self, path:str):
        """
        Instantiates the figure template for the path and caption
        """
        add_pic=f"\\addpic{{{path}}}"
        return add_pic
        #print(add_pic) # replace to write to oputput file instead

    def gen_table_row(self, dir_nm:str, proc:str):
        """
        Capture CPU,MEM charts for the current directory
        """
        utils = []
        row = []
        # CPU util on left, MEM util on right
        for metric in [ "cpu", "mem" ]:
            fn = glob.glob(f"{dir_nm}/{proc}_*_top_{metric}.png")
            if fn:
                #logger.info(f"found {fn[0]}")
                row.append(self.instance_fig(fn[0]))
                utils.append(f"{fn[0]}")
        self.entries.update( { f"{dir_nm}": utils} )
        return row

    def get_iops_entry(self, osd_num, reactor_num):
        """
        Generate a IOPs table: columns are the .JSON dict keys,
        row_index is the test stage (num alien threads, num reactors, num OSD)
        """
        entry = self.entries['OSD'][str(osd_num)]["reactors"][str(reactor_num)]
        entry.update({ "aliens": {} })

        for at_num in self.ALIEN_LIST:
            entry["aliens"].update( {str(at_num): {} })
            dir_nm = f"crimson_{osd_num}osd_{reactor_num}reactor_{at_num}at_8fio_lt_1procs_randread"
            fn = glob.glob(f"{dir_nm}/fio_{dir_nm}.json")
            if fn:
                with open(fn[0], 'r') as f:
                    entry["aliens"][str(at_num)] = json.load(f)
                    f.close()

    def gen_iops_table(self, osd_num, reactor_num):
        """
        Generate a results table: colums are measurements, row index is a test config 
        index
        """
        TBL_TAIL =f"""
   \\hline
\\end{{tabular}}
\\caption{{Performance on {osd_num} OSD, {reactor_num} reactors.}}
\\label{{table:iops-{osd_num}osd-{reactor_num}reactor}}
\\end{{table}}
"""
        table = ""
        # This dict has keys measurements
        # To generalise: need reduce (min-max/avg) into a dict
        entry_table = self.entries['OSD'][str(osd_num)]["reactors"][str(reactor_num)]
        body_table = self.body['OSD'][str(osd_num)]["reactors"][str(reactor_num)]
        body_table.update({"table":""}) 
        for at_num in self.ALIEN_LIST:
            entry = entry_table["aliens"][str(at_num)]
            if not table:
                table = self.TBL_HEAD 
                table += r"Alien\\Threads & "
                table += " & ".join(map(lambda x: x.replace(r"_",r"\_"), list(entry.keys())))
                table += r"\\" + "\n" + r"\hline" + "\n"
            table += f" {at_num} & "
            table += " & ".join(map("{:.2f}".format,list(entry.values())))
            table += r"\\" + "\n"
        table += TBL_TAIL
        body_table["table"] = table 

    def gen_charts_table(self, osd_num, reactor_num):
        """
        Generate a charts util table: colums are measurements, row index is a test config 
        index
        """
        body_table = self.body['OSD'][str(osd_num)]["reactors"][str(reactor_num)]
        body_table.update({"charts_table":""}) 
        dt = ""
        for proc in [ "OSD", "FIO" ]:
            # identify the {FIO,OSD}*_top{cpu,mem}.png files to pass to the template
            # One table per process
            dt += self.start_fig_table([r"Alien\\threads", "CPU", "Mem"])
            for at_num in self.ALIEN_LIST:
                row=[]
                # TEST_RESULT
                # Pickup FIO_*.json out -- which can be a list
                dir_nm = f"crimson_{osd_num}osd_{reactor_num}reactor_{at_num}at_8fio_lt_1procs_randread"
                logger.info(f"examining {dir_nm}")
                #os.chdir(dir_nm)
                row.append(str(at_num))
                row += self.gen_table_row(dir_nm, proc)
                dt += r' & '.join(row) + r'\\' + "\n"
                #print(r' & '.join(row) + r'\\')
            dt += self.end_fig_table(
                f"{osd_num} OSD, {reactor_num} Reactors, 4k Random read: {proc} utilisation")
        body_table["charts_table"] = dt

    def start(self):
        """
        Entry point
        """
        self.entries.update({'OSD': {}})
        self.body.update({'OSD': {}})
        # Ideally, load a .json with the file names ordered
        for osd_num in self.OSD_LIST:
            self.entries['OSD'].update({ str(osd_num): { "reactors": {} }})
            self.body['OSD'].update({ str(osd_num): { "reactors": {} }})
            # Chapter header
            #self.body += f"\\chapter{{{osd_num} OSD, 4k Random read}}\n"
            for reactor_num in self.REACTOR_LIST:
                self.entries['OSD'][str(osd_num)]["reactors"].update({ str(reactor_num): {}})
                self.body['OSD'][str(osd_num)]["reactors"].update({ str(reactor_num): {}})
                # Section header: all alien threads in a single table
                #self.body += f"\\section{{{reactor_num} Reactors}}\n"
                self.get_iops_entry(osd_num, reactor_num)
                self.gen_iops_table(osd_num, reactor_num)
                self.gen_charts_table(osd_num, reactor_num)

    def compile(self):
        """
        Compile the .tex document, twice to ensure the references are correct
        """
        for osd_num in self.OSD_LIST:
            print(f"\\chapter{{{osd_num} OSD, 4k Random read}}")
            for reactor_num in self.REACTOR_LIST:
                #print(f"\\section{{{reactor_num} Reactors}}")
                dt = self.body['OSD'][str(osd_num)]["reactors"][str(reactor_num)]
                print(dt["table"])
                #print(dt["charts_table"])
            for reactor_num in self.REACTOR_LIST:
                #print(f"\\section{{{reactor_num} Reactors}}")
                dt = self.body['OSD'][str(osd_num)]["reactors"][str(reactor_num)]
                #print(dt["table"])
                print(dt["charts_table"])
        #print(self.body)
        if self.jsonName:
            with open(self.jsonName, 'w', encoding='utf-8') as f:
                json.dump(self.entries, f, indent=4 ) #, sort_keys=True, cls=TopEntryJSONEncoder)
                f.close()

def main(argv):
    examples = """
    Examples:
    # Produce a performance test report from the current directory
        %prog aprgOutput.log

    # Produce a latency target report from the current directory:
    #
        %prog --latarget latency_target.log

    """
    parser = argparse.ArgumentParser(description="""This tool is used to parse output from the top command""",
                                     epilog=examples, formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument("jsonName", type=str, default=None,
                        help="JSON specifying the performance test results")
    parser.add_argument("-l", "--latarget", action='store_true',
                        help="True to assume latency target run", default=False)
    parser.add_argument("-v", "--verbose", action='store_true',
                        help="True to enable verbose logging mode", default=False)

    options = parser.parse_args(argv)

    if options.verbose:
        logLevel = logging.DEBUG
    else:
        logLevel = logging.INFO

    with tempfile.NamedTemporaryFile(dir='/tmp', delete=False) as tmpfile:
        #print(f"logname: {tmpfile.name}")
        logging.basicConfig(filename=tmpfile.name, encoding='utf-8',level=logLevel)

    logger.debug(f"Got options: {options}")

    report = Reporter(options.jsonName)
    report.start()
    report.compile()

if __name__ == "__main__":
    main(sys.argv[1:])
