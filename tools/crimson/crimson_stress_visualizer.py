#!/usr/bin/env python3
import argparse
import pandas as pd
from pandas.core.frame import DataFrame
import seaborn as sns
import matplotlib as mpl
from matplotlib import rcParams
import itertools

# This is a tool that convert stress test results to figures.
# We add some data preprocessing procedure and encapsulate
# them together for easily use. DrawTask is defined to
# preprocess the data if you want combine some columns and
# specify the figure arguments such as figure's title ,size
# and outpath. Plot is defined to do the plotting work. It
# does not care about specific data.
# For developer, You can easily customize your task based
# on the templates and adding it to Map (before the __main__).
# Plotting librarySeaborn and Matplotlib are called in this tool.
# For more infomation:
# https://seaborn.pydata.org/
# https://matplotlib.org/


# Define the plotting method here
# Not related to data and specific scenarios
class Plot():
    # Read required paremeters from task
    def __init__(self, task):
        self.to_draw = task.to_draw
        self.X = task.x_index
        self.Y = task.y_index
        self.Z = task.z_index

    # Global settings for plot method
    # More instructions can be found on the top websites
    @staticmethod
    def plotSet():
        pass

    # Specific plot method
    def plot(self):
        self.ax = None
        return self.ax


# Specific drawtask for one picture
# Assign data parameter such as x,y,z variable
# and specific picture parameter such as title
class DrawTask():
    # Do not need to rewrite the method
    def __init__(self, env, data):
        self.plotFuc = env.plotType
        self.data = data
        self.args = env.task_args
        self.x_index = ""
        self.y_index = ""
        self.z_index = []

    # Set some assistant parameters
    def parameter_set(self):
        pass

    # Set the parameter required for drawing
    def args_set(self):
        pass

    # Clean the data such as combine two columns
    def data_clean(self):
        pass

    # Do not need to rewrite the method
    def draw(self):
        p = self.plotFuc(self)
        self.ax = p.plot()

    # Make targeted changes to images
    # More instructions can be found on the top websites
    def figset(self):
        pass

    # Export the figure
    def figout(self):
        pass

    # Do not need to rewrite the method
    def run(self):
        self.parameter_set()
        self.args_set()
        self.data_clean()
        self.draw()
        self.figset()
        self.figout()

    # Show help information of the task
    @staticmethod
    def taskinfo():
        pass


# Plot method for line chart
class LinePlot(Plot):
    def __init__(self, task):
        super().__init__(task)
        while len(self.Z) < 3:
            self.Z.append(None)

    @staticmethod
    def plotSet():
        sns.set_theme(style="whitegrid")        # Choose from  white, dark, whitegrid, darkgrid, ticks
        sns.set_context(context="notebook")     # Choose from paper, notebook, talk, poster
        rcParams['font.family'] = 'Arial'       # Set font as Arial

    def plot(self):
        print(self.Z)
        return sns.relplot(
            data=self.to_draw,
            kind="line",   # plot line chart
            marker='o',    # Line with points
            x=self.X,
            y=self.Y,
            hue=self.Z[0],
            style=self.Z[1],
            # markers=True,  # Use markers to distinguish Z-type
            dashes=True,  # Use dashes to distinguish Z-type
        )


class CephDrawTask(DrawTask):
    # Parse given args
    def args_set(self):
        self.x_index = "Thread_num"
        self.z_index = []
        self.to_draw = DataFrame()

        if self.args:
            args_list = list(self.args)
            args_len = len(args_list)
            for i in range(args_len):
                index = args_list[i]
                if index not in self.data:
                    raise Exception("Index Wrong")
                elif i == 0:
                    self.y_index = index
                elif i == 1:
                    self.x_index = index
                elif i >= 5:
                    raise Exception("Too many parameters")
                else:
                    self.z_index.append(index)
        if not self.z_index:
            if self.multi_stores:
                self.z_index = ["OSD"] + ["Store"]
            else:
                self.z_index = ["OSD"]

    def parameter_set(self):
        self.optype = ""
        self.block_size = ""
        self.client_num = ""
        self.test_tool = ""
        self.test_time = ""
        self.title = ""
        self.fig_title = ""
        self.multi_stores = False
        titletype = ""
        block_name = "Block_size"
        op_name = "OPtype"
        df = self.data
        
        self.block_size = df[block_name][0]
        if self.block_size == "4K":
            titletype += " 4K "
        elif self.block_size == "4M":
            titletype += " 4M "
        else:
            raise Exception("Block size not clear")

        op_col = list(df[op_name].unique())
        if len(op_col) != 1:
            raise Exception("Operation type Not Unique")
        else:
            op = df[op_name][0]
            if "write" in op.lower():
                self.optype = "write"
            elif "read" in op.lower():
                self.optype = "read"
            titletype += op

        time_col = list(df['Time'].unique())
        if len(time_col) != 1:
            raise Exception("Test Time Not Unique")
        else:
            self.test_time = time_col[0]

        tool_col = list(df['Tool'].unique())
        if len(tool_col) != 1:
            raise Exception("Test Tool Not Unique")
        else:
            self.test_tool = tool_col[0]
        
        client_col = list(df['Client_num'].unique())
        if len(client_col) != 1:
            raise Exception("Client_num Not Unique")
        else:
            self.client_num = client_col[0]
        
        osd_col = list(df['OSD'].unique())
        if len(osd_col) != 1:
            crimson_df = df.loc[df['OSD']=="Crimson"]
            classic_df = df.loc[df['OSD']!="Crimson"]
            crimson_store_col = list(crimson_df['Store'].unique())
            classic_store_col = list(classic_df['Store'].unique())
            if len(crimson_store_col) != 1:
                self.multi_stores = True
            else:
                self.title += "Crimson " + crimson_store_col[0] + " vs "
            if len(classic_store_col) != 1:
                raise Exception("Classic Store Not Unique")
            else:
                self.title += "Classic " + classic_store_col[0]
        else:
            store_col = list(df['Store'].unique())
            if len(store_col) == 1:
                self.title += osd_col[0] + " " + store_col[0] + " (Different Versions)"
            else:
                raise Exception("Store Not Unique")
           
        self.title += titletype

        self.fig_title = "1 OSD, " + str(self.client_num) + " Client, " \
                    + self.test_tool +", Time=" + str(self.test_time) +"s"

        self.outpath = self.optype + "_" + self.block_size + "_"   

    def figset(self):
        self.ax.figure.suptitle(self.title,  # Set super title
                                x=0.43,
                                y=1.06,
                                fontsize=16)
        self.ax.ax.set_title(self.fig_title,       # Set title
                             fontsize=12,
                             x=0.45,
                             y=1)
        self.ax.fig.text(0.75,  # Set comment
                         0.8,
                         "",
                         linespacing=1.5)
        self.ax.fig.set_size_inches(12, 6)       # Set figure size
        sns.move_legend(self.ax,                # Set legend's position
                        "center right",
                        bbox_to_anchor=(0.9, 0.5))

    def figout(self):
        self.ax.savefig(self.outpath, dpi=300)  # Save the figure


class CephCommonTask(CephDrawTask):
    def args_set(self):
        self.y_index = "Latency(ms)"
        super().args_set()
        self.outpath += self.y_index.split("(")[0]
    
    def data_clean(self):
        const_index = self.z_index + [self.x_index, "Version"]
        self.to_draw = self.data[[self.y_index] + const_index].copy()
        self.to_draw['OSD'] = self.to_draw['OSD'] + "(" + self.to_draw['Version'].map(str) + ")"

    @staticmethod
    def taskinfo():
        print("Common ceph drawing method")
        print("Default Y-index is 'Latency(ms)', "
              + "X-index is 'Thread_num', "
              + "Z-index is 'OSD'")
        print("Add args to task-args to change them(Y, X, Z in order)")


class CephBandWidthTask(CephDrawTask):
    def args_set(self):
        super().args_set()
        self.y_index = "Bandwidth(MB/s)"
        self.outpath += "Bandwidth"

    # Combine bandwidth in client and device
    def data_clean(self):
        const_index = self.z_index + [self.x_index, "Version"]
        temp_data = self.data[[self.y_index] + const_index].copy()
        temp_data.loc[:, "Type"] = "Client"
        data_device = DataFrame()
        dr_name = "Device_Read(MB/s)"
        dw_name = "Device_Write(MB/s)"

        if self.optype == "read":
            data_device = self.data[[dr_name] + const_index].copy()
            data_device.rename(columns={dr_name: self.y_index}, inplace=True)
            data_device.loc[:, "Type"] = "Device"
        else:
            data_dr = self.data[[dr_name] + const_index].copy()
            data_dr.rename(columns={dr_name: self.y_index}, inplace=True)
            data_dr.loc[:, "Type"] = "Device-read"
            data_dw = self.data[[dw_name] + const_index].copy()
            data_dw.rename(columns={dw_name: self.y_index}, inplace=True)
            data_dw.loc[:, "Type"] = "Device-write"
            data_device = pd.concat([data_dr, data_dw], ignore_index=True)

        self.z_index.append("Type")
        self.to_draw = pd.concat([temp_data, data_device], ignore_index=True)
        self.to_draw['OSD'] = self.to_draw['OSD'] + "(" + self.to_draw['Version'].map(str) + ")"

    @staticmethod
    def taskinfo():
        print("Combine client bandwidth and device bandwidth, "
              + "add Z-index 'Type' to distinguish them")
        print("Default X-index is 'Thread_num', Z-index is 'OSD'")
        print("Add args to task-args to change them(Y, X, Z in order)")


class CrimsonUtilizationTask(CephDrawTask):
    def args_set(self):
        super().args_set()
        self.y_index = "CPU-Utilization"
        self.outpath += "Utilization"

    # Combine cpu-utilization and reactor-utilization
    def data_clean(self):
        Rname = "Reactor_Utilization"
        const_index = self.z_index + [self.x_index, "Version"]
        temp_data = self.data[[self.y_index] + const_index].copy()
        data_reactor = self.data[[Rname] + const_index].copy()
        data_reactor = data_reactor.loc[data_reactor["OSD"] == "Crimson"]
        data_reactor.loc[:, 'OSD'] = "Crimson-reactor"
        data_reactor.rename(columns={Rname: self.y_index}, inplace=True)
        self.to_draw = pd.concat([temp_data, data_reactor],
                                 ignore_index=True)
        self.to_draw['OSD'] = self.to_draw['OSD'] + "(" + self.to_draw['Version'].map(str) + ")"

    @staticmethod
    def taskinfo():
        print("Combine cpu-utilization and reactor-utilization, "
              + "add new name 'Crimson-reactor' to distinguish them")
        print("Default X-index is 'Thread_num', Z-index is 'OSD'")
        print("Add args to task-args to change them(Y, X, Z in order)")


class Runner():
    def __init__(self, env):
        self.env = env

    def run(self):
        if self.env.args.task_info:
            self.env.show_info()
        elif self.env.args.show_available:
            self.env.show_map()
        else:
            self.env.init_parameter()
            self.env.load_data()
            self.env.classify_data()
            for task in self.env.task_list:
                print("Task " + str(task) + " start:")
                for data in self.env.datalist:
                    T = task(self.env, data)
                    T.run()


class Environment():
    def __init__(self, args):
        self.args = args
        self.classify_list = []
        self.help_classify = []
        self.task_list = []
        self.data = DataFrame()
        self.datalist = []
        self.plotType = None
        self.task_args = []

    # Load data to Dataframe
    def load_data(self):
        print(self.args)
        for datapath in self.args.data:
            datain = pd.read_csv(datapath)
            self.data = pd.concat([self.data, datain], ignore_index=True)

        if self.data.empty:
            raise Exception("Data is empty")
        if(len(self.args.data) > 1):
            self.data.to_csv("out.csv")

    # Read command line arguments
    def init_parameter(self):
        for task_name in self.args.task:
            if task_name in TaskMap:
                self.task_list.append(TaskMap[task_name])
            else:
                raise Exception("Task not definded")

        if self.args.fig_type in PlotMap:
            self.plotType = PlotMap[self.args.fig_type]
        else:
            raise Exception("Plot not definded")

        if self.args.divide:
            for type in self.args.divide:
                self.classify_list.append(type)

        if self.args.task_args:
            self.task_args = self.args.task_args
            if len(self.task_list) > 1:
                raise Exception("args can only be passed to one task")

        self.plotType.plotSet()    # Make global parameters take effect

    # If you have data in different scenarios
    # you can separate them according to this category
    def classify_data(self):
        if not self.classify_list:
            self.datalist.append(self.data)
        else:
            for name in self.classify_list:
                type = self.data[name].unique()
                self.help_classify.append(type)
            for result in itertools.product(*self.help_classify):
                print(result)
                dataout = self.data
                for colname, colval in zip(self.classify_list, result):
                    dataout = dataout[dataout[colname] == colval].copy()
                dataout.reset_index(drop=True, inplace=True)
                if not dataout.empty:
                    self.datalist.append(dataout)

    def show_info(self):
        for task_name in TaskMap:
            print(task_name + ":")
            TaskMap[task_name].taskinfo()
            print()

    def show_map(self):
        print("Available task:")
        for task_name in TaskMap:
            print(task_name)
        print()
        print("Available figure type:")
        for type in PlotMap:
            print(type)


# If you add a task or figtype
# add it here
TaskMap = {
    "ceph": CephCommonTask,
    "ceph-bandwidth": CephBandWidthTask,
    "crimson-utilization": CrimsonUtilizationTask,
}
PlotMap = {
    "line": LinePlot,
}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='')

    parser.add_argument('--data',
                        type=str,
                        nargs='+',
                        help='data(s) in csv format')

    parser.add_argument('--task',
                        type=str,
                        nargs='+',
                        help='draw task')

    parser.add_argument('--task-args',
                        type=str,
                        nargs='+',
                        help='Args passed to one task, e.g \'IOPS\' \'Core\'')

    parser.add_argument('--divide',
                        type=str,
                        nargs='+',
                        help='Classify the data based on given column')

    parser.add_argument('--fig-type',
                        type=str,
                        default='line',
                        help='The type of figure')

    parser.add_argument('--task-info',
                        action='store_true',
                        help='Show the task information')

    parser.add_argument('--show-available',
                        action='store_true',
                        help='Show existing tasks and figure types')

    args = parser.parse_args()
    R = Runner(Environment(args))
    R.run()

