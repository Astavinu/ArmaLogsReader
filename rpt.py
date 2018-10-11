import os
from argparse import ArgumentParser as ArgParser
import pandas as pd
import re
import datetime


class LogReport:
    def __init__(self, file):
        self.log = self.sort(self.read(file)).transpose()

    def rpt_missions(self):
        pass

    def rpt_playtime(self):
        df = pd.DataFrame(columns=pd.unique(self.log.transpose().server))
        results = {}
        parser_state = {}

        for line in self.log:
            c = self.log[line]
            if c.event == 1:
                last = parser_state.get(c.player)
                if last is not None and last["event"] == 2:
                    if results.get(c.player) is None:
                        results.update({c.player: {}})
                    duration = results.get(c.player).get(c.server)
                    if duration is None:
                        duration = datetime.timedelta(0)
                    duration += LogReport.get_time(last["time"]) - LogReport.get_time(c.time)
                    results.get(c.player).update({c.server: duration})
                    df.update(pd.DataFrame({c.server: duration}, index=c.player))
            parser_state.update({c.player: {"server": c.server, "event": c.event, "date": c.date, "time": c.time}})

        return results

    def read(self, file):
        if os.path.isfile(file):
            with open(file, "rb") as f:
                df = pd.read_csv(f, sep=",")
                return df
        print("Input file '{0}' does not exist!".format(file))
        exit(1)

    def sort(self, df):
        df = df.sort_values(by=["date", "time"], ascending=False)
        return df

    @staticmethod
    def get_time(line):
        if re.match('[\s\d]{1,2}:[\d]{2}:[\d]{2}', line):
            time = line[:8].replace(" ", "")
            time = datetime.datetime.strptime(time, '%H:%M:%S')
            return time
        return None


class CLI:
    @staticmethod
    def main(args, method_name=None):
        """Maps CLI input to direct function calls
        :param args: arguments to pass over
        :param method_name: method to call (default None)
        :return: result of invoked method
        """

        if method_name is None:
            if args.command is None:
                return 1
            method_name = args.command
        class_name = CLI

        try:
            method = getattr(class_name, method_name)
        except AttributeError:
            raise NotImplementedError(
                "Class `{}` does not implement `{}`".format(class_name.__class__.__name__, method_name))
        return method(args)

    @staticmethod
    def playtime(args):
        rpt = LogReport(args.input_file)
        rpt.rpt_playtime()


def parser_args():
    """Parses user input via CLI and provides help
    :return: argument object
    """
    description = (
        'Command line interface for extracting arma server log information'
        '\n'
        'https://github.com/Astavinu/ArmaLogsReader')

    parser = ArgParser(description=description)
    command = parser.add_subparsers(dest="command")
    command.add_parser("playtime",
                       help='extracts playtime information per server and player')
    command.add_parser("missions",
                       help='extracts mission information per server')

    parser.add_argument("input_file", nargs="?", default="connects.csv",
                        help="This is the file to be read")
    parser.add_argument("-o", "--output-file", default="report.csv",
                        help="Specifies which file to write to")

    options = parser.parse_args()
    if isinstance(options, tuple):
        args = options[0]
    else:
        args = options

    return args


if __name__ == "__main__":
    args = parser_args()
    CLI.main(args)
