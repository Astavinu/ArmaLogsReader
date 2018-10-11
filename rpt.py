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
        df = self.log.transpose()
        df = df.sort_values(by=['player', 'time'])
        data = []

        for row in df.itertuples(index=True):
            if getattr(row, "event") == 1:
                last = row
            if last is not None and getattr(row, "event") == 2:
                if not any(d.get('player', None) == getattr(row, "player") for d in data):
                    data.insert(0, {'player': getattr(row, "player"), 'duration': datetime.timedelta(0), 'sessions': 0})
                duration = [d for d in data if d['player'] == getattr(row, "player")][0]['duration']
                duration += LogReport.get_time(getattr(row, "time")) - LogReport.get_time(getattr(last, "time"))
                [d for d in data if d['player'] == getattr(row, "player")][0]['duration'] = duration
                [d for d in data if d['player'] == getattr(row, "player")][0]['sessions'] += 1

        results = pd.DataFrame(data)
        results = results.set_index('player').sort_values(by=['duration'], ascending=False)

        print(results)
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
