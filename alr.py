from argparse import ArgumentParser as ArgParser
import glob, os
import datetime
import re
import pandas as pd


def find_arma_dir(root_list):
    for root in root_list:
        search = os.path.join(root, "**/addons")
        for dir in glob.glob(search, recursive=True):
            dir = os.path.normpath(os.path.dirname(dir))
            yield dir


def find_log_files(dir):
    dir = os.path.join(dir, "**/*.log")
    dir = os.path.normpath(dir)
    for file in glob.glob(dir, recursive=True):
        yield file


class LogParser:
    def __init__(self, path):
        self.path = path
        self.time_created = datetime.datetime.fromtimestamp(os.path.getatime(path))
        self.current_days_offset = datetime.timedelta(days=0)
        self.events = pd.DataFrame(columns=["date", "time", "server", "event", "player"])

    @staticmethod
    def init_csv(path):
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
        with open(path, "w") as f:
            f.write("date,time,server,event,player\n")

    @staticmethod
    def get_time(line):
        if re.match('[\s\d]{1,2}:[\d]{2}:[\d]{2}', line):
            time = line[:8].replace(" ", "")
            time = datetime.datetime.strptime(time, '%H:%M:%S').time()
            return time
        return None

    def check_next_day(self, last_line, line):
        try:
            if LogParser.get_time(last_line) > LogParser.get_time(line):
                return datetime.timedelta(days=1)
        except TypeError:
            pass
        return datetime.timedelta(days=0)

    def parse(self):
        with open(self.path, "rb") as f:
            last_line = ""
            self.current_days_offset = datetime.timedelta(days=0)
            for line in f:
                line = line.decode("latin-1")
                self.current_days_offset += self.check_next_day(last_line, line)

                date = (self.time_created + self.current_days_offset).date()
                time = LogParser.get_time(line)
                server = LogParser.get_server_name(self.path)
                parsers = [self.parse_connect, self.parse_disconnect]
                for p in parsers:
                    id, player = p(line)
                    if player is not None:
                        self.__add_event(date, time, server, id, player)
                last_line = line

    def __add_event(self, date, time, server, event, player):
        self.events = self.events.append(
            {"date": date, "time": time, "server": server, "event": event, "player": player}, ignore_index=True)

    def save(self, path):
        self.events.to_csv(path, sep=",")

    @staticmethod
    def get_server_name(path):
        server_folder = path
        server_folder = os.path.normpath(server_folder + os.sep + os.path.pardir)
        server_folder = os.path.normpath(server_folder + os.sep + os.path.pardir)
        server_folder = os.path.basename(server_folder)
        return server_folder

    @staticmethod
    def parse_connect(line):
        if "BattlEye Server:" in line:
            if " connected" in line:
                line = line[9:]
                line = line.strip("BattlEye Server: Player #")
                split = line.split(" ")
                player = ' '.join(split[1:-2])
                return 1, player
        return None, None

    @staticmethod
    def parse_disconnect(line):
        if "BattlEye Server:" in line:
            if " disconnected" in line:
                line = line[9:]
                line = line.strip("BattlEye Server: Player #")
                split = line.split(" ")
                player = ' '.join(split[1:-1])
                return 2, player
        return None, None


def parser_args():
    """Parses user input via CLI and provides help
    :return: argument object
    """
    description = (
        'Command line interface for extracting arma server log information'
        '\n'
        'https://github.com/Astavinu/ArmaLogsReader')

    parser = ArgParser(description=description)

    parser.add_argument("root", nargs="*", default=["."],
                        help="This is the root directory of the log file search")
    parser.add_argument("-o", "--output-file", default="connects.csv",
                        help="Specifies which file to write to")

    options = parser.parse_args()
    if isinstance(options, tuple):
        args = options[0]
    else:
        args = options

    return args


if __name__ == "__main__":
    args = parser_args()
    df = pd.DataFrame(columns=["date", "time", "server", "event", "player"])
    for folder in find_arma_dir(args.root):
        print("Scanning {0}".format(folder))
        for log in find_log_files(folder):
            p = LogParser(log)
            p.parse()
            df = pd.concat([df, p.events], ignore_index=True)
            print("Events found: {0:4d} in {1}".format(len(p.events), log))

    print("Writing %d Events to %s" % (len(df), args.output_file))
    df.to_csv(args.output_file)
