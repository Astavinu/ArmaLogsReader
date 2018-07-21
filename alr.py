import glob, os
import datetime
import re
import pandas as pd


def find_arma_dir():
    for dir in glob.glob("**/A3Server/", recursive=True):
        dir = os.path.normpath(dir + os.path.pardir)
        yield dir


def find_log_files(dir):
    dir = os.path.normpath(dir+"/A3Server")
    for file in glob.glob(dir+"/*"):
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
        self.events = self.events.append({"date": date, "time": time, "server": server, "event": event, "player": player}, ignore_index=True)

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


if __name__ == "__main__":
    path_output = "connects.csv"
    # LogParser.init_csv(path_output)
    df = pd.DataFrame(columns=["date", "time", "server", "event", "player"])
    for folder in find_arma_dir():
        for log in find_log_files(folder):
            if ".rpt" not in log and os.path.isfile(log):
                print(log)
                p = LogParser(log)
                p.parse()
                df = pd.concat([df, p.events], ignore_index=True)
    print(df)
    df.to_csv(path_output)
