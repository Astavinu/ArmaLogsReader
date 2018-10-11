import os
from argparse import ArgumentParser as ArgParser
import pandas as pd
import re
import datetime


class PlayerData:
    def __init__(self):
        self.player = ""
        self.duration = datetime.timedelta(0)
        self.sessions = 0
        self.errors = 0
        self.mission = ""
        self.server = ""
        self.datetime = datetime.datetime(year=1990, month=1, day=1)

    def get_duration_string(self):
        hours, remainder = divmod(self.duration.total_seconds(), 3600)
        minutes, seconds = divmod(remainder, 60)

        # Formatted only for hours and minutes as requested
        return '{:02.0f}:{:02.0f}'.format(hours, minutes)

    def get_current_mission(self, missions):
        missions = missions.sort_values(by=["datetime"], ascending=False)
        for m in missions.itertuples():
            if m.server is self.server and m.datetime < self.datetime:
                return m.mission
        return ""


class LogReport:
    def __init__(self, file):
        self.log = self.sort(self.read(file))

    def rpt_missions(self):
        pass

    def rpt_playtime_total(self):
        df = self.log
        df = df.sort_values(by=['player', 'date', 'time'])
        data = []
        player_data = PlayerData()
        last = None

        for row in df.itertuples(index=True):
            if row.event == 1:
                if last is None:
                    last = row
                    continue
                # if connect event
                if last.event == 1:
                    player_data.errors += 1

                if row.player is not last.player:
                    # if new player: save
                    data.insert(0, {'player': player_data.player,
                                    'duration': player_data.get_duration_string(),
                                    'errors': player_data.errors,
                                    'sessions': player_data.sessions})
                    player_data = PlayerData()

            elif row.event == 2 and last.event == 1:
                # if disconnect event after connect event
                if row.player is last.player:
                    player_data.player = row.player
                    player_data.sessions += 1
                    player_data.duration += LogReport.get_time(row) - LogReport.get_time(last)
            last = row

        results = pd.DataFrame(data)
        results = results.set_index('player').sort_values(by=['duration'], ascending=False)

        return results

    def rpt_playtime_server(self):
        df = self.log
        df = df.sort_values(by=['player', 'server', 'date', 'time'])
        data = []
        player_data = PlayerData()
        last = None

        for row in df.itertuples(index=True):
            if row.event == 1:
                # if connect event
                if last is None:
                    last = row
                    continue

                if row.player is not last.player or last.server is not row.server:
                    # if new player: save
                    data.insert(0, {'player': player_data.player,
                                    'server': player_data.server,
                                    'duration': player_data.get_duration_string(),
                                    'errors': player_data.errors,
                                    'sessions': player_data.sessions})
                    player_data = PlayerData()

                if last.event == 1:
                    player_data.errors += 1

            elif row.event == 2 and last.event == 1:
                # if disconnect event after connect event
                if row.player is last.player:
                    player_data.player = row.player
                    player_data.server = row.server
                    player_data.sessions += 1
                    player_data.duration += LogReport.get_time(row) - LogReport.get_time(last)
            last = row

        results = pd.DataFrame(data)
        results = results.sort_values(by=['player', 'server'], ascending=True)

        return results

    def rpt_playtime_missions(self):
        df = self.log
        df = df.sort_values(by=['player', 'server', 'date', 'time'])
        data = []
        player_data = PlayerData()
        last = None

        missions_list = []
        for row in df.itertuples(index=True):
            if row.event == 3:
                missions_list.insert(0, {'datetime': LogReport.get_time(row), 'mission': row.mission, 'server': row.server})
        missions = pd.DataFrame(missions_list)

        for row in df.itertuples(index=True):
            if row.event == 1:
                # if connect event
                if last is None:
                    last = row
                    continue

                if row.player is not last.player or last.mission is not row.mission:
                    # if new player: save
                    data.insert(0, {'player': player_data.player,
                                    'datetime': player_data.datetime,
                                    'mission': player_data.get_current_mission(missions),
                                    'duration': player_data.get_duration_string(),
                                    'errors': player_data.errors,
                                    'sessions': player_data.sessions})
                    player_data = PlayerData()

                if last.event == 1:
                    player_data.errors += 1

            elif row.event == 2 and last.event == 1:
                # if disconnect event after connect event
                if row.player is last.player:
                    player_data.player = row.player
                    player_data.server = row.server
                    player_data.datetime = LogReport.get_time(row)
                    player_data.sessions += 1
                    player_data.duration += LogReport.get_time(row) - LogReport.get_time(last)

            if row.event == 3:
                player_data.mission = row.mission

            last = row

        results = pd.DataFrame(data)
        results = results.sort_values(by=['player', 'duration'], ascending=False)

        return results

    @staticmethod
    def read(file):
        if os.path.isfile(file):
            with open(file, "rb") as f:
                df = pd.read_csv(f, sep=",")
                return df
        print("Input file '{0}' does not exist!".format(file))
        exit(1)

    @staticmethod
    def sort(df):
        df = df.sort_values(by=["date", "time"], ascending=False)
        return df

    @staticmethod
    def get_time(row):
        time_line = row.time
        date_line = row.date
        if re.match('[\s\d]{1,2}:[\d]{2}:[\d]{2}', time_line):
            time = time_line[:8].replace(" ", "")
            if re.match('[\s\d]{4}-[\d]{2}-[\d]{2}', date_line):
                date = date_line[:10]
                date = datetime.datetime.strptime("{}/{}".format(date, time), '%Y-%m-%d/%H:%M:%S')
                return date
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
        df = rpt.rpt_playtime_total()
        df.to_csv(args.output_file)

    @staticmethod
    def playtime_server(args):
        rpt = LogReport(args.input_file)
        df = rpt.rpt_playtime_server()
        df.to_csv(args.output_file)

    @staticmethod
    def playtime_missions(args):
        rpt = LogReport(args.input_file)
        df = rpt.rpt_playtime_missions()
        df.to_csv(args.output_file)


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
                       help='extracts playtime information per player')
    command.add_parser("playtime_server",
                       help='extracts playtime information per player and server')
    command.add_parser("playtime_missions",
                       help='extracts playtime information per player and mission')
    command.add_parser("missions",
                       help='extracts mission information per mission')

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
