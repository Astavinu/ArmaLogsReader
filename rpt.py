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
        print(df)
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
        with open(file, "rb") as f:
            df = pd.read_csv(f, sep=",")
            return df

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

if __name__ == "__main__":
    rpt = LogReport("connects.csv")
    print(rpt.rpt_playtime())
