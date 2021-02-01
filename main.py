#%%
import os
import copy

import numpy as np
import pandas as pd

class BestTrack:
    def __init__(self):
        self.columns = {
            "JMA": ("date", "002", "grade", "lat", "lon", "pres", "vmax",
                    "hiiii", "r50_short", "kllll", "r30_short", "landfall_or_passage"),
            "JTWC": ("basin", "cy", "date", "technum", "tech", "tau", "latN/S", "lonE/W", "vmax", 
                    "pres", "grade", "rad", "windcode", "rad1", "rad2", "rad3", "rad4", "radp", "rrp", "mrd", 
                    "gusts", "eye", "subregion", "maxseas", "initials", "dir", "speed", "stormname", 
                    "depth", "seas", "seascode", "seas1", "seas2", "seas3", "seas4", "_1", "_2", "_3", "_4")
        }
    @classmethod
    def from_agency(self, filenames, agency):
        if agency in ("JMA", "RSMC-Tokyo"):
            return RSMCTokyoReader(filenames)
        if agency in ("JTWC"):
            return JTWCReader(filenames)

    @staticmethod
    def str2int(x):
        if not x == '':
            return int(x)
    @staticmethod
    def str_or_none(x):
        if x == '':
            return None
        else:
            return str(x)

    def copy(self, deep=True):
        return copy.deepcopy(self)

class BestTrackJMA(BestTrack):
    def __init__(self, filename):
        super().__init__()
        self.tcid = None
        self.name = None
        self.data = None
        self.filename = filename
        self.tc_dict = self.get_tc_dict(filename)
    
    def __repr__(self):
        if self.tcid is None:
            return f"{self.filename}\n{self.tc_dict}"
        return f"{self.filename}\n" +\
               f"{self.tcid}\n" +\
               f"{self.name}\n" +\
               f"{self.data}\n"

    def get_tc_dict(self, filename):
        def convert_id(tID, century_bound = 51):
            return int("19"+tID) if int(tID[:2]) >= century_bound else int("20"+tID)
        tc_dict = {}
        with open(filename, 'r', encoding='utf-8') as bst_f:
            for i, line in enumerate(bst_f):
                if line[:5] == '66666':
                    tID = line[6:10]
                    name = line[30:50].strip()
                    if name == "":
                        name = "NONAME"
                    yyyynn = convert_id(tID)
                    year = yyyynn//100
                    number = int(tID[2:])
                    tc_dict[tID] = {"name": name, "yyyynn": yyyynn, "yyyy": year, "number": number, "start_line": i, "nline": int(line[12:15])}
        return tc_dict

    def read(self, id):
        if tcid not in self.tc_dict.keys():
            raise ValueError(f"ID {tcid} is not valid.")
        bt = self.copy(deep=True)
        tc_info = bt.tc_dict[tcid]
        bt.tcid = tcid
        bt.name = tc_info["name"]
        skiprows = tc_info["start_line"] + 1
        nrows = tc_info["nline"]
        names = bt.columns["JMA"]
        converters = {
            "grade": int, "lat": float, "lon": float, "pres": int, "vmax": int,
            "hiiii": bt.str_or_none, "r50_short": bt.str2int, "kllll": bt.str_or_none, "r30_short": bt.str2int
        }
        date_parser = lambda x: pd.to_datetime(x, format="%y%m%d%H")
        bt.data = pd.read_csv(bt.filename, skiprows=skiprows, nrows=nrows, names=names, delim_whitespace=True,
                                index_col=False, parse_dates=["date"], date_parser=date_parser, converters=converters)
        bt.data[['lon', 'lat']] = bt.data[['lon', 'lat']] * 0.1
        bt.data["dir50"] = bt.data["hiiii"].str[0].dropna().astype(np.int64)
        bt.data["r50_long"] = bt.data["hiiii"].str[1:].dropna().astype(np.int64) * 1852
        bt.data["r50_short"] = bt.data["r50_short"].dropna() * 1852
        bt.data["dir30"] = bt.data["kllll"].str[0].dropna().astype(np.int64)
        bt.data["r30_long"] = bt.data["kllll"].str[1:].dropna().astype(np.int64) * 1852
        bt.data["r30_short"] = bt.data["r30_short"].dropna() * 1852
        bt.data["landfall_or_passage"] = np.where(bt.data["landfall_or_passage"] == "#", True, False)
        bt.data = bt.data.drop(["002", "hiiii", "kllll"], axis=1)
        return bt


#%%
bt = BestTrackJMA("bst_all.txt")

#%%
# 2015年以降の全台風のベストトラック記載時刻をファイルに書き出す
tc_dict = pd.DataFrame.from_dict(bt.tc_dict).T
after2015tc = tc_dict[tc_dict["yyyy"]>=2015].index
datetimes = []
for tcid in after2015tc:
    _bt = bt.read(id=tcid)
    datetimes.extend(_bt.data["date"].values)
datetimes = np.array(datetimes).astype("datetime64[s]")
np.savetxt("bt_time_after_2015.txt", datetimes, fmt="%s")

#%%
