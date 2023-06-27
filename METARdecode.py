#TODO use ECCODES to convert to BUFR
#https://www.ecmwf.int/sites/default/files/elibrary/2013/13982-conversion-metar-avxml-iwxxm-using-eccodes-and-pyxb.pdf
#OR python-metar package: https://github.com/python-metar/python-metar

from metar import Metar
obs = Metar.Metar('METAR KEWR 111851Z VRB03G19KT 2SM R04R/3000VP6000FT TSRA BR FEW015 BKN040CB BKN065 OVC200 22/22 A2987 RMK AO2 PK WND 29028/1817 WSHFT 1812 TSB05RAB22 SLP114 FRQ LTGICCCCG TS OHD AND NW -N-E MOV NE P0013 T02270215')
print(obs.string())
print(obs)
