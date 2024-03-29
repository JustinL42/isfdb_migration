# This file contains a list of title id's taken from isdb's list of
# highly award winning novels and the most viewed novels on the site.
# They are used to present new users with a list of titles they are
# likely to have read for the purpose of the cold start. The list is
# transformed into a dictionary for populating the cold_start_rank
# propery during import.

# TODO: automate the population of this list by scraping:\
# http://www.isfdb.org/cgi-bin/stats.cgi?14
# http://www.isfdb.org/cgi-bin/most_popular.cgi?1+all

COLD_START_LIST = [
    1972,
    1632550,
    364691,
    1017019,
    1629,
    979458,
    2103,
    19616,
    2319,
    1475,
    2036,
    413,
    1112,
    1330,
    1234,
    1254319,
    2004,
    1095,
    153332,
    158099,
    2283,
    20971,
    12305,
    1878015,
    257301,
    2087,
    8656,
    1208773,
    1182,
    2594511,
    1117,
    186521,
    1502,
    19870,
    872410,
    879726,
    1749,
    2313427,
    2248,
    7662,
    1181,
    2122,
    2735816,
    1319,
    1779018,
    1495,
    2234,
    2485,
    872030,
    1148,
    1573744,
    1648,
    1884,
    2084,
    23340,
    2423,
    866747,
    1574,
    188038,
    630797,
    1611,
    7659,
    2037,
    1063,
    1695,
    1624,
    1911,
    1375279,
    867793,
    1298,
    1863,
    19493,
    7700,
    1245152,
    1358,
    1243,
    15862,
    1098026,
    1952,
    1842005,
    1375,
    2266,
    2191551,
    25149,
    1017352,
    2370690,
    157782,
    2225,
    1816,
    1493,
    7653,
    2243,
    985187,
    1773,
    2080,
    2346745,
    21883,
    1912,
    19763,
    1158,
    6130,
    29172,
    21401,
    1368,
    2500310,
    948,
    2592014,
    914,
    1766349,
    1044,
    11471,
    2009,
    2309,
    1589,
    937,
    16515,
    2733317,
    13981,
    2086430,
    1532,
    1970,
    1861,
    977935,
    8150,
    3658,
    2276,
    1372055,
    7526,
    1896926,
    1485,
    1098746,
    2233,
    1840,
    20728,
    1363996,
    1927,
    1398,
    1928,
    2758983,
    1272,
    7977,
    1511,
    4261,
    2484,
    3161,
    17694,
    1068869,
    1899,
    2389,
    1248655,
    2139,
    1439,
    8401,
    1098025,
    1772,
    1678924,
    181278,
    2419,
    895,
    1111,
    9795,
    1149,
    1961,
    2370764,
    1425,
    265,
    743721,
    2192,
    957,
    2681329,
    2231,
    1840457,
    1406,
    1209198,
    1159,
    222811,
    1964,
    2378,
    23594,
    11,
    876498,
    1976720,
    1766,
    12,
    30701,
    1000,
    1451,
    1303,
    1139069,
    7666,
    19920,
    2072,
    1391453,
    21818,
    1678159,
    1017,
    220141,
    13569,
    123,
    1564195,
    1811,
    1069551,
    437,
    20127,
    1608,
    1110,
    6123,
    2048,
    5530,
    1321113,
    1627,
    2185,
    1712007,
    1120,
    1272139,
    6132,
    1136305,
    7879,
    69917,
    1247,
    6698,
    6520,
    1904,
    1699,
    2394,
    1215600,
    152099,
    1213,
    1325681,
    8151,
    1798,
    1867,
    23596,
    1847,
    2226723,
    11852,
    23363,
    863,
    861,
    23854,
    1122710,
    19766,
    1779,
    1043,
    8659,
    306491,
    2208,
    2574799,
    1290,
    23634,
    1284,
    2773927,
    1498983,
    935,
    158238,
    1328,
    23658,
    2320,
    1180,
    991,
    998,
    1460,
    21411,
    1121,
    22802,
    6102,
    8652,
    2247,
    1666,
    1164,
    1401,
    2357481,
    928435,
    1792080,
    6129,
    3349,
    1034,
    2195,
    2373,
    2493,
    2340,
    1193,
    5748,
    1396,
    981,
    2259280,
    19765,
    882,
    2358630,
    22074,
    8658,
    990,
    1399,
    1197,
    263611,
    795,
    180684,
    4225,
    1724,
    1638,
    2593386,
    1009,
    152789,
    1702,
    2029,
    220901,
    2482,
    2437,
    1499,
    6541,
    1262,
    275041,
    7924,
    171190,
    1192,
    12262,
    1585,
    2470721,
    20150,
    1860004,
    8530,
    158105,
    1754847,
    6131,
    972557,
    2027,
    158237,
    1199,
    2753471,
    766,
    222211,
    6101,
    6886,
    2462,
    2179,
    9841,
    2059610,
    177551,
    1008,
    23859,
    878,
    156797,
    19764,
    2279,
    1720,
    1572,
    26690,
    6154,
    4435,
    2358305,
    1119,
    8605,
    2033,
    915,
    9502,
    1505,
    1937,
    1813,
    8657,
    1796,
    1147,
    2250,
    1903,
    2019754,
    2127,
    1045096,
    2306,
    659129,
    1910,
    19893,
    58505,
    2608610,
    279,
    1339,
    2110,
    4662,
    2003071,
    221281,
    1360,
    6546,
    1198,
    936,
    1289,
    1004,
    8973,
    1879049,
    2200,
    2031,
    2336013,
    1205,
    2013560,
    20131,
    20935,
    5537,
    1712838,
    454,
    20999,
    1957,
    1459,
    5267,
    1444424,
    7661,
    7349,
    1969,
    841,
    1708191,
    6543,
    1307896,
    1892,
    924,
    2432,
    4863,
    1605,
    2506,
    2594330,
    170655,
    1678,
    829,
    1194,
    1516,
    1905,
    1785845,
    2022,
    1947564,
    1690,
    1439280,
    1019,
    221031,
    877,
    172854,
    1564,
    7348,
    6100,
    1494,
    14252,
    1733,
    1452,
    1054,
    6005,
    7656,
    1092,
    31505,
    180697,
    1797,
    2035003,
    772057,
    352,
    185955,
    431,
    2059,
    19615,
    1604,
    2717735,
    481,
    1448141,
    2527,
    714493,
    899,
    1240075,
    2026,
    1422355,
    21392,
    182777,
    2078,
    11661,
    1380,
    1646,
    1385,
    2016,
    7654,
    862,
    775,
    2746820,
    231461,
    1443914,
    901,
    4809,
    2013,
    1938602,
    11934,
    2514379,
    1312,
    2152974,
    33261,
    2055699,
    8157,
    2237,
    373891,
    1191,
    6535,
    2504,
    865,
    1959,
    2096050,
    1732,
    2003946,
    2060,
    1104964,
    1639,
    307811,
    19907,
    2234630,
    1595,
    1920,
    2511,
    2210922,
    3413,
    2191670,
    5268,
    1828,
    1479,
    2322,
    1228,
    1108448,
    1297291,
    874805,
    3044,
    2945,
    407,
    1150,
    1721,
    1613,
    1011,
    14353,
    2343,
    541,
    2617530,
    2507,
    224351,
    1940,
    5697,
    10303,
    2421,
    1610,
    1083,
    2526,
    1327,
    1696,
    14293,
    2946,
    1427,
    9600,
    2018,
    7134,
    2075,
    2409157,
    2821,
    1314249,
    2324,
    1046939,
    2483,
    1967,
    826,
    1868,
    2370793,
    2370680,
    1446491,
    1285,
    1149382,
    820,
    1068946,
    7655,
    208301,
    3418,
    23041,
    3734,
    21365,
    807,
    21443,
    933,
    1372,
    2317,
    799,
    136364,
    1299,
    988116,
    2892201,
    1200,
    870900,
    1609,
    12530,
    3045,
    8599,
    9063,
    6240,
    949,
    1012,
    9505,
    1371,
    1029,
    186583,
    102957,
    2077,
    2013970,
    787,
    1862882,
    6349,
    6124,
    1093793,
    2270,
    168621,
    1056,
    22722,
    1340,
    23326,
    11176,
    1636,
    80,
    6155,
    11454,
    773,
    1076,
    1860,
    1489547,
    5340,
    23386,
    8153,
    12598,
    1576,
    1709,
    1712148,
    4436,
    1391750,
    8736,
    1156610,
    1415,
    21015,
    1196,
    4668,
    27661,
    2763013,
    3350,
    2608628,
    5266,
    2212737,
    2203,
    1101348,
    186453,
    1195,
    2108,
    22964,
    2131883,
    1947856,
    1373,
    1059,
    2300763,
    2325,
    171778,
    2439,
    172869,
    6703,
    1898,
    71,
    2528993,
    3568,
    1165640,
    3241,
    184895,
    2049,
    4803,
    2008,
    19767,
    920,
    6529,
    864,
    1941020,
    3512,
    1016791,
    886,
    22723,
    1042,
    2158,
    1342153,
    6006,
    1034377,
    14558,
    5818,
    1438,
    1606,
    801,
    2773823,
    1819,
    2772637,
    1440,
    2226695,
    1665490,
    1575254,
    1157,
    1564624,
    23591,
    1132116,
    6776,
    908306,
    1177,
    184811,
    2025,
    171348,
    1908,
    897,
    9147,
    2756656,
    64,
    2170862,
    2082,
    1468367,
    1580,
    1143220,
    2162,
    4804,
    5347,
    2259277,
    1457,
    1142449,
    5274,
    1719,
    1144,
    2878074,
    453,
    2331286,
    116,
    2333231,
    1883,
    1989076,
    2298,
    1933685,
    2064,
    5344,
    154552,
    2329,
    11453,
    2696088,
    151679,
    2247289,
    2236614,
    1753,
    1808032,
    273621,
    1717433,
    1232,
    851536,
    197281,
    467,
    390661,
    1790,
    698725,
    1304,
    20856,
    2081,
    19918,
    1404,
    12274,
    6897,
    2372,
    8238,
    1066,
    1783,
    982,
    20170,
    2772326,
    1865,
    2422620,
    2034,
    1708645,
    2030,
    21041,
    5275,
    2494,
    1529,
    14997,
    950,
    2260453,
    1028,
    185131,
    9565,
    218,
    171524,
    8531,
    23394,
    17097,
    1997,
    1815,
    13718,
    9367,
    1712830,
    12233,
    3416,
    2110087,
    847,
    170838,
    828338,
    6908,
    72,
    1293,
    9506,
    2454144,
    2209,
    1760617,
    961,
    22983,
    1567,
    6909,
    2269,
    374,
    4806,
    2315,
    21729,
    1222,
    1201,
    1987906,
    8156,
    1754844,
    870588,
    1495549,
    827333,
    866,
    829343,
    4036,
    1246,
    2204,
    357261,
    2756720,
    5270,
    1620311,
    20847,
    1554708,
    69,
    829497,
    8993,
    181865,
    9601,
    20108,
    9607,
    2321,
    2136,
    1357,
    3732,
    6532,
    1346,
    1143,
    1486,
    13434,
    2471,
    7810,
    2007905,
    2201,
    179323,
    5269,
    152469,
    22812,
    2112,
    20983,
    289,
    1104,
    1391996,
    2062,
    156904,
    3979,
    1414,
    6504,
    2281367,
    1471714,
    1873,
    1403967,
    6133,
    3509,
    424,
    2503163,
    1691,
    2516387,
    2131547,
    12361,
    1960212,
    10010,
    1840895,
    1214616,
    9647,
    155428,
    2107,
    1488,
    7778,
    2121,
    7754,
    2431,
    1496,
    2754023,
    762,
    2054005,
    1825,
    1888423,
    9336,
    1041,
]

cold_start_ranks = {
    COLD_START_LIST[i]: i + 1 for i in range(len(COLD_START_LIST))
}
