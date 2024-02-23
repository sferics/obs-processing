#TODO outsource all different BUFR main function versions to this file and import in respective script
#from decode_bufr_functions import decode_bufr_XX

#TODO do all the necessary imports at the beginning of each function (or wherever needed)

"""
def decode_bufr_gt

        generator, BufrFile = plbufr.read_bufr(PATH, columns=bf.relevant_keys, required_columns=bf.required_keys, filter_method=all, return_method="gen", skip_na=True)

        time_period = ""

        for row in generator:
            if debug: print("ROW", row)
            #TODO possibly BUG in pdbufr? timePeriod=0 never exists; write bug report in github!
            try:
                if row[bf.tp] is not None:
                    time_period = row[bf.tp]
            except: pass

            try:
                repl_10 = row[bf.replication] == 10 or row[bf.ext_replication] == 10
                if time_period == -1 and repl_10 and (row[bf.ww] is not None or row[bf.rr] is not None):
                    continue
            except: pass

            location = str(row[bf.wmo]) + "0"
            if location not in known_stations: continue

            datetime = row[bf.dt]
            if datetime is None:
                if verbose: print("NO DATETIME:", FILE)
                continue

            if location not in obs_bufr[ID]:            obs_bufr[ID][location]           = {}
            if datetime not in obs_bufr[ID][location]:  obs_bufr[ID][location][datetime] = {}

            modifier_list = []
            for key in (bf.obs_sequence, bf.sensor_height, bf.sensor_depth, bf.vertical_signf):
                try:
                    if row[key] is not None:
                        modifier_list.append((key, row[key]))
                except: continue

            obs_list = []

            #for ignore_key in bf.ignore_keys:
            for ignore_key in bf.ignore_keys.intersection(set(row.keys())):
                try:    del row[ignore_key]
                except: pass

            for key in row:
                if row[key] is not None: obs_list.append((key, row[key]))

            if modifier_list and obs_list: obs_list = modifier_list + obs_list
            if obs_list:
                try:    obs_bufr[ID][location][datetime][time_period] += obs_list
                except: obs_bufr[ID][location][datetime][time_period] = obs_list
                new_obs += 1

        if new_obs:
            file_statuses.add( ("parsed", ID) )
            log.debug(f"PARSED: '{FILE}'")
        else:
            file_statuses.add( ("empty", ID) )
            log.info(f"EMPTY:  '{FILE}'")

        # close the file handle of the BufrFile object
        BufrFile.close()
        #stop_time = dt.utcnow()
"""
