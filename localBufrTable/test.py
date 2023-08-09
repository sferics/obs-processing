import eccodes as ec
import sys, os

old_path = ec.codes_definition_path()
print(old_path)
os.putenv('ECCODES_DEFINITION_PATH', "mydefs" + ":" + old_path)
os.environ['ECCODES_DEFINITION_PATH'] = "mydefs" + ":" + old_path
print(os.environ['ECCODES_DEFINITION_PATH'])

FILE="ikco_217.local.bufr"

with open(FILE, "rb") as f:
    try:
        bufr = ec.codes_bufr_new_from_file(f)
        if bufr is None: print("empty FILE:", FILE)
        ec.codes_set(bufr, "unpack", 1)
    except Exception as e:
        print(e); sys.exit()

    iterid = ec.codes_bufr_keys_iterator_new(bufr)

    while ec.codes_bufr_keys_iterator_next(iterid):
        try:                    key = ec.codes_bufr_keys_iterator_get_name(iterid)
        except Exception as e:  print(e)
        else:
            print(key)
            print(ec.codes_get_array(bufr, key))

os.environ['ECCODES_DEFINITION_PATH'] = old_path
