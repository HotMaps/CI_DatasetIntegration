# python 3.5
import sys
import json
from pprint import pprint
import os.path
import jsonschema


print_with_color = False
class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def print(text, bcolor):
    if print_with_color:
        print(bcolor + text + bcolors.ENDC)
    else:
        print(text)

# git directory
# g_dir = '/home/hud/hotmaps/ci-datasets/git-repos/HotmapsLAU'
# g_dir = '/home/hud/hotmaps/ci-datasets/git-repos/pop_tot_curr_density'


def validate_datapackage(file_path):
    # read datapackage.json (dp)
    base_path = os.path.dirname(os.path.abspath(__file__))
    g_dir = base_path
    try:
        if not os.path.exists(file_path):
            print("datapackage.json file missing. Please make sure to place it at root of repository.", bcolors.FAIL)
            return

        dp = json.load(open(file_path))
        if not 'profile' in dp:
            print("datapackage.json file missing 'profile' attribute", bcolors.FAIL)
            return

        profile = dp['profile']
        schema = None

        if profile == 'vector-data-resource':
            print('vector-data-resource', bcolors.OKBLUE)
            schema = json.load(open(g_dir + '/vector-schema.json'))
        elif profile == 'raster-data-resource':
            print('raster-data-resource', bcolors.OKBLUE)
            schema = json.load(open(g_dir + '/raster-schema.json'))
        elif profile == 'tabular-data-resource':
            print('tabular-data-resource', bcolors.OKBLUE)
            schema = json.load(open(g_dir + '/tabular-schema.json'))
        else:
            print('\'profile\' contains an unsupported value! Use only vector-data-resource, raster-data-resource or '
                  'tabular-data-resource', bcolors.FAIL)
            return

        # validate json
        v = jsonschema.Draft4Validator(schema)
        for error in sorted(v.iter_errors(dp), key=str):
            print(error.message, bcolors.FAIL)

        if jsonschema.Draft4Validator(schema).is_valid(dp):
            print('datapackage.json is OK.', bcolors.OKGREEN)
        else:
            print('datapackage.json is not OK! Please check the file again.', bcolors.FAIL)

    except Exception as e:
        print(str(e), bcolors.FAIL)
        return

# list_dirs = [name for name in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, name))]
# list_dirs = sorted(list_dirs)
# for d in list_dirs:
#    d_file_path = os.path.join(base_path, d, 'datapackage.json')
#    print()
#    print("#########################")
#    validate_datapackage.print(d, bcolors.HEADER)
#    print("#########################")
#    validate_datapackage.validate_datapackage(d_file_path)
