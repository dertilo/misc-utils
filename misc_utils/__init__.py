from misc_utils.beartyped_dataclass_patch import (
    beartype_all_dataclasses_of_this_files_parent,
)

beartype_all_dataclasses_of_this_files_parent(__file__)

import sys

if not (sys.version_info.major == 3 and sys.version_info.minor == 9):
    raise Exception("Python 3.9 is required.")
