#----------------------------------------------------------------------------------
#
# Copyright 2022 by Canadian Wildlife Federation, Alberta Environment and Parks
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#----------------------------------------------------------------------------------

#
# This script creates the database tables that follow the structure
# in the gdb file.
#
 
import appconfig


workingWatershedId = appconfig.config['PROCESSING']['watershed_id']

print ("Processing: " + workingWatershedId)

from processing_scripts import preprocess_watershed
from processing_scripts import load_barriers_cabd 
from processing_scripts import snap_and_break_barriers
from processing_scripts import assign_raw_z
from processing_scripts import smooth_z
from processing_scripts import compute_mainstems
from processing_scripts import compute_gradient

print ("Processing Complete: " + workingWatershedId)
