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

from processing_scripts import preprocess_watershed
from processing_scripts import load_and_snap_barriers_cabd 
from processing_scripts import compute_modelled_crossings
from processing_scripts import load_assessment_data
from processing_scripts import compute_mainstems
from processing_scripts import assign_raw_z
from processing_scripts import smooth_z
from processing_scripts import compute_vertex_gradient
from processing_scripts import compute_segment_gradient
from processing_scripts import break_streams_at_barriers
from processing_scripts import load_and_snap_fishobservation
from processing_scripts import compute_gradient_accessibility
from processing_scripts import compute_updown_barriers_fish
from processing_scripts import compute_habitat_models
from processing_scripts import compute_modelled_crossings_upstream_values

iniSection = appconfig.args.args[0]

workingWatershedId = appconfig.config[iniSection]['watershed_id']

print ("Processing: " + workingWatershedId)

# re-load unbroken stream table
preprocess_watershed.main()
# load_and_snap_barriers_cabd.main()
# compute_modelled_crossings.main()
# load_assessment_data.main()
compute_mainstems.main()
assign_raw_z.main()
smooth_z.main()
compute_vertex_gradient.main()
break_streams_at_barriers.main()
# re-assign elevations to broken streams
assign_raw_z.main()
smooth_z.main()
compute_segment_gradient.main()
# load_and_snap_fishobservation.main()
compute_updown_barriers_fish.main()
compute_gradient_accessibility.main()
compute_habitat_models.main()
compute_modelled_crossings_upstream_values.main()

print ("Processing Complete: " + workingWatershedId)
