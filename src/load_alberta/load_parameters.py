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
# This script creates the fish_species table containing accessibility and 
# habitat parameters for species of interest from a CSV specified by the user
#
 
import appconfig

query = f"""
    create table {appconfig.dataSchema}.{appconfig.fishSpeciesTable}(
        code varchar(4),
        name varchar,
        allcodes varchar[],
        
        accessibility_gradient double precision not null,
        
        spawn_gradient_min numeric,
        spawn_gradient_max numeric,
        rear_gradient_min numeric,
        rear_gradient_max numeric,
        
        spawn_discharge_min numeric,
        spawn_discharge_max numeric,
        rear_discharge_min numeric,
        rear_discharge_max numeric,
        
        spawn_channel_confinement_min numeric,
        spawn_channel_confinement_max numeric,
        rear_channel_confinement_min numeric,
        rear_channel_confinement_max numeric,
        
        primary key (code)
"""

with appconfig.connectdb() as conn:
    with conn.cursor() as cursor:
        cursor.execute(query)

print (f"""Table {appconfig.dataSchema}.{appconfig.fishSpeciesTable} created""")    