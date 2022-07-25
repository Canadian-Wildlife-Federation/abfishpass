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

roadTable = appconfig.config['CREATE_LOAD_SCRIPT']['road_table'];
railTable = appconfig.config['CREATE_LOAD_SCRIPT']['rail_table'];
trailTable = appconfig.config['CREATE_LOAD_SCRIPT']['trail_table'];


query = f"""
    drop schema if exists {appconfig.dataSchema} cascade;
    
    create schema {appconfig.dataSchema};
    
    create table {appconfig.dataSchema}.{appconfig.streamTable} (
        id uuid not null,
        watershed_id varchar,
        waterbody_id integer,
        stream_name varchar,
        feature_type varchar,
        strahler_order integer,
        hydro_code varchar,
        fish_species varchar,
        geometry geometry(linestring, {appconfig.dataSrid}) not null,
        primary key(id)
    );

    create index {appconfig.streamTable}_geom2d_idx on {appconfig.dataSchema}.{appconfig.streamTable} using gist(geometry);
    
    create table {appconfig.dataSchema}.{roadTable} ( 
        id uuid not null,
        feature_type integer,
        name varchar,
        highway_number varchar,
        road_class varchar,
        geo_source varchar,
        geo_date timestamp,
        feature_type_source varchar,
        feature_type_date timestamp,
        globalid varchar,
        update_date timestamp,
        geometry geometry(geometry, {appconfig.dataSrid}) not null,
        primary key(id)
    );
    create index {roadTable}_geom_idx on {appconfig.dataSchema}.{roadTable} using gist(geometry);
    
    create table {appconfig.dataSchema}.{railTable} ( 
        id uuid not null,
        feature_type integer,
        geo_source varchar,
        geo_date timestamp,
        feature_type_source varchar,
        feature_type_date timestamp,
        globalid varchar,
        update_date timestamp,
        geometry geometry(linestring, {appconfig.dataSrid}) not null,
        primary key(id)
    );
    create index {railTable}_geom_idx on {appconfig.dataSchema}.{railTable} using gist(geometry);

    create table {appconfig.dataSchema}.{trailTable} ( 
        id uuid not null,
        name varchar(100),
        trailid varchar(20),
        type varchar(15),
        status varchar(10),
        season varchar(15),
        designation varchar(20),
        surface varchar(25),
        use_type varchar(20),
        accessibility varchar(15),
        commercial_operator varchar(5),
        adopted varchar(50),
        land_ownership varchar(30),
        average_width double precision,
        minimum_clearing_width double precision,
        hike varchar(30),
        bike varchar(30),
        winter_bike varchar(30),
        horse varchar(30),
        wagon varchar(30),
        ohv varchar(30),
        two_wheel_motor varchar(30),
        side_x_side varchar(30),
        vehicle_4x4 varchar(30),
        snowshoe varchar(30),
        skateski varchar(30),
        classicski varchar(30),
        skitour varchar(30),
        skijoring varchar(30),
        snowvehicle varchar(30),
        dogsled varchar(30),
        timerestriction1_start varchar(30),
        timerestriction1_end varchar(30),
        timerestriction2_start varchar(30),
        timerestriction2_end varchar(30),
        timerestriction3_start varchar(30),
        timerestriction3_end varchar(30),
        spatialrestriction1 varchar(150),
        spatialrestriction2 varchar(150),
        trail_condition varchar(10),
        trail_report_date timestamp,
        condition_source varchar(15),
        comments varchar(250),
        datasource varchar(30),
        disposition_number varchar(15),
        dateupdate timestamp,
        updateby varchar(35),
        datasourcedate timestamp,
        link varchar(50),
        data_display varchar(150),
        level_develop varchar(40),
        use_type2 varchar(25),
        picture integer,
        geometry geometry(geometry, {appconfig.dataSrid}) not null,
        primary key(id)
    );
    create index {trailTable}_geom_idx on {appconfig.dataSchema}.{trailTable} using gist(geometry);
    
    create table {appconfig.dataSchema}.{appconfig.fishSpeciesTable}(
        code varchar(4),
        name varchar,
        allcodes varchar[],
        
        accessibility_gradient double precision not null,
        
        habitat_gradient_min numeric,
        habitat_gradient_max numeric,
        
        habitat_discharge_min numeric,
        habitat_discharge_max numeric,
        
        habitat_channel_confinement_min numeric,
        habitat_channel_confinement_max numeric,
        
        primary key (code)
    );
    insert into {appconfig.dataSchema}.{appconfig.fishSpeciesTable}(
        code, name, allcodes, accessibility_gradient, 
        habitat_gradient_min,habitat_gradient_max,habitat_discharge_min,habitat_discharge_max,
        habitat_channel_confinement_min, habitat_channel_confinement_max)
    values 
        ('argr', 'Arctic Grayling', ARRAY['argr'], 0.35, 0, 0.35, 0, 100, 0, 100),
        ('wbtr', 'Western Arctic Bull Trout', ARRAY['wbtr', 'bltr'], 0.35, 0, 0.35, 0, 100, 0, 100),
        ('mnwh', 'Mountain Whitefish', ARRAY['mnwh'], 0.35, 0, 0.35, 0, 100, 0, 100),
        ('artr', 'Athabasca Rainbow Trout', ARRAY['artr', 'rntr'], 0.35, 0, 0.35, 0, 100, 0, 100);
"""
#print (query)
with appconfig.connectdb() as conn:
    with conn.cursor() as cursor:
        cursor.execute(query)

print (f"""Database schema {appconfig.dataSchema} created and ready for data """)    
    