#!/usr/bin/env bash


array[1]="potential_solar"
array[2]="industrial_sites_Industrial_Database"
array[3]="potential_municipal_solid_waste"
array[4]="gfa_res_curr_density"
array[5]="gfa_nonres_curr_density"

array[6]="vol_res_curr_density"
array[7]="vol_tot_curr_density"
array[8]="vol_nonres_curr_density"
array[9]="heat_nonres_curr_density "
array[10]="pop_tot_curr_density"

array[11]="potential_biomass"
array[12]="gfa_tot_curr_density "
array[13]="potential_wind"
array[14]="heat_res_curr_density "
array[15]="potential_shallowgeothermal"


for element in ${array[@]}
do
    echo  "Start integration of" $element
    python ci_datasetintegration.py $element
    echo   $element "was integrated"
done
