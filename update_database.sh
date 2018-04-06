#!/bin/bash

#pip install -r requirements.txt

# sudo apt-get install python-numpy libgdal1h gdal-bin libgdal-dev

array[1]="heat_res_curr_density"
array[2]="heat_nonres_curr_density"
array[3]="gfa_tot_curr_density"
array[4]="gfa_res_curr_density"
array[5]="gfa_nonres_curr_density"

array[6]="vol_res_curr_density"
array[7]="vol_tot_curr_density"
array[8]="vol_nonres_curr_density"
array[9]="industrial_sites_Industrial_Database"
array[10]="pop_tot_curr_density"

array[11]="potential_biomass"
array[12]="potential_municipal_solid_waste"
array[13]="potential_wind"
array[14]="potential_solar"
array[15]="potential_shallowgeothermal"


for element in ${array[@]}
do
    echo  "Start integration of" $element
    python ci_run_dataset_integration.py $element
    echo   $element "was integrated"
done

