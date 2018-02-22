select ( 
	select (ST_SummaryStatsAgg( ST_Clip(heat_tot_curr_density.rast, 1, st_transform(tbl_lau.geom,3035), true),1,true)) 
	from geo.heat_tot_curr_density where st_intersects(heat_tot_curr_density.rast,st_transform(tbl_lau.geom,3035)) 
).*, tbl_lau.nuts_id 
from geo.nuts as tbl_lau 
limit 10;

select ( 
	select (ST_SummaryStatsAgg( ST_Clip(heat_tot_curr_density.rast, 1, st_transform(tbl_lau.geom,3035), true),1,true)) 
	from geo.heat_tot_curr_density where st_intersects(heat_tot_curr_density.rast,st_transform(tbl_lau.geom,3035)) 
).*, tbl_lau.comm_id 
from public.lau as tbl_lau 
limit 10;