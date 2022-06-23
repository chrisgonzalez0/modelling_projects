with sched as (
	select 
	nts.boxscore_id ,
	nts.home_away ,
	nts.team_id ,
	row_number() over (partition by nts.boxscore_id order by nts."year" desc) "ranker" 
	from nfl_team_schedules nts 
)

select 
nbs.*,
sched1.*,
sched2.*,
case 
	when sched1.home_away='@' then sched1.team_id
	when sched2.home_away='@' then sched2.team_id
	when sched1.ranker=1 then sched1.team_id
end as away_team,
case 
	when sched1.home_away is null then sched1.team_id
	when sched2.home_away is null then sched2.team_id
	when sched2.ranker=2 then sched2.team_id
end as home_team,

coalesce(cast(nbs."Off._Num" as float),0)+coalesce(cast(nbs."ST_Num" as float),0)+coalesce(cast(nbs."Def._Num" as float),0) as total_snaps

from nfl_boxscore_snapcount nbs 
left join sched sched1 on sched1.boxscore_id=nbs.boxscore_id and sched1.ranker=1
left join sched sched2 on sched2.boxscore_id=nbs.boxscore_id and sched2.ranker=2
where sched1.home_away='N'
order by 
	nbs.boxscore_id asc,
	coalesce(cast(nbs."Off._Num" as float),0)+coalesce(cast(nbs."ST_Num" as float),0)+coalesce(cast(nbs."Def._Num" as float),0) desc	

	
	
	




select 
	sum(case when nts.home_away is null then 1 else 0 end) "home",
	sum(case when nts.home_away='@' then 1 else 0 end) "away"
from nfl_team_schedules nts 


select * 
from nfl_team_schedules nts 
where nts.home_away is not null and nts.home_away !='@'



