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

	

select *
from nfl_team_rosters ntr 
limit 1000 	
	
select 
	coalesce(nbo.boxscore_id,nbd.boxscore_id,nbkr.boxscore_id,nbk.boxscore_id) "boxscore_id",
	coalesce(nbo."Player",nbd."Player",nbkr."Player",nbk."Player") "Player",
	coalesce(nbo.player_id,nbd.player_id,nbkr.player_id,nbk.player_id) "player_id",
	coalesce(nbo."Tm",nbd."Tm",nbkr."Tm",nbk."Tm") "team_id",
	/* Offense Data */
	coalesce(nbo."Passing_Cmp",0) ,
	nbo."Passing_Att" ,
	nbo."Passing_Yds" ,
	nbo."Passing_TD" ,
	nbo."Passing_Int" ,
	nbo."Passing_Sk" ,
	nbo."Rushing_Att" ,
	nbo."Rushing_Yds" ,
	nbo."Rushing_TD" ,
	nbo."Receiving_Tgt" ,
	nbo."Receiving_Rec" ,
	nbo."Receiving_Yds" ,
	nbo."Receiving_TD" ,
	nbo."Fumbles_Fmb" ,
	nbo."Fumbles_FL" ,
	/* Defense Data */
	nbd."Def Interceptions_Int" ,
	nbd."Def Interceptions_Yds" ,
	nbd."Def Interceptions_TD" ,
	nbd."Def Interceptions_PD" ,
	nbd."Sk" ,
	nbd."Tackles_Comb" ,
	nbd."Tackles_Solo" ,
	nbd."Tackles_Ast" ,
	nbd."Tackles_TFL" ,
	nbd."Tackles_QBHits" ,
	nbd."Fumbles_FR" ,
	nbd."Fumbles_Yds" ,
	nbd."Fumbles_TD" ,
	nbd."Fumbles_FF",
	/* Kick Returns */
	nbkr."Kick Returns_Rt" ,
	nbkr."Kick Returns_Yds" ,
	nbkr."Kick Returns_TD" ,
	nbkr."Punt Returns_Ret" ,
	nbkr."Punt Returns_Yds" ,
	nbkr."Punt Returns_TD" ,
	/* Kicking */
	nbk."Scoring_XPM" ,
	nbk."Scoring_XPA" ,
	nbk."Scoring_FGM" ,
	nbk."Scoring_FGA" ,
	nbk."Punting_Pnt" ,
	nbk."Punting_Yds" 
from nfl_boxscore_offense nbo 
full outer join nfl_boxscore_defense nbd on nbd.player_id=nbo.player_id and nbd.boxscore_id=nbo.boxscore_id 
full outer join nfl_boxscore_kick_returns nbkr on nbkr.player_id=coalesce(nbo.player_id,nbd.player_id) and nbkr.boxscore_id=coalesce(nbo.boxscore_id,nbd.boxscore_id) 
full outer join nfl_boxscore_kicking nbk on nbk.player_id=coalesce(nbo.player_id,nbd.player_id,nbkr.player_id) and nbk.boxscore_id=coalesce(nbo.boxscore_id,nbd.boxscore_id,nbkr.boxscore_id) 
order by coalesce(nbo.boxscore_id,nbd.boxscore_id,nbkr.boxscore_id,nbk.boxscore_id) desc
limit 1000 
	
	
select *
from nfl_boxscore_defense nbd 
limit 1000 
	
	





select 
	sum(case when nts.home_away is null then 1 else 0 end) "home",
	sum(case when nts.home_away='@' then 1 else 0 end) "away"
from nfl_team_schedules nts 


select * 
from nfl_team_schedules nts 
where nts.home_away is not null and nts.home_away !='@'



