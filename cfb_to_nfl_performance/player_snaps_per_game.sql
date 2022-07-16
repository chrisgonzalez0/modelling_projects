drop table if exists nfl_player_snaps_table;

with sched as (
	select 
		nts.boxscore_id ,
		nts.home_away ,
		nts.team_id ,
		row_number() over (partition by nts.boxscore_id order by nts."year" desc) "ranker" 
	from nfl_team_schedules nts 
),
snapcount_games as (
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
		end as home_team
	from nfl_boxscore_snapcount nbs 
	left join sched sched1 on sched1.boxscore_id=nbs.boxscore_id and sched1.ranker=1
	left join sched sched2 on sched2.boxscore_id=nbs.boxscore_id and sched2.ranker=2
),
snapcount as (
	select 
	distinct 
	sg."Player",
	sg.player_id,
	sg."Pos",
	coalesce(ntr1.team_id ,ntr2.team_id) as "team_id",
	sg.boxscore_id,
	sg."Off._Num" as off_num,
	sg."Off._Pct" as off_pct,
	sg."Def._Num" as def_num,
	sg."Def._Pct" as def_pct,
	sg."ST_Num" as st_num,
	sg."ST_Pct" as st_pct	
	from snapcount_games sg 
	left join nfl_team_rosters ntr1 on sg.player_id=ntr1.player_id and sg.away_team=ntr1.team_id 
	left join nfl_team_rosters ntr2 on sg.player_id=ntr2.player_id and sg.home_team=ntr2.team_id 
	--where sg.boxscore_id='202202130cin'
),
base_stats as (
	/* Stopped Here, this might be the base */
	select 
		pk.college_id,
		sc.*,
		ntr.yrs,ntr.age,ntr.ht,ntr.wt,ntr.pos,
		nts."year",
		boxscores."Passing_Cmp",
		boxscores."Passing_Att",
		boxscores."Passing_Yds",
		boxscores."Passing_TD",
		boxscores."Passing_Int",
		boxscores."Passing_Sk",
		boxscores."Passing_Sk_Yds",
		boxscores."Rushing_Att",
		boxscores."Rushing_Yds",
		boxscores."Rushing_TD",
		boxscores."Receiving_Tgt",
		boxscores."Receiving_Rec",
		boxscores."Receiving_Yds",
		boxscores."Receiving_TD",		
		boxscores."Fumbles_Fmb",
		boxscores."Fumbles_FL",		
		boxscores."Def Interceptions_Int",
		boxscores."Def Interceptions_Yds",		
		boxscores."Def Interceptions_TD",
		boxscores."Def Interceptions_PD",
		boxscores."Sk",
		boxscores."Tackles_Comb",		
		boxscores."Tackles_Solo",
		boxscores."Tackles_Ast",		
		boxscores."Tackles_TFL",		
		boxscores."Tackles_QBHits",
		boxscores."Fumbles_FR",
		boxscores."Fumbles_Yds",		
		boxscores."Fumbles_TD",
		boxscores."Fumbles_FF",
		boxscores."Kick Returns_Rt",		
		boxscores."Kick Returns_Yds",
		boxscores."Kick Returns_TD",
		boxscores."Punt Returns_Ret",		
		boxscores."Punt Returns_Yds",
		boxscores."Punt Returns_TD",		
		boxscores."Scoring_XPM",		
		boxscores."Scoring_XPA",
		boxscores."Scoring_FGM",
		boxscores."Scoring_FGA",		
		boxscores."Punting_Pnt",
		boxscores."Punting_Yds"		
	from snapcount sc
	left join nfl_team_schedules nts on nts.team_id=sc.team_id and nts.boxscore_id=sc.boxscore_id
	left join nfl_team_rosters ntr on ntr.player_id=sc.player_id and ntr.team_id=sc.team_id and ntr."year"=nts."year" 
	left join (
		select 
			coalesce(nbo.boxscore_id,nbd.boxscore_id,nbkr.boxscore_id,nbk.boxscore_id) "boxscore_id",
			coalesce(nbo."Player",nbd."Player",nbkr."Player",nbk."Player") "Player",
			coalesce(nbo.player_id,nbd.player_id,nbkr.player_id,nbk.player_id) "player_id",
			coalesce(nbo."Tm",nbd."Tm",nbkr."Tm",nbk."Tm") "team_id",
			/* Offense Data */
			coalesce(cast(nbo."Passing_Cmp" as float),0) as "Passing_Cmp" ,
			coalesce(cast(nbo."Passing_Att" as float),0) as "Passing_Att" ,
			coalesce(cast(nbo."Passing_Yds" as float),0) as "Passing_Yds" ,
			coalesce(cast(nbo."Passing_TD" as float),0)  as "Passing_TD"  ,
			coalesce(cast(nbo."Passing_Int" as float),0) as "Passing_Int" ,
			coalesce(cast(nbo."Passing_Sk" as float),0) as "Passing_Sk" ,
			coalesce(cast(nbo."Passing_Yds.1" as float),0) as "Passing_Sk_Yds" ,
			coalesce(cast(nbo."Rushing_Att" as float),0) as "Rushing_Att" ,
			coalesce(cast(nbo."Rushing_Yds" as float),0) as "Rushing_Yds" ,
			coalesce(cast(nbo."Rushing_TD" as float),0) as "Rushing_TD" ,
			coalesce(cast(nbo."Receiving_Tgt" as float),0) as "Receiving_Tgt" ,
			coalesce(cast(nbo."Receiving_Rec" as float),0) as "Receiving_Rec" ,
			coalesce(cast(nbo."Receiving_Yds" as float),0) as "Receiving_Yds" ,
			coalesce(cast(nbo."Receiving_TD" as float),0) as "Receiving_TD" ,
			coalesce(cast(nbo."Fumbles_Fmb" as float),0) as "Fumbles_Fmb" ,
			coalesce(cast(nbo."Fumbles_FL" as float),0) as "Fumbles_FL" ,
			/* Defense Data */
			coalesce(cast(nbd."Def Interceptions_Int" as float),0) as "Def Interceptions_Int",
			coalesce(cast(nbd."Def Interceptions_Yds" as float),0) as "Def Interceptions_Yds",
			coalesce(cast(nbd."Def Interceptions_TD" as float),0) as "Def Interceptions_TD",
			coalesce(cast(nbd."Def Interceptions_PD" as float),0) as "Def Interceptions_PD",
			coalesce(cast(nbd."Sk" as float),0) as "Sk",
			coalesce(cast(nbd."Tackles_Comb" as float),0) as "Tackles_Comb",
			coalesce(cast(nbd."Tackles_Solo" as float),0) as "Tackles_Solo",
			coalesce(cast(nbd."Tackles_Ast" as float),0) as "Tackles_Ast",
			coalesce(cast(nbd."Tackles_TFL" as float),0) as "Tackles_TFL",
			coalesce(cast(nbd."Tackles_QBHits" as float),0) as "Tackles_QBHits",
			coalesce(cast(nbd."Fumbles_FR" as float),0) as "Fumbles_FR",
			coalesce(cast(nbd."Fumbles_Yds" as float),0) as "Fumbles_Yds",
			coalesce(cast(nbd."Fumbles_TD" as float),0) as "Fumbles_TD",
			coalesce(cast(nbd."Fumbles_FF" as float),0) as "Fumbles_FF",
			/* Kick Returns */
			coalesce(cast(nbkr."Kick Returns_Rt" as float),0) as "Kick Returns_Rt",
			coalesce(cast(nbkr."Kick Returns_Yds" as float),0) as "Kick Returns_Yds",
			coalesce(cast(nbkr."Kick Returns_TD" as float),0) as "Kick Returns_TD",
			coalesce(cast(nbkr."Punt Returns_Ret" as float),0) as "Punt Returns_Ret",
			coalesce(cast(nbkr."Punt Returns_Yds" as float),0) as "Punt Returns_Yds",
			coalesce(cast(nbkr."Punt Returns_TD" as float),0) as "Punt Returns_TD",
			/* Kicking */
			coalesce(cast(nbk."Scoring_XPM" as float),0) as "Scoring_XPM",
			coalesce(cast(nbk."Scoring_XPA" as float),0) as "Scoring_XPA",
			coalesce(cast(nbk."Scoring_FGM" as float),0) as "Scoring_FGM",
			coalesce(cast(nbk."Scoring_FGA" as float),0) as "Scoring_FGA",
			coalesce(cast(nbk."Punting_Pnt" as float),0) as "Punting_Pnt",
			coalesce(cast(nbk."Punting_Yds" as float),0) as "Punting_Yds"
			
		from nfl_boxscore_offense nbo 
		full outer join nfl_boxscore_defense nbd on nbd.player_id=nbo.player_id and nbd.boxscore_id=nbo.boxscore_id 
		full outer join nfl_boxscore_kick_returns nbkr on nbkr.player_id=coalesce(nbo.player_id,nbd.player_id) and nbkr.boxscore_id=coalesce(nbo.boxscore_id,nbd.boxscore_id) 
		full outer join nfl_boxscore_kicking nbk on nbk.player_id=coalesce(nbo.player_id,nbd.player_id,nbkr.player_id) and nbk.boxscore_id=coalesce(nbo.boxscore_id,nbd.boxscore_id,nbkr.boxscore_id) 
	) boxscores on boxscores.player_id=sc.player_id and boxscores.boxscore_id=sc.boxscore_id
	left join player_keys pk on pk.player_id=sc.player_id
	where 
		ntr.yrs in ('Rook','1','2')
		
		and sc.player_id in (select distinct ntr.player_id 
		                     from nfl_team_rosters ntr 
		                     where ntr.yrs='Rook' and ntr."year" >= 2012)		
) 


/* This has the total number of 
   snaps played in rookie season */
select 
	bs.college_id,
	bs.player_id,
	bs."Player",
	count(distinct bs.yrs) as years_played,
	sum(bs.off_num+bs.def_num+bs.st_num) as snapcounts,
	min(min_year.first_year) as first_year,
	sum(bs."Passing_Cmp") "Passing_Cmp",
	sum(bs."Passing_Att") "Passing_Att",
	sum(bs."Passing_Yds") "Passing_Yds",
	sum(bs."Passing_TD") "Passing_TD",
	sum(bs."Passing_Int") "Passing_Int",
	sum(bs."Passing_Sk") "Passing_Sk",
	sum(bs."Passing_Sk_Yds") "Passing_Sk_Yds",
	sum(bs."Rushing_Att") "Rushing_Att",
	sum(bs."Rushing_Yds") "Rushing_Yds",
	sum(bs."Rushing_TD") "Rushing_TD",
	sum(bs."Receiving_Tgt") "Receiving_Tgt",
	sum(bs."Receiving_Rec") "Receiving_Rec",
	sum(bs."Receiving_Yds") "Receiving_Yds",
	sum(bs."Receiving_TD") "Receiving_TD",		
	sum(bs."Fumbles_Fmb") "Fumbles_Fmb",
	sum(bs."Fumbles_FL") "Fumbles_FL",		
	sum(bs."Def Interceptions_Int") "Def Interceptions_Int",
	sum(bs."Def Interceptions_Yds") "Def Interceptions_Yds",		
	sum(bs."Def Interceptions_TD") "Def Interceptions_TD",
	sum(bs."Def Interceptions_PD") "Def Interceptions_PD",
	sum(bs."Sk") "Sk",
	sum(bs."Tackles_Comb") "Tackles_Comb",		
	sum(bs."Tackles_Solo") "Tackles_Solo",
	sum(bs."Tackles_Ast") "Tackles_Ast",		
	sum(bs."Tackles_TFL") "Tackles_TFL",		
	sum(bs."Tackles_QBHits") "Tackles_QBHits",
	sum(bs."Fumbles_FR") "Fumbles_FR",
	sum(bs."Fumbles_Yds") "Fumbles_Yds",		
	sum(bs."Fumbles_TD") "Fumbles_TD",
	sum(bs."Fumbles_FF") "Fumbles_FF",
	sum(bs."Kick Returns_Rt") "Kick Returns_Rt",		
	sum(bs."Kick Returns_Yds") "Kick Returns_Yds",
	sum(bs."Kick Returns_TD") "Kick Returns_TD",
	sum(bs."Punt Returns_Ret") "Punt Returns_Ret",		
	sum(bs."Punt Returns_Yds") "Punt Returns_Yds",
	sum(bs."Punt Returns_TD") "Punt Returns_TD",		
	sum(bs."Scoring_XPM") "Scoring_XPM",		
	sum(bs."Scoring_XPA") "Scoring_XPA",
	sum(bs."Scoring_FGM") "Scoring_FGM",
	sum(bs."Scoring_FGA") "Scoring_FGA",		
	sum(bs."Punting_Pnt") "Punting_Pnt",
	sum(bs."Punting_Yds") "Punting_Yds"		

into nfl_player_snaps_table	
from base_stats bs
left join (
	select 
	bs.player_id,
	min(bs."year") as first_year
	from base_stats bs 
	group by bs.player_id
) min_year on min_year.player_id=bs.player_id
group by 
	bs.college_id,
	bs.player_id,
	bs."Player"
order by 
	sum(bs.off_num+bs.def_num+bs.st_num) desc
;	


	