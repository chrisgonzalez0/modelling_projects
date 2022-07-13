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
		ntr.yrs,ntr.age,ntr.ht,ntr.wt,ntr.pos
		--,boxscores.*
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
) 

/* This has the total number of 
   snaps played in rookie season */
select 
	bs.college_id,
	bs.player_id,
	bs."Player",
	bs."Pos",
	bs.team_id,
	sum(bs.off_num+bs.def_num+bs.st_num)
from base_stats bs
where bs."Pos"='RB'
group by 
	bs.college_id,
	bs.player_id,
	bs."Player",
	bs."Pos",
	bs.team_id
order by 
	sum(bs.off_num+bs.def_num+bs.st_num) desc
	
	
	
	
	
	
	
	