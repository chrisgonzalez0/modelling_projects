drop table if exists cfb_player_query_table;

select 
	cs.college_href,
	cs."School",
	cct.conference_href, 
	regexp_replace(cs."School" , '[^0-9]+', '', 'g') as team_rank,
	cs."Opponent",
	cs.opp_href ,
	cs.opp_conf_href,
	regexp_replace(cs."Opponent" , '[^0-9]+', '', 'g') as opp_rank,	
	cs.boxscore_href,
	cs."year" ,
	cs."G",
	coalesce(cs.home_away,'H') as home_away,
	coalesce(cs.w_l,'') as w_l,
	cs."Pts",cs."Opp" ,
	
	case when cs.w_l='W' then cs."W"-1 else cs."W" end as "wins_so_far",
	case when cs.w_l='L' then cs."L"-1 else cs."L" end as "losses_so_far",

	
	cr."Player" ,
	cr.player_href,
	coalesce(cr."Class",'') "Class" ,
	coalesce(cr."Pos",'') "Pos" ,
	cphw.height,
	cast(split_part(cphw.height,'-',1) as float)*12+cast(split_part(cphw.height,'-',2) as float) as height_inches,	
	cphw.weight ,
	case when cphw.height is null or cphw.weight is null then 1 else 0 end "missing_hw",
	
	coalesce(stats."Passing_Att",0) "Passing_Att",
	coalesce(stats."Passing_Cmp",0) "Passing_Cmp",
	coalesce(stats."Passing_Yds",0) "Passing_Yds",
	coalesce(stats."Passing_TD",0) "Passing_TD",
	coalesce(stats."Passing_Int",0) "Passing_Int",
	coalesce(stats."Rushing_Att",0) "Rushing_Att",
	coalesce(stats."Rushing_Yds",0) "Rushing_Yds",
	coalesce(stats."Rushing_TD",0) "Rushing_TD",
	coalesce(stats."Receiving_Rec",0) "Receiving_Rec",
	coalesce(stats."Receiving_Yds",0) "Receiving_Yds",
	coalesce(stats."Receiving_TD",0) "Receiving_TD",
	coalesce(stats."Tackles_Solo",0) "Tackles_Solo",
	coalesce(stats."Tackles_Ast",0) "Tackles_Ast",
	coalesce(stats."Tackles_Tot",0) "Tackles_Tot",
	coalesce(stats."Tackles_Loss",0) "Tackles_Loss",
	coalesce(stats."Tackles_Sk",0) "Tackles_Sk",
	coalesce(stats."Def Int_Int",0) "Def Int_Int",
	coalesce(stats."Def Int_Yds",0) "Def Int_Yds",
	coalesce(stats."Def Int_TD",0) "Def Int_TD",
	coalesce(stats."Def Int_PD",0) "Def Int_PD",
	coalesce(stats."Fumbles_FR",0) "Fumbles_FR",
	coalesce(stats."Fumbles_Yds",0) "Fumbles_Yds",
	coalesce(stats."Fumbles_TD",0) "Fumbles_TD",
	coalesce(stats."Fumbles_FF",0) "Fumbles_FF",
	coalesce(stats."Kick Ret_Ret",0) "Kick Ret_Ret",
	coalesce(stats."Kick Ret_Yds",0) "Kick Ret_Yds",
	coalesce(stats."Kick Ret_TD",0) "Kick Ret_TD",
	coalesce(stats."Punt Ret_Ret",0) "Punt Ret_Ret",
	coalesce(stats."Punt Ret_Yds",0) "Punt Ret_Yds",
	coalesce(stats."Punt Ret_TD",0) "Punt Ret_TD",
	coalesce(stats."Kicking_XPM",0) "Kicking_XPM",
	coalesce(stats."Kicking_XPA",0) "Kicking_XPA",
	coalesce(stats."Kicking_FGM",0) "Kicking_FGM",
	coalesce(stats."Kicking_FGA",0) "Kicking_FGA",
	coalesce(stats."Punting_Punts",0) "Punting_Punts",
	coalesce(stats."Punting_Yds",0) "Punting_Yds"
	
into cfb_player_query_table	
from cfb_schedule cs 
left join cfb_conference_teams cct on cct."year" =cs."year" and cct.college_href =cs.college_href
left join cfb_rosters cr on cr."year" =cs."year" and cr.college_href =cs.college_href 
left join cfb_player_ht_wt cphw on cphw.player_href=cr.player_href  
left join (
select 
	coalesce(ck.player_href,ckr.player_href,cd.player_href,crr.player_href,cp.player_href) as player_href,
	coalesce(ck.boxscore_href,ckr.boxscore_href,cd.boxscore_href,crr.boxscore_href,cp.boxscore_href) as boxscore_href,
	cp."Passing_Att",
	cp."Passing_Cmp",
	cp."Passing_Yds",
	cp."Passing_TD",
	cp."Passing_Int",
	crr."Rushing_Att",
	crr."Rushing_Yds",
	crr."Rushing_TD",
	crr."Receiving_Rec",
	crr."Receiving_Yds",
	crr."Receiving_TD",
	cd."Tackles_Solo",
	cd."Tackles_Ast",
	cd."Tackles_Tot",
	cd."Tackles_Loss",
	cd."Tackles_Sk",
	cd."Def Int_Int",
	cd."Def Int_Yds",
	cd."Def Int_TD",
	cd."Def Int_PD",
	cd."Fumbles_FR",
	cd."Fumbles_Yds",
	cd."Fumbles_TD",
	cd."Fumbles_FF",
	ckr."Kick Ret_Ret",
	ckr."Kick Ret_Yds",
	ckr."Kick Ret_TD",
	ckr."Punt Ret_Ret",
	ckr."Punt Ret_Yds",
	ckr."Punt Ret_TD",
	ck."Kicking_XPM",
	ck."Kicking_XPA",
	ck."Kicking_FGM",
	ck."Kicking_FGA",	
	ck."Punting_Punts",
	ck."Punting_Yds"
from cfb_passing cp 
full outer join cfb_rush_rec crr on crr.player_href=cp.player_href and crr.boxscore_href=cp.boxscore_href 
full outer join cfb_defense cd on cd.player_href=coalesce(crr.player_href,cp.player_href) and cd.boxscore_href=coalesce(crr.boxscore_href,cp.boxscore_href) 
full outer join cfb_kick_returns ckr on ckr.player_href=coalesce(cd.player_href,crr.player_href,cp.player_href) and ckr.boxscore_href=coalesce(cd.boxscore_href,crr.boxscore_href,cp.boxscore_href) 
full outer join cfb_kicking ck on ck.player_href=coalesce(ckr.player_href,cd.player_href,crr.player_href,cp.player_href) and ck.boxscore_href=coalesce(ckr.boxscore_href,cd.boxscore_href,crr.boxscore_href,cp.boxscore_href)
) stats on stats.player_href=cr.player_href and stats.boxscore_href=cs.boxscore_href ;

