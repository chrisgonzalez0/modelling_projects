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
	cs.home_away ,
	cs.w_l,
	cs."Pts",cs."Opp" ,
	cr."Player" ,
	cr.player_href,
	cr."Class" ,
	cr."Pos" ,
	cphw.height,cphw.weight ,
	stats."Passing_Att",
	stats."Passing_Cmp",
	stats."Passing_Yds",
	stats."Passing_TD",
	stats."Passing_Int",
	stats."Rushing_Att",
	stats."Rushing_Yds",
	stats."Rushing_TD",
	stats."Receiving_Rec",
	stats."Receiving_Yds",
	stats."Receiving_TD",
	stats."Tackles_Solo",
	stats."Tackles_Ast",
	stats."Tackles_Tot",
	stats."Tackles_Loss",
	stats."Tackles_Sk",
	stats."Def Int_Int",
	stats."Def Int_Yds",
	stats."Def Int_TD",
	stats."Def Int_PD",
	stats."Fumbles_FR",
	stats."Fumbles_Yds",
	stats."Fumbles_TD",
	stats."Fumbles_FF"
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
	cd."Fumbles_FF"
from cfb_passing cp 
full outer join cfb_rush_rec crr on crr.player_href=cp.player_href and crr.boxscore_href=cp.boxscore_href 
full outer join cfb_defense cd on cd.player_href=coalesce(crr.player_href,cp.player_href) and cd.boxscore_href=coalesce(crr.boxscore_href,cp.boxscore_href) 
full outer join cfb_kick_returns ckr on ckr.player_href=coalesce(cd.player_href,crr.player_href,cp.player_href) and ckr.boxscore_href=coalesce(cd.boxscore_href,crr.boxscore_href,cp.boxscore_href) 
full outer join cfb_kicking ck on ck.player_href=coalesce(ckr.player_href,cd.player_href,crr.player_href,cp.player_href) and ck.boxscore_href=coalesce(ckr.boxscore_href,cd.boxscore_href,crr.boxscore_href,cp.boxscore_href)
) stats on stats.player_href=cr.player_href and stats.boxscore_href=cs.boxscore_href ;

