


select *
from cfb_rosters cr 
order by cr."year" desc
limit 100 



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
	cphw.height,cphw.weight  
from cfb_schedule cs 
left join cfb_conference_teams cct on cct."year" =cs."year" and cct.college_href =cs.college_href
left join cfb_rosters cr on cr."year" =cs."year" and cr.college_href =cs.college_href 
left join cfb_player_ht_wt cphw on cphw.player_href=cr.player_href  
where cr."Player" ='Joe Burrow'
order by cs.boxscore_href desc
limit 1000 


select 
	cp.player_href ,
	cp.college_href ,
	cp.boxscore_href ,
	cp."year" ,
	cp."Passing_Cmp" ,
	cp."Passing_Att" ,
	cp."Passing_Yds" ,
	cp."Passing_TD" ,
	cp."Passing_Int" 
from cfb_passing cp 
order by cp."Passing_Yds" desc
limit 1000 


select 
cp."Player" ,
cp.player_href ,
cp."School",
cp.college_href ,
count(*)
from cfb_passing cp 
group by 
cp."Player" ,
cp.player_href ,
cp."School",
cp.college_href 
order by 
count(*) desc
limit 1000



select *
from cfb_rosters cr 
where 
cr.college_href='texas-tech'
and cr."year" =2016



select *
from cfb_schedule cs 
where 
--cs.boxscore_href ='2016-10-22-texas-tech'
cs.college_href ='texas-tech'
and cs."year" =2016
