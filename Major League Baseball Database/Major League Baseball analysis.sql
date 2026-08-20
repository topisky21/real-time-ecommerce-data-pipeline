# Section A : School Analysis

# 1. In each decade, how many schools were there that produced MLB players
select round(yearID, -1) as decade, count(distinct schoolID) as no_of_sales
from schools
group by decade
order by decade;

#2 What are the names of the top 5 schools that produced the most players?
with school_details as(
select
	s.playerID, 
	s.schoolID, 
	sd.name_full,  
	p.nameGiven
from schools s
join players p 
	on s.playerID = p.playerID
join school_details sd
	on s.schoolID= sd.schoolID
)

select name_full, count(distinct nameGiven) as number_of_students
from school_details
group by name_full
order by number_of_students DESC
LIMIT 5;


# 3 For each decade, what were the names of the top 3 schools that produced the most players?
with school_details as(
select
	s.playerID, 
	s.schoolID, 
	sd.name_full,  
	p.nameGiven,
    round(s.yearID, -1) as decade    
from schools s
join players p 
	on s.playerID = p.playerID
join school_details sd
	on s.schoolID= sd.schoolID

),
ranked_schools as (

select decade, 
	name_full,	
    count(distinct nameGiven) as number_of_students,
    row_number() over (partition by decade order by count(distinct nameGiven) desc) as rank_position
from school_details
group by decade, name_full
)

select *
from ranked_schools
where rank_position <=3;


# Section B : Salary Analysis

#1 Return the top 20% of teams in terms of average annual spending
with team_salary as (
				select teamID, yearID, sum(salary) as total_spend
				from salaries
				group by teamID, yearID
				ORDER BY  teamID, yearID
),

ranked_teams as (
				select 
					teamID, 
					avg(total_spend) as average_spend,
					ntile(5) over (order by avg(total_spend) desc) as average_spend_pct
				from team_salary
				group by teamID
)

select 
	teamID, 
    round(average_spend/1000000,1) as average_sp_millions
from ranked_teams
where average_spend_pct =1;

#2 For each team, show the cumulative sum of spending over the years
with team_annual_salary as (
				select teamID, yearID, sum(salary) as total_spend
				from salaries
				group by teamID, yearID
				ORDER BY  teamID, yearID
),

ranked_teams as (
				select 
					teamID, 
                    yearID,
					total_spend,
					sum(total_spend) over (partition by teamID order by yearID) as cumulative_spend
				from team_annual_salary
				group by teamID, yearID, total_spend
)
select 
	teamID,    
    yearID,
    total_spend,
    cumulative_spend
from ranked_teams;

#3 Return the first year that each team’s cumulative spending surpassed 1 billion
with team_annual_salary as (
				select teamID, yearID, sum(salary) as total_spend
				from salaries
				group by teamID, yearID
				ORDER BY  teamID, yearID
),

ranked_teams as (
				select 
					teamID, 
                    yearID,
					total_spend,
					sum(total_spend) over (partition by teamID order by yearID) as cumulative_spend
				from team_annual_salary
				group by teamID, yearID, total_spend
),
rank_by_cumulative as (
					select 
						teamID,    
						yearID,
						total_spend,
						cumulative_spend
					from ranked_teams
					where cumulative_spend >=1000000000
),
final_rank as (
			select 
				*,
				row_number() over (partition by teamID order by yearID) as rn
			from rank_by_cumulative			
)
select 
	teamID, 
	yearID, 
    cumulative_spend
from final_rank
where rn = 1;

# Section C: Player Career Analysis

#1. For each player, calculate their age at their first (debut) game, their last game, and their career length (all in years). Sort from longest career to shortest career.
with player_details as (
					select
						*,
						cast(concat(birthYear,"-",birthMonth,"-",birthDay)as date)  as birthDate
					from players
)
select 
	nameGiven,
    birthDate,
    timestampdiff(YEAR, birthDate, debut) AS start_date,
    timestampdiff(YEAR, birthDate, finalGame) as final_date,
	(timestampdiff(YEAR, birthDate, finalGame) - timestampdiff(YEAR,  birthDate, debut)) as career_length
from player_details;

#2. What team did each player play on for their starting and ending years?

select
	p.nameGiven,
    p.playerID,
    YEAR(p.debut) as debut_year,
    (sd.teamID) as debut_team,
    YEAR(p.finalGame) final_game_year,
    (sf.teamID) as final_team
from players p
inner Join salaries sd 
	on p.playerID = sd.playerID 
    and  YEAR(p.debut) = sd.yearID
inner Join salaries sf 
	on p.playerID = sf.playerID 
    and  YEAR(p.finalGame) = sf.yearID;
    
    
#3. How many players started and ended on the same team and also played for over a decade?

with player_details as (
					select
						p.nameGiven,
						p.playerID,
						p.debut,
                        p.finalGame,
						YEAR(p.debut) as debut_year,
						(sd.teamID) as debut_team,
						YEAR(p.finalGame) final_game_year,
						(sf.teamID) as final_team
					from players p
					inner Join salaries sd 
						on p.playerID = sd.playerID 
						and  YEAR(p.debut) = sd.yearID
					inner Join salaries sf 
						on p.playerID = sf.playerID 
						and  YEAR(p.finalGame) = sf.yearID
)

select 
	*, 
    timestampdiff(YEAR, debut, finalGame) as career_length
from player_details
where debut_team = final_team
and timestampdiff(YEAR, debut, finalGame) >10
order by career_length desc;

