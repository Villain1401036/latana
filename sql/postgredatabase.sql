--creating the stg table
create table posts(
  created_utc bigint , score integer, domain text, id varchar primary key , title text , ups integer, downs integer,
       num_comments integer, permalink text , selftext text, link_flair_text text ,
      over_18 bool , thumbnail text, subreddit_id varchar, edited text  ,
       link_flair_css_class text, author_flair_css_class text, is_self bool ,
       name varchar, url varchar, distinguished varchar
 )

 --creating the sink table
 create table posts_2013(
	id varchar primary key,
   created_utc bigint,
   score integer,
	ups integer,
	downs integer ,
	permalink text,
	
	subreddit_id varchar
   
)

--creating appropriate index

create index score_idx on posts_2013(score desc)
create index ups_idx on posts_2013(ups desc);
create index downs_idx on posts_2013(downs desc)
create index sreddit_idx on posts_2013(subreddit_id)


select subreddit_id ,max(score) as score from posts_2013 where ups > 5000 and downs > 5000 and abs(ups - downs) < 10000 group by subreddit_id order by score desc limit 10
