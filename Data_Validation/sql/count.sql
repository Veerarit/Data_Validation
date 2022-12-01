select count(distinct({id})) from mongo.dim_{table}
where date(created_at) between '{min_date}' and '{max_date}';