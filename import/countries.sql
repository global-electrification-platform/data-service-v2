create table gep.countries
(
     `id` String,
     `name` String
)
Engine=MergeTree()
order by (id)
primary key (id)
