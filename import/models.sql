CREATE TABLE gep.models (
    `id` String,
    `updatedAt` date,
    `attribution` String,
    `levers` String,
    `filters` String,
    `map` String,
    `name` String,
    `version` String,
    `description` String,
    `country` String,
    `type` String,
    `timesteps` String,
    `baseYear` Int16,
    `sourceData` String,
    `disclaimer` String
)
Engine=MergeTree()
order by (id, country)
primary key (id, country)
