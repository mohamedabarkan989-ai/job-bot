create table if not exists jobs (
  uid        text primary key,
  title      text,
  company    text,
  location   text,
  source     text,
  url        text,
  contract   text,
  salary     text,
  sector     text,
  sent       boolean default false,
  created_at timestamptz default now()
);

create index if not exists jobs_sent_contract on jobs(sent, contract);
create index if not exists jobs_created_at    on jobs(created_at desc);

-- server-side aggregates (replaces full table scan in stats())
create or replace function stats_by_contract()
returns table(contract text, count bigint) language sql as $$
  select contract, count(*) from jobs group by contract order by count(*) desc;
$$;

create or replace function stats_by_source()
returns table(source text, count bigint) language sql as $$
  select source, count(*) from jobs group by source order by count(*) desc;
$$;

-- score column (add if upgrading from v3)
alter table jobs add column if not exists score int default 0;
create index if not exists jobs_score on jobs(score desc);
