ALTER TABLE route_logs
ADD COLUMN IF NOT EXISTS status VARCHAR(30)
NOT NULL DEFAULT 'received';

alter table route_logs 
add column if not exists tracking_number varchar(30);

alter table route_logs 
add column if not exists current_location varchar(80)
not null default 'Seattle Alpha';

alter table route_logs 
add column if not exists last_updated timestamp
not null default CURRENT_TIMESTAMP;

update route_logs 
set tracking_number = 'TRK-' || LPAD(route_id::text, 8, '0')
where tracking_number is null;

alter table route_logs
drop constraint if exists route_logs_pkey;

truncate table route_logs;

analyze route_logs;