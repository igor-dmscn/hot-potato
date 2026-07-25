-- Applied at boot by Store.Migrate. Written to be idempotent because there is
-- no migration tool here: a tool earns its place when there are versions to
-- migrate between, and there is exactly one.

create extension if not exists citext;

create table if not exists users (
    id            text primary key,
    email         citext unique not null,
    display_name  text not null,
    password_hash text not null,
    created_at    timestamptz not null default now()
);

create table if not exists sessions (
    id         text primary key,
    user_id    text not null references users (id) on delete cascade,
    created_at timestamptz not null default now(),
    expires_at timestamptz not null
);

create index if not exists sessions_user_id_idx on sessions (user_id);

-- ponytail: no reaper for expired rows. Get already refuses them, and a
-- session row is ~100 bytes; add "delete from sessions where expires_at <
-- now()" to a ticker if the table ever gets big enough to notice.
create index if not exists sessions_expires_at_idx on sessions (expires_at);
