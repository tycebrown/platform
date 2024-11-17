CREATE TYPE public."SyncState" AS ENUM (
    'PENDING',
    'SYNCED'
);
ALTER TABLE public."GlossEvent"
    ADD COLUMN "syncState" public."SyncState" DEFAULT 'SYNCED'::public."SyncState" NOT NULL