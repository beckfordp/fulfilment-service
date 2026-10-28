-- Table name is quoted throughout as a defensive habit: several plausible domain
-- names (e.g. "user", "group", "order") are reserved PostgreSQL keywords.
CREATE TABLE "fulfilment" (
    id UUID PRIMARY KEY,
    -- codegen:fields:SQL_CREATE_COLUMN
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
