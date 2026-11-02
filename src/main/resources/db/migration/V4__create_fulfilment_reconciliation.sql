-- US-21.4: a cancellation that lost to dispatch (DISPATCHED is final).
create table fulfilment_reconciliation (
    order_id uuid primary key
        references "fulfilment_task" (order_id) on delete cascade,
    reason text not null,
    created_at timestamptz not null default now()
);
