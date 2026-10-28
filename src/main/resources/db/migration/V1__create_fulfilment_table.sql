CREATE TABLE "fulfilment_task" (
    order_id UUID PRIMARY KEY,
    state TEXT NOT NULL CHECK (state IN ('PENDING', 'PICKING', 'PACKED', 'DISPATCHED', 'DISPATCH_FAILED')),
    items JSONB NOT NULL,
    shipping_address JSONB NOT NULL,
    tracking_no TEXT,
    carrier TEXT,
    failure_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
