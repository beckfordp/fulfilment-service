-- US-9.5: a cancelled order aborts a task that has not dispatched.
ALTER TABLE "fulfilment_task" DROP CONSTRAINT fulfilment_task_state_check;
ALTER TABLE "fulfilment_task"
    ADD CONSTRAINT fulfilment_task_state_check
    CHECK (state IN ('PENDING', 'PICKING', 'PACKED', 'DISPATCHED',
                     'DISPATCH_FAILED', 'CANCELLED'));
