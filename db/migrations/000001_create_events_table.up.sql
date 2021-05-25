BEGIN;

	CREATE TABLE IF NOT EXISTS events
	(
		id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
		event_id UUID NOT NULL,
		account_id UUID NOT NULL,
		payload BYTES NOT NULL,
		observed_at TIMESTAMP NOT NULL
	);

COMMIT;
