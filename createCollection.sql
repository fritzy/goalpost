DO LANGUAGE plpgsql $$
BEGIN
  IF (SELECT to_regclass($1)) IS NULL THEN
    CREATE SEQUENCE $3~ START 1;
    CREATE TABLE $1~ (id TEXT PRIMARY KEY unique default nextval($3)::TEXT, doc JSONB, created TIMESTAMP, updated TIMESTAMP default now());
    CREATE INDEX $2~ ON $1~ USING gin (doc);
  END IF;
END$$;
