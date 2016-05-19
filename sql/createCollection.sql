DO LANGUAGE plpgsql $$
BEGIN
  IF (SELECT to_regclass($1)) IS NULL THEN
    CREATE SEQUENCE $3~ START 1;
    CREATE TABLE $1~ (id TEXT PRIMARY KEY unique default nextval($3)::TEXT, doc JSONB, created TIMESTAMP, updated TIMESTAMP default now());
    CREATE INDEX $2~ ON $1~ USING gin (doc);
  END IF;

  IF exists (select 1 from pg_type where typname = 'collection_type') = false THEN
    CREATE TYPE collection_type as (id TEXT, doc JSONB, created TIMESTAMP, updated TIMESTAMP);
  END IF;

  CREATE OR REPLACE FUNCTION GOALPOST_FINDv$4^(tname TEXT, opts JSONB)
  RETURNS SETOF collection_type AS $find$
  DECLARE
    queryt TEXT;
  BEGIN
    queryt := format('SELECT id, doc, created, updated FROM %I', tname);
    IF opts ? 'where' THEN
      queryt := queryt || ' WHERE ' || GOALPOST_WHERECLAUSEv$4^(opts->'where');
    END IF;
    IF opts ? 'limit' THEN
      queryt := queryt || format(' LIMIT %L', opts->>'limit');
    END IF;
    IF opts ? 'offset' THEN
      queryt := queryt || format(' OFFSET %L', opts->>'offset');
    END IF;
    RETURN QUERY EXECUTE queryt;
  END; $find$ LANGUAGE plpgsql;

  CREATE OR REPLACE FUNCTION GOALPOST_WHERECLAUSEv$4^(wherej JSONB)
  RETURNS TEXT AS $whereclause$
  DECLARE
    keys RECORD;
    queryt TEXT;
  BEGIN
    queryt := '';
    FOR keys IN SELECT * FROM jsonb_each(wherej) ORDER BY key DESC LOOP
      IF keys.key = '$and' THEN
        queryt :=  queryt || ' AND (' || GOALPOST_WHERECLAUSEv$4^(keys.value) || ')';

        queryt :=  queryt || ' OR (' || GOALPOST_WHERECLAUSEv$4^(keys.value) || ')';
      ELSE
        IF char_length(queryt) > 0 THEN
          queryt := queryt || ' AND';
        END IF;
        queryt := queryt || ' doc->''' || array_to_string(string_to_array(keys.key, '.'), '->''') || '''';
        IF wherej->((keys.key)::TEXT) ? 'op' THEN
          IF wherej->((keys.key)::TEXT)->>'op' IN ('>', '=', '<', '>=', '<=', 'LIKE', 'ILIKE') THEN
            queryt := queryt || ' ' || (wherej->(keys.key)::TEXT->>'op') || ' ';
          ELSE
            RAISE EXCEPTION 'Invalid operator %L', wherej->((keys.key)::TEXT)->'op';
          END IF;
          IF wherej->(keys.key)::TEXT ? 'column' THEN
            queryt := queryt || ' doc->''' || array_to_string(string_to_array(wherej->((keys.key)::TEXT)->>'column', '.'), '->''') || '''';
          ELSE
            queryt := queryt || format('%L', wherej->((keys.key)::TEXT)->>'value');
          END IF;
        ELSE
          queryt := queryt || format('=%L', wherej->>((keys.key)::TEXT));
        END IF;
      END IF;
    END LOOP;

    RETURN queryt;
  END; $whereclause$ LANGUAGE plpgsql;

  CREATE OR REPLACE FUNCTION GOALPOST_PUTv$4^(tname TEXT, in_id TEXT, in_doc JSONB)
  RETURNS SETOF collection_type AS $put$
  BEGIN
    RETURN QUERY EXECUTE format('INSERT INTO %I (id, doc, created) VALUES (%L, %L, now())
      ON CONFLICT (id) DO UPDATE SET doc=EXCLUDED.doc
      RETURNING id, doc, created, updated', tname, in_id, in_doc);
  END; $put$ LANGUAGE plpgsql;
  
  CREATE OR REPLACE FUNCTION GOALPOST_POSTv$4^(tname TEXT, in_doc JSONB)
  RETURNS SETOF collection_type AS $put$
  BEGIN
    RETURN QUERY EXECUTE format('INSERT INTO %I (doc, created) VALUES (%L, now()) RETURNING id, doc, created, updated', tname, in_doc);
  END; $put$ LANGUAGE plpgsql;

  CREATE OR REPLACE FUNCTION GOALPOST_DESTROYv$4^(tname TEXT, sname TEXT)
  RETURNS void AS $destroy$
  BEGIN
    EXECUTE format('DROP TABLE %I', tname);
    EXECUTE format('DROP SEQUENCE %I', sname);
  END; $destroy$ LANGUAGE plpgsql;
  
  CREATE OR REPLACE FUNCTION GOALPOST_GETv$4^(tname TEXT, in_id TEXT)
  RETURNS SETOF collection_type AS $get$
  BEGIN
    RETURN QUERY EXECUTE format('SELECT id, doc, created, updated FROM %I WHERE id=%L', tname, in_id);
 END; $get$ LANGUAGE plpgsql;

END$$;
