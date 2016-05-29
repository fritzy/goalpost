DO LANGUAGE plpgsql $$
DECLARE
  created boolean;
BEGIN
  created := false;

  CREATE TABLE IF NOT EXISTS __GOALPOST_VERSION (id SERIAL, version TEXT, created TIMESTAMP);

  IF (SELECT to_regclass($1)) IS NULL THEN
    CREATE SEQUENCE $3~ START 1;
    CREATE TABLE $1~ (id TEXT PRIMARY KEY unique default nextval($3)::TEXT, doc JSONB, created TIMESTAMP, updated TIMESTAMP default now());
    CREATE TABLE GOALPOST_LOG_$1^ (id TEXT PRIMARY KEY UNIQUE, op TEXT, updated TIMESTAMP default now());
    CREATE INDEX $2~ ON $1~ USING gin (doc);
    created := true;
  END IF;

  IF NOT EXISTS (SELECT id FROM __GOALPOST_VERSION WHERE version=$4) THEN

    INSERT INTO __GOALPOST_VERSION (version, created) VALUES ($4, now());

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

    CREATE OR REPLACE FUNCTION GOALPOST_UPDATEv$4^(tname TEXT, in_id TEXT, in_doc JSONB)
    RETURNS SETOF collection_type AS $put$
    BEGIN
      RETURN QUERY EXECUTE format('UPDATE %I SET doc = (doc || %L), updated=now() WHERE id=%L RETURNING id, doc, created, updated', tname, in_doc, in_id);
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
      EXECUTE format('DROP TABLE %I', 'goalpost_log_' || tname);
    END; $destroy$ LANGUAGE plpgsql;

    CREATE OR REPLACE FUNCTION GOALPOST_GETv$4^(tname TEXT, in_id TEXT)
    RETURNS SETOF collection_type AS $get$
    BEGIN
      RETURN QUERY EXECUTE format('SELECT id, doc, created, updated FROM %I WHERE id=%L', tname, in_id);
    END; $get$ LANGUAGE plpgsql;

    CREATE OR REPLACE FUNCTION GOALPOST_DELETEv$4^(tname TEXT, in_id TEXT)
    RETURNS void AS $deleted$
    BEGIN
      EXECUTE format('DELETE FROM %I WHERE id=%L', tname, in_id);
    END; $deleted$ LANGUAGE plpgsql;
    
    CREATE OR REPLACE FUNCTION GOALPOST_SIZEOFv$4^(tname TEXT)
    RETURNS BIGINT AS $sizeof$
    DECLARE
      sizeof BIGINT;
    BEGIN
      EXECUTE format('SELECT count(*) FROM %I', tname) INTO sizeof;
      return sizeof;
    END; $sizeof$ LANGUAGE plpgsql;

    CREATE OR REPLACE FUNCTION GOALPOST_DELETEWHEREv$4^(tname TEXT, in_where JSONB)
    RETURNS SETOF collection_type AS $deletewhere$
    DECLARE
      queryt TEXT;
    BEGIN
      queryt := format('DELETE FROM %I WHERE ', tname) || GOALPOST_WHERECLAUSEv$4^(in_where) || ' RETURNING id, doc, created, updated';
      RETURN QUERY EXECUTE queryt;
    END; $deletewhere$ LANGUAGE plpgsql;

    CREATE OR REPLACE FUNCTION GOALPOST_UPDATE_NOTIFY() RETURNS trigger AS $notify$
    DECLARE
      id TEXT;
      op TEXT;
      log_table TEXT;
      log_point RECORD;
    BEGIN
      log_table := 'goalpost_log_' || TG_TABLE_NAME;
      IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN
        id = NEW.id::TEXT;
        op := 'put';
      ELSE
        id = OLD.id::TEXT;
        op := 'delete';
      END IF;
      EXECUTE format('INSERT INTO %I (id, op, updated) VALUES (%L, %L, now())
        ON CONFLICT (id) DO UPDATE SET op=EXCLUDED.op, updated=now()', log_table, id, op);
      PERFORM pg_notify('goalpost_hint_' || TG_TABLE_NAME, json_build_object('collection', TG_TABLE_NAME, 'id', id, 'type', op)::text);
      IF (op != 'delete') THEN
        PERFORM pg_notify('goalpost_full_' || TG_TABLE_NAME, json_build_object('collection', TG_TABLE_NAME, 'id', id, 'type', op, 'data', NEW.doc)::text);
      ELSE
        PERFORM pg_notify('goalpost_full_' || TG_TABLE_NAME, json_build_object('collection', TG_TABLE_NAME, 'id', id, 'type', op)::text);
      END IF;

      EXECUTE format('SELECT updated FROM %I ORDER BY updated LIMIT 1 OFFSET 1000', log_table) INTO log_point;
      IF FOUND THEN
        EXECUTE format('DELETE FROM %I WHERE updated > %L', log_table, log_point.updated);
      END IF;

      RETURN NEW;
    END;
    $notify$ LANGUAGE plpgsql;
    
  END IF;


  IF created THEN
    CREATE TRIGGER trigger_goalpost_update_notify_$4^ AFTER UPDATE OR INSERT OR DELETE ON $1~ FOR EACH ROW EXECUTE PROCEDURE GOALPOST_UPDATE_NOTIFY();
  END IF;

END$$;
