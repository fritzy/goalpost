DROP FUNCTION IF EXISTS GOALPOST_WHERECLAUSEv1x0x0(jsonb);
DROP FUNCTION IF EXISTS GOALPOST_FINDv1x0x0(text, jsonb);
CREATE TYPE collection_type as (id TEXT, doc JSONB, created TIMESTAMP, updated TIMESTAMP);

CREATE OR REPLACE FUNCTION GOALPOST_FINDv1x0x0(tname TEXT, opts JSONB)
RETURNS SETOF collection_type AS $$
DECLARE
  queryt TEXT;
BEGIN
  queryt := format('SELECT id, doc, created, updated FROM %I', tname);
  IF opts ? 'where' THEN
    queryt := queryt || ' WHERE ' || GOALPOST_WHERECLAUSEv1x0x0(opts->'where');
  END IF;
  IF opts ? 'limit' THEN
    queryt := queryt || format(' LIMIT %L', opts->>'limit');
  END IF;
  IF opts ? 'offset' THEN
    queryt := queryt || format(' OFFSET %L', opts->>'offset');
  END IF;
  RETURN QUERY EXECUTE queryt;
END; $$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION GOALPOST_WHERECLAUSEv1x0x0(wherej JSONB)
RETURNS TEXT AS $$
DECLARE
  keys RECORD;
  queryt TEXT;
BEGIN
  queryt := '';
  FOR keys IN SELECT * FROM jsonb_each(wherej) ORDER BY key DESC LOOP
    IF keys.key = '$and' THEN
      queryt :=  queryt || ' AND (' || GOALPOST_WHERECLAUSEv1x0x0(keys.value) || ')';
    ELSEIF keys.key = '$or' THEN
      queryt :=  queryt || ' OR (' || GOALPOST_WHERECLAUSEv1x0x0(keys.value) || ')';
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
END; $$ LANGUAGE plpgsql;

