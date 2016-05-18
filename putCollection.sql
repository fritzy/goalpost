INSERT INTO $1~ (id, doc, created) VALUES ($2, $3, now())
  ON CONFLICT (id) DO UPDATE SET doc=excluded.id
  RETURNING id, doc;
