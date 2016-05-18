INSERT INTO $1~ (doc, created) VALUES ($2, now()) RETURNING id, doc;
