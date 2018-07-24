create table locopy_test(
  id INTEGER,
  address VARCHAR(500),
  country VARCHAR(10),
  latitude DECIMAL(19,4),
  locality VARCHAR(500),
  longitude DECIMAL(19,4),
  name VARCHAR(200),
  postcode VARCHAR(10),
  region VARCHAR(10)
)
distkey(id)
interleaved sortkey(id, postcode);
