-- The Python `bytes` parameters and return value map to RisingWave BYTEA.
CREATE TABLE images (
    id BIGINT PRIMARY KEY,
    image BYTEA
);

CREATE FUNCTION image_metadata(BYTEA) RETURNS JSONB
AS image_metadata
USING LINK 'http://localhost:8815';

CREATE FUNCTION image_thumbnail(BYTEA) RETURNS BYTEA
AS image_thumbnail
USING LINK 'http://localhost:8815';

-- If an image arrives as base64 text, convert it to BYTEA before storing it:
-- INSERT INTO images VALUES (1, decode('<base64 image>', 'base64'));

SELECT id, image_metadata(image) AS metadata
FROM images;

SELECT id, image_thumbnail(image) AS thumbnail
FROM images;
