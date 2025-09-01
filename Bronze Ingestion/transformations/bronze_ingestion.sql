-- Ingesting streaming album data
CREATE OR REFRESH STREAMING TABLE bronze.album
COMMENT 'Raw album data from volumes to bronze layer'
AS
SELECT * FROM cloud_files(
  '/Volumes/apple/raw/album_files',
  'csv',
  map('header', 'true')
);


-- Ingesting streaming artists data
CREATE OR REFRESH STREAMING TABLE bronze.artist
COMMENT 'Raw artist data from volumes to bronze layer'
AS
SELECT * FROM cloud_files(
  '/Volumes/apple/raw/artists_files',
  'csv',
  map('header', 'true')
);


-- Ingesting streaming customers data
CREATE OR REFRESH STREAMING TABLE bronze.customer
COMMENT 'Raw customer data from volumes to bronze layer'
AS
SELECT * FROM cloud_files(
  '/Volumes/apple/raw/customers_files',
  'csv',
  map('header', 'true')
);


-- Ingesting streaming employee data
CREATE OR REFRESH STREAMING TABLE bronze.employee
COMMENT 'Raw employee data from volumes to bronze layer'
AS
SELECT * FROM cloud_files(
  '/Volumes/apple/raw/employee_files',
  'csv',
  map('header', 'true')
);


-- Ingesting streaming genre data
CREATE OR REFRESH STREAMING TABLE bronze.genre
COMMENT 'Raw genre data from volumes to bronze layer'
AS
SELECT * FROM cloud_files(
  '/Volumes/apple/raw/genre_files',
  'csv',
  map('header', 'true')
);


-- Ingesting streaming invoice inline data
CREATE OR REFRESH STREAMING TABLE bronze.invoice_inline
COMMENT 'Raw invoice inline data from volumes to bronze layer'
AS
SELECT * FROM cloud_files(
  '/Volumes/apple/raw/invoice_inline_files',
  'csv',
  map('header', 'true')
);


-- Ingesting streaming invoice data
CREATE OR REFRESH STREAMING TABLE bronze.invoice
COMMENT 'Raw invoice data from volumes to bronze layer'
AS
SELECT * FROM cloud_files(
  '/Volumes/apple/raw/invoices_files',
  'csv',
  map('header', 'true')
);


-- Ingesting streaming media type data
CREATE OR REFRESH STREAMING TABLE bronze.mediatype
COMMENT 'Raw media type data from volumes to bronze layer'
AS
SELECT * FROM cloud_files(
  '/Volumes/apple/raw/media_type_files',
  'csv',
  map('header', 'true')
);


-- Ingesting streaming playlist data
CREATE OR REFRESH STREAMING TABLE bronze.playlist
COMMENT 'Raw playlist data from volumes to bronze layer'
AS
SELECT * FROM cloud_files(
  '/Volumes/apple/raw/playlist_files',
  'csv',
  map('header', 'true')
);


-- Ingesting streaming track data
CREATE OR REFRESH STREAMING TABLE bronze.track
COMMENT 'Raw playlist_track data from volumes to bronze layer'
AS
SELECT * FROM cloud_files(
  '/Volumes/apple/raw/track_files',
  'csv',
  map('header', 'true')
);