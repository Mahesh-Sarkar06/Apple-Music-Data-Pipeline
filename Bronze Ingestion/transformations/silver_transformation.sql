CREATE OR REFRESH STREAMING TABLE silver.trans_album
AS
SELECT 
    cast(album_id AS int) AS album_id,
    cast(title AS string) AS title,
    cast(artist_id AS int) AS artist_id
FROM STREAM(bronze.album);


CREATE OR REFRESH STREAMING TABLE silver.trans_artist
AS
SELECT
    cast(artist_id AS int) AS artist_id,
    cast(name AS string) AS artist_name
FROM STREAM(bronze.artist);


CREATE OR REFRESH STREAMING TABLE silver.trans_customer
AS
SELECT
    cast(customer_id AS int) AS customer_id,
    cast(first_name AS string) AS first_name,
    cast(last_name AS string) AS last_name,
    cast(company AS string) AS company,
    cast(address AS string) AS address,
    cast(city AS string) AS city,
    cast(state AS string) AS state,
    cast(country AS string) AS country,
    cast(postal_code AS string) AS postal_code,
    cast(phone AS string) AS phone,
    cast(fax AS string) AS fax,
    cast(email AS string) AS email,
    cast(support_rep_id AS int) AS support_rep_id
FROM STREAM(bronze.customer);


CREATE OR REFRESH STREAMING TABLE silver.trans_employee
AS
SELECT
    cast(employee_id AS int) AS employee_id,
    cast(last_name AS string) AS last_name,
    cast(first_name AS string) AS first_name,
    cast(title AS string) AS title,
    cast(reports_to AS int) AS reports_to,
    cast(levels AS string) AS levels,
    cast(birthdate AS date) AS birth_date,
    cast(hire_date AS date) AS hire_date,
    cast(address AS string) AS address,
    cast(city AS string) AS city,
    cast(state AS string) AS state,
    cast(country AS string) AS country,
    cast(postal_code AS string) AS postal_code,
    cast(phone AS string) AS phone,
    cast(fax AS string) AS fax,
    cast(email AS string) AS email
FROM STREAM(bronze.employee);


CREATE OR REFRESH STREAMING TABLE silver.trans_genre
AS
SELECT
    cast(genre_id AS int) AS genre_id,
    cast(name AS string) AS genre_name
FROM STREAM(bronze.genre);


CREATE OR REFRESH STREAMING TABLE silver.trans_invoice
AS
SELECT
    cast(invoice_id AS int) AS invoice_id,
    cast(customer_id AS int) AS customer_id,
    cast(invoice_date AS date) AS invoice_date,
    cast(billing_address AS string) AS billing_address,
    cast(billing_city AS string) AS billing_city,
    cast(billing_state AS string) AS billing_state,
    cast(billing_country AS string) AS billing_country,
    cast(billing_postal_code AS string) AS billing_postal_code,
    cast(total AS double) AS total
FROM STREAM(bronze.invoice);


CREATE OR REFRESH STREAMING TABLE silver.trans_invoice_line
AS
SELECT
    cast(invoice_line_id AS int) AS invoice_line_id,
    cast(invoice_id AS int) AS invoice_id,
    cast(track_id AS int) AS track_id,
    cast(unit_price AS double) AS unit_price,
    cast(quantity AS int) AS quantity
FROM STREAM(bronze.invoice_inline);


CREATE OR REFRESH STREAMING TABLE silver.trans_mediatype
AS
SELECT
    cast(media_type_id AS int) AS media_type_id,
    cast(name AS string) AS media_type_name
FROM STREAM(bronze.mediatype);


CREATE OR REFRESH STREAMING TABLE silver.trans_playlist
AS
SELECT
    cast(playlist_id AS int) AS playlist_id,
    cast(name AS string) AS playlist_name
FROM STREAM(bronze.playlist);


CREATE OR REFRESH STREAMING TABLE silver.trans_track
AS
SELECT
    cast(playlist_id AS int) AS playlist_id,
    cast(track_id AS int) AS track_id
FROM stream(bronze.track);