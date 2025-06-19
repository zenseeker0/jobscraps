SELECT
  id,
  site,
  job_url,
  job_url_direct,
  title,
  company,
  location,
  date_posted,
  min_amount,
  max_amount,
  is_remote,
--  SUBSTR(description, 1, 1000) AS description,
  date_scraped,
  search_query
FROM
  scraped_jobs
WHERE
--  description ILIKE '%commission%'
  description ilike '%words%'
ORDER BY
  title, company
  