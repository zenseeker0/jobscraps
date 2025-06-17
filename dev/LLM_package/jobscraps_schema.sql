--
-- PostgreSQL database dump
--

-- Dumped from database version 14.18 (Debian 14.18-1.pgdg120+1)
-- Dumped by pg_dump version 17.5

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

DROP INDEX IF EXISTS public.idx_search_history_timestamp;
DROP INDEX IF EXISTS public.idx_search_history_search_query;
DROP INDEX IF EXISTS public.idx_scraped_jobs_title_company;
DROP INDEX IF EXISTS public.idx_scraped_jobs_title;
DROP INDEX IF EXISTS public.idx_scraped_jobs_site;
DROP INDEX IF EXISTS public.idx_scraped_jobs_search_query_lower;
DROP INDEX IF EXISTS public.idx_scraped_jobs_search_query;
DROP INDEX IF EXISTS public.idx_scraped_jobs_location;
DROP INDEX IF EXISTS public.idx_scraped_jobs_is_remote;
DROP INDEX IF EXISTS public.idx_scraped_jobs_date_scraped;
DROP INDEX IF EXISTS public.idx_scraped_jobs_date_posted;
DROP INDEX IF EXISTS public.idx_scraped_jobs_company_location;
DROP INDEX IF EXISTS public.idx_scraped_jobs_company;
ALTER TABLE IF EXISTS ONLY public.search_history DROP CONSTRAINT IF EXISTS search_history_pkey;
ALTER TABLE IF EXISTS ONLY public.scraped_jobs DROP CONSTRAINT IF EXISTS scraped_jobs_pkey;
ALTER TABLE IF EXISTS public.search_history ALTER COLUMN id DROP DEFAULT;
DROP SEQUENCE IF EXISTS public.search_history_id_seq;
DROP TABLE IF EXISTS public.search_history;
DROP TABLE IF EXISTS public.scraped_jobs;
DROP EXTENSION IF EXISTS pg_trgm;
-- *not* dropping schema, since initdb creates it
--
-- Name: public; Type: SCHEMA; Schema: -; Owner: -
--

-- *not* creating schema, since initdb creates it


--
-- Name: pg_trgm; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;


--
-- Name: EXTENSION pg_trgm; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION pg_trgm IS 'text similarity measurement and index searching based on trigrams';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: scraped_jobs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.scraped_jobs (
    id text NOT NULL,
    site text,
    job_url text,
    job_url_direct text,
    title text,
    company text,
    location text,
    date_posted text,
    job_type text,
    salary_source text,
    "interval" text,
    min_amount numeric(12,2),
    max_amount numeric(12,2),
    currency text,
    is_remote boolean,
    job_level text,
    job_function text,
    listing_type text,
    emails text,
    description text,
    company_industry text,
    company_url text,
    company_logo text,
    company_url_direct text,
    company_addresses text,
    company_num_employees text,
    company_revenue text,
    company_description text,
    skills text,
    experience_range text,
    company_rating text,
    company_reviews_count text,
    vacancy_count text,
    work_from_home_type text,
    date_scraped timestamp without time zone,
    search_query text
);


--
-- Name: TABLE scraped_jobs; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.scraped_jobs IS 'Main table storing job postings scraped from various job sites';


--
-- Name: COLUMN scraped_jobs.min_amount; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.scraped_jobs.min_amount IS 'Minimum salary amount in specified currency';


--
-- Name: COLUMN scraped_jobs.max_amount; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.scraped_jobs.max_amount IS 'Maximum salary amount in specified currency';


--
-- Name: COLUMN scraped_jobs.is_remote; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.scraped_jobs.is_remote IS 'Boolean flag indicating if job allows remote work';


--
-- Name: COLUMN scraped_jobs.date_scraped; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.scraped_jobs.date_scraped IS 'Timestamp when this job was scraped';


--
-- Name: COLUMN scraped_jobs.search_query; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.scraped_jobs.search_query IS 'Search query that found this job';


--
-- Name: search_history; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.search_history (
    id integer NOT NULL,
    search_query text,
    parameters text,
    "timestamp" timestamp without time zone,
    jobs_found integer
);


--
-- Name: TABLE search_history; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.search_history IS 'Log of search operations performed by the scraper';


--
-- Name: search_history_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.search_history_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: search_history_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.search_history_id_seq OWNED BY public.search_history.id;


--
-- Name: search_history id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.search_history ALTER COLUMN id SET DEFAULT nextval('public.search_history_id_seq'::regclass);


--
-- Name: scraped_jobs scraped_jobs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scraped_jobs
    ADD CONSTRAINT scraped_jobs_pkey PRIMARY KEY (id);


--
-- Name: search_history search_history_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.search_history
    ADD CONSTRAINT search_history_pkey PRIMARY KEY (id);


--
-- Name: idx_scraped_jobs_company; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_scraped_jobs_company ON public.scraped_jobs USING btree (company);


--
-- Name: idx_scraped_jobs_company_location; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_scraped_jobs_company_location ON public.scraped_jobs USING btree (company, location);


--
-- Name: idx_scraped_jobs_date_posted; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_scraped_jobs_date_posted ON public.scraped_jobs USING btree (date_posted);


--
-- Name: idx_scraped_jobs_date_scraped; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_scraped_jobs_date_scraped ON public.scraped_jobs USING btree (date_scraped);


--
-- Name: idx_scraped_jobs_is_remote; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_scraped_jobs_is_remote ON public.scraped_jobs USING btree (is_remote);


--
-- Name: idx_scraped_jobs_location; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_scraped_jobs_location ON public.scraped_jobs USING btree (location);


--
-- Name: idx_scraped_jobs_search_query; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_scraped_jobs_search_query ON public.scraped_jobs USING btree (search_query);


--
-- Name: idx_scraped_jobs_search_query_lower; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_scraped_jobs_search_query_lower ON public.scraped_jobs USING btree (lower(search_query));


--
-- Name: idx_scraped_jobs_site; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_scraped_jobs_site ON public.scraped_jobs USING btree (site);


--
-- Name: idx_scraped_jobs_title; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_scraped_jobs_title ON public.scraped_jobs USING btree (title);


--
-- Name: idx_scraped_jobs_title_company; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_scraped_jobs_title_company ON public.scraped_jobs USING btree (title, company);


--
-- Name: idx_search_history_search_query; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_search_history_search_query ON public.search_history USING btree (search_query);


--
-- Name: idx_search_history_timestamp; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_search_history_timestamp ON public.search_history USING btree ("timestamp");


--
-- PostgreSQL database dump complete
--

