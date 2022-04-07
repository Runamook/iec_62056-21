--
-- PostgreSQL database dump
--

-- Dumped from database version 12.9 (Ubuntu 12.9-0ubuntu0.20.04.1)
-- Dumped by pg_dump version 12.9 (Ubuntu 12.9-0ubuntu0.20.04.1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: schema_name; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA schema_name;


ALTER SCHEMA schema_name OWNER TO postgres;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: data; Type: TABLE; Schema: schema_name; Owner: postgres
--

CREATE TABLE schema_name.data (
    meter_id integer,
    ts timestamp with time zone,
    obis_id integer,
    value character varying(40)
);


ALTER TABLE schema_name.data OWNER TO postgres;

--
-- Name: meters; Type: TABLE; Schema: schema_name; Owner: postgres
--

CREATE TABLE schema_name.meters (
    id integer NOT NULL,
    melo character varying(40),
    description character varying(100),
    manufacturer character varying(40),
    installation_date timestamp with time zone,
    is_active boolean,
    meter_id character varying(40) NOT NULL,
    ip_address inet NOT NULL,
    port integer DEFAULT 8000 NOT NULL,
    voltagefactor integer NOT NULL,
    currentfactor integer NOT NULL,
    org character varying(40),
    guid uuid,
    source character varying(40),
    password character varying(40),
    use_password boolean DEFAULT false NOT NULL,
    timezone character varying DEFAULT 'CET'::character varying
);


ALTER TABLE schema_name.meters OWNER TO postgres;

--
-- Name: meters_schema_id_seq; Type: SEQUENCE; Schema: meters; Owner: postgres
--

CREATE SEQUENCE schema_name.meters_schema_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE schema_name.meters_schema_id_seq OWNER TO postgres;

--
-- Name: meters_schema_id_seq; Type: SEQUENCE OWNED BY; Schema: meters; Owner: postgres
--

ALTER SEQUENCE schema_name.meters_schema_id_seq OWNED BY schema_name.meters.id;


--
-- Name: obis; Type: TABLE; Schema: meters; Owner: postgres
--

CREATE TABLE schema_name.obis (
    id integer NOT NULL,
    obis character varying(40),
    description_short character varying(128),
    description_full character varying(128),
    currentfactor boolean DEFAULT false,
    voltagefactor boolean DEFAULT false
);


ALTER TABLE schema_name.obis OWNER TO postgres;

--
-- Name: obis_id_seq; Type: SEQUENCE; Schema: meters; Owner: postgres
--

CREATE SEQUENCE schema_name.obis_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE schema_name.obis_id_seq OWNER TO postgres;

--
-- Name: obis_id_seq; Type: SEQUENCE OWNED BY; Schema: meters; Owner: postgres
--

ALTER SEQUENCE schema_name.obis_id_seq OWNED BY schema_name.obis.id;


--
-- Name: queries; Type: TABLE; Schema: meters; Owner: postgres
--

CREATE TABLE schema_name.queries (
    id integer NOT NULL,
    p01 integer DEFAULT 0,
    p02 integer DEFAULT 0,
    list1 integer DEFAULT 0,
    list2 integer DEFAULT 0,
    list3 integer DEFAULT 0,
    list4 integer DEFAULT 0,
    p98 integer DEFAULT 0
);


ALTER TABLE schema_name.queries OWNER TO postgres;

--
-- Name: meters id; Type: DEFAULT; Schema: meters; Owner: postgres
--

ALTER TABLE ONLY schema_name.meters ALTER COLUMN id SET DEFAULT nextval('schema_name.meters_schema_id_seq'::regclass);


--
-- Name: obis id; Type: DEFAULT; Schema: meters; Owner: postgres
--

ALTER TABLE ONLY schema_name.obis ALTER COLUMN id SET DEFAULT nextval('schema_name.obis_id_seq'::regclass);


--
-- Name: data data_un; Type: CONSTRAINT; Schema: meters; Owner: postgres
--

ALTER TABLE ONLY schema_name.data
    ADD CONSTRAINT data_un UNIQUE (meter_id, ts, obis_id);


--
-- Name: meters meters_schema_id_key; Type: CONSTRAINT; Schema: meters; Owner: postgres
--

ALTER TABLE ONLY schema_name.meters
    ADD CONSTRAINT meters_schema_id_key UNIQUE (id);


--
-- Name: meters meters_schema_meter_id_key; Type: CONSTRAINT; Schema: meters; Owner: postgres
--

ALTER TABLE ONLY schema_name.meters
    ADD CONSTRAINT meters_schema_meter_id_key UNIQUE (meter_id);


--
-- Name: meters meters_schema_un; Type: CONSTRAINT; Schema: meters; Owner: postgres
--

ALTER TABLE ONLY schema_name.meters
    ADD CONSTRAINT meters_schema_un UNIQUE (guid);


--
-- Name: obis obis_id_key; Type: CONSTRAINT; Schema: meters; Owner: postgres
--

ALTER TABLE ONLY schema_name.obis
    ADD CONSTRAINT obis_id_key UNIQUE (id);


--
-- Name: obis obis_obis_key; Type: CONSTRAINT; Schema: meters; Owner: postgres
--

ALTER TABLE ONLY schema_name.obis
    ADD CONSTRAINT obis_obis_key UNIQUE (obis);


--
-- Name: queries queries_id_key; Type: CONSTRAINT; Schema: meters; Owner: postgres
--

ALTER TABLE ONLY schema_name.queries
    ADD CONSTRAINT queries_id_key UNIQUE (id);


--
-- Name: data_meter_id_idx; Type: INDEX; Schema: meters; Owner: postgres
--

CREATE INDEX data_meter_id_idx ON schema_name.data USING btree (meter_id);


--
-- Name: data_obis_id_idx; Type: INDEX; Schema: meters; Owner: postgres
--

CREATE INDEX data_obis_id_idx ON schema_name.data USING btree (obis_id);


--
-- Name: data_ts_idx; Type: INDEX; Schema: meters; Owner: postgres
--

CREATE INDEX data_ts_idx ON schema_name.data USING btree (ts);


--
-- Name: meters meter_deleted; Type: TRIGGER; Schema: meters; Owner: postgres
--

CREATE TRIGGER meter_deleted AFTER DELETE ON schema_name.meters FOR EACH ROW EXECUTE FUNCTION public.delete_queries_on_delete();


--
-- Name: meters meter_inserted; Type: TRIGGER; Schema: meters; Owner: postgres
--

CREATE TRIGGER meter_inserted AFTER INSERT ON schema_name.meters FOR EACH ROW EXECUTE FUNCTION public.create_queries_on_insert();


--
-- PostgreSQL database dump complete
--

