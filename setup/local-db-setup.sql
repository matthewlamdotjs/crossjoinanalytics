-- Table: public.daily_prices_temp_tbl
-- Description: table for ingesting daily stock prices

CREATE TABLE public.daily_prices_temp_tbl
(
    symbol text COLLATE pg_catalog."default" NOT NULL,
    date date NOT NULL,
    price_high numeric(8,4),
    price_low numeric(8,4),
    price_open numeric(8,4),
    price_close numeric(8,4),
    price_usd numeric(8,4),
    CONSTRAINT "symbol-date-index" PRIMARY KEY (symbol, date)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.daily_prices_temp_tbl
    OWNER to postgres;


-- Table: public.symbol_master_tbl
-- Description: master table for all stock symbols

CREATE TABLE public.symbol_master_tbl
(
    symbol text COLLATE pg_catalog."default" NOT NULL,
    name text COLLATE pg_catalog."default" NOT NULL,
    type text COLLATE pg_catalog."default" NOT NULL,
    region text COLLATE pg_catalog."default" NOT NULL,
    timezone text COLLATE pg_catalog."default" NOT NULL,
    currency text COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT symbol_master_tbl_pkey PRIMARY KEY (symbol)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.symbol_master_tbl
    OWNER to postgres;

GRANT ALL ON TABLE public.symbol_master_tbl TO postgres;

-- Table: public.volatility_aggregation_tbl
-- Description: table to hold aggregate values (standard deviation) per time window (2 weeks)

CREATE TABLE public.volatility_aggregation_tbl
(
    symbol text COLLATE pg_catalog."default" NOT NULL,
    start_date date NOT NULL,
    end_date date NOT NULL,
    price_deviation numeric(8,4) NOT NULL,
    average_price numeric(8,4)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.volatility_aggregation_tbl
    OWNER to postgres;


-- Table: public.users
-- Description: table for dashboard user information

CREATE TABLE public.users
(
    id SERIAL NOT NULL,
    username character varying(255) COLLATE pg_catalog."default" NOT NULL,
    password character varying(100) COLLATE pg_catalog."default" NOT NULL,
    type character varying(50) COLLATE pg_catalog."default",
    CONSTRAINT users_pkey PRIMARY KEY (id),
    CONSTRAINT users_username_key UNIQUE (username)

)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.users
    OWNER to postgres;

-- Procedure: public."addSymbol"(text, text, text, text, text, text)
-- Description: function to add a new stock symbol checking if exists first

CREATE OR REPLACE PROCEDURE public."addSymbol"(
	text,
	text,
	text,
	text,
	text,
	text)
LANGUAGE 'plpgsql'

AS $BODY$BEGIN
    IF NOT EXISTS(SELECT 1 FROM symbol_master_tbl WHERE symbol = $1) THEN
		INSERT INTO symbol_master_tbl (
			symbol,
			name,
			type,
			region,
			timezone,
			currency
		)
		VALUES ($1, $2, $3, $4, $5, $6);
	END IF;

END;$BODY$;