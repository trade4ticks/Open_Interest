--
-- PostgreSQL database dump
--

\restrict eCgUI3D9eZ34cZaLTbbfsjgQpew7mptovpPXwL1LZLYbFotwjAygkUO9emoQQdE

-- Dumped from database version 16.13 (Debian 16.13-1.pgdg13+1)
-- Dumped by pg_dump version 16.13 (Debian 16.13-1.pgdg13+1)

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
-- Name: ensure_equity_atm_partition(date); Type: FUNCTION; Schema: public; Owner: portfolio
--

CREATE FUNCTION public.ensure_equity_atm_partition(d date) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
    month_start DATE := date_trunc('month', d)::DATE;
    month_end   DATE := (date_trunc('month', d) + INTERVAL '1 month')::DATE;
    part_name   TEXT := 'equity_atm_' || to_char(month_start, 'YYYYMM');
BEGIN
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF equity_atm '
        'FOR VALUES FROM (%L) TO (%L)', part_name, month_start, month_end);
EXCEPTION WHEN duplicate_table OR invalid_object_definition THEN
    NULL;
END;
$$;


ALTER FUNCTION public.ensure_equity_atm_partition(d date) OWNER TO portfolio;

--
-- Name: ensure_equity_metrics_partition(date); Type: FUNCTION; Schema: public; Owner: portfolio
--

CREATE FUNCTION public.ensure_equity_metrics_partition(d date) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
    month_start DATE := date_trunc('month', d)::DATE;
    month_end   DATE := (date_trunc('month', d) + INTERVAL '1 month')::DATE;
BEGIN
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF equity_metrics '
        'FOR VALUES FROM (%L) TO (%L)',
        'equity_metrics_' || to_char(month_start, 'YYYYMM'),
        month_start, month_end);
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF equity_metrics_z '
        'FOR VALUES FROM (%L) TO (%L)',
        'equity_metrics_z_' || to_char(month_start, 'YYYYMM'),
        month_start, month_end);
EXCEPTION WHEN duplicate_table OR invalid_object_definition THEN
    NULL;
END;
$$;


ALTER FUNCTION public.ensure_equity_metrics_partition(d date) OWNER TO portfolio;

--
-- Name: ensure_equity_surface_partition(date); Type: FUNCTION; Schema: public; Owner: portfolio
--

CREATE FUNCTION public.ensure_equity_surface_partition(d date) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
    month_start DATE := date_trunc('month', d)::DATE;
    month_end   DATE := (date_trunc('month', d) + INTERVAL '1 month')::DATE;
    part_name   TEXT := 'equity_surface_' || to_char(month_start, 'YYYYMM');
BEGIN
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF equity_surface '
        'FOR VALUES FROM (%L) TO (%L)', part_name, month_start, month_end);
EXCEPTION WHEN duplicate_table OR invalid_object_definition THEN
    NULL;
END;
$$;


ALTER FUNCTION public.ensure_equity_surface_partition(d date) OWNER TO portfolio;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: analyze_cache_outcome; Type: TABLE; Schema: public; Owner: portfolio
--

CREATE TABLE public.analyze_cache_outcome (
    cache_key text NOT NULL,
    outcome text NOT NULL,
    payload jsonb NOT NULL,
    payload_bytes integer,
    cached_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.analyze_cache_outcome OWNER TO portfolio;

--
-- Name: analyze_cache_slim; Type: TABLE; Schema: public; Owner: portfolio
--

CREATE TABLE public.analyze_cache_slim (
    cache_key text NOT NULL,
    ticker text NOT NULL,
    metric text NOT NULL,
    mode text NOT NULL,
    cutoff_date date,
    payload jsonb NOT NULL,
    payload_bytes integer,
    cached_at timestamp with time zone DEFAULT now() NOT NULL,
    last_accessed timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.analyze_cache_slim OWNER TO portfolio;

--
-- Name: analyze_cache_trade_meta; Type: TABLE; Schema: public; Owner: portfolio
--

CREATE TABLE public.analyze_cache_trade_meta (
    cache_key text NOT NULL,
    payload jsonb NOT NULL,
    payload_bytes integer,
    cached_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.analyze_cache_trade_meta OWNER TO portfolio;

--
-- Name: analyze_primary_cache; Type: TABLE; Schema: public; Owner: portfolio
--

CREATE TABLE public.analyze_primary_cache (
    cache_key text NOT NULL,
    ticker text NOT NULL,
    metric text NOT NULL,
    outcome text NOT NULL,
    mode text NOT NULL,
    cutoff_date date,
    date_from date,
    date_to date,
    payload jsonb NOT NULL,
    payload_bytes integer,
    cached_at timestamp with time zone DEFAULT now() NOT NULL,
    last_accessed timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.analyze_primary_cache OWNER TO portfolio;

--
-- Name: backtest_call_spread; Type: TABLE; Schema: public; Owner: portfolio
--

CREATE TABLE public.backtest_call_spread (
    id integer NOT NULL,
    ticker text NOT NULL,
    trade_date date NOT NULL,
    exit_date date NOT NULL,
    fired_systems text,
    spot_entry_open double precision,
    spot_exit_close double precision,
    expiration date,
    long_strike double precision,
    short_strike double precision,
    long_entry_bid double precision,
    long_entry_ask double precision,
    long_entry_mid double precision,
    long_entry_spread double precision,
    short_entry_bid double precision,
    short_entry_ask double precision,
    short_entry_mid double precision,
    short_entry_spread double precision,
    net_entry_debit double precision,
    long_exit_bid double precision,
    long_exit_ask double precision,
    long_exit_mid double precision,
    long_exit_spread double precision,
    short_exit_bid double precision,
    short_exit_ask double precision,
    short_exit_mid double precision,
    short_exit_spread double precision,
    net_exit_value double precision,
    max_risk_per_contract double precision,
    max_profit_per_contract double precision,
    qty integer,
    capital_deployed double precision,
    total_pnl double precision,
    pnl_pct double precision,
    status text,
    created_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.backtest_call_spread OWNER TO portfolio;

--
-- Name: backtest_call_spread_id_seq; Type: SEQUENCE; Schema: public; Owner: portfolio
--

CREATE SEQUENCE public.backtest_call_spread_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.backtest_call_spread_id_seq OWNER TO portfolio;

--
-- Name: backtest_call_spread_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: portfolio
--

ALTER SEQUENCE public.backtest_call_spread_id_seq OWNED BY public.backtest_call_spread.id;


--
-- Name: corner_scan_1f; Type: TABLE; Schema: public; Owner: portfolio
--

CREATE TABLE public.corner_scan_1f (
    metric text NOT NULL,
    extreme text NOT NULL,
    outcome text NOT NULL,
    d_avg_ret double precision,
    d_ret_per_day double precision,
    d_n integer,
    q_avg_ret double precision,
    q_ret_per_day double precision,
    q_n integer,
    as_of date NOT NULL,
    mode text DEFAULT 'walk_forward'::text NOT NULL,
    scanned_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.corner_scan_1f OWNER TO portfolio;

--
-- Name: corner_scan_2f; Type: TABLE; Schema: public; Owner: portfolio
--

CREATE TABLE public.corner_scan_2f (
    primary_metric text NOT NULL,
    secondary_metric text NOT NULL,
    corner_direction text NOT NULL,
    outcome text NOT NULL,
    d_avg_ret double precision,
    d_ret_per_day double precision,
    d_n integer,
    q_avg_ret double precision,
    q_ret_per_day double precision,
    q_n integer,
    as_of date NOT NULL,
    mode text DEFAULT 'walk_forward'::text NOT NULL,
    scanned_at timestamp with time zone DEFAULT now() NOT NULL,
    d_train_avg_ret double precision,
    d_train_n integer,
    d_test_avg_ret double precision,
    d_test_n integer,
    q_train_avg_ret double precision,
    q_train_n integer,
    q_test_avg_ret double precision,
    q_test_n integer,
    cutoff_date date
);


ALTER TABLE public.corner_scan_2f OWNER TO portfolio;

--
-- Name: corner_scan_notes; Type: TABLE; Schema: public; Owner: portfolio
--

CREATE TABLE public.corner_scan_notes (
    primary_metric text NOT NULL,
    secondary_metric text NOT NULL,
    corner_direction text NOT NULL,
    outcome text NOT NULL,
    note text DEFAULT ''::text NOT NULL,
    reviewed boolean DEFAULT false NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    saved boolean DEFAULT false NOT NULL
);


ALTER TABLE public.corner_scan_notes OWNER TO portfolio;

--
-- Name: daily_features; Type: TABLE; Schema: public; Owner: portfolio
--

CREATE TABLE public.daily_features (
    ticker text NOT NULL,
    trade_date date NOT NULL,
    total_oi bigint,
    call_oi bigint,
    put_oi bigint,
    put_call_oi_ratio double precision,
    max_oi_strike_call double precision,
    max_oi_strike_put double precision,
    pct_oi_in_front_expiry double precision,
    d1_total_oi_change bigint,
    d5_total_oi_change bigint,
    d20_total_oi_change bigint,
    rv_5d double precision,
    rv_20d double precision,
    ret_1d_fwd_oc double precision,
    ret_3d_fwd_oc double precision,
    ret_5d_fwd_oc double precision,
    ret_7d_fwd_oc double precision,
    ret_10d_fwd_oc double precision,
    ret_20d_fwd_oc double precision,
    top5_strikes_pct_total_oi double precision,
    top10_strikes_pct_total_oi double precision,
    weighted_avg_dte double precision,
    pct_oi_0_30d double precision,
    pct_oi_31_90d double precision,
    pct_oi_91_365d double precision,
    pct_oi_next_monthly double precision,
    d1_total_oi_pct_change double precision,
    d5_total_oi_pct_change double precision,
    d1_d5_ratio_total_oi_pct_change double precision,
    d1_put_call_oi_ratio_change double precision,
    d5_put_call_oi_ratio_change double precision,
    zscore_d1_oi_change_3m double precision,
    zscore_d5_oi_change_3m double precision,
    zscore_put_call_oi_ratio_3m double precision,
    oi_weighted_call double precision,
    oi_weighted_put double precision,
    oi_weighted_all double precision,
    oi_weighted_all_0_30d double precision,
    oi_weighted_call_0_30d double precision,
    oi_weighted_put_0_30d double precision,
    oi_weighted_all_31_90d double precision,
    oi_weighted_call_31_90d double precision,
    oi_weighted_put_31_90d double precision,
    spot_pc double precision,
    spot_co double precision,
    oi_within_5pct_pc bigint,
    oi_within_5pct_co bigint,
    oi_within_10pct_pc bigint,
    oi_within_10pct_co bigint,
    oi_above_spot_pc bigint,
    oi_above_spot_co bigint,
    oi_below_spot_pc bigint,
    oi_below_spot_co bigint,
    oi_above_below_ratio_pc double precision,
    oi_above_below_ratio_co double precision,
    pct_oi_within_5pct_pc double precision,
    pct_oi_within_5pct_co double precision,
    pct_oi_within_10pct_pc double precision,
    pct_oi_within_10pct_co double precision,
    pct_oi_above_spot_pc double precision,
    pct_oi_above_spot_co double precision,
    pct_oi_below_spot_pc double precision,
    pct_oi_below_spot_co double precision,
    oi_weighted_call_minus_spot_pc double precision,
    oi_weighted_call_minus_spot_co double precision,
    oi_weighted_put_minus_spot_pc double precision,
    oi_weighted_put_minus_spot_co double precision,
    oi_weighted_all_minus_spot_pc double precision,
    oi_weighted_all_minus_spot_co double precision,
    oi_weighted_call_div_spot_pc double precision,
    oi_weighted_call_div_spot_co double precision,
    oi_weighted_put_div_spot_pc double precision,
    oi_weighted_put_div_spot_co double precision,
    oi_weighted_all_div_spot_pc double precision,
    oi_weighted_all_div_spot_co double precision,
    oi_weighted_all_0_30d_div_spot_pc double precision,
    oi_weighted_all_0_30d_div_spot_co double precision,
    oi_weighted_call_0_30d_div_spot_pc double precision,
    oi_weighted_call_0_30d_div_spot_co double precision,
    oi_weighted_put_0_30d_div_spot_pc double precision,
    oi_weighted_put_0_30d_div_spot_co double precision,
    oi_weighted_all_31_90d_div_spot_pc double precision,
    oi_weighted_all_31_90d_div_spot_co double precision,
    oi_weighted_call_31_90d_div_spot_pc double precision,
    oi_weighted_call_31_90d_div_spot_co double precision,
    oi_weighted_put_31_90d_div_spot_pc double precision,
    oi_weighted_put_31_90d_div_spot_co double precision,
    oi_weighted_next_monthly_div_spot_pc double precision,
    oi_weighted_next_monthly_div_spot_co double precision,
    d1_oi_weighted_all_div_spot_change_pc double precision,
    d1_oi_weighted_all_div_spot_change_co double precision,
    d5_oi_weighted_all_div_spot_change_pc double precision,
    d5_oi_weighted_all_div_spot_change_co double precision,
    zscore_oi_weighted_all_div_spot_3m_pc double precision,
    zscore_oi_weighted_all_div_spot_3m_co double precision,
    zscore_oi_above_below_ratio_3m_pc double precision,
    zscore_oi_above_below_ratio_3m_co double precision,
    ret_5d double precision,
    ret_10d double precision,
    ret_20d double precision,
    pct_from_ma20 double precision,
    pct_from_ma50 double precision,
    pct_from_52w_high double precision,
    pct_from_52w_low double precision,
    donchian_pos_20d double precision,
    ma20_slope_5d double precision,
    pct_up_days_20d double precision,
    rv_ratio_5d_20d double precision,
    cum_signed_vol_20d double precision,
    atr_normalized_ret_5d double precision,
    zscore_price_vs_ma20 double precision,
    zscore_price_vs_ma50 double precision,
    zscore_underlying_vol_20d double precision,
    relative_strength_vs_spy_20d double precision,
    put_call_ratio_vol double precision,
    vol_oi_ratio_all double precision,
    vol_oi_ratio_call double precision,
    vol_oi_ratio_put double precision,
    pct_vol_0_30d double precision,
    pct_vol_31_90d double precision,
    net_new_oi_div_vol double precision,
    zscore_put_call_ratio_vol double precision,
    zscore_vol_oi_ratio_all double precision,
    zscore_vol_oi_ratio_call double precision,
    zscore_vol_oi_ratio_put double precision,
    atm_iv_7d double precision,
    atm_iv_30d double precision,
    atm_iv_90d double precision,
    iv_25d_call_30d double precision,
    iv_25d_put_30d double precision,
    rr_25d_30d double precision,
    bf_25d_30d double precision,
    skew_25p_atm_30d double precision,
    skew_atm_25c_30d double precision,
    term_7d_30d double precision,
    term_30d_90d double precision,
    vrp_30d double precision,
    iv_rv_ratio_30d double precision,
    d1_atm_iv_7d_change double precision,
    d5_atm_iv_7d_change double precision,
    d1_atm_iv_30d_change double precision,
    d5_atm_iv_30d_change double precision,
    zscore_iv_7d double precision,
    zscore_iv_30d double precision,
    zscore_iv_90d double precision,
    zscore_rr_25d_30d double precision,
    zscore_term_7d_30d double precision,
    zscore_term_30d_90d double precision,
    zscore_vrp_30d double precision,
    zscore_iv_rv_ratio_30d double precision,
    vol_weighted_call_div_spot_pc double precision,
    vol_weighted_put_div_spot_pc double precision,
    vol_weighted_all_div_spot_pc double precision,
    vol_above_below_ratio_pc double precision,
    pct_vol_within_5pct_pc double precision,
    pct_vol_within_10pct_pc double precision,
    zscore_vol_above_below_ratio_pc double precision,
    weighted_avg_dte_vol double precision,
    iv_25d_call_7d double precision,
    iv_25d_put_7d double precision,
    rr_25d_7d double precision,
    bf_25d_7d double precision,
    skew_25p_atm_7d double precision,
    skew_atm_25c_7d double precision,
    zscore_rr_25d_7d double precision,
    ret_1d_fwd_cc double precision,
    ret_3d_fwd_cc double precision,
    ret_5d_fwd_cc double precision,
    ret_7d_fwd_cc double precision,
    ret_10d_fwd_cc double precision,
    ret_20d_fwd_cc double precision
);


ALTER TABLE public.daily_features OWNER TO portfolio;

--
-- Name: earnings_calendar; Type: TABLE; Schema: public; Owner: portfolio
--

CREATE TABLE public.earnings_calendar (
    ticker text NOT NULL,
    earnings_date date NOT NULL,
    earnings_ts timestamp with time zone,
    earnings_session text,
    eps_estimate double precision,
    reported_eps double precision,
    surprise_pct double precision,
    is_estimated boolean DEFAULT false NOT NULL,
    fetched_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.earnings_calendar OWNER TO portfolio;

--
-- Name: earnings_coverage; Type: TABLE; Schema: public; Owner: portfolio
--

CREATE TABLE public.earnings_coverage (
    ticker text NOT NULL,
    has_earnings boolean NOT NULL,
    n_dates integer DEFAULT 0 NOT NULL,
    next_earnings_date date,
    last_status text NOT NULL,
    last_error text,
    last_fetched_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.earnings_coverage OWNER TO portfolio;

--
-- Name: earnings_coverage_alert; Type: VIEW; Schema: public; Owner: portfolio
--

CREATE VIEW public.earnings_coverage_alert AS
 SELECT ticker,
    last_status,
    last_error,
    next_earnings_date,
    last_fetched_at,
        CASE
            WHEN (last_status = 'error'::text) THEN 'fetch failed'::text
            WHEN (has_earnings AND (next_earnings_date IS NULL)) THEN 'no future date'::text
            WHEN (has_earnings AND (next_earnings_date < CURRENT_DATE)) THEN 'next date is in the past'::text
            WHEN (last_fetched_at < (now() - '3 days'::interval)) THEN 'stale'::text
            ELSE NULL::text
        END AS issue
   FROM public.earnings_coverage
  WHERE ((last_status = 'error'::text) OR (has_earnings AND ((next_earnings_date IS NULL) OR (next_earnings_date < CURRENT_DATE))) OR (last_fetched_at < (now() - '3 days'::interval)));


ALTER VIEW public.earnings_coverage_alert OWNER TO portfolio;

--
-- Name: equity_atm; Type: TABLE; Schema: public; Owner: portfolio
--

CREATE TABLE public.equity_atm (
    ticker text NOT NULL,
    trade_date date NOT NULL,
    snapshot text NOT NULL,
    dte smallint NOT NULL,
    atm_put_delta double precision NOT NULL,
    atm_strike double precision NOT NULL,
    atm_iv double precision NOT NULL,
    atm_forward double precision NOT NULL,
    total_var double precision,
    underlying_price double precision,
    price double precision,
    theta double precision,
    vega double precision,
    gamma double precision,
    dte_actual double precision,
    captured_at timestamp without time zone,
    source text
)
PARTITION BY RANGE (trade_date);


ALTER TABLE public.equity_atm OWNER TO portfolio;

--
-- Name: equity_metrics; Type: TABLE; Schema: public; Owner: portfolio
--

CREATE TABLE public.equity_metrics (
    ticker text NOT NULL,
    trade_date date NOT NULL,
    snapshot text NOT NULL,
    built_at timestamp without time zone DEFAULT now() NOT NULL,
    spot double precision,
    forward_7d double precision,
    forward_14d double precision,
    forward_21d double precision,
    forward_30d double precision,
    forward_60d double precision,
    forward_90d double precision,
    iv_7d_10p double precision,
    iv_7d_25p double precision,
    iv_7d_atm double precision,
    iv_7d_25c double precision,
    iv_7d_10c double precision,
    iv_14d_10p double precision,
    iv_14d_25p double precision,
    iv_14d_atm double precision,
    iv_14d_25c double precision,
    iv_14d_10c double precision,
    iv_21d_10p double precision,
    iv_21d_25p double precision,
    iv_21d_atm double precision,
    iv_21d_25c double precision,
    iv_21d_10c double precision,
    iv_30d_10p double precision,
    iv_30d_25p double precision,
    iv_30d_atm double precision,
    iv_30d_25c double precision,
    iv_30d_10c double precision,
    iv_60d_10p double precision,
    iv_60d_25p double precision,
    iv_60d_atm double precision,
    iv_60d_25c double precision,
    iv_60d_10c double precision,
    iv_90d_10p double precision,
    iv_90d_25p double precision,
    iv_90d_atm double precision,
    iv_90d_25c double precision,
    iv_90d_10c double precision,
    skew_7d_10p_25p double precision,
    skew_7d_25p_atm double precision,
    skew_7d_10p_atm double precision,
    skew_7d_atm_25c double precision,
    skew_7d_atm_10c double precision,
    skew_7d_25p_25c double precision,
    skew_14d_10p_25p double precision,
    skew_14d_25p_atm double precision,
    skew_14d_10p_atm double precision,
    skew_14d_atm_25c double precision,
    skew_14d_atm_10c double precision,
    skew_14d_25p_25c double precision,
    skew_21d_10p_25p double precision,
    skew_21d_25p_atm double precision,
    skew_21d_10p_atm double precision,
    skew_21d_atm_25c double precision,
    skew_21d_atm_10c double precision,
    skew_21d_25p_25c double precision,
    skew_30d_10p_25p double precision,
    skew_30d_25p_atm double precision,
    skew_30d_10p_atm double precision,
    skew_30d_atm_25c double precision,
    skew_30d_atm_10c double precision,
    skew_30d_25p_25c double precision,
    skew_60d_10p_25p double precision,
    skew_60d_25p_atm double precision,
    skew_60d_10p_atm double precision,
    skew_60d_atm_25c double precision,
    skew_60d_atm_10c double precision,
    skew_60d_25p_25c double precision,
    skew_90d_10p_25p double precision,
    skew_90d_25p_atm double precision,
    skew_90d_10p_atm double precision,
    skew_90d_atm_25c double precision,
    skew_90d_atm_10c double precision,
    skew_90d_25p_25c double precision,
    convex_7d_10p_25p_atm double precision,
    convex_7d_atm_25c_10c double precision,
    convex_7d_25p_atm_25c double precision,
    convex_7d_10p_atm_10c double precision,
    convex_14d_10p_25p_atm double precision,
    convex_14d_atm_25c_10c double precision,
    convex_14d_25p_atm_25c double precision,
    convex_14d_10p_atm_10c double precision,
    convex_21d_10p_25p_atm double precision,
    convex_21d_atm_25c_10c double precision,
    convex_21d_25p_atm_25c double precision,
    convex_21d_10p_atm_10c double precision,
    convex_30d_10p_25p_atm double precision,
    convex_30d_atm_25c_10c double precision,
    convex_30d_25p_atm_25c double precision,
    convex_30d_10p_atm_10c double precision,
    convex_60d_10p_25p_atm double precision,
    convex_60d_atm_25c_10c double precision,
    convex_60d_25p_atm_25c double precision,
    convex_60d_10p_atm_10c double precision,
    convex_90d_10p_25p_atm double precision,
    convex_90d_atm_25c_10c double precision,
    convex_90d_25p_atm_25c double precision,
    convex_90d_10p_atm_10c double precision,
    rr_7d_25 double precision,
    rr_7d_10 double precision,
    rr_14d_25 double precision,
    rr_14d_10 double precision,
    rr_21d_25 double precision,
    rr_21d_10 double precision,
    rr_30d_25 double precision,
    rr_30d_10 double precision,
    rr_60d_25 double precision,
    rr_60d_10 double precision,
    rr_90d_25 double precision,
    rr_90d_10 double precision,
    term_ratio_7d_14d double precision,
    term_ratio_14d_30d double precision,
    term_ratio_30d_90d double precision,
    term_ratio_7d_30d double precision,
    term_slope_7d_14d_25p double precision,
    term_slope_7d_14d_atm double precision,
    term_slope_7d_14d_25c double precision,
    term_slope_14d_30d_25p double precision,
    term_slope_14d_30d_atm double precision,
    term_slope_14d_30d_25c double precision,
    term_slope_30d_90d_25p double precision,
    term_slope_30d_90d_atm double precision,
    term_slope_30d_90d_25c double precision,
    term_slope_7d_30d_25p double precision,
    term_slope_7d_30d_atm double precision,
    term_slope_7d_30d_25c double precision,
    ratio_price_7d double precision,
    straddle_price_7d double precision,
    rr_price_7d double precision,
    wing_cost_10p_5p_7d double precision,
    zc_width_sigma_7d double precision,
    zc_short_delta_7d double precision,
    cost_at_delta_neutral_7d double precision,
    ratio_price_14d double precision,
    straddle_price_14d double precision,
    rr_price_14d double precision,
    wing_cost_10p_5p_14d double precision,
    zc_width_sigma_14d double precision,
    zc_short_delta_14d double precision,
    cost_at_delta_neutral_14d double precision,
    ratio_price_21d double precision,
    straddle_price_21d double precision,
    rr_price_21d double precision,
    wing_cost_10p_5p_21d double precision,
    zc_width_sigma_21d double precision,
    zc_short_delta_21d double precision,
    cost_at_delta_neutral_21d double precision,
    ratio_price_30d double precision,
    straddle_price_30d double precision,
    rr_price_30d double precision,
    wing_cost_10p_5p_30d double precision,
    zc_width_sigma_30d double precision,
    zc_short_delta_30d double precision,
    cost_at_delta_neutral_30d double precision,
    ratio_price_60d double precision,
    straddle_price_60d double precision,
    rr_price_60d double precision,
    wing_cost_10p_5p_60d double precision,
    zc_width_sigma_60d double precision,
    zc_short_delta_60d double precision,
    cost_at_delta_neutral_60d double precision,
    ratio_price_90d double precision,
    straddle_price_90d double precision,
    rr_price_90d double precision,
    wing_cost_10p_5p_90d double precision,
    zc_width_sigma_90d double precision,
    zc_short_delta_90d double precision,
    cost_at_delta_neutral_90d double precision,
    log_ret_d double precision,
    log_ret_7d double precision,
    log_ret_30d double precision,
    rv_7d double precision,
    rv_park_7d double precision,
    rv_gk_7d double precision,
    rv_30d double precision,
    rv_park_30d double precision,
    rv_gk_30d double precision,
    rv_90d double precision,
    rv_park_90d double precision,
    rv_gk_90d double precision,
    vrp_7d double precision,
    vrp_ratio_7d double precision,
    vrp_30d double precision,
    vrp_ratio_30d double precision,
    vrp_90d double precision,
    vrp_ratio_90d double precision,
    vov_30d_1m double precision,
    spotvol_beta_30d_1m double precision,
    spotvol_r2_30d_1m double precision,
    spotvol_beta_30d_3m double precision,
    spotvol_r2_30d_3m double precision,
    downside_semivol_30d double precision,
    extrap_10p_7d boolean,
    extrap_25p_7d boolean,
    extrap_atm_7d boolean,
    extrap_25c_7d boolean,
    extrap_10c_7d boolean,
    extrap_10p_14d boolean,
    extrap_25p_14d boolean,
    extrap_atm_14d boolean,
    extrap_25c_14d boolean,
    extrap_10c_14d boolean,
    extrap_10p_21d boolean,
    extrap_25p_21d boolean,
    extrap_atm_21d boolean,
    extrap_25c_21d boolean,
    extrap_10c_21d boolean,
    extrap_10p_30d boolean,
    extrap_25p_30d boolean,
    extrap_atm_30d boolean,
    extrap_25c_30d boolean,
    extrap_10c_30d boolean,
    extrap_10p_60d boolean,
    extrap_25p_60d boolean,
    extrap_atm_60d boolean,
    extrap_25c_60d boolean,
    extrap_10c_60d boolean,
    extrap_10p_90d boolean,
    extrap_25p_90d boolean,
    extrap_atm_90d boolean,
    extrap_25c_90d boolean,
    extrap_10c_90d boolean,
    extrap_rate_short double precision,
    n_expiries_fitted integer,
    n_expiries_skipped integer,
    pct_spot_fallback double precision,
    n_butterfly_arb integer,
    n_calendar_arb integer,
    median_domain_reach double precision,
    median_n_strikes_clean double precision,
    source text,
    captured_at timestamp without time zone,
    day_of_week smallint,
    days_to_monthly_opex smallint,
    days_to_earnings smallint,
    long_sigma_7d double precision,
    long_sigma_14d double precision,
    long_sigma_21d double precision,
    long_sigma_30d double precision,
    long_sigma_60d double precision,
    long_sigma_90d double precision,
    rv_14d double precision,
    rv_park_14d double precision,
    rv_gk_14d double precision,
    rv_21d double precision,
    rv_park_21d double precision,
    rv_gk_21d double precision,
    rv_60d double precision,
    rv_park_60d double precision,
    rv_gk_60d double precision,
    vrp_14d double precision,
    vrp_ratio_14d double precision,
    vrp_21d double precision,
    vrp_ratio_21d double precision,
    vrp_60d double precision,
    vrp_ratio_60d double precision,
    log_ret_14d double precision,
    log_ret_21d double precision,
    log_ret_60d double precision,
    log_ret_90d double precision,
    vov_7d_1m double precision,
    vov_14d_1m double precision,
    vov_21d_1m double precision,
    vov_60d_1m double precision,
    vov_90d_1m double precision,
    spotvol_beta_7d_1m double precision,
    spotvol_r2_7d_1m double precision,
    spotvol_beta_7d_3m double precision,
    spotvol_r2_7d_3m double precision,
    spotvol_beta_14d_1m double precision,
    spotvol_r2_14d_1m double precision,
    spotvol_beta_14d_3m double precision,
    spotvol_r2_14d_3m double precision,
    spotvol_beta_21d_1m double precision,
    spotvol_r2_21d_1m double precision,
    spotvol_beta_21d_3m double precision,
    spotvol_r2_21d_3m double precision,
    spotvol_beta_60d_1m double precision,
    spotvol_r2_60d_1m double precision,
    spotvol_beta_60d_3m double precision,
    spotvol_r2_60d_3m double precision,
    spotvol_beta_90d_1m double precision,
    spotvol_r2_90d_1m double precision,
    spotvol_beta_90d_3m double precision,
    spotvol_r2_90d_3m double precision,
    downside_semivol_7d double precision,
    downside_semivol_14d double precision,
    downside_semivol_21d double precision,
    downside_semivol_60d double precision,
    downside_semivol_90d double precision
)
PARTITION BY RANGE (trade_date);


ALTER TABLE public.equity_metrics OWNER TO portfolio;

--
-- Name: equity_metrics_catalog; Type: TABLE; Schema: public; Owner: portfolio
--

CREATE TABLE public.equity_metrics_catalog (
    column_name text NOT NULL,
    table_name text NOT NULL,
    family text NOT NULL,
    tenor smallint,
    wing text,
    form text NOT NULL,
    base_column text NOT NULL,
    units text NOT NULL,
    description text,
    formula text
);


ALTER TABLE public.equity_metrics_catalog OWNER TO portfolio;

--
-- Name: equity_metrics_compat; Type: VIEW; Schema: public; Owner: portfolio
--

CREATE VIEW public.equity_metrics_compat AS
 SELECT ticker,
    trade_date,
    snapshot,
    built_at,
    spot,
    forward_7d,
    forward_14d,
    forward_21d,
    forward_30d,
    forward_60d,
    forward_90d,
    iv_7d_10p,
    iv_7d_25p,
    iv_7d_atm,
    iv_7d_25c,
    iv_7d_10c,
    iv_14d_10p,
    iv_14d_25p,
    iv_14d_atm,
    iv_14d_25c,
    iv_14d_10c,
    iv_21d_10p,
    iv_21d_25p,
    iv_21d_atm,
    iv_21d_25c,
    iv_21d_10c,
    iv_30d_10p,
    iv_30d_25p,
    iv_30d_atm,
    iv_30d_25c,
    iv_30d_10c,
    iv_60d_10p,
    iv_60d_25p,
    iv_60d_atm,
    iv_60d_25c,
    iv_60d_10c,
    iv_90d_10p,
    iv_90d_25p,
    iv_90d_atm,
    iv_90d_25c,
    iv_90d_10c,
    skew_7d_10p_25p,
    skew_7d_25p_atm,
    skew_7d_10p_atm,
    skew_7d_atm_25c,
    skew_7d_atm_10c,
    skew_7d_25p_25c,
    skew_14d_10p_25p,
    skew_14d_25p_atm,
    skew_14d_10p_atm,
    skew_14d_atm_25c,
    skew_14d_atm_10c,
    skew_14d_25p_25c,
    skew_21d_10p_25p,
    skew_21d_25p_atm,
    skew_21d_10p_atm,
    skew_21d_atm_25c,
    skew_21d_atm_10c,
    skew_21d_25p_25c,
    skew_30d_10p_25p,
    skew_30d_25p_atm,
    skew_30d_10p_atm,
    skew_30d_atm_25c,
    skew_30d_atm_10c,
    skew_30d_25p_25c,
    skew_60d_10p_25p,
    skew_60d_25p_atm,
    skew_60d_10p_atm,
    skew_60d_atm_25c,
    skew_60d_atm_10c,
    skew_60d_25p_25c,
    skew_90d_10p_25p,
    skew_90d_25p_atm,
    skew_90d_10p_atm,
    skew_90d_atm_25c,
    skew_90d_atm_10c,
    skew_90d_25p_25c,
    convex_7d_10p_25p_atm,
    convex_7d_atm_25c_10c,
    convex_7d_25p_atm_25c,
    convex_7d_10p_atm_10c,
    convex_14d_10p_25p_atm,
    convex_14d_atm_25c_10c,
    convex_14d_25p_atm_25c,
    convex_14d_10p_atm_10c,
    convex_21d_10p_25p_atm,
    convex_21d_atm_25c_10c,
    convex_21d_25p_atm_25c,
    convex_21d_10p_atm_10c,
    convex_30d_10p_25p_atm,
    convex_30d_atm_25c_10c,
    convex_30d_25p_atm_25c,
    convex_30d_10p_atm_10c,
    convex_60d_10p_25p_atm,
    convex_60d_atm_25c_10c,
    convex_60d_25p_atm_25c,
    convex_60d_10p_atm_10c,
    convex_90d_10p_25p_atm,
    convex_90d_atm_25c_10c,
    convex_90d_25p_atm_25c,
    convex_90d_10p_atm_10c,
    rr_7d_25,
    rr_7d_10,
    rr_14d_25,
    rr_14d_10,
    rr_21d_25,
    rr_21d_10,
    rr_30d_25,
    rr_30d_10,
    rr_60d_25,
    rr_60d_10,
    rr_90d_25,
    rr_90d_10,
    term_ratio_7d_14d,
    term_ratio_14d_30d,
    term_ratio_30d_90d,
    term_ratio_7d_30d,
    term_slope_7d_14d_25p,
    term_slope_7d_14d_atm,
    term_slope_7d_14d_25c,
    term_slope_14d_30d_25p,
    term_slope_14d_30d_atm,
    term_slope_14d_30d_25c,
    term_slope_30d_90d_25p,
    term_slope_30d_90d_atm,
    term_slope_30d_90d_25c,
    term_slope_7d_30d_25p,
    term_slope_7d_30d_atm,
    term_slope_7d_30d_25c,
    ratio_price_7d,
    straddle_price_7d,
    rr_price_7d,
    wing_cost_10p_5p_7d,
    zc_width_sigma_7d,
    zc_short_delta_7d,
    cost_at_delta_neutral_7d,
    ratio_price_14d,
    straddle_price_14d,
    rr_price_14d,
    wing_cost_10p_5p_14d,
    zc_width_sigma_14d,
    zc_short_delta_14d,
    cost_at_delta_neutral_14d,
    ratio_price_21d,
    straddle_price_21d,
    rr_price_21d,
    wing_cost_10p_5p_21d,
    zc_width_sigma_21d,
    zc_short_delta_21d,
    cost_at_delta_neutral_21d,
    ratio_price_30d,
    straddle_price_30d,
    rr_price_30d,
    wing_cost_10p_5p_30d,
    zc_width_sigma_30d,
    zc_short_delta_30d,
    cost_at_delta_neutral_30d,
    ratio_price_60d,
    straddle_price_60d,
    rr_price_60d,
    wing_cost_10p_5p_60d,
    zc_width_sigma_60d,
    zc_short_delta_60d,
    cost_at_delta_neutral_60d,
    ratio_price_90d,
    straddle_price_90d,
    rr_price_90d,
    wing_cost_10p_5p_90d,
    zc_width_sigma_90d,
    zc_short_delta_90d,
    cost_at_delta_neutral_90d,
    log_ret_d,
    log_ret_7d,
    log_ret_30d,
    rv_7d,
    rv_park_7d,
    rv_gk_7d,
    rv_30d,
    rv_park_30d,
    rv_gk_30d,
    rv_90d,
    rv_park_90d,
    rv_gk_90d,
    vrp_7d,
    vrp_ratio_7d,
    vrp_30d,
    vrp_ratio_30d,
    vrp_90d,
    vrp_ratio_90d,
    vov_30d_1m,
    spotvol_beta_30d_1m,
    spotvol_r2_30d_1m,
    spotvol_beta_30d_3m,
    spotvol_r2_30d_3m,
    downside_semivol_30d,
    extrap_10p_7d,
    extrap_25p_7d,
    extrap_atm_7d,
    extrap_25c_7d,
    extrap_10c_7d,
    extrap_10p_14d,
    extrap_25p_14d,
    extrap_atm_14d,
    extrap_25c_14d,
    extrap_10c_14d,
    extrap_10p_21d,
    extrap_25p_21d,
    extrap_atm_21d,
    extrap_25c_21d,
    extrap_10c_21d,
    extrap_10p_30d,
    extrap_25p_30d,
    extrap_atm_30d,
    extrap_25c_30d,
    extrap_10c_30d,
    extrap_10p_60d,
    extrap_25p_60d,
    extrap_atm_60d,
    extrap_25c_60d,
    extrap_10c_60d,
    extrap_10p_90d,
    extrap_25p_90d,
    extrap_atm_90d,
    extrap_25c_90d,
    extrap_10c_90d,
    extrap_rate_short,
    n_expiries_fitted,
    n_expiries_skipped,
    pct_spot_fallback,
    n_butterfly_arb,
    n_calendar_arb,
    median_domain_reach,
    median_n_strikes_clean,
    source,
    captured_at,
    day_of_week,
    days_to_monthly_opex,
    days_to_earnings,
    long_sigma_7d,
    long_sigma_14d,
    long_sigma_21d,
    long_sigma_30d,
    long_sigma_60d,
    long_sigma_90d,
    rv_14d,
    rv_park_14d,
    rv_gk_14d,
    rv_21d,
    rv_park_21d,
    rv_gk_21d,
    rv_60d,
    rv_park_60d,
    rv_gk_60d,
    vrp_14d,
    vrp_ratio_14d,
    vrp_21d,
    vrp_ratio_21d,
    vrp_60d,
    vrp_ratio_60d,
    log_ret_14d,
    log_ret_21d,
    log_ret_60d,
    log_ret_90d,
    vov_7d_1m,
    vov_14d_1m,
    vov_21d_1m,
    vov_60d_1m,
    vov_90d_1m,
    spotvol_beta_7d_1m,
    spotvol_r2_7d_1m,
    spotvol_beta_7d_3m,
    spotvol_r2_7d_3m,
    spotvol_beta_14d_1m,
    spotvol_r2_14d_1m,
    spotvol_beta_14d_3m,
    spotvol_r2_14d_3m,
    spotvol_beta_21d_1m,
    spotvol_r2_21d_1m,
    spotvol_beta_21d_3m,
    spotvol_r2_21d_3m,
    spotvol_beta_60d_1m,
    spotvol_r2_60d_1m,
    spotvol_beta_60d_3m,
    spotvol_r2_60d_3m,
    spotvol_beta_90d_1m,
    spotvol_r2_90d_1m,
    spotvol_beta_90d_3m,
    spotvol_r2_90d_3m,
    downside_semivol_7d,
    downside_semivol_14d,
    downside_semivol_21d,
    downside_semivol_60d,
    downside_semivol_90d,
    rv_7d AS rv_1w,
    rv_park_7d AS rv_park_1w,
    rv_gk_7d AS rv_gk_1w,
    vrp_7d AS vrp_1w,
    vrp_ratio_7d AS vrp_ratio_1w,
    rv_30d AS rv_1m,
    rv_park_30d AS rv_park_1m,
    rv_gk_30d AS rv_gk_1m,
    vrp_30d AS vrp_1m,
    vrp_ratio_30d AS vrp_ratio_1m,
    rv_90d AS rv_3m,
    rv_park_90d AS rv_park_3m,
    rv_gk_90d AS rv_gk_3m,
    vrp_90d AS vrp_3m,
    vrp_ratio_90d AS vrp_ratio_3m,
    log_ret_7d AS log_ret_1w,
    log_ret_30d AS log_ret_1m,
    downside_semivol_30d AS downside_semivol_1m,
    spotvol_beta_30d_1m AS spotvol_beta_1m,
    spotvol_r2_30d_1m AS spotvol_r2_1m,
    spotvol_beta_30d_3m AS spotvol_beta_3m,
    spotvol_r2_30d_3m AS spotvol_r2_3m
   FROM public.equity_metrics m;


ALTER VIEW public.equity_metrics_compat OWNER TO portfolio;

--
-- Name: VIEW equity_metrics_compat; Type: COMMENT; Schema: public; Owner: portfolio
--

COMMENT ON VIEW public.equity_metrics_compat IS 'Deprecated-name shim for the rv/vrp tenor rename. rv_1w/rv_1m/rv_3m and their vrp siblings are aliases of rv_7d/rv_30d/rv_90d. Point readers at the real column names and drop this view.';


--
-- Name: equity_metrics_z; Type: TABLE; Schema: public; Owner: portfolio
--

CREATE TABLE public.equity_metrics_z (
    ticker text NOT NULL,
    trade_date date NOT NULL,
    snapshot text NOT NULL,
    built_at timestamp without time zone DEFAULT now() NOT NULL,
    iv_7d_10p_z_63 double precision,
    iv_7d_25p_z_63 double precision,
    iv_7d_atm_z_63 double precision,
    iv_7d_25c_z_63 double precision,
    iv_7d_10c_z_63 double precision,
    iv_14d_10p_z_63 double precision,
    iv_14d_25p_z_63 double precision,
    iv_14d_atm_z_63 double precision,
    iv_14d_25c_z_63 double precision,
    iv_14d_10c_z_63 double precision,
    iv_21d_10p_z_63 double precision,
    iv_21d_25p_z_63 double precision,
    iv_21d_atm_z_63 double precision,
    iv_21d_25c_z_63 double precision,
    iv_21d_10c_z_63 double precision,
    iv_30d_10p_z_63 double precision,
    iv_30d_25p_z_63 double precision,
    iv_30d_atm_z_63 double precision,
    iv_30d_25c_z_63 double precision,
    iv_30d_10c_z_63 double precision,
    iv_60d_10p_z_63 double precision,
    iv_60d_25p_z_63 double precision,
    iv_60d_atm_z_63 double precision,
    iv_60d_25c_z_63 double precision,
    iv_60d_10c_z_63 double precision,
    iv_90d_10p_z_63 double precision,
    iv_90d_25p_z_63 double precision,
    iv_90d_atm_z_63 double precision,
    iv_90d_25c_z_63 double precision,
    iv_90d_10c_z_63 double precision,
    skew_7d_10p_25p_z_63 double precision,
    skew_7d_25p_atm_z_63 double precision,
    skew_7d_10p_atm_z_63 double precision,
    skew_7d_atm_25c_z_63 double precision,
    skew_7d_atm_10c_z_63 double precision,
    skew_7d_25p_25c_z_63 double precision,
    skew_14d_10p_25p_z_63 double precision,
    skew_14d_25p_atm_z_63 double precision,
    skew_14d_10p_atm_z_63 double precision,
    skew_14d_atm_25c_z_63 double precision,
    skew_14d_atm_10c_z_63 double precision,
    skew_14d_25p_25c_z_63 double precision,
    skew_21d_10p_25p_z_63 double precision,
    skew_21d_25p_atm_z_63 double precision,
    skew_21d_10p_atm_z_63 double precision,
    skew_21d_atm_25c_z_63 double precision,
    skew_21d_atm_10c_z_63 double precision,
    skew_21d_25p_25c_z_63 double precision,
    skew_30d_10p_25p_z_63 double precision,
    skew_30d_25p_atm_z_63 double precision,
    skew_30d_10p_atm_z_63 double precision,
    skew_30d_atm_25c_z_63 double precision,
    skew_30d_atm_10c_z_63 double precision,
    skew_30d_25p_25c_z_63 double precision,
    skew_60d_10p_25p_z_63 double precision,
    skew_60d_25p_atm_z_63 double precision,
    skew_60d_10p_atm_z_63 double precision,
    skew_60d_atm_25c_z_63 double precision,
    skew_60d_atm_10c_z_63 double precision,
    skew_60d_25p_25c_z_63 double precision,
    skew_90d_10p_25p_z_63 double precision,
    skew_90d_25p_atm_z_63 double precision,
    skew_90d_10p_atm_z_63 double precision,
    skew_90d_atm_25c_z_63 double precision,
    skew_90d_atm_10c_z_63 double precision,
    skew_90d_25p_25c_z_63 double precision,
    convex_7d_10p_25p_atm_z_63 double precision,
    convex_7d_atm_25c_10c_z_63 double precision,
    convex_7d_25p_atm_25c_z_63 double precision,
    convex_7d_10p_atm_10c_z_63 double precision,
    convex_14d_10p_25p_atm_z_63 double precision,
    convex_14d_atm_25c_10c_z_63 double precision,
    convex_14d_25p_atm_25c_z_63 double precision,
    convex_14d_10p_atm_10c_z_63 double precision,
    convex_21d_10p_25p_atm_z_63 double precision,
    convex_21d_atm_25c_10c_z_63 double precision,
    convex_21d_25p_atm_25c_z_63 double precision,
    convex_21d_10p_atm_10c_z_63 double precision,
    convex_30d_10p_25p_atm_z_63 double precision,
    convex_30d_atm_25c_10c_z_63 double precision,
    convex_30d_25p_atm_25c_z_63 double precision,
    convex_30d_10p_atm_10c_z_63 double precision,
    convex_60d_10p_25p_atm_z_63 double precision,
    convex_60d_atm_25c_10c_z_63 double precision,
    convex_60d_25p_atm_25c_z_63 double precision,
    convex_60d_10p_atm_10c_z_63 double precision,
    convex_90d_10p_25p_atm_z_63 double precision,
    convex_90d_atm_25c_10c_z_63 double precision,
    convex_90d_25p_atm_25c_z_63 double precision,
    convex_90d_10p_atm_10c_z_63 double precision,
    rr_7d_25_z_63 double precision,
    rr_7d_10_z_63 double precision,
    rr_14d_25_z_63 double precision,
    rr_14d_10_z_63 double precision,
    rr_21d_25_z_63 double precision,
    rr_21d_10_z_63 double precision,
    rr_30d_25_z_63 double precision,
    rr_30d_10_z_63 double precision,
    rr_60d_25_z_63 double precision,
    rr_60d_10_z_63 double precision,
    rr_90d_25_z_63 double precision,
    rr_90d_10_z_63 double precision,
    term_ratio_7d_14d_z_63 double precision,
    term_ratio_14d_30d_z_63 double precision,
    term_ratio_30d_90d_z_63 double precision,
    term_ratio_7d_30d_z_63 double precision,
    term_slope_7d_14d_25p_z_63 double precision,
    term_slope_7d_14d_atm_z_63 double precision,
    term_slope_7d_14d_25c_z_63 double precision,
    term_slope_14d_30d_25p_z_63 double precision,
    term_slope_14d_30d_atm_z_63 double precision,
    term_slope_14d_30d_25c_z_63 double precision,
    term_slope_30d_90d_25p_z_63 double precision,
    term_slope_30d_90d_atm_z_63 double precision,
    term_slope_30d_90d_25c_z_63 double precision,
    term_slope_7d_30d_25p_z_63 double precision,
    term_slope_7d_30d_atm_z_63 double precision,
    term_slope_7d_30d_25c_z_63 double precision,
    ratio_price_7d_z_63 double precision,
    straddle_price_7d_z_63 double precision,
    rr_price_7d_z_63 double precision,
    wing_cost_10p_5p_7d_z_63 double precision,
    zc_width_sigma_7d_z_63 double precision,
    zc_short_delta_7d_z_63 double precision,
    cost_at_delta_neutral_7d_z_63 double precision,
    ratio_price_14d_z_63 double precision,
    straddle_price_14d_z_63 double precision,
    rr_price_14d_z_63 double precision,
    wing_cost_10p_5p_14d_z_63 double precision,
    zc_width_sigma_14d_z_63 double precision,
    zc_short_delta_14d_z_63 double precision,
    cost_at_delta_neutral_14d_z_63 double precision,
    ratio_price_21d_z_63 double precision,
    straddle_price_21d_z_63 double precision,
    rr_price_21d_z_63 double precision,
    wing_cost_10p_5p_21d_z_63 double precision,
    zc_width_sigma_21d_z_63 double precision,
    zc_short_delta_21d_z_63 double precision,
    cost_at_delta_neutral_21d_z_63 double precision,
    ratio_price_30d_z_63 double precision,
    straddle_price_30d_z_63 double precision,
    rr_price_30d_z_63 double precision,
    wing_cost_10p_5p_30d_z_63 double precision,
    zc_width_sigma_30d_z_63 double precision,
    zc_short_delta_30d_z_63 double precision,
    cost_at_delta_neutral_30d_z_63 double precision,
    ratio_price_60d_z_63 double precision,
    straddle_price_60d_z_63 double precision,
    rr_price_60d_z_63 double precision,
    wing_cost_10p_5p_60d_z_63 double precision,
    zc_width_sigma_60d_z_63 double precision,
    zc_short_delta_60d_z_63 double precision,
    cost_at_delta_neutral_60d_z_63 double precision,
    ratio_price_90d_z_63 double precision,
    straddle_price_90d_z_63 double precision,
    rr_price_90d_z_63 double precision,
    wing_cost_10p_5p_90d_z_63 double precision,
    zc_width_sigma_90d_z_63 double precision,
    zc_short_delta_90d_z_63 double precision,
    cost_at_delta_neutral_90d_z_63 double precision,
    log_ret_d_z_63 double precision,
    log_ret_7d_z_63 double precision,
    log_ret_30d_z_63 double precision,
    rv_7d_z_63 double precision,
    rv_park_7d_z_63 double precision,
    rv_gk_7d_z_63 double precision,
    rv_30d_z_63 double precision,
    rv_park_30d_z_63 double precision,
    rv_gk_30d_z_63 double precision,
    rv_90d_z_63 double precision,
    rv_park_90d_z_63 double precision,
    rv_gk_90d_z_63 double precision,
    vrp_7d_z_63 double precision,
    vrp_ratio_7d_z_63 double precision,
    vrp_30d_z_63 double precision,
    vrp_ratio_30d_z_63 double precision,
    vrp_90d_z_63 double precision,
    vrp_ratio_90d_z_63 double precision,
    vov_30d_1m_z_63 double precision,
    spotvol_beta_30d_1m_z_63 double precision,
    spotvol_r2_30d_1m_z_63 double precision,
    spotvol_beta_30d_3m_z_63 double precision,
    spotvol_r2_30d_3m_z_63 double precision,
    downside_semivol_30d_z_63 double precision,
    iv_7d_10p_z_252 double precision,
    iv_7d_25p_z_252 double precision,
    iv_7d_atm_z_252 double precision,
    iv_7d_25c_z_252 double precision,
    iv_7d_10c_z_252 double precision,
    iv_14d_10p_z_252 double precision,
    iv_14d_25p_z_252 double precision,
    iv_14d_atm_z_252 double precision,
    iv_14d_25c_z_252 double precision,
    iv_14d_10c_z_252 double precision,
    iv_21d_10p_z_252 double precision,
    iv_21d_25p_z_252 double precision,
    iv_21d_atm_z_252 double precision,
    iv_21d_25c_z_252 double precision,
    iv_21d_10c_z_252 double precision,
    iv_30d_10p_z_252 double precision,
    iv_30d_25p_z_252 double precision,
    iv_30d_atm_z_252 double precision,
    iv_30d_25c_z_252 double precision,
    iv_30d_10c_z_252 double precision,
    iv_60d_10p_z_252 double precision,
    iv_60d_25p_z_252 double precision,
    iv_60d_atm_z_252 double precision,
    iv_60d_25c_z_252 double precision,
    iv_60d_10c_z_252 double precision,
    iv_90d_10p_z_252 double precision,
    iv_90d_25p_z_252 double precision,
    iv_90d_atm_z_252 double precision,
    iv_90d_25c_z_252 double precision,
    iv_90d_10c_z_252 double precision,
    skew_7d_10p_25p_z_252 double precision,
    skew_7d_25p_atm_z_252 double precision,
    skew_7d_10p_atm_z_252 double precision,
    skew_7d_atm_25c_z_252 double precision,
    skew_7d_atm_10c_z_252 double precision,
    skew_7d_25p_25c_z_252 double precision,
    skew_14d_10p_25p_z_252 double precision,
    skew_14d_25p_atm_z_252 double precision,
    skew_14d_10p_atm_z_252 double precision,
    skew_14d_atm_25c_z_252 double precision,
    skew_14d_atm_10c_z_252 double precision,
    skew_14d_25p_25c_z_252 double precision,
    skew_21d_10p_25p_z_252 double precision,
    skew_21d_25p_atm_z_252 double precision,
    skew_21d_10p_atm_z_252 double precision,
    skew_21d_atm_25c_z_252 double precision,
    skew_21d_atm_10c_z_252 double precision,
    skew_21d_25p_25c_z_252 double precision,
    skew_30d_10p_25p_z_252 double precision,
    skew_30d_25p_atm_z_252 double precision,
    skew_30d_10p_atm_z_252 double precision,
    skew_30d_atm_25c_z_252 double precision,
    skew_30d_atm_10c_z_252 double precision,
    skew_30d_25p_25c_z_252 double precision,
    skew_60d_10p_25p_z_252 double precision,
    skew_60d_25p_atm_z_252 double precision,
    skew_60d_10p_atm_z_252 double precision,
    skew_60d_atm_25c_z_252 double precision,
    skew_60d_atm_10c_z_252 double precision,
    skew_60d_25p_25c_z_252 double precision,
    skew_90d_10p_25p_z_252 double precision,
    skew_90d_25p_atm_z_252 double precision,
    skew_90d_10p_atm_z_252 double precision,
    skew_90d_atm_25c_z_252 double precision,
    skew_90d_atm_10c_z_252 double precision,
    skew_90d_25p_25c_z_252 double precision,
    convex_7d_10p_25p_atm_z_252 double precision,
    convex_7d_atm_25c_10c_z_252 double precision,
    convex_7d_25p_atm_25c_z_252 double precision,
    convex_7d_10p_atm_10c_z_252 double precision,
    convex_14d_10p_25p_atm_z_252 double precision,
    convex_14d_atm_25c_10c_z_252 double precision,
    convex_14d_25p_atm_25c_z_252 double precision,
    convex_14d_10p_atm_10c_z_252 double precision,
    convex_21d_10p_25p_atm_z_252 double precision,
    convex_21d_atm_25c_10c_z_252 double precision,
    convex_21d_25p_atm_25c_z_252 double precision,
    convex_21d_10p_atm_10c_z_252 double precision,
    convex_30d_10p_25p_atm_z_252 double precision,
    convex_30d_atm_25c_10c_z_252 double precision,
    convex_30d_25p_atm_25c_z_252 double precision,
    convex_30d_10p_atm_10c_z_252 double precision,
    convex_60d_10p_25p_atm_z_252 double precision,
    convex_60d_atm_25c_10c_z_252 double precision,
    convex_60d_25p_atm_25c_z_252 double precision,
    convex_60d_10p_atm_10c_z_252 double precision,
    convex_90d_10p_25p_atm_z_252 double precision,
    convex_90d_atm_25c_10c_z_252 double precision,
    convex_90d_25p_atm_25c_z_252 double precision,
    convex_90d_10p_atm_10c_z_252 double precision,
    rr_7d_25_z_252 double precision,
    rr_7d_10_z_252 double precision,
    rr_14d_25_z_252 double precision,
    rr_14d_10_z_252 double precision,
    rr_21d_25_z_252 double precision,
    rr_21d_10_z_252 double precision,
    rr_30d_25_z_252 double precision,
    rr_30d_10_z_252 double precision,
    rr_60d_25_z_252 double precision,
    rr_60d_10_z_252 double precision,
    rr_90d_25_z_252 double precision,
    rr_90d_10_z_252 double precision,
    term_ratio_7d_14d_z_252 double precision,
    term_ratio_14d_30d_z_252 double precision,
    term_ratio_30d_90d_z_252 double precision,
    term_ratio_7d_30d_z_252 double precision,
    term_slope_7d_14d_25p_z_252 double precision,
    term_slope_7d_14d_atm_z_252 double precision,
    term_slope_7d_14d_25c_z_252 double precision,
    term_slope_14d_30d_25p_z_252 double precision,
    term_slope_14d_30d_atm_z_252 double precision,
    term_slope_14d_30d_25c_z_252 double precision,
    term_slope_30d_90d_25p_z_252 double precision,
    term_slope_30d_90d_atm_z_252 double precision,
    term_slope_30d_90d_25c_z_252 double precision,
    term_slope_7d_30d_25p_z_252 double precision,
    term_slope_7d_30d_atm_z_252 double precision,
    term_slope_7d_30d_25c_z_252 double precision,
    ratio_price_7d_z_252 double precision,
    straddle_price_7d_z_252 double precision,
    rr_price_7d_z_252 double precision,
    wing_cost_10p_5p_7d_z_252 double precision,
    zc_width_sigma_7d_z_252 double precision,
    zc_short_delta_7d_z_252 double precision,
    cost_at_delta_neutral_7d_z_252 double precision,
    ratio_price_14d_z_252 double precision,
    straddle_price_14d_z_252 double precision,
    rr_price_14d_z_252 double precision,
    wing_cost_10p_5p_14d_z_252 double precision,
    zc_width_sigma_14d_z_252 double precision,
    zc_short_delta_14d_z_252 double precision,
    cost_at_delta_neutral_14d_z_252 double precision,
    ratio_price_21d_z_252 double precision,
    straddle_price_21d_z_252 double precision,
    rr_price_21d_z_252 double precision,
    wing_cost_10p_5p_21d_z_252 double precision,
    zc_width_sigma_21d_z_252 double precision,
    zc_short_delta_21d_z_252 double precision,
    cost_at_delta_neutral_21d_z_252 double precision,
    ratio_price_30d_z_252 double precision,
    straddle_price_30d_z_252 double precision,
    rr_price_30d_z_252 double precision,
    wing_cost_10p_5p_30d_z_252 double precision,
    zc_width_sigma_30d_z_252 double precision,
    zc_short_delta_30d_z_252 double precision,
    cost_at_delta_neutral_30d_z_252 double precision,
    ratio_price_60d_z_252 double precision,
    straddle_price_60d_z_252 double precision,
    rr_price_60d_z_252 double precision,
    wing_cost_10p_5p_60d_z_252 double precision,
    zc_width_sigma_60d_z_252 double precision,
    zc_short_delta_60d_z_252 double precision,
    cost_at_delta_neutral_60d_z_252 double precision,
    ratio_price_90d_z_252 double precision,
    straddle_price_90d_z_252 double precision,
    rr_price_90d_z_252 double precision,
    wing_cost_10p_5p_90d_z_252 double precision,
    zc_width_sigma_90d_z_252 double precision,
    zc_short_delta_90d_z_252 double precision,
    cost_at_delta_neutral_90d_z_252 double precision,
    log_ret_d_z_252 double precision,
    log_ret_7d_z_252 double precision,
    log_ret_30d_z_252 double precision,
    rv_7d_z_252 double precision,
    rv_park_7d_z_252 double precision,
    rv_gk_7d_z_252 double precision,
    rv_30d_z_252 double precision,
    rv_park_30d_z_252 double precision,
    rv_gk_30d_z_252 double precision,
    rv_90d_z_252 double precision,
    rv_park_90d_z_252 double precision,
    rv_gk_90d_z_252 double precision,
    vrp_7d_z_252 double precision,
    vrp_ratio_7d_z_252 double precision,
    vrp_30d_z_252 double precision,
    vrp_ratio_30d_z_252 double precision,
    vrp_90d_z_252 double precision,
    vrp_ratio_90d_z_252 double precision,
    vov_30d_1m_z_252 double precision,
    spotvol_beta_30d_1m_z_252 double precision,
    spotvol_r2_30d_1m_z_252 double precision,
    spotvol_beta_30d_3m_z_252 double precision,
    spotvol_r2_30d_3m_z_252 double precision,
    downside_semivol_30d_z_252 double precision,
    long_sigma_7d_z_63 double precision,
    long_sigma_14d_z_63 double precision,
    long_sigma_21d_z_63 double precision,
    long_sigma_30d_z_63 double precision,
    long_sigma_60d_z_63 double precision,
    long_sigma_90d_z_63 double precision,
    long_sigma_7d_z_252 double precision,
    long_sigma_14d_z_252 double precision,
    long_sigma_21d_z_252 double precision,
    long_sigma_30d_z_252 double precision,
    long_sigma_60d_z_252 double precision,
    long_sigma_90d_z_252 double precision,
    rv_14d_z_63 double precision,
    rv_park_14d_z_63 double precision,
    rv_gk_14d_z_63 double precision,
    rv_21d_z_63 double precision,
    rv_park_21d_z_63 double precision,
    rv_gk_21d_z_63 double precision,
    rv_60d_z_63 double precision,
    rv_park_60d_z_63 double precision,
    rv_gk_60d_z_63 double precision,
    vrp_14d_z_63 double precision,
    vrp_ratio_14d_z_63 double precision,
    vrp_21d_z_63 double precision,
    vrp_ratio_21d_z_63 double precision,
    vrp_60d_z_63 double precision,
    vrp_ratio_60d_z_63 double precision,
    rv_14d_z_252 double precision,
    rv_park_14d_z_252 double precision,
    rv_gk_14d_z_252 double precision,
    rv_21d_z_252 double precision,
    rv_park_21d_z_252 double precision,
    rv_gk_21d_z_252 double precision,
    rv_60d_z_252 double precision,
    rv_park_60d_z_252 double precision,
    rv_gk_60d_z_252 double precision,
    vrp_14d_z_252 double precision,
    vrp_ratio_14d_z_252 double precision,
    vrp_21d_z_252 double precision,
    vrp_ratio_21d_z_252 double precision,
    vrp_60d_z_252 double precision,
    vrp_ratio_60d_z_252 double precision,
    log_ret_14d_z_63 double precision,
    log_ret_21d_z_63 double precision,
    log_ret_60d_z_63 double precision,
    log_ret_90d_z_63 double precision,
    vov_7d_1m_z_63 double precision,
    vov_14d_1m_z_63 double precision,
    vov_21d_1m_z_63 double precision,
    vov_60d_1m_z_63 double precision,
    vov_90d_1m_z_63 double precision,
    spotvol_beta_7d_1m_z_63 double precision,
    spotvol_r2_7d_1m_z_63 double precision,
    spotvol_beta_7d_3m_z_63 double precision,
    spotvol_r2_7d_3m_z_63 double precision,
    spotvol_beta_14d_1m_z_63 double precision,
    spotvol_r2_14d_1m_z_63 double precision,
    spotvol_beta_14d_3m_z_63 double precision,
    spotvol_r2_14d_3m_z_63 double precision,
    spotvol_beta_21d_1m_z_63 double precision,
    spotvol_r2_21d_1m_z_63 double precision,
    spotvol_beta_21d_3m_z_63 double precision,
    spotvol_r2_21d_3m_z_63 double precision,
    spotvol_beta_60d_1m_z_63 double precision,
    spotvol_r2_60d_1m_z_63 double precision,
    spotvol_beta_60d_3m_z_63 double precision,
    spotvol_r2_60d_3m_z_63 double precision,
    spotvol_beta_90d_1m_z_63 double precision,
    spotvol_r2_90d_1m_z_63 double precision,
    spotvol_beta_90d_3m_z_63 double precision,
    spotvol_r2_90d_3m_z_63 double precision,
    downside_semivol_7d_z_63 double precision,
    downside_semivol_14d_z_63 double precision,
    downside_semivol_21d_z_63 double precision,
    downside_semivol_60d_z_63 double precision,
    downside_semivol_90d_z_63 double precision,
    log_ret_14d_z_252 double precision,
    log_ret_21d_z_252 double precision,
    log_ret_60d_z_252 double precision,
    log_ret_90d_z_252 double precision,
    vov_7d_1m_z_252 double precision,
    vov_14d_1m_z_252 double precision,
    vov_21d_1m_z_252 double precision,
    vov_60d_1m_z_252 double precision,
    vov_90d_1m_z_252 double precision,
    spotvol_beta_7d_1m_z_252 double precision,
    spotvol_r2_7d_1m_z_252 double precision,
    spotvol_beta_7d_3m_z_252 double precision,
    spotvol_r2_7d_3m_z_252 double precision,
    spotvol_beta_14d_1m_z_252 double precision,
    spotvol_r2_14d_1m_z_252 double precision,
    spotvol_beta_14d_3m_z_252 double precision,
    spotvol_r2_14d_3m_z_252 double precision,
    spotvol_beta_21d_1m_z_252 double precision,
    spotvol_r2_21d_1m_z_252 double precision,
    spotvol_beta_21d_3m_z_252 double precision,
    spotvol_r2_21d_3m_z_252 double precision,
    spotvol_beta_60d_1m_z_252 double precision,
    spotvol_r2_60d_1m_z_252 double precision,
    spotvol_beta_60d_3m_z_252 double precision,
    spotvol_r2_60d_3m_z_252 double precision,
    spotvol_beta_90d_1m_z_252 double precision,
    spotvol_r2_90d_1m_z_252 double precision,
    spotvol_beta_90d_3m_z_252 double precision,
    spotvol_r2_90d_3m_z_252 double precision,
    downside_semivol_7d_z_252 double precision,
    downside_semivol_14d_z_252 double precision,
    downside_semivol_21d_z_252 double precision,
    downside_semivol_60d_z_252 double precision,
    downside_semivol_90d_z_252 double precision
)
PARTITION BY RANGE (trade_date);


ALTER TABLE public.equity_metrics_z OWNER TO portfolio;

--
-- Name: equity_metrics_z_compat; Type: VIEW; Schema: public; Owner: portfolio
--

CREATE VIEW public.equity_metrics_z_compat AS
 SELECT ticker,
    trade_date,
    snapshot,
    built_at,
    iv_7d_10p_z_63,
    iv_7d_25p_z_63,
    iv_7d_atm_z_63,
    iv_7d_25c_z_63,
    iv_7d_10c_z_63,
    iv_14d_10p_z_63,
    iv_14d_25p_z_63,
    iv_14d_atm_z_63,
    iv_14d_25c_z_63,
    iv_14d_10c_z_63,
    iv_21d_10p_z_63,
    iv_21d_25p_z_63,
    iv_21d_atm_z_63,
    iv_21d_25c_z_63,
    iv_21d_10c_z_63,
    iv_30d_10p_z_63,
    iv_30d_25p_z_63,
    iv_30d_atm_z_63,
    iv_30d_25c_z_63,
    iv_30d_10c_z_63,
    iv_60d_10p_z_63,
    iv_60d_25p_z_63,
    iv_60d_atm_z_63,
    iv_60d_25c_z_63,
    iv_60d_10c_z_63,
    iv_90d_10p_z_63,
    iv_90d_25p_z_63,
    iv_90d_atm_z_63,
    iv_90d_25c_z_63,
    iv_90d_10c_z_63,
    skew_7d_10p_25p_z_63,
    skew_7d_25p_atm_z_63,
    skew_7d_10p_atm_z_63,
    skew_7d_atm_25c_z_63,
    skew_7d_atm_10c_z_63,
    skew_7d_25p_25c_z_63,
    skew_14d_10p_25p_z_63,
    skew_14d_25p_atm_z_63,
    skew_14d_10p_atm_z_63,
    skew_14d_atm_25c_z_63,
    skew_14d_atm_10c_z_63,
    skew_14d_25p_25c_z_63,
    skew_21d_10p_25p_z_63,
    skew_21d_25p_atm_z_63,
    skew_21d_10p_atm_z_63,
    skew_21d_atm_25c_z_63,
    skew_21d_atm_10c_z_63,
    skew_21d_25p_25c_z_63,
    skew_30d_10p_25p_z_63,
    skew_30d_25p_atm_z_63,
    skew_30d_10p_atm_z_63,
    skew_30d_atm_25c_z_63,
    skew_30d_atm_10c_z_63,
    skew_30d_25p_25c_z_63,
    skew_60d_10p_25p_z_63,
    skew_60d_25p_atm_z_63,
    skew_60d_10p_atm_z_63,
    skew_60d_atm_25c_z_63,
    skew_60d_atm_10c_z_63,
    skew_60d_25p_25c_z_63,
    skew_90d_10p_25p_z_63,
    skew_90d_25p_atm_z_63,
    skew_90d_10p_atm_z_63,
    skew_90d_atm_25c_z_63,
    skew_90d_atm_10c_z_63,
    skew_90d_25p_25c_z_63,
    convex_7d_10p_25p_atm_z_63,
    convex_7d_atm_25c_10c_z_63,
    convex_7d_25p_atm_25c_z_63,
    convex_7d_10p_atm_10c_z_63,
    convex_14d_10p_25p_atm_z_63,
    convex_14d_atm_25c_10c_z_63,
    convex_14d_25p_atm_25c_z_63,
    convex_14d_10p_atm_10c_z_63,
    convex_21d_10p_25p_atm_z_63,
    convex_21d_atm_25c_10c_z_63,
    convex_21d_25p_atm_25c_z_63,
    convex_21d_10p_atm_10c_z_63,
    convex_30d_10p_25p_atm_z_63,
    convex_30d_atm_25c_10c_z_63,
    convex_30d_25p_atm_25c_z_63,
    convex_30d_10p_atm_10c_z_63,
    convex_60d_10p_25p_atm_z_63,
    convex_60d_atm_25c_10c_z_63,
    convex_60d_25p_atm_25c_z_63,
    convex_60d_10p_atm_10c_z_63,
    convex_90d_10p_25p_atm_z_63,
    convex_90d_atm_25c_10c_z_63,
    convex_90d_25p_atm_25c_z_63,
    convex_90d_10p_atm_10c_z_63,
    rr_7d_25_z_63,
    rr_7d_10_z_63,
    rr_14d_25_z_63,
    rr_14d_10_z_63,
    rr_21d_25_z_63,
    rr_21d_10_z_63,
    rr_30d_25_z_63,
    rr_30d_10_z_63,
    rr_60d_25_z_63,
    rr_60d_10_z_63,
    rr_90d_25_z_63,
    rr_90d_10_z_63,
    term_ratio_7d_14d_z_63,
    term_ratio_14d_30d_z_63,
    term_ratio_30d_90d_z_63,
    term_ratio_7d_30d_z_63,
    term_slope_7d_14d_25p_z_63,
    term_slope_7d_14d_atm_z_63,
    term_slope_7d_14d_25c_z_63,
    term_slope_14d_30d_25p_z_63,
    term_slope_14d_30d_atm_z_63,
    term_slope_14d_30d_25c_z_63,
    term_slope_30d_90d_25p_z_63,
    term_slope_30d_90d_atm_z_63,
    term_slope_30d_90d_25c_z_63,
    term_slope_7d_30d_25p_z_63,
    term_slope_7d_30d_atm_z_63,
    term_slope_7d_30d_25c_z_63,
    ratio_price_7d_z_63,
    straddle_price_7d_z_63,
    rr_price_7d_z_63,
    wing_cost_10p_5p_7d_z_63,
    zc_width_sigma_7d_z_63,
    zc_short_delta_7d_z_63,
    cost_at_delta_neutral_7d_z_63,
    ratio_price_14d_z_63,
    straddle_price_14d_z_63,
    rr_price_14d_z_63,
    wing_cost_10p_5p_14d_z_63,
    zc_width_sigma_14d_z_63,
    zc_short_delta_14d_z_63,
    cost_at_delta_neutral_14d_z_63,
    ratio_price_21d_z_63,
    straddle_price_21d_z_63,
    rr_price_21d_z_63,
    wing_cost_10p_5p_21d_z_63,
    zc_width_sigma_21d_z_63,
    zc_short_delta_21d_z_63,
    cost_at_delta_neutral_21d_z_63,
    ratio_price_30d_z_63,
    straddle_price_30d_z_63,
    rr_price_30d_z_63,
    wing_cost_10p_5p_30d_z_63,
    zc_width_sigma_30d_z_63,
    zc_short_delta_30d_z_63,
    cost_at_delta_neutral_30d_z_63,
    ratio_price_60d_z_63,
    straddle_price_60d_z_63,
    rr_price_60d_z_63,
    wing_cost_10p_5p_60d_z_63,
    zc_width_sigma_60d_z_63,
    zc_short_delta_60d_z_63,
    cost_at_delta_neutral_60d_z_63,
    ratio_price_90d_z_63,
    straddle_price_90d_z_63,
    rr_price_90d_z_63,
    wing_cost_10p_5p_90d_z_63,
    zc_width_sigma_90d_z_63,
    zc_short_delta_90d_z_63,
    cost_at_delta_neutral_90d_z_63,
    log_ret_d_z_63,
    log_ret_7d_z_63,
    log_ret_30d_z_63,
    rv_7d_z_63,
    rv_park_7d_z_63,
    rv_gk_7d_z_63,
    rv_30d_z_63,
    rv_park_30d_z_63,
    rv_gk_30d_z_63,
    rv_90d_z_63,
    rv_park_90d_z_63,
    rv_gk_90d_z_63,
    vrp_7d_z_63,
    vrp_ratio_7d_z_63,
    vrp_30d_z_63,
    vrp_ratio_30d_z_63,
    vrp_90d_z_63,
    vrp_ratio_90d_z_63,
    vov_30d_1m_z_63,
    spotvol_beta_30d_1m_z_63,
    spotvol_r2_30d_1m_z_63,
    spotvol_beta_30d_3m_z_63,
    spotvol_r2_30d_3m_z_63,
    downside_semivol_30d_z_63,
    iv_7d_10p_z_252,
    iv_7d_25p_z_252,
    iv_7d_atm_z_252,
    iv_7d_25c_z_252,
    iv_7d_10c_z_252,
    iv_14d_10p_z_252,
    iv_14d_25p_z_252,
    iv_14d_atm_z_252,
    iv_14d_25c_z_252,
    iv_14d_10c_z_252,
    iv_21d_10p_z_252,
    iv_21d_25p_z_252,
    iv_21d_atm_z_252,
    iv_21d_25c_z_252,
    iv_21d_10c_z_252,
    iv_30d_10p_z_252,
    iv_30d_25p_z_252,
    iv_30d_atm_z_252,
    iv_30d_25c_z_252,
    iv_30d_10c_z_252,
    iv_60d_10p_z_252,
    iv_60d_25p_z_252,
    iv_60d_atm_z_252,
    iv_60d_25c_z_252,
    iv_60d_10c_z_252,
    iv_90d_10p_z_252,
    iv_90d_25p_z_252,
    iv_90d_atm_z_252,
    iv_90d_25c_z_252,
    iv_90d_10c_z_252,
    skew_7d_10p_25p_z_252,
    skew_7d_25p_atm_z_252,
    skew_7d_10p_atm_z_252,
    skew_7d_atm_25c_z_252,
    skew_7d_atm_10c_z_252,
    skew_7d_25p_25c_z_252,
    skew_14d_10p_25p_z_252,
    skew_14d_25p_atm_z_252,
    skew_14d_10p_atm_z_252,
    skew_14d_atm_25c_z_252,
    skew_14d_atm_10c_z_252,
    skew_14d_25p_25c_z_252,
    skew_21d_10p_25p_z_252,
    skew_21d_25p_atm_z_252,
    skew_21d_10p_atm_z_252,
    skew_21d_atm_25c_z_252,
    skew_21d_atm_10c_z_252,
    skew_21d_25p_25c_z_252,
    skew_30d_10p_25p_z_252,
    skew_30d_25p_atm_z_252,
    skew_30d_10p_atm_z_252,
    skew_30d_atm_25c_z_252,
    skew_30d_atm_10c_z_252,
    skew_30d_25p_25c_z_252,
    skew_60d_10p_25p_z_252,
    skew_60d_25p_atm_z_252,
    skew_60d_10p_atm_z_252,
    skew_60d_atm_25c_z_252,
    skew_60d_atm_10c_z_252,
    skew_60d_25p_25c_z_252,
    skew_90d_10p_25p_z_252,
    skew_90d_25p_atm_z_252,
    skew_90d_10p_atm_z_252,
    skew_90d_atm_25c_z_252,
    skew_90d_atm_10c_z_252,
    skew_90d_25p_25c_z_252,
    convex_7d_10p_25p_atm_z_252,
    convex_7d_atm_25c_10c_z_252,
    convex_7d_25p_atm_25c_z_252,
    convex_7d_10p_atm_10c_z_252,
    convex_14d_10p_25p_atm_z_252,
    convex_14d_atm_25c_10c_z_252,
    convex_14d_25p_atm_25c_z_252,
    convex_14d_10p_atm_10c_z_252,
    convex_21d_10p_25p_atm_z_252,
    convex_21d_atm_25c_10c_z_252,
    convex_21d_25p_atm_25c_z_252,
    convex_21d_10p_atm_10c_z_252,
    convex_30d_10p_25p_atm_z_252,
    convex_30d_atm_25c_10c_z_252,
    convex_30d_25p_atm_25c_z_252,
    convex_30d_10p_atm_10c_z_252,
    convex_60d_10p_25p_atm_z_252,
    convex_60d_atm_25c_10c_z_252,
    convex_60d_25p_atm_25c_z_252,
    convex_60d_10p_atm_10c_z_252,
    convex_90d_10p_25p_atm_z_252,
    convex_90d_atm_25c_10c_z_252,
    convex_90d_25p_atm_25c_z_252,
    convex_90d_10p_atm_10c_z_252,
    rr_7d_25_z_252,
    rr_7d_10_z_252,
    rr_14d_25_z_252,
    rr_14d_10_z_252,
    rr_21d_25_z_252,
    rr_21d_10_z_252,
    rr_30d_25_z_252,
    rr_30d_10_z_252,
    rr_60d_25_z_252,
    rr_60d_10_z_252,
    rr_90d_25_z_252,
    rr_90d_10_z_252,
    term_ratio_7d_14d_z_252,
    term_ratio_14d_30d_z_252,
    term_ratio_30d_90d_z_252,
    term_ratio_7d_30d_z_252,
    term_slope_7d_14d_25p_z_252,
    term_slope_7d_14d_atm_z_252,
    term_slope_7d_14d_25c_z_252,
    term_slope_14d_30d_25p_z_252,
    term_slope_14d_30d_atm_z_252,
    term_slope_14d_30d_25c_z_252,
    term_slope_30d_90d_25p_z_252,
    term_slope_30d_90d_atm_z_252,
    term_slope_30d_90d_25c_z_252,
    term_slope_7d_30d_25p_z_252,
    term_slope_7d_30d_atm_z_252,
    term_slope_7d_30d_25c_z_252,
    ratio_price_7d_z_252,
    straddle_price_7d_z_252,
    rr_price_7d_z_252,
    wing_cost_10p_5p_7d_z_252,
    zc_width_sigma_7d_z_252,
    zc_short_delta_7d_z_252,
    cost_at_delta_neutral_7d_z_252,
    ratio_price_14d_z_252,
    straddle_price_14d_z_252,
    rr_price_14d_z_252,
    wing_cost_10p_5p_14d_z_252,
    zc_width_sigma_14d_z_252,
    zc_short_delta_14d_z_252,
    cost_at_delta_neutral_14d_z_252,
    ratio_price_21d_z_252,
    straddle_price_21d_z_252,
    rr_price_21d_z_252,
    wing_cost_10p_5p_21d_z_252,
    zc_width_sigma_21d_z_252,
    zc_short_delta_21d_z_252,
    cost_at_delta_neutral_21d_z_252,
    ratio_price_30d_z_252,
    straddle_price_30d_z_252,
    rr_price_30d_z_252,
    wing_cost_10p_5p_30d_z_252,
    zc_width_sigma_30d_z_252,
    zc_short_delta_30d_z_252,
    cost_at_delta_neutral_30d_z_252,
    ratio_price_60d_z_252,
    straddle_price_60d_z_252,
    rr_price_60d_z_252,
    wing_cost_10p_5p_60d_z_252,
    zc_width_sigma_60d_z_252,
    zc_short_delta_60d_z_252,
    cost_at_delta_neutral_60d_z_252,
    ratio_price_90d_z_252,
    straddle_price_90d_z_252,
    rr_price_90d_z_252,
    wing_cost_10p_5p_90d_z_252,
    zc_width_sigma_90d_z_252,
    zc_short_delta_90d_z_252,
    cost_at_delta_neutral_90d_z_252,
    log_ret_d_z_252,
    log_ret_7d_z_252,
    log_ret_30d_z_252,
    rv_7d_z_252,
    rv_park_7d_z_252,
    rv_gk_7d_z_252,
    rv_30d_z_252,
    rv_park_30d_z_252,
    rv_gk_30d_z_252,
    rv_90d_z_252,
    rv_park_90d_z_252,
    rv_gk_90d_z_252,
    vrp_7d_z_252,
    vrp_ratio_7d_z_252,
    vrp_30d_z_252,
    vrp_ratio_30d_z_252,
    vrp_90d_z_252,
    vrp_ratio_90d_z_252,
    vov_30d_1m_z_252,
    spotvol_beta_30d_1m_z_252,
    spotvol_r2_30d_1m_z_252,
    spotvol_beta_30d_3m_z_252,
    spotvol_r2_30d_3m_z_252,
    downside_semivol_30d_z_252,
    long_sigma_7d_z_63,
    long_sigma_14d_z_63,
    long_sigma_21d_z_63,
    long_sigma_30d_z_63,
    long_sigma_60d_z_63,
    long_sigma_90d_z_63,
    long_sigma_7d_z_252,
    long_sigma_14d_z_252,
    long_sigma_21d_z_252,
    long_sigma_30d_z_252,
    long_sigma_60d_z_252,
    long_sigma_90d_z_252,
    rv_14d_z_63,
    rv_park_14d_z_63,
    rv_gk_14d_z_63,
    rv_21d_z_63,
    rv_park_21d_z_63,
    rv_gk_21d_z_63,
    rv_60d_z_63,
    rv_park_60d_z_63,
    rv_gk_60d_z_63,
    vrp_14d_z_63,
    vrp_ratio_14d_z_63,
    vrp_21d_z_63,
    vrp_ratio_21d_z_63,
    vrp_60d_z_63,
    vrp_ratio_60d_z_63,
    rv_14d_z_252,
    rv_park_14d_z_252,
    rv_gk_14d_z_252,
    rv_21d_z_252,
    rv_park_21d_z_252,
    rv_gk_21d_z_252,
    rv_60d_z_252,
    rv_park_60d_z_252,
    rv_gk_60d_z_252,
    vrp_14d_z_252,
    vrp_ratio_14d_z_252,
    vrp_21d_z_252,
    vrp_ratio_21d_z_252,
    vrp_60d_z_252,
    vrp_ratio_60d_z_252,
    log_ret_14d_z_63,
    log_ret_21d_z_63,
    log_ret_60d_z_63,
    log_ret_90d_z_63,
    vov_7d_1m_z_63,
    vov_14d_1m_z_63,
    vov_21d_1m_z_63,
    vov_60d_1m_z_63,
    vov_90d_1m_z_63,
    spotvol_beta_7d_1m_z_63,
    spotvol_r2_7d_1m_z_63,
    spotvol_beta_7d_3m_z_63,
    spotvol_r2_7d_3m_z_63,
    spotvol_beta_14d_1m_z_63,
    spotvol_r2_14d_1m_z_63,
    spotvol_beta_14d_3m_z_63,
    spotvol_r2_14d_3m_z_63,
    spotvol_beta_21d_1m_z_63,
    spotvol_r2_21d_1m_z_63,
    spotvol_beta_21d_3m_z_63,
    spotvol_r2_21d_3m_z_63,
    spotvol_beta_60d_1m_z_63,
    spotvol_r2_60d_1m_z_63,
    spotvol_beta_60d_3m_z_63,
    spotvol_r2_60d_3m_z_63,
    spotvol_beta_90d_1m_z_63,
    spotvol_r2_90d_1m_z_63,
    spotvol_beta_90d_3m_z_63,
    spotvol_r2_90d_3m_z_63,
    downside_semivol_7d_z_63,
    downside_semivol_14d_z_63,
    downside_semivol_21d_z_63,
    downside_semivol_60d_z_63,
    downside_semivol_90d_z_63,
    log_ret_14d_z_252,
    log_ret_21d_z_252,
    log_ret_60d_z_252,
    log_ret_90d_z_252,
    vov_7d_1m_z_252,
    vov_14d_1m_z_252,
    vov_21d_1m_z_252,
    vov_60d_1m_z_252,
    vov_90d_1m_z_252,
    spotvol_beta_7d_1m_z_252,
    spotvol_r2_7d_1m_z_252,
    spotvol_beta_7d_3m_z_252,
    spotvol_r2_7d_3m_z_252,
    spotvol_beta_14d_1m_z_252,
    spotvol_r2_14d_1m_z_252,
    spotvol_beta_14d_3m_z_252,
    spotvol_r2_14d_3m_z_252,
    spotvol_beta_21d_1m_z_252,
    spotvol_r2_21d_1m_z_252,
    spotvol_beta_21d_3m_z_252,
    spotvol_r2_21d_3m_z_252,
    spotvol_beta_60d_1m_z_252,
    spotvol_r2_60d_1m_z_252,
    spotvol_beta_60d_3m_z_252,
    spotvol_r2_60d_3m_z_252,
    spotvol_beta_90d_1m_z_252,
    spotvol_r2_90d_1m_z_252,
    spotvol_beta_90d_3m_z_252,
    spotvol_r2_90d_3m_z_252,
    downside_semivol_7d_z_252,
    downside_semivol_14d_z_252,
    downside_semivol_21d_z_252,
    downside_semivol_60d_z_252,
    downside_semivol_90d_z_252,
    rv_7d_z_63 AS rv_1w_z_63,
    rv_7d_z_252 AS rv_1w_z_252,
    rv_park_7d_z_63 AS rv_park_1w_z_63,
    rv_park_7d_z_252 AS rv_park_1w_z_252,
    rv_gk_7d_z_63 AS rv_gk_1w_z_63,
    rv_gk_7d_z_252 AS rv_gk_1w_z_252,
    vrp_7d_z_63 AS vrp_1w_z_63,
    vrp_7d_z_252 AS vrp_1w_z_252,
    vrp_ratio_7d_z_63 AS vrp_ratio_1w_z_63,
    vrp_ratio_7d_z_252 AS vrp_ratio_1w_z_252,
    rv_30d_z_63 AS rv_1m_z_63,
    rv_30d_z_252 AS rv_1m_z_252,
    rv_park_30d_z_63 AS rv_park_1m_z_63,
    rv_park_30d_z_252 AS rv_park_1m_z_252,
    rv_gk_30d_z_63 AS rv_gk_1m_z_63,
    rv_gk_30d_z_252 AS rv_gk_1m_z_252,
    vrp_30d_z_63 AS vrp_1m_z_63,
    vrp_30d_z_252 AS vrp_1m_z_252,
    vrp_ratio_30d_z_63 AS vrp_ratio_1m_z_63,
    vrp_ratio_30d_z_252 AS vrp_ratio_1m_z_252,
    rv_90d_z_63 AS rv_3m_z_63,
    rv_90d_z_252 AS rv_3m_z_252,
    rv_park_90d_z_63 AS rv_park_3m_z_63,
    rv_park_90d_z_252 AS rv_park_3m_z_252,
    rv_gk_90d_z_63 AS rv_gk_3m_z_63,
    rv_gk_90d_z_252 AS rv_gk_3m_z_252,
    vrp_90d_z_63 AS vrp_3m_z_63,
    vrp_90d_z_252 AS vrp_3m_z_252,
    vrp_ratio_90d_z_63 AS vrp_ratio_3m_z_63,
    vrp_ratio_90d_z_252 AS vrp_ratio_3m_z_252,
    log_ret_7d_z_63 AS log_ret_1w_z_63,
    log_ret_7d_z_252 AS log_ret_1w_z_252,
    log_ret_30d_z_63 AS log_ret_1m_z_63,
    log_ret_30d_z_252 AS log_ret_1m_z_252,
    downside_semivol_30d_z_63 AS downside_semivol_1m_z_63,
    downside_semivol_30d_z_252 AS downside_semivol_1m_z_252,
    spotvol_beta_30d_1m_z_63 AS spotvol_beta_1m_z_63,
    spotvol_beta_30d_1m_z_252 AS spotvol_beta_1m_z_252,
    spotvol_r2_30d_1m_z_63 AS spotvol_r2_1m_z_63,
    spotvol_r2_30d_1m_z_252 AS spotvol_r2_1m_z_252,
    spotvol_beta_30d_3m_z_63 AS spotvol_beta_3m_z_63,
    spotvol_beta_30d_3m_z_252 AS spotvol_beta_3m_z_252,
    spotvol_r2_30d_3m_z_63 AS spotvol_r2_3m_z_63,
    spotvol_r2_30d_3m_z_252 AS spotvol_r2_3m_z_252
   FROM public.equity_metrics_z z;


ALTER VIEW public.equity_metrics_z_compat OWNER TO portfolio;

--
-- Name: VIEW equity_metrics_z_compat; Type: COMMENT; Schema: public; Owner: portfolio
--

COMMENT ON VIEW public.equity_metrics_z_compat IS 'Deprecated-name shim for the rv/vrp tenor rename, z variants. See equity_metrics_compat.';


--
-- Name: equity_structure_presets; Type: TABLE; Schema: public; Owner: portfolio
--

CREATE TABLE public.equity_structure_presets (
    id integer NOT NULL,
    name text NOT NULL,
    tenor integer NOT NULL,
    payload jsonb NOT NULL,
    note text,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.equity_structure_presets OWNER TO portfolio;

--
-- Name: equity_structure_presets_id_seq; Type: SEQUENCE; Schema: public; Owner: portfolio
--

CREATE SEQUENCE public.equity_structure_presets_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.equity_structure_presets_id_seq OWNER TO portfolio;

--
-- Name: equity_structure_presets_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: portfolio
--

ALTER SEQUENCE public.equity_structure_presets_id_seq OWNED BY public.equity_structure_presets.id;


--
-- Name: equity_surface; Type: TABLE; Schema: public; Owner: portfolio
--

CREATE TABLE public.equity_surface (
    ticker text NOT NULL,
    trade_date date NOT NULL,
    snapshot text NOT NULL,
    dte smallint NOT NULL,
    put_delta smallint NOT NULL,
    iv double precision NOT NULL,
    strike double precision,
    forward double precision,
    log_moneyness double precision,
    price double precision,
    theta double precision,
    vega double precision,
    gamma double precision,
    dte_actual double precision,
    extrapolated boolean DEFAULT false NOT NULL,
    captured_at timestamp without time zone,
    source text,
    call_price double precision
)
PARTITION BY RANGE (trade_date);


ALTER TABLE public.equity_surface OWNER TO portfolio;

--
-- Name: equity_surface_diagnostics; Type: TABLE; Schema: public; Owner: portfolio
--

CREATE TABLE public.equity_surface_diagnostics (
    ticker text NOT NULL,
    trade_date date NOT NULL,
    snapshot text NOT NULL,
    expiry date NOT NULL,
    dte_actual double precision,
    forward_price double precision,
    risk_free_rate double precision,
    forward_method text,
    n_strikes_raw integer,
    n_strikes_clean integer,
    k_min double precision,
    k_max double precision,
    spline_rmse double precision,
    calendar_arb_flag boolean DEFAULT false NOT NULL,
    butterfly_arb_flag boolean DEFAULT false NOT NULL,
    skipped boolean DEFAULT false NOT NULL,
    skip_reason text,
    domain_reach double precision,
    excluded_from_bracketing boolean DEFAULT false NOT NULL,
    r_solved_raw double precision
);


ALTER TABLE public.equity_surface_diagnostics OWNER TO portfolio;

--
-- Name: global_bins_cache; Type: TABLE; Schema: public; Owner: portfolio
--

CREATE TABLE public.global_bins_cache (
    cache_key text NOT NULL,
    outcome text NOT NULL,
    ticker text NOT NULL,
    n_bins integer NOT NULL,
    mode text NOT NULL,
    payload jsonb NOT NULL,
    cached_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.global_bins_cache OWNER TO portfolio;

--
-- Name: ic_batch_cache; Type: TABLE; Schema: public; Owner: portfolio
--

CREATE TABLE public.ic_batch_cache (
    cache_key text NOT NULL,
    ticker text NOT NULL,
    outcome text NOT NULL,
    window_size integer NOT NULL,
    cutoff_date date,
    payload jsonb NOT NULL,
    cached_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.ic_batch_cache OWNER TO portfolio;

--
-- Name: is_bins; Type: TABLE; Schema: public; Owner: portfolio
--

CREATE TABLE public.is_bins (
    ticker text NOT NULL,
    trade_date date NOT NULL,
    frac_atm_iv_30d double precision,
    bin20_atm_iv_30d smallint DEFAULT 0 NOT NULL,
    frac_atm_iv_7d double precision,
    bin20_atm_iv_7d smallint DEFAULT 0 NOT NULL,
    frac_atm_iv_90d double precision,
    bin20_atm_iv_90d smallint DEFAULT 0 NOT NULL,
    frac_atr_normalized_ret_5d double precision,
    bin20_atr_normalized_ret_5d smallint DEFAULT 0 NOT NULL,
    frac_call_oi double precision,
    bin20_call_oi smallint DEFAULT 0 NOT NULL,
    frac_cum_signed_vol_20d double precision,
    bin20_cum_signed_vol_20d smallint DEFAULT 0 NOT NULL,
    frac_d1_atm_iv_30d_change double precision,
    bin20_d1_atm_iv_30d_change smallint DEFAULT 0 NOT NULL,
    frac_d1_atm_iv_7d_change double precision,
    bin20_d1_atm_iv_7d_change smallint DEFAULT 0 NOT NULL,
    frac_d1_d5_ratio_total_oi_pct_change double precision,
    bin20_d1_d5_ratio_total_oi_pct_change smallint DEFAULT 0 NOT NULL,
    frac_d1_oi_weighted_all_div_spot_change_co double precision,
    bin20_d1_oi_weighted_all_div_spot_change_co smallint DEFAULT 0 NOT NULL,
    frac_d1_oi_weighted_all_div_spot_change_pc double precision,
    bin20_d1_oi_weighted_all_div_spot_change_pc smallint DEFAULT 0 NOT NULL,
    frac_d1_put_call_oi_ratio_change double precision,
    bin20_d1_put_call_oi_ratio_change smallint DEFAULT 0 NOT NULL,
    frac_d1_total_oi_change double precision,
    bin20_d1_total_oi_change smallint DEFAULT 0 NOT NULL,
    frac_d1_total_oi_pct_change double precision,
    bin20_d1_total_oi_pct_change smallint DEFAULT 0 NOT NULL,
    frac_d20_total_oi_change double precision,
    bin20_d20_total_oi_change smallint DEFAULT 0 NOT NULL,
    frac_d5_atm_iv_30d_change double precision,
    bin20_d5_atm_iv_30d_change smallint DEFAULT 0 NOT NULL,
    frac_d5_atm_iv_7d_change double precision,
    bin20_d5_atm_iv_7d_change smallint DEFAULT 0 NOT NULL,
    frac_d5_oi_weighted_all_div_spot_change_co double precision,
    bin20_d5_oi_weighted_all_div_spot_change_co smallint DEFAULT 0 NOT NULL,
    frac_d5_oi_weighted_all_div_spot_change_pc double precision,
    bin20_d5_oi_weighted_all_div_spot_change_pc smallint DEFAULT 0 NOT NULL,
    frac_d5_put_call_oi_ratio_change double precision,
    bin20_d5_put_call_oi_ratio_change smallint DEFAULT 0 NOT NULL,
    frac_d5_total_oi_change double precision,
    bin20_d5_total_oi_change smallint DEFAULT 0 NOT NULL,
    frac_d5_total_oi_pct_change double precision,
    bin20_d5_total_oi_pct_change smallint DEFAULT 0 NOT NULL,
    frac_donchian_pos_20d double precision,
    bin20_donchian_pos_20d smallint DEFAULT 0 NOT NULL,
    frac_iv_rv_ratio_30d double precision,
    bin20_iv_rv_ratio_30d smallint DEFAULT 0 NOT NULL,
    frac_ma20_slope_5d double precision,
    bin20_ma20_slope_5d smallint DEFAULT 0 NOT NULL,
    frac_max_oi_strike_call double precision,
    bin20_max_oi_strike_call smallint DEFAULT 0 NOT NULL,
    frac_max_oi_strike_put double precision,
    bin20_max_oi_strike_put smallint DEFAULT 0 NOT NULL,
    frac_net_new_oi_div_vol double precision,
    bin20_net_new_oi_div_vol smallint DEFAULT 0 NOT NULL,
    frac_oi_above_below_ratio_co double precision,
    bin20_oi_above_below_ratio_co smallint DEFAULT 0 NOT NULL,
    frac_oi_above_below_ratio_pc double precision,
    bin20_oi_above_below_ratio_pc smallint DEFAULT 0 NOT NULL,
    frac_oi_above_spot_co double precision,
    bin20_oi_above_spot_co smallint DEFAULT 0 NOT NULL,
    frac_oi_above_spot_pc double precision,
    bin20_oi_above_spot_pc smallint DEFAULT 0 NOT NULL,
    frac_oi_below_spot_co double precision,
    bin20_oi_below_spot_co smallint DEFAULT 0 NOT NULL,
    frac_oi_below_spot_pc double precision,
    bin20_oi_below_spot_pc smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_all double precision,
    bin20_oi_weighted_all smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_all_0_30d double precision,
    bin20_oi_weighted_all_0_30d smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_all_0_30d_div_spot_co double precision,
    bin20_oi_weighted_all_0_30d_div_spot_co smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_all_0_30d_div_spot_pc double precision,
    bin20_oi_weighted_all_0_30d_div_spot_pc smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_all_31_90d double precision,
    bin20_oi_weighted_all_31_90d smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_all_31_90d_div_spot_co double precision,
    bin20_oi_weighted_all_31_90d_div_spot_co smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_all_31_90d_div_spot_pc double precision,
    bin20_oi_weighted_all_31_90d_div_spot_pc smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_all_div_spot_co double precision,
    bin20_oi_weighted_all_div_spot_co smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_all_div_spot_pc double precision,
    bin20_oi_weighted_all_div_spot_pc smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_all_minus_spot_co double precision,
    bin20_oi_weighted_all_minus_spot_co smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_all_minus_spot_pc double precision,
    bin20_oi_weighted_all_minus_spot_pc smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_call double precision,
    bin20_oi_weighted_call smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_call_0_30d double precision,
    bin20_oi_weighted_call_0_30d smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_call_0_30d_div_spot_co double precision,
    bin20_oi_weighted_call_0_30d_div_spot_co smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_call_0_30d_div_spot_pc double precision,
    bin20_oi_weighted_call_0_30d_div_spot_pc smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_call_31_90d double precision,
    bin20_oi_weighted_call_31_90d smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_call_31_90d_div_spot_co double precision,
    bin20_oi_weighted_call_31_90d_div_spot_co smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_call_31_90d_div_spot_pc double precision,
    bin20_oi_weighted_call_31_90d_div_spot_pc smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_call_div_spot_co double precision,
    bin20_oi_weighted_call_div_spot_co smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_call_div_spot_pc double precision,
    bin20_oi_weighted_call_div_spot_pc smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_call_minus_spot_co double precision,
    bin20_oi_weighted_call_minus_spot_co smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_call_minus_spot_pc double precision,
    bin20_oi_weighted_call_minus_spot_pc smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_next_monthly_div_spot_co double precision,
    bin20_oi_weighted_next_monthly_div_spot_co smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_next_monthly_div_spot_pc double precision,
    bin20_oi_weighted_next_monthly_div_spot_pc smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_put double precision,
    bin20_oi_weighted_put smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_put_0_30d double precision,
    bin20_oi_weighted_put_0_30d smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_put_0_30d_div_spot_co double precision,
    bin20_oi_weighted_put_0_30d_div_spot_co smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_put_0_30d_div_spot_pc double precision,
    bin20_oi_weighted_put_0_30d_div_spot_pc smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_put_31_90d double precision,
    bin20_oi_weighted_put_31_90d smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_put_31_90d_div_spot_co double precision,
    bin20_oi_weighted_put_31_90d_div_spot_co smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_put_31_90d_div_spot_pc double precision,
    bin20_oi_weighted_put_31_90d_div_spot_pc smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_put_div_spot_co double precision,
    bin20_oi_weighted_put_div_spot_co smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_put_div_spot_pc double precision,
    bin20_oi_weighted_put_div_spot_pc smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_put_minus_spot_co double precision,
    bin20_oi_weighted_put_minus_spot_co smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_put_minus_spot_pc double precision,
    bin20_oi_weighted_put_minus_spot_pc smallint DEFAULT 0 NOT NULL,
    frac_oi_within_10pct_co double precision,
    bin20_oi_within_10pct_co smallint DEFAULT 0 NOT NULL,
    frac_oi_within_10pct_pc double precision,
    bin20_oi_within_10pct_pc smallint DEFAULT 0 NOT NULL,
    frac_oi_within_5pct_co double precision,
    bin20_oi_within_5pct_co smallint DEFAULT 0 NOT NULL,
    frac_oi_within_5pct_pc double precision,
    bin20_oi_within_5pct_pc smallint DEFAULT 0 NOT NULL,
    frac_pct_from_52w_high double precision,
    bin20_pct_from_52w_high smallint DEFAULT 0 NOT NULL,
    frac_pct_from_52w_low double precision,
    bin20_pct_from_52w_low smallint DEFAULT 0 NOT NULL,
    frac_pct_from_ma20 double precision,
    bin20_pct_from_ma20 smallint DEFAULT 0 NOT NULL,
    frac_pct_from_ma50 double precision,
    bin20_pct_from_ma50 smallint DEFAULT 0 NOT NULL,
    frac_pct_oi_0_30d double precision,
    bin20_pct_oi_0_30d smallint DEFAULT 0 NOT NULL,
    frac_pct_oi_31_90d double precision,
    bin20_pct_oi_31_90d smallint DEFAULT 0 NOT NULL,
    frac_pct_oi_91_365d double precision,
    bin20_pct_oi_91_365d smallint DEFAULT 0 NOT NULL,
    frac_pct_oi_above_spot_co double precision,
    bin20_pct_oi_above_spot_co smallint DEFAULT 0 NOT NULL,
    frac_pct_oi_above_spot_pc double precision,
    bin20_pct_oi_above_spot_pc smallint DEFAULT 0 NOT NULL,
    frac_pct_oi_below_spot_co double precision,
    bin20_pct_oi_below_spot_co smallint DEFAULT 0 NOT NULL,
    frac_pct_oi_below_spot_pc double precision,
    bin20_pct_oi_below_spot_pc smallint DEFAULT 0 NOT NULL,
    frac_pct_oi_in_front_expiry double precision,
    bin20_pct_oi_in_front_expiry smallint DEFAULT 0 NOT NULL,
    frac_pct_oi_next_monthly double precision,
    bin20_pct_oi_next_monthly smallint DEFAULT 0 NOT NULL,
    frac_pct_oi_within_10pct_co double precision,
    bin20_pct_oi_within_10pct_co smallint DEFAULT 0 NOT NULL,
    frac_pct_oi_within_10pct_pc double precision,
    bin20_pct_oi_within_10pct_pc smallint DEFAULT 0 NOT NULL,
    frac_pct_oi_within_5pct_co double precision,
    bin20_pct_oi_within_5pct_co smallint DEFAULT 0 NOT NULL,
    frac_pct_oi_within_5pct_pc double precision,
    bin20_pct_oi_within_5pct_pc smallint DEFAULT 0 NOT NULL,
    frac_pct_up_days_20d double precision,
    bin20_pct_up_days_20d smallint DEFAULT 0 NOT NULL,
    frac_pct_vol_0_30d double precision,
    bin20_pct_vol_0_30d smallint DEFAULT 0 NOT NULL,
    frac_pct_vol_31_90d double precision,
    bin20_pct_vol_31_90d smallint DEFAULT 0 NOT NULL,
    frac_pct_vol_within_10pct_pc double precision,
    bin20_pct_vol_within_10pct_pc smallint DEFAULT 0 NOT NULL,
    frac_pct_vol_within_5pct_pc double precision,
    bin20_pct_vol_within_5pct_pc smallint DEFAULT 0 NOT NULL,
    frac_put_call_oi_ratio double precision,
    bin20_put_call_oi_ratio smallint DEFAULT 0 NOT NULL,
    frac_put_call_ratio_vol double precision,
    bin20_put_call_ratio_vol smallint DEFAULT 0 NOT NULL,
    frac_put_oi double precision,
    bin20_put_oi smallint DEFAULT 0 NOT NULL,
    frac_relative_strength_vs_spy_20d double precision,
    bin20_relative_strength_vs_spy_20d smallint DEFAULT 0 NOT NULL,
    frac_ret_10d double precision,
    bin20_ret_10d smallint DEFAULT 0 NOT NULL,
    frac_ret_20d double precision,
    bin20_ret_20d smallint DEFAULT 0 NOT NULL,
    frac_ret_5d double precision,
    bin20_ret_5d smallint DEFAULT 0 NOT NULL,
    frac_rv_20d double precision,
    bin20_rv_20d smallint DEFAULT 0 NOT NULL,
    frac_rv_5d double precision,
    bin20_rv_5d smallint DEFAULT 0 NOT NULL,
    frac_rv_ratio_5d_20d double precision,
    bin20_rv_ratio_5d_20d smallint DEFAULT 0 NOT NULL,
    frac_spot_co double precision,
    bin20_spot_co smallint DEFAULT 0 NOT NULL,
    frac_spot_pc double precision,
    bin20_spot_pc smallint DEFAULT 0 NOT NULL,
    frac_term_30d_90d double precision,
    bin20_term_30d_90d smallint DEFAULT 0 NOT NULL,
    frac_term_7d_30d double precision,
    bin20_term_7d_30d smallint DEFAULT 0 NOT NULL,
    frac_top10_strikes_pct_total_oi double precision,
    bin20_top10_strikes_pct_total_oi smallint DEFAULT 0 NOT NULL,
    frac_top5_strikes_pct_total_oi double precision,
    bin20_top5_strikes_pct_total_oi smallint DEFAULT 0 NOT NULL,
    frac_total_oi double precision,
    bin20_total_oi smallint DEFAULT 0 NOT NULL,
    frac_vol_above_below_ratio_pc double precision,
    bin20_vol_above_below_ratio_pc smallint DEFAULT 0 NOT NULL,
    frac_vol_oi_ratio_all double precision,
    bin20_vol_oi_ratio_all smallint DEFAULT 0 NOT NULL,
    frac_vol_oi_ratio_call double precision,
    bin20_vol_oi_ratio_call smallint DEFAULT 0 NOT NULL,
    frac_vol_oi_ratio_put double precision,
    bin20_vol_oi_ratio_put smallint DEFAULT 0 NOT NULL,
    frac_vol_weighted_all_div_spot_pc double precision,
    bin20_vol_weighted_all_div_spot_pc smallint DEFAULT 0 NOT NULL,
    frac_vol_weighted_call_div_spot_pc double precision,
    bin20_vol_weighted_call_div_spot_pc smallint DEFAULT 0 NOT NULL,
    frac_vol_weighted_put_div_spot_pc double precision,
    bin20_vol_weighted_put_div_spot_pc smallint DEFAULT 0 NOT NULL,
    frac_vrp_30d double precision,
    bin20_vrp_30d smallint DEFAULT 0 NOT NULL,
    frac_weighted_avg_dte double precision,
    bin20_weighted_avg_dte smallint DEFAULT 0 NOT NULL,
    frac_weighted_avg_dte_vol double precision,
    bin20_weighted_avg_dte_vol smallint DEFAULT 0 NOT NULL,
    frac_zscore_d1_oi_change_3m double precision,
    bin20_zscore_d1_oi_change_3m smallint DEFAULT 0 NOT NULL,
    frac_zscore_d5_oi_change_3m double precision,
    bin20_zscore_d5_oi_change_3m smallint DEFAULT 0 NOT NULL,
    frac_zscore_iv_30d double precision,
    bin20_zscore_iv_30d smallint DEFAULT 0 NOT NULL,
    frac_zscore_iv_7d double precision,
    bin20_zscore_iv_7d smallint DEFAULT 0 NOT NULL,
    frac_zscore_iv_90d double precision,
    bin20_zscore_iv_90d smallint DEFAULT 0 NOT NULL,
    frac_zscore_iv_rv_ratio_30d double precision,
    bin20_zscore_iv_rv_ratio_30d smallint DEFAULT 0 NOT NULL,
    frac_zscore_oi_above_below_ratio_3m_co double precision,
    bin20_zscore_oi_above_below_ratio_3m_co smallint DEFAULT 0 NOT NULL,
    frac_zscore_oi_above_below_ratio_3m_pc double precision,
    bin20_zscore_oi_above_below_ratio_3m_pc smallint DEFAULT 0 NOT NULL,
    frac_zscore_oi_weighted_all_div_spot_3m_co double precision,
    bin20_zscore_oi_weighted_all_div_spot_3m_co smallint DEFAULT 0 NOT NULL,
    frac_zscore_oi_weighted_all_div_spot_3m_pc double precision,
    bin20_zscore_oi_weighted_all_div_spot_3m_pc smallint DEFAULT 0 NOT NULL,
    frac_zscore_price_vs_ma20 double precision,
    bin20_zscore_price_vs_ma20 smallint DEFAULT 0 NOT NULL,
    frac_zscore_price_vs_ma50 double precision,
    bin20_zscore_price_vs_ma50 smallint DEFAULT 0 NOT NULL,
    frac_zscore_put_call_oi_ratio_3m double precision,
    bin20_zscore_put_call_oi_ratio_3m smallint DEFAULT 0 NOT NULL,
    frac_zscore_put_call_ratio_vol double precision,
    bin20_zscore_put_call_ratio_vol smallint DEFAULT 0 NOT NULL,
    frac_zscore_term_30d_90d double precision,
    bin20_zscore_term_30d_90d smallint DEFAULT 0 NOT NULL,
    frac_zscore_term_7d_30d double precision,
    bin20_zscore_term_7d_30d smallint DEFAULT 0 NOT NULL,
    frac_zscore_underlying_vol_20d double precision,
    bin20_zscore_underlying_vol_20d smallint DEFAULT 0 NOT NULL,
    frac_zscore_vol_above_below_ratio_pc double precision,
    bin20_zscore_vol_above_below_ratio_pc smallint DEFAULT 0 NOT NULL,
    frac_zscore_vol_oi_ratio_all double precision,
    bin20_zscore_vol_oi_ratio_all smallint DEFAULT 0 NOT NULL,
    frac_zscore_vol_oi_ratio_call double precision,
    bin20_zscore_vol_oi_ratio_call smallint DEFAULT 0 NOT NULL,
    frac_zscore_vol_oi_ratio_put double precision,
    bin20_zscore_vol_oi_ratio_put smallint DEFAULT 0 NOT NULL,
    frac_zscore_vrp_30d double precision,
    bin20_zscore_vrp_30d smallint DEFAULT 0 NOT NULL,
    frac_bf_25d_30d double precision,
    bin20_bf_25d_30d smallint DEFAULT 0 NOT NULL,
    frac_bf_25d_7d double precision,
    bin20_bf_25d_7d smallint DEFAULT 0 NOT NULL,
    frac_iv_25d_call_30d double precision,
    bin20_iv_25d_call_30d smallint DEFAULT 0 NOT NULL,
    frac_iv_25d_call_7d double precision,
    bin20_iv_25d_call_7d smallint DEFAULT 0 NOT NULL,
    frac_iv_25d_put_30d double precision,
    bin20_iv_25d_put_30d smallint DEFAULT 0 NOT NULL,
    frac_iv_25d_put_7d double precision,
    bin20_iv_25d_put_7d smallint DEFAULT 0 NOT NULL,
    frac_rr_25d_30d double precision,
    bin20_rr_25d_30d smallint DEFAULT 0 NOT NULL,
    frac_rr_25d_7d double precision,
    bin20_rr_25d_7d smallint DEFAULT 0 NOT NULL,
    frac_skew_25p_atm_30d double precision,
    bin20_skew_25p_atm_30d smallint DEFAULT 0 NOT NULL,
    frac_skew_25p_atm_7d double precision,
    bin20_skew_25p_atm_7d smallint DEFAULT 0 NOT NULL,
    frac_skew_atm_25c_30d double precision,
    bin20_skew_atm_25c_30d smallint DEFAULT 0 NOT NULL,
    frac_skew_atm_25c_7d double precision,
    bin20_skew_atm_25c_7d smallint DEFAULT 0 NOT NULL,
    frac_zscore_rr_25d_30d double precision,
    bin20_zscore_rr_25d_30d smallint DEFAULT 0 NOT NULL,
    frac_zscore_rr_25d_7d double precision,
    bin20_zscore_rr_25d_7d smallint DEFAULT 0 NOT NULL
);


ALTER TABLE public.is_bins OWNER TO portfolio;

--
-- Name: metric_classification; Type: TABLE; Schema: public; Owner: portfolio
--

CREATE TABLE public.metric_classification (
    metric text NOT NULL,
    family_num integer NOT NULL,
    family_name text NOT NULL,
    tier text NOT NULL,
    eligible_as_metric boolean NOT NULL,
    updated_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.metric_classification OWNER TO portfolio;

--
-- Name: option_iv_daily; Type: TABLE; Schema: public; Owner: portfolio
--

CREATE TABLE public.option_iv_daily (
    ticker text NOT NULL,
    trade_date date NOT NULL,
    atm_iv_7d double precision,
    atm_iv_30d double precision,
    atm_iv_90d double precision,
    iv_25d_call_30d double precision,
    iv_25d_put_30d double precision,
    source_session date
);


ALTER TABLE public.option_iv_daily OWNER TO portfolio;

--
-- Name: option_volume_daily; Type: TABLE; Schema: public; Owner: portfolio
--

CREATE TABLE public.option_volume_daily (
    ticker text NOT NULL,
    trade_date date NOT NULL,
    total_call_vol bigint,
    total_put_vol bigint,
    total_vol bigint,
    vol_0_30d bigint,
    vol_31_90d bigint,
    vol_weighted_strike_call double precision,
    vol_weighted_strike_put double precision,
    vol_weighted_strike_all double precision,
    vol_above_spot bigint,
    vol_below_spot bigint,
    vol_within_5pct bigint,
    vol_within_10pct bigint,
    weighted_avg_dte_vol double precision,
    source_session date
);


ALTER TABLE public.option_volume_daily OWNER TO portfolio;

--
-- Name: research_charts; Type: TABLE; Schema: public; Owner: portfolio
--

CREATE TABLE public.research_charts (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    run_id uuid NOT NULL,
    ticker text,
    x_col text,
    y_col text,
    chart_type text NOT NULL,
    title text,
    png_data bytea,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.research_charts OWNER TO portfolio;

--
-- Name: research_followups; Type: TABLE; Schema: public; Owner: portfolio
--

CREATE TABLE public.research_followups (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    run_id uuid NOT NULL,
    question text NOT NULL,
    answer text,
    created_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.research_followups OWNER TO portfolio;

--
-- Name: research_results; Type: TABLE; Schema: public; Owner: portfolio
--

CREATE TABLE public.research_results (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    run_id uuid NOT NULL,
    ticker text,
    x_col text NOT NULL,
    y_col text NOT NULL,
    analysis_type text NOT NULL,
    result jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.research_results OWNER TO portfolio;

--
-- Name: research_runs; Type: TABLE; Schema: public; Owner: portfolio
--

CREATE TABLE public.research_runs (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    name text NOT NULL,
    question text NOT NULL,
    config jsonb NOT NULL,
    status text DEFAULT 'running'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    completed_at timestamp with time zone,
    ai_summary text,
    error_msg text
);


ALTER TABLE public.research_runs OWNER TO portfolio;

--
-- Name: research_series; Type: TABLE; Schema: public; Owner: portfolio
--

CREATE TABLE public.research_series (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    run_id uuid NOT NULL,
    ticker text,
    x_col text NOT NULL,
    y_col text,
    series_name text NOT NULL,
    data jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.research_series OWNER TO portfolio;

--
-- Name: sec_scan_cache; Type: TABLE; Schema: public; Owner: portfolio
--

CREATE TABLE public.sec_scan_cache (
    structural_key text NOT NULL,
    ticker text NOT NULL,
    primary_metric text NOT NULL,
    selected_bins jsonb NOT NULL,
    outcome text NOT NULL,
    mode text NOT NULL,
    cutoff_date date,
    n_bins integer NOT NULL,
    data_as_of date NOT NULL,
    n_input_rows integer,
    payload jsonb NOT NULL,
    cached_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.sec_scan_cache OWNER TO portfolio;

--
-- Name: signals; Type: TABLE; Schema: public; Owner: portfolio
--

CREATE TABLE public.signals (
    id integer NOT NULL,
    name text NOT NULL,
    primary_metric text NOT NULL,
    secondary_metric text NOT NULL,
    outcome text NOT NULL,
    n_bins integer DEFAULT 10 NOT NULL,
    cell_set jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now(),
    agg_avg_ret double precision,
    agg_n integer,
    per_cell_stats jsonb,
    stats_updated_at timestamp with time zone,
    status text DEFAULT 'Test'::text NOT NULL,
    color_slot integer,
    corner text,
    selection_mode text,
    selection_cutoff date
);


ALTER TABLE public.signals OWNER TO portfolio;

--
-- Name: signals_id_seq; Type: SEQUENCE; Schema: public; Owner: portfolio
--

CREATE SEQUENCE public.signals_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.signals_id_seq OWNER TO portfolio;

--
-- Name: signals_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: portfolio
--

ALTER SEQUENCE public.signals_id_seq OWNED BY public.signals.id;


--
-- Name: ticker_analysis_chain_cache; Type: TABLE; Schema: public; Owner: portfolio
--

CREATE TABLE public.ticker_analysis_chain_cache (
    cache_key text NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.ticker_analysis_chain_cache OWNER TO portfolio;

--
-- Name: ticker_analysis_layouts; Type: TABLE; Schema: public; Owner: portfolio
--

CREATE TABLE public.ticker_analysis_layouts (
    id integer NOT NULL,
    name text NOT NULL,
    layout_json jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.ticker_analysis_layouts OWNER TO portfolio;

--
-- Name: ticker_analysis_layouts_id_seq; Type: SEQUENCE; Schema: public; Owner: portfolio
--

CREATE SEQUENCE public.ticker_analysis_layouts_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.ticker_analysis_layouts_id_seq OWNER TO portfolio;

--
-- Name: ticker_analysis_layouts_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: portfolio
--

ALTER SEQUENCE public.ticker_analysis_layouts_id_seq OWNED BY public.ticker_analysis_layouts.id;


--
-- Name: trade_path_rules; Type: TABLE; Schema: public; Owner: portfolio
--

CREATE TABLE public.trade_path_rules (
    rule_key text NOT NULL,
    family text NOT NULL,
    side text NOT NULL,
    fill_mode text NOT NULL,
    params jsonb NOT NULL,
    exit_bar_col text NOT NULL,
    exit_return_col text NOT NULL,
    is_horizon boolean DEFAULT false NOT NULL
);


ALTER TABLE public.trade_path_rules OWNER TO portfolio;

--
-- Name: trade_paths; Type: TABLE; Schema: public; Owner: portfolio
--

CREATE TABLE public.trade_paths (
    ticker text NOT NULL,
    trade_date date NOT NULL,
    entry_anchor text NOT NULL,
    entry_price real,
    entry_bar_ts timestamp without time zone,
    atr_14d real,
    swing_low_1 real,
    swing_low_3 real,
    swing_low_5 real,
    n_bars integer,
    n_sessions smallint,
    path_status text DEFAULT 'ok'::text NOT NULL,
    built_at timestamp without time zone DEFAULT now() NOT NULL,
    xb_fixed_stop__0p5 smallint,
    xr_fixed_stop__0p5 real,
    xb_fixed_stop__1 smallint,
    xr_fixed_stop__1 real,
    xb_fixed_stop__1p5 smallint,
    xr_fixed_stop__1p5 real,
    xb_fixed_stop__2 smallint,
    xr_fixed_stop__2 real,
    xb_fixed_stop__2p5 smallint,
    xr_fixed_stop__2p5 real,
    xb_fixed_stop__3 smallint,
    xr_fixed_stop__3 real,
    xb_fixed_stop__4 smallint,
    xr_fixed_stop__4 real,
    xb_atr_stop__0p5 smallint,
    xr_atr_stop__0p5 real,
    xb_atr_stop__1 smallint,
    xr_atr_stop__1 real,
    xb_atr_stop__1p5 smallint,
    xr_atr_stop__1p5 real,
    xb_atr_stop__2 smallint,
    xr_atr_stop__2 real,
    xb_atr_stop__2p5 smallint,
    xr_atr_stop__2p5 real,
    xb_swing_low__1 smallint,
    xr_swing_low__1 real,
    xb_swing_low__3 smallint,
    xr_swing_low__3 real,
    xb_swing_low__5 smallint,
    xr_swing_low__5 real,
    xb_trail__1_act0 smallint,
    xr_trail__1_act0 real,
    xb_trail__1_act1 smallint,
    xr_trail__1_act1 real,
    xb_trail__1_act2 smallint,
    xr_trail__1_act2 real,
    xb_trail__1_act3 smallint,
    xr_trail__1_act3 real,
    xb_trail__2_act0 smallint,
    xr_trail__2_act0 real,
    xb_trail__2_act1 smallint,
    xr_trail__2_act1 real,
    xb_trail__2_act2 smallint,
    xr_trail__2_act2 real,
    xb_trail__2_act3 smallint,
    xr_trail__2_act3 real,
    xb_trail__3_act0 smallint,
    xr_trail__3_act0 real,
    xb_trail__3_act1 smallint,
    xr_trail__3_act1 real,
    xb_trail__3_act2 smallint,
    xr_trail__3_act2 real,
    xb_trail__3_act3 smallint,
    xr_trail__3_act3 real,
    xb_trail__4_act0 smallint,
    xr_trail__4_act0 real,
    xb_trail__4_act1 smallint,
    xr_trail__4_act1 real,
    xb_trail__4_act2 smallint,
    xr_trail__4_act2 real,
    xb_trail__4_act3 smallint,
    xr_trail__4_act3 real,
    xb_breakeven__1 smallint,
    xr_breakeven__1 real,
    xb_breakeven__1p5 smallint,
    xr_breakeven__1p5 real,
    xb_breakeven__2 smallint,
    xr_breakeven__2 real,
    xb_breakeven__2p5 smallint,
    xr_breakeven__2p5 real,
    xb_fixed_target__2 smallint,
    xr_fixed_target__2 real,
    xb_fixed_target__3 smallint,
    xr_fixed_target__3 real,
    xb_fixed_target__4 smallint,
    xr_fixed_target__4 real,
    xb_fixed_target__5 smallint,
    xr_fixed_target__5 real,
    xb_fixed_target__7 smallint,
    xr_fixed_target__7 real,
    xb_fixed_target__10 smallint,
    xr_fixed_target__10 real,
    xb_atr_target__1 smallint,
    xr_atr_target__1 real,
    xb_atr_target__2 smallint,
    xr_atr_target__2 real,
    xb_atr_target__3 smallint,
    xr_atr_target__3 real,
    xb_atr_target__4 smallint,
    xr_atr_target__4 real,
    xb_atr_target__5 smallint,
    xr_atr_target__5 real,
    xb_max_days__1 smallint,
    xr_max_days__1 real,
    xb_max_days__3 smallint,
    xr_max_days__3 real,
    xb_max_days__5 smallint,
    xr_max_days__5 real,
    xb_max_days__7 smallint,
    xr_max_days__7 real,
    xb_max_days__10 smallint,
    xr_max_days__10 real,
    xb_max_days__15 smallint,
    xr_max_days__15 real,
    xb_max_days__20 smallint,
    xr_max_days__20 real,
    xb_no_progress__1_d2 smallint,
    xr_no_progress__1_d2 real,
    xb_no_progress__1_d5 smallint,
    xr_no_progress__1_d5 real,
    xb_no_progress__2_d2 smallint,
    xr_no_progress__2_d2 real,
    xb_no_progress__2_d5 smallint,
    xr_no_progress__2_d5 real,
    xb_ma_close_below__10 smallint,
    xr_ma_close_below__10 real,
    xb_ma_close_below__20 smallint,
    xr_ma_close_below__20 real,
    xb_atr_stop__3 smallint,
    xr_atr_stop__3 real,
    xb_atr_stop__4 smallint,
    xr_atr_stop__4 real,
    xb_trail__1_act5 smallint,
    xr_trail__1_act5 real,
    xb_trail__1_act7 smallint,
    xr_trail__1_act7 real,
    xb_trail__1_act10 smallint,
    xr_trail__1_act10 real,
    xb_trail__2_act5 smallint,
    xr_trail__2_act5 real,
    xb_trail__2_act7 smallint,
    xr_trail__2_act7 real,
    xb_trail__2_act10 smallint,
    xr_trail__2_act10 real,
    xb_trail__3_act5 smallint,
    xr_trail__3_act5 real,
    xb_trail__3_act7 smallint,
    xr_trail__3_act7 real,
    xb_trail__3_act10 smallint,
    xr_trail__3_act10 real,
    xb_trail__4_act5 smallint,
    xr_trail__4_act5 real,
    xb_trail__4_act7 smallint,
    xr_trail__4_act7 real,
    xb_trail__4_act10 smallint,
    xr_trail__4_act10 real,
    xb_trail__5_act0 smallint,
    xr_trail__5_act0 real,
    xb_trail__5_act1 smallint,
    xr_trail__5_act1 real,
    xb_trail__5_act2 smallint,
    xr_trail__5_act2 real,
    xb_trail__5_act3 smallint,
    xr_trail__5_act3 real,
    xb_trail__5_act5 smallint,
    xr_trail__5_act5 real,
    xb_trail__5_act7 smallint,
    xr_trail__5_act7 real,
    xb_trail__5_act10 smallint,
    xr_trail__5_act10 real,
    xb_trail__6_act0 smallint,
    xr_trail__6_act0 real,
    xb_trail__6_act1 smallint,
    xr_trail__6_act1 real,
    xb_trail__6_act2 smallint,
    xr_trail__6_act2 real,
    xb_trail__6_act3 smallint,
    xr_trail__6_act3 real,
    xb_trail__6_act5 smallint,
    xr_trail__6_act5 real,
    xb_trail__6_act7 smallint,
    xr_trail__6_act7 real,
    xb_trail__6_act10 smallint,
    xr_trail__6_act10 real,
    xb_trail__8_act0 smallint,
    xr_trail__8_act0 real,
    xb_trail__8_act1 smallint,
    xr_trail__8_act1 real,
    xb_trail__8_act2 smallint,
    xr_trail__8_act2 real,
    xb_trail__8_act3 smallint,
    xr_trail__8_act3 real,
    xb_trail__8_act5 smallint,
    xr_trail__8_act5 real,
    xb_trail__8_act7 smallint,
    xr_trail__8_act7 real,
    xb_trail__8_act10 smallint,
    xr_trail__8_act10 real,
    xb_trail__10_act0 smallint,
    xr_trail__10_act0 real,
    xb_trail__10_act1 smallint,
    xr_trail__10_act1 real,
    xb_trail__10_act2 smallint,
    xr_trail__10_act2 real,
    xb_trail__10_act3 smallint,
    xr_trail__10_act3 real,
    xb_trail__10_act5 smallint,
    xr_trail__10_act5 real,
    xb_trail__10_act7 smallint,
    xr_trail__10_act7 real,
    xb_trail__10_act10 smallint,
    xr_trail__10_act10 real,
    xb_atr_trail__1_act0 smallint,
    xr_atr_trail__1_act0 real,
    xb_atr_trail__1_act1 smallint,
    xr_atr_trail__1_act1 real,
    xb_atr_trail__1_act2 smallint,
    xr_atr_trail__1_act2 real,
    xb_atr_trail__1_act3 smallint,
    xr_atr_trail__1_act3 real,
    xb_atr_trail__1_act5 smallint,
    xr_atr_trail__1_act5 real,
    xb_atr_trail__1_act7 smallint,
    xr_atr_trail__1_act7 real,
    xb_atr_trail__1_act10 smallint,
    xr_atr_trail__1_act10 real,
    xb_atr_trail__1p5_act0 smallint,
    xr_atr_trail__1p5_act0 real,
    xb_atr_trail__1p5_act1 smallint,
    xr_atr_trail__1p5_act1 real,
    xb_atr_trail__1p5_act2 smallint,
    xr_atr_trail__1p5_act2 real,
    xb_atr_trail__1p5_act3 smallint,
    xr_atr_trail__1p5_act3 real,
    xb_atr_trail__1p5_act5 smallint,
    xr_atr_trail__1p5_act5 real,
    xb_atr_trail__1p5_act7 smallint,
    xr_atr_trail__1p5_act7 real,
    xb_atr_trail__1p5_act10 smallint,
    xr_atr_trail__1p5_act10 real,
    xb_atr_trail__2_act0 smallint,
    xr_atr_trail__2_act0 real,
    xb_atr_trail__2_act1 smallint,
    xr_atr_trail__2_act1 real,
    xb_atr_trail__2_act2 smallint,
    xr_atr_trail__2_act2 real,
    xb_atr_trail__2_act3 smallint,
    xr_atr_trail__2_act3 real,
    xb_atr_trail__2_act5 smallint,
    xr_atr_trail__2_act5 real,
    xb_atr_trail__2_act7 smallint,
    xr_atr_trail__2_act7 real,
    xb_atr_trail__2_act10 smallint,
    xr_atr_trail__2_act10 real,
    xb_atr_trail__3_act0 smallint,
    xr_atr_trail__3_act0 real,
    xb_atr_trail__3_act1 smallint,
    xr_atr_trail__3_act1 real,
    xb_atr_trail__3_act2 smallint,
    xr_atr_trail__3_act2 real,
    xb_atr_trail__3_act3 smallint,
    xr_atr_trail__3_act3 real,
    xb_atr_trail__3_act5 smallint,
    xr_atr_trail__3_act5 real,
    xb_atr_trail__3_act7 smallint,
    xr_atr_trail__3_act7 real,
    xb_atr_trail__3_act10 smallint,
    xr_atr_trail__3_act10 real,
    xb_atr_trail__4_act0 smallint,
    xr_atr_trail__4_act0 real,
    xb_atr_trail__4_act1 smallint,
    xr_atr_trail__4_act1 real,
    xb_atr_trail__4_act2 smallint,
    xr_atr_trail__4_act2 real,
    xb_atr_trail__4_act3 smallint,
    xr_atr_trail__4_act3 real,
    xb_atr_trail__4_act5 smallint,
    xr_atr_trail__4_act5 real,
    xb_atr_trail__4_act7 smallint,
    xr_atr_trail__4_act7 real,
    xb_atr_trail__4_act10 smallint,
    xr_atr_trail__4_act10 real,
    xb_fixed_target__15 smallint,
    xr_fixed_target__15 real,
    xb_fixed_target__20 smallint,
    xr_fixed_target__20 real,
    xb_atr_target__6 smallint,
    xr_atr_target__6 real,
    xb_atr_target__8 smallint,
    xr_atr_target__8 real,
    xb_atr_target__10 smallint,
    xr_atr_target__10 real,
    xb_max_days__30 smallint,
    xr_max_days__30 real,
    xb_max_days__40 smallint,
    xr_max_days__40 real
);


ALTER TABLE public.trade_paths OWNER TO portfolio;

--
-- Name: trade_paths_manifest; Type: TABLE; Schema: public; Owner: portfolio
--

CREATE TABLE public.trade_paths_manifest (
    ticker text NOT NULL,
    entry_anchor text NOT NULL,
    status text NOT NULL,
    n_entries integer,
    n_resolved integer,
    built_at timestamp without time zone DEFAULT now() NOT NULL,
    note text
);


ALTER TABLE public.trade_paths_manifest OWNER TO portfolio;

--
-- Name: tt_bins; Type: TABLE; Schema: public; Owner: portfolio
--

CREATE TABLE public.tt_bins (
    ticker text NOT NULL,
    trade_date date NOT NULL,
    cutoff_date date DEFAULT '2024-01-01'::date NOT NULL,
    bin20_atm_iv_30d smallint DEFAULT 0 NOT NULL,
    bin20_atm_iv_7d smallint DEFAULT 0 NOT NULL,
    bin20_atm_iv_90d smallint DEFAULT 0 NOT NULL,
    bin20_atr_normalized_ret_5d smallint DEFAULT 0 NOT NULL,
    bin20_call_oi smallint DEFAULT 0 NOT NULL,
    bin20_cum_signed_vol_20d smallint DEFAULT 0 NOT NULL,
    bin20_d1_atm_iv_30d_change smallint DEFAULT 0 NOT NULL,
    bin20_d1_atm_iv_7d_change smallint DEFAULT 0 NOT NULL,
    bin20_d1_d5_ratio_total_oi_pct_change smallint DEFAULT 0 NOT NULL,
    bin20_d1_oi_weighted_all_div_spot_change_co smallint DEFAULT 0 NOT NULL,
    bin20_d1_oi_weighted_all_div_spot_change_pc smallint DEFAULT 0 NOT NULL,
    bin20_d1_put_call_oi_ratio_change smallint DEFAULT 0 NOT NULL,
    bin20_d1_total_oi_change smallint DEFAULT 0 NOT NULL,
    bin20_d1_total_oi_pct_change smallint DEFAULT 0 NOT NULL,
    bin20_d20_total_oi_change smallint DEFAULT 0 NOT NULL,
    bin20_d5_atm_iv_30d_change smallint DEFAULT 0 NOT NULL,
    bin20_d5_atm_iv_7d_change smallint DEFAULT 0 NOT NULL,
    bin20_d5_oi_weighted_all_div_spot_change_co smallint DEFAULT 0 NOT NULL,
    bin20_d5_oi_weighted_all_div_spot_change_pc smallint DEFAULT 0 NOT NULL,
    bin20_d5_put_call_oi_ratio_change smallint DEFAULT 0 NOT NULL,
    bin20_d5_total_oi_change smallint DEFAULT 0 NOT NULL,
    bin20_d5_total_oi_pct_change smallint DEFAULT 0 NOT NULL,
    bin20_donchian_pos_20d smallint DEFAULT 0 NOT NULL,
    bin20_iv_rv_ratio_30d smallint DEFAULT 0 NOT NULL,
    bin20_ma20_slope_5d smallint DEFAULT 0 NOT NULL,
    bin20_max_oi_strike_call smallint DEFAULT 0 NOT NULL,
    bin20_max_oi_strike_put smallint DEFAULT 0 NOT NULL,
    bin20_net_new_oi_div_vol smallint DEFAULT 0 NOT NULL,
    bin20_oi_above_below_ratio_co smallint DEFAULT 0 NOT NULL,
    bin20_oi_above_below_ratio_pc smallint DEFAULT 0 NOT NULL,
    bin20_oi_above_spot_co smallint DEFAULT 0 NOT NULL,
    bin20_oi_above_spot_pc smallint DEFAULT 0 NOT NULL,
    bin20_oi_below_spot_co smallint DEFAULT 0 NOT NULL,
    bin20_oi_below_spot_pc smallint DEFAULT 0 NOT NULL,
    bin20_oi_weighted_all smallint DEFAULT 0 NOT NULL,
    bin20_oi_weighted_all_0_30d smallint DEFAULT 0 NOT NULL,
    bin20_oi_weighted_all_0_30d_div_spot_co smallint DEFAULT 0 NOT NULL,
    bin20_oi_weighted_all_0_30d_div_spot_pc smallint DEFAULT 0 NOT NULL,
    bin20_oi_weighted_all_31_90d smallint DEFAULT 0 NOT NULL,
    bin20_oi_weighted_all_31_90d_div_spot_co smallint DEFAULT 0 NOT NULL,
    bin20_oi_weighted_all_31_90d_div_spot_pc smallint DEFAULT 0 NOT NULL,
    bin20_oi_weighted_all_div_spot_co smallint DEFAULT 0 NOT NULL,
    bin20_oi_weighted_all_div_spot_pc smallint DEFAULT 0 NOT NULL,
    bin20_oi_weighted_all_minus_spot_co smallint DEFAULT 0 NOT NULL,
    bin20_oi_weighted_all_minus_spot_pc smallint DEFAULT 0 NOT NULL,
    bin20_oi_weighted_call smallint DEFAULT 0 NOT NULL,
    bin20_oi_weighted_call_0_30d smallint DEFAULT 0 NOT NULL,
    bin20_oi_weighted_call_0_30d_div_spot_co smallint DEFAULT 0 NOT NULL,
    bin20_oi_weighted_call_0_30d_div_spot_pc smallint DEFAULT 0 NOT NULL,
    bin20_oi_weighted_call_31_90d smallint DEFAULT 0 NOT NULL,
    bin20_oi_weighted_call_31_90d_div_spot_co smallint DEFAULT 0 NOT NULL,
    bin20_oi_weighted_call_31_90d_div_spot_pc smallint DEFAULT 0 NOT NULL,
    bin20_oi_weighted_call_div_spot_co smallint DEFAULT 0 NOT NULL,
    bin20_oi_weighted_call_div_spot_pc smallint DEFAULT 0 NOT NULL,
    bin20_oi_weighted_call_minus_spot_co smallint DEFAULT 0 NOT NULL,
    bin20_oi_weighted_call_minus_spot_pc smallint DEFAULT 0 NOT NULL,
    bin20_oi_weighted_next_monthly_div_spot_co smallint DEFAULT 0 NOT NULL,
    bin20_oi_weighted_next_monthly_div_spot_pc smallint DEFAULT 0 NOT NULL,
    bin20_oi_weighted_put smallint DEFAULT 0 NOT NULL,
    bin20_oi_weighted_put_0_30d smallint DEFAULT 0 NOT NULL,
    bin20_oi_weighted_put_0_30d_div_spot_co smallint DEFAULT 0 NOT NULL,
    bin20_oi_weighted_put_0_30d_div_spot_pc smallint DEFAULT 0 NOT NULL,
    bin20_oi_weighted_put_31_90d smallint DEFAULT 0 NOT NULL,
    bin20_oi_weighted_put_31_90d_div_spot_co smallint DEFAULT 0 NOT NULL,
    bin20_oi_weighted_put_31_90d_div_spot_pc smallint DEFAULT 0 NOT NULL,
    bin20_oi_weighted_put_div_spot_co smallint DEFAULT 0 NOT NULL,
    bin20_oi_weighted_put_div_spot_pc smallint DEFAULT 0 NOT NULL,
    bin20_oi_weighted_put_minus_spot_co smallint DEFAULT 0 NOT NULL,
    bin20_oi_weighted_put_minus_spot_pc smallint DEFAULT 0 NOT NULL,
    bin20_oi_within_10pct_co smallint DEFAULT 0 NOT NULL,
    bin20_oi_within_10pct_pc smallint DEFAULT 0 NOT NULL,
    bin20_oi_within_5pct_co smallint DEFAULT 0 NOT NULL,
    bin20_oi_within_5pct_pc smallint DEFAULT 0 NOT NULL,
    bin20_pct_from_52w_high smallint DEFAULT 0 NOT NULL,
    bin20_pct_from_52w_low smallint DEFAULT 0 NOT NULL,
    bin20_pct_from_ma20 smallint DEFAULT 0 NOT NULL,
    bin20_pct_from_ma50 smallint DEFAULT 0 NOT NULL,
    bin20_pct_oi_0_30d smallint DEFAULT 0 NOT NULL,
    bin20_pct_oi_31_90d smallint DEFAULT 0 NOT NULL,
    bin20_pct_oi_91_365d smallint DEFAULT 0 NOT NULL,
    bin20_pct_oi_above_spot_co smallint DEFAULT 0 NOT NULL,
    bin20_pct_oi_above_spot_pc smallint DEFAULT 0 NOT NULL,
    bin20_pct_oi_below_spot_co smallint DEFAULT 0 NOT NULL,
    bin20_pct_oi_below_spot_pc smallint DEFAULT 0 NOT NULL,
    bin20_pct_oi_in_front_expiry smallint DEFAULT 0 NOT NULL,
    bin20_pct_oi_next_monthly smallint DEFAULT 0 NOT NULL,
    bin20_pct_oi_within_10pct_co smallint DEFAULT 0 NOT NULL,
    bin20_pct_oi_within_10pct_pc smallint DEFAULT 0 NOT NULL,
    bin20_pct_oi_within_5pct_co smallint DEFAULT 0 NOT NULL,
    bin20_pct_oi_within_5pct_pc smallint DEFAULT 0 NOT NULL,
    bin20_pct_up_days_20d smallint DEFAULT 0 NOT NULL,
    bin20_pct_vol_0_30d smallint DEFAULT 0 NOT NULL,
    bin20_pct_vol_31_90d smallint DEFAULT 0 NOT NULL,
    bin20_pct_vol_within_10pct_pc smallint DEFAULT 0 NOT NULL,
    bin20_pct_vol_within_5pct_pc smallint DEFAULT 0 NOT NULL,
    bin20_put_call_oi_ratio smallint DEFAULT 0 NOT NULL,
    bin20_put_call_ratio_vol smallint DEFAULT 0 NOT NULL,
    bin20_put_oi smallint DEFAULT 0 NOT NULL,
    bin20_relative_strength_vs_spy_20d smallint DEFAULT 0 NOT NULL,
    bin20_ret_10d smallint DEFAULT 0 NOT NULL,
    bin20_ret_20d smallint DEFAULT 0 NOT NULL,
    bin20_ret_5d smallint DEFAULT 0 NOT NULL,
    bin20_rv_20d smallint DEFAULT 0 NOT NULL,
    bin20_rv_5d smallint DEFAULT 0 NOT NULL,
    bin20_rv_ratio_5d_20d smallint DEFAULT 0 NOT NULL,
    bin20_spot_co smallint DEFAULT 0 NOT NULL,
    bin20_spot_pc smallint DEFAULT 0 NOT NULL,
    bin20_term_30d_90d smallint DEFAULT 0 NOT NULL,
    bin20_term_7d_30d smallint DEFAULT 0 NOT NULL,
    bin20_top10_strikes_pct_total_oi smallint DEFAULT 0 NOT NULL,
    bin20_top5_strikes_pct_total_oi smallint DEFAULT 0 NOT NULL,
    bin20_total_oi smallint DEFAULT 0 NOT NULL,
    bin20_vol_above_below_ratio_pc smallint DEFAULT 0 NOT NULL,
    bin20_vol_oi_ratio_all smallint DEFAULT 0 NOT NULL,
    bin20_vol_oi_ratio_call smallint DEFAULT 0 NOT NULL,
    bin20_vol_oi_ratio_put smallint DEFAULT 0 NOT NULL,
    bin20_vol_weighted_all_div_spot_pc smallint DEFAULT 0 NOT NULL,
    bin20_vol_weighted_call_div_spot_pc smallint DEFAULT 0 NOT NULL,
    bin20_vol_weighted_put_div_spot_pc smallint DEFAULT 0 NOT NULL,
    bin20_vrp_30d smallint DEFAULT 0 NOT NULL,
    bin20_weighted_avg_dte smallint DEFAULT 0 NOT NULL,
    bin20_weighted_avg_dte_vol smallint DEFAULT 0 NOT NULL,
    bin20_zscore_d1_oi_change_3m smallint DEFAULT 0 NOT NULL,
    bin20_zscore_d5_oi_change_3m smallint DEFAULT 0 NOT NULL,
    bin20_zscore_iv_30d smallint DEFAULT 0 NOT NULL,
    bin20_zscore_iv_7d smallint DEFAULT 0 NOT NULL,
    bin20_zscore_iv_90d smallint DEFAULT 0 NOT NULL,
    bin20_zscore_iv_rv_ratio_30d smallint DEFAULT 0 NOT NULL,
    bin20_zscore_oi_above_below_ratio_3m_co smallint DEFAULT 0 NOT NULL,
    bin20_zscore_oi_above_below_ratio_3m_pc smallint DEFAULT 0 NOT NULL,
    bin20_zscore_oi_weighted_all_div_spot_3m_co smallint DEFAULT 0 NOT NULL,
    bin20_zscore_oi_weighted_all_div_spot_3m_pc smallint DEFAULT 0 NOT NULL,
    bin20_zscore_price_vs_ma20 smallint DEFAULT 0 NOT NULL,
    bin20_zscore_price_vs_ma50 smallint DEFAULT 0 NOT NULL,
    bin20_zscore_put_call_oi_ratio_3m smallint DEFAULT 0 NOT NULL,
    bin20_zscore_put_call_ratio_vol smallint DEFAULT 0 NOT NULL,
    bin20_zscore_term_30d_90d smallint DEFAULT 0 NOT NULL,
    bin20_zscore_term_7d_30d smallint DEFAULT 0 NOT NULL,
    bin20_zscore_underlying_vol_20d smallint DEFAULT 0 NOT NULL,
    bin20_zscore_vol_above_below_ratio_pc smallint DEFAULT 0 NOT NULL,
    bin20_zscore_vol_oi_ratio_all smallint DEFAULT 0 NOT NULL,
    bin20_zscore_vol_oi_ratio_call smallint DEFAULT 0 NOT NULL,
    bin20_zscore_vol_oi_ratio_put smallint DEFAULT 0 NOT NULL,
    bin20_zscore_vrp_30d smallint DEFAULT 0 NOT NULL,
    frac_atm_iv_30d double precision,
    frac_atm_iv_7d double precision,
    frac_atm_iv_90d double precision,
    frac_atr_normalized_ret_5d double precision,
    frac_call_oi double precision,
    frac_cum_signed_vol_20d double precision,
    frac_d1_atm_iv_30d_change double precision,
    frac_d1_atm_iv_7d_change double precision,
    frac_d1_d5_ratio_total_oi_pct_change double precision,
    frac_d1_oi_weighted_all_div_spot_change_co double precision,
    frac_d1_oi_weighted_all_div_spot_change_pc double precision,
    frac_d1_put_call_oi_ratio_change double precision,
    frac_d1_total_oi_change double precision,
    frac_d1_total_oi_pct_change double precision,
    frac_d20_total_oi_change double precision,
    frac_d5_atm_iv_30d_change double precision,
    frac_d5_atm_iv_7d_change double precision,
    frac_d5_oi_weighted_all_div_spot_change_co double precision,
    frac_d5_oi_weighted_all_div_spot_change_pc double precision,
    frac_d5_put_call_oi_ratio_change double precision,
    frac_d5_total_oi_change double precision,
    frac_d5_total_oi_pct_change double precision,
    frac_donchian_pos_20d double precision,
    frac_iv_rv_ratio_30d double precision,
    frac_ma20_slope_5d double precision,
    frac_max_oi_strike_call double precision,
    frac_max_oi_strike_put double precision,
    frac_net_new_oi_div_vol double precision,
    frac_oi_above_below_ratio_co double precision,
    frac_oi_above_below_ratio_pc double precision,
    frac_oi_above_spot_co double precision,
    frac_oi_above_spot_pc double precision,
    frac_oi_below_spot_co double precision,
    frac_oi_below_spot_pc double precision,
    frac_oi_weighted_all double precision,
    frac_oi_weighted_all_0_30d double precision,
    frac_oi_weighted_all_0_30d_div_spot_co double precision,
    frac_oi_weighted_all_0_30d_div_spot_pc double precision,
    frac_oi_weighted_all_31_90d double precision,
    frac_oi_weighted_all_31_90d_div_spot_co double precision,
    frac_oi_weighted_all_31_90d_div_spot_pc double precision,
    frac_oi_weighted_all_div_spot_co double precision,
    frac_oi_weighted_all_div_spot_pc double precision,
    frac_oi_weighted_all_minus_spot_co double precision,
    frac_oi_weighted_all_minus_spot_pc double precision,
    frac_oi_weighted_call double precision,
    frac_oi_weighted_call_0_30d double precision,
    frac_oi_weighted_call_0_30d_div_spot_co double precision,
    frac_oi_weighted_call_0_30d_div_spot_pc double precision,
    frac_oi_weighted_call_31_90d double precision,
    frac_oi_weighted_call_31_90d_div_spot_co double precision,
    frac_oi_weighted_call_31_90d_div_spot_pc double precision,
    frac_oi_weighted_call_div_spot_co double precision,
    frac_oi_weighted_call_div_spot_pc double precision,
    frac_oi_weighted_call_minus_spot_co double precision,
    frac_oi_weighted_call_minus_spot_pc double precision,
    frac_oi_weighted_next_monthly_div_spot_co double precision,
    frac_oi_weighted_next_monthly_div_spot_pc double precision,
    frac_oi_weighted_put double precision,
    frac_oi_weighted_put_0_30d double precision,
    frac_oi_weighted_put_0_30d_div_spot_co double precision,
    frac_oi_weighted_put_0_30d_div_spot_pc double precision,
    frac_oi_weighted_put_31_90d double precision,
    frac_oi_weighted_put_31_90d_div_spot_co double precision,
    frac_oi_weighted_put_31_90d_div_spot_pc double precision,
    frac_oi_weighted_put_div_spot_co double precision,
    frac_oi_weighted_put_div_spot_pc double precision,
    frac_oi_weighted_put_minus_spot_co double precision,
    frac_oi_weighted_put_minus_spot_pc double precision,
    frac_oi_within_10pct_co double precision,
    frac_oi_within_10pct_pc double precision,
    frac_oi_within_5pct_co double precision,
    frac_oi_within_5pct_pc double precision,
    frac_pct_from_52w_high double precision,
    frac_pct_from_52w_low double precision,
    frac_pct_from_ma20 double precision,
    frac_pct_from_ma50 double precision,
    frac_pct_oi_0_30d double precision,
    frac_pct_oi_31_90d double precision,
    frac_pct_oi_91_365d double precision,
    frac_pct_oi_above_spot_co double precision,
    frac_pct_oi_above_spot_pc double precision,
    frac_pct_oi_below_spot_co double precision,
    frac_pct_oi_below_spot_pc double precision,
    frac_pct_oi_in_front_expiry double precision,
    frac_pct_oi_next_monthly double precision,
    frac_pct_oi_within_10pct_co double precision,
    frac_pct_oi_within_10pct_pc double precision,
    frac_pct_oi_within_5pct_co double precision,
    frac_pct_oi_within_5pct_pc double precision,
    frac_pct_up_days_20d double precision,
    frac_pct_vol_0_30d double precision,
    frac_pct_vol_31_90d double precision,
    frac_pct_vol_within_10pct_pc double precision,
    frac_pct_vol_within_5pct_pc double precision,
    frac_put_call_oi_ratio double precision,
    frac_put_call_ratio_vol double precision,
    frac_put_oi double precision,
    frac_relative_strength_vs_spy_20d double precision,
    frac_ret_10d double precision,
    frac_ret_20d double precision,
    frac_ret_5d double precision,
    frac_rv_20d double precision,
    frac_rv_5d double precision,
    frac_rv_ratio_5d_20d double precision,
    frac_spot_co double precision,
    frac_spot_pc double precision,
    frac_term_30d_90d double precision,
    frac_term_7d_30d double precision,
    frac_top10_strikes_pct_total_oi double precision,
    frac_top5_strikes_pct_total_oi double precision,
    frac_total_oi double precision,
    frac_vol_above_below_ratio_pc double precision,
    frac_vol_oi_ratio_all double precision,
    frac_vol_oi_ratio_call double precision,
    frac_vol_oi_ratio_put double precision,
    frac_vol_weighted_all_div_spot_pc double precision,
    frac_vol_weighted_call_div_spot_pc double precision,
    frac_vol_weighted_put_div_spot_pc double precision,
    frac_vrp_30d double precision,
    frac_weighted_avg_dte double precision,
    frac_weighted_avg_dte_vol double precision,
    frac_zscore_d1_oi_change_3m double precision,
    frac_zscore_d5_oi_change_3m double precision,
    frac_zscore_iv_30d double precision,
    frac_zscore_iv_7d double precision,
    frac_zscore_iv_90d double precision,
    frac_zscore_iv_rv_ratio_30d double precision,
    frac_zscore_oi_above_below_ratio_3m_co double precision,
    frac_zscore_oi_above_below_ratio_3m_pc double precision,
    frac_zscore_oi_weighted_all_div_spot_3m_co double precision,
    frac_zscore_oi_weighted_all_div_spot_3m_pc double precision,
    frac_zscore_price_vs_ma20 double precision,
    frac_zscore_price_vs_ma50 double precision,
    frac_zscore_put_call_oi_ratio_3m double precision,
    frac_zscore_put_call_ratio_vol double precision,
    frac_zscore_term_30d_90d double precision,
    frac_zscore_term_7d_30d double precision,
    frac_zscore_underlying_vol_20d double precision,
    frac_zscore_vol_above_below_ratio_pc double precision,
    frac_zscore_vol_oi_ratio_all double precision,
    frac_zscore_vol_oi_ratio_call double precision,
    frac_zscore_vol_oi_ratio_put double precision,
    frac_zscore_vrp_30d double precision,
    frac_bf_25d_30d double precision,
    bin20_bf_25d_30d smallint DEFAULT 0 NOT NULL,
    frac_bf_25d_7d double precision,
    bin20_bf_25d_7d smallint DEFAULT 0 NOT NULL,
    frac_iv_25d_call_30d double precision,
    bin20_iv_25d_call_30d smallint DEFAULT 0 NOT NULL,
    frac_iv_25d_call_7d double precision,
    bin20_iv_25d_call_7d smallint DEFAULT 0 NOT NULL,
    frac_iv_25d_put_30d double precision,
    bin20_iv_25d_put_30d smallint DEFAULT 0 NOT NULL,
    frac_iv_25d_put_7d double precision,
    bin20_iv_25d_put_7d smallint DEFAULT 0 NOT NULL,
    frac_rr_25d_30d double precision,
    bin20_rr_25d_30d smallint DEFAULT 0 NOT NULL,
    frac_rr_25d_7d double precision,
    bin20_rr_25d_7d smallint DEFAULT 0 NOT NULL,
    frac_skew_25p_atm_30d double precision,
    bin20_skew_25p_atm_30d smallint DEFAULT 0 NOT NULL,
    frac_skew_25p_atm_7d double precision,
    bin20_skew_25p_atm_7d smallint DEFAULT 0 NOT NULL,
    frac_skew_atm_25c_30d double precision,
    bin20_skew_atm_25c_30d smallint DEFAULT 0 NOT NULL,
    frac_skew_atm_25c_7d double precision,
    bin20_skew_atm_25c_7d smallint DEFAULT 0 NOT NULL,
    frac_zscore_rr_25d_30d double precision,
    bin20_zscore_rr_25d_30d smallint DEFAULT 0 NOT NULL,
    frac_zscore_rr_25d_7d double precision,
    bin20_zscore_rr_25d_7d smallint DEFAULT 0 NOT NULL
);


ALTER TABLE public.tt_bins OWNER TO portfolio;

--
-- Name: tt_thresholds; Type: TABLE; Schema: public; Owner: portfolio
--

CREATE TABLE public.tt_thresholds (
    metric text NOT NULL,
    ticker text NOT NULL,
    cutoff_date date NOT NULL,
    history_vals double precision[] NOT NULL,
    n_train integer NOT NULL
);


ALTER TABLE public.tt_thresholds OWNER TO portfolio;

--
-- Name: underlying_ohlc; Type: TABLE; Schema: public; Owner: portfolio
--

CREATE TABLE public.underlying_ohlc (
    ticker text NOT NULL,
    trade_date date NOT NULL,
    open double precision,
    high double precision,
    low double precision,
    close double precision,
    adj_close double precision,
    volume bigint,
    dividends double precision,
    splits double precision,
    open_source text,
    open_asof_ts timestamp with time zone
);


ALTER TABLE public.underlying_ohlc OWNER TO portfolio;

--
-- Name: v_features_with_returns; Type: VIEW; Schema: public; Owner: portfolio
--

CREATE VIEW public.v_features_with_returns AS
 SELECT f.ticker,
    f.trade_date,
    f.total_oi,
    f.call_oi,
    f.put_oi,
    f.put_call_oi_ratio,
    f.max_oi_strike_call,
    f.max_oi_strike_put,
    f.pct_oi_in_front_expiry,
    f.d1_total_oi_change,
    f.d5_total_oi_change,
    f.d20_total_oi_change,
    f.rv_5d,
    f.rv_20d,
    f.ret_1d_fwd_oc,
    f.ret_3d_fwd_oc,
    f.ret_5d_fwd_oc,
    f.ret_7d_fwd_oc,
    f.ret_10d_fwd_oc,
    f.ret_20d_fwd_oc,
    f.top5_strikes_pct_total_oi,
    f.top10_strikes_pct_total_oi,
    f.weighted_avg_dte,
    f.pct_oi_0_30d,
    f.pct_oi_31_90d,
    f.pct_oi_91_365d,
    f.pct_oi_next_monthly,
    f.d1_total_oi_pct_change,
    f.d5_total_oi_pct_change,
    f.d1_d5_ratio_total_oi_pct_change,
    f.d1_put_call_oi_ratio_change,
    f.d5_put_call_oi_ratio_change,
    f.zscore_d1_oi_change_3m,
    f.zscore_d5_oi_change_3m,
    f.zscore_put_call_oi_ratio_3m,
    f.oi_weighted_call,
    f.oi_weighted_put,
    f.oi_weighted_all,
    f.oi_weighted_all_0_30d,
    f.oi_weighted_call_0_30d,
    f.oi_weighted_put_0_30d,
    f.oi_weighted_all_31_90d,
    f.oi_weighted_call_31_90d,
    f.oi_weighted_put_31_90d,
    f.spot_pc,
    f.spot_co,
    f.oi_within_5pct_pc,
    f.oi_within_5pct_co,
    f.oi_within_10pct_pc,
    f.oi_within_10pct_co,
    f.oi_above_spot_pc,
    f.oi_above_spot_co,
    f.oi_below_spot_pc,
    f.oi_below_spot_co,
    f.oi_above_below_ratio_pc,
    f.oi_above_below_ratio_co,
    f.pct_oi_within_5pct_pc,
    f.pct_oi_within_5pct_co,
    f.pct_oi_within_10pct_pc,
    f.pct_oi_within_10pct_co,
    f.pct_oi_above_spot_pc,
    f.pct_oi_above_spot_co,
    f.pct_oi_below_spot_pc,
    f.pct_oi_below_spot_co,
    f.oi_weighted_call_minus_spot_pc,
    f.oi_weighted_call_minus_spot_co,
    f.oi_weighted_put_minus_spot_pc,
    f.oi_weighted_put_minus_spot_co,
    f.oi_weighted_all_minus_spot_pc,
    f.oi_weighted_all_minus_spot_co,
    f.oi_weighted_call_div_spot_pc,
    f.oi_weighted_call_div_spot_co,
    f.oi_weighted_put_div_spot_pc,
    f.oi_weighted_put_div_spot_co,
    f.oi_weighted_all_div_spot_pc,
    f.oi_weighted_all_div_spot_co,
    f.oi_weighted_all_0_30d_div_spot_pc,
    f.oi_weighted_all_0_30d_div_spot_co,
    f.oi_weighted_call_0_30d_div_spot_pc,
    f.oi_weighted_call_0_30d_div_spot_co,
    f.oi_weighted_put_0_30d_div_spot_pc,
    f.oi_weighted_put_0_30d_div_spot_co,
    f.oi_weighted_all_31_90d_div_spot_pc,
    f.oi_weighted_all_31_90d_div_spot_co,
    f.oi_weighted_call_31_90d_div_spot_pc,
    f.oi_weighted_call_31_90d_div_spot_co,
    f.oi_weighted_put_31_90d_div_spot_pc,
    f.oi_weighted_put_31_90d_div_spot_co,
    f.oi_weighted_next_monthly_div_spot_pc,
    f.oi_weighted_next_monthly_div_spot_co,
    f.d1_oi_weighted_all_div_spot_change_pc,
    f.d1_oi_weighted_all_div_spot_change_co,
    f.d5_oi_weighted_all_div_spot_change_pc,
    f.d5_oi_weighted_all_div_spot_change_co,
    f.zscore_oi_weighted_all_div_spot_3m_pc,
    f.zscore_oi_weighted_all_div_spot_3m_co,
    f.zscore_oi_above_below_ratio_3m_pc,
    f.zscore_oi_above_below_ratio_3m_co,
    f.ret_5d,
    f.ret_10d,
    f.ret_20d,
    f.pct_from_ma20,
    f.pct_from_ma50,
    f.pct_from_52w_high,
    f.pct_from_52w_low,
    f.donchian_pos_20d,
    f.ma20_slope_5d,
    f.pct_up_days_20d,
    f.rv_ratio_5d_20d,
    f.cum_signed_vol_20d,
    f.atr_normalized_ret_5d,
    f.zscore_price_vs_ma20,
    f.zscore_price_vs_ma50,
    f.zscore_underlying_vol_20d,
    f.relative_strength_vs_spy_20d,
    f.put_call_ratio_vol,
    f.vol_oi_ratio_all,
    f.vol_oi_ratio_call,
    f.vol_oi_ratio_put,
    f.pct_vol_0_30d,
    f.pct_vol_31_90d,
    f.net_new_oi_div_vol,
    f.zscore_put_call_ratio_vol,
    f.zscore_vol_oi_ratio_all,
    f.zscore_vol_oi_ratio_call,
    f.zscore_vol_oi_ratio_put,
    f.atm_iv_7d,
    f.atm_iv_30d,
    f.atm_iv_90d,
    f.iv_25d_call_30d,
    f.iv_25d_put_30d,
    f.rr_25d_30d,
    f.bf_25d_30d,
    f.skew_25p_atm_30d,
    f.skew_atm_25c_30d,
    f.term_7d_30d,
    f.term_30d_90d,
    f.vrp_30d,
    f.iv_rv_ratio_30d,
    f.d1_atm_iv_7d_change,
    f.d5_atm_iv_7d_change,
    f.d1_atm_iv_30d_change,
    f.d5_atm_iv_30d_change,
    f.zscore_iv_7d,
    f.zscore_iv_30d,
    f.zscore_iv_90d,
    f.zscore_rr_25d_30d,
    f.zscore_term_7d_30d,
    f.zscore_term_30d_90d,
    f.zscore_vrp_30d,
    f.zscore_iv_rv_ratio_30d,
    f.vol_weighted_call_div_spot_pc,
    f.vol_weighted_put_div_spot_pc,
    f.vol_weighted_all_div_spot_pc,
    f.vol_above_below_ratio_pc,
    f.pct_vol_within_5pct_pc,
    f.pct_vol_within_10pct_pc,
    f.zscore_vol_above_below_ratio_pc,
    f.weighted_avg_dte_vol,
    f.iv_25d_call_7d,
    f.iv_25d_put_7d,
    f.rr_25d_7d,
    f.bf_25d_7d,
    f.skew_25p_atm_7d,
    f.skew_atm_25c_7d,
    f.zscore_rr_25d_7d,
    f.ret_1d_fwd_cc,
    f.ret_3d_fwd_cc,
    f.ret_5d_fwd_cc,
    f.ret_7d_fwd_cc,
    f.ret_10d_fwd_cc,
    f.ret_20d_fwd_cc,
    o.close AS close_today,
    o.adj_close AS adj_close_today
   FROM (public.daily_features f
     LEFT JOIN public.underlying_ohlc o USING (ticker, trade_date))
  ORDER BY f.ticker, f.trade_date;


ALTER VIEW public.v_features_with_returns OWNER TO portfolio;

--
-- Name: wf_bins; Type: TABLE; Schema: public; Owner: portfolio
--

CREATE TABLE public.wf_bins (
    ticker text NOT NULL,
    trade_date date NOT NULL,
    frac_atm_iv_30d double precision,
    bin20_atm_iv_30d smallint DEFAULT 0 NOT NULL,
    frac_atm_iv_7d double precision,
    bin20_atm_iv_7d smallint DEFAULT 0 NOT NULL,
    frac_atm_iv_90d double precision,
    bin20_atm_iv_90d smallint DEFAULT 0 NOT NULL,
    frac_atr_normalized_ret_5d double precision,
    bin20_atr_normalized_ret_5d smallint DEFAULT 0 NOT NULL,
    frac_call_oi double precision,
    bin20_call_oi smallint DEFAULT 0 NOT NULL,
    frac_cum_signed_vol_20d double precision,
    bin20_cum_signed_vol_20d smallint DEFAULT 0 NOT NULL,
    frac_d1_atm_iv_30d_change double precision,
    bin20_d1_atm_iv_30d_change smallint DEFAULT 0 NOT NULL,
    frac_d1_atm_iv_7d_change double precision,
    bin20_d1_atm_iv_7d_change smallint DEFAULT 0 NOT NULL,
    frac_d1_d5_ratio_total_oi_pct_change double precision,
    bin20_d1_d5_ratio_total_oi_pct_change smallint DEFAULT 0 NOT NULL,
    frac_d1_oi_weighted_all_div_spot_change_co double precision,
    bin20_d1_oi_weighted_all_div_spot_change_co smallint DEFAULT 0 NOT NULL,
    frac_d1_oi_weighted_all_div_spot_change_pc double precision,
    bin20_d1_oi_weighted_all_div_spot_change_pc smallint DEFAULT 0 NOT NULL,
    frac_d1_put_call_oi_ratio_change double precision,
    bin20_d1_put_call_oi_ratio_change smallint DEFAULT 0 NOT NULL,
    frac_d1_total_oi_change double precision,
    bin20_d1_total_oi_change smallint DEFAULT 0 NOT NULL,
    frac_d1_total_oi_pct_change double precision,
    bin20_d1_total_oi_pct_change smallint DEFAULT 0 NOT NULL,
    frac_d20_total_oi_change double precision,
    bin20_d20_total_oi_change smallint DEFAULT 0 NOT NULL,
    frac_d5_atm_iv_30d_change double precision,
    bin20_d5_atm_iv_30d_change smallint DEFAULT 0 NOT NULL,
    frac_d5_atm_iv_7d_change double precision,
    bin20_d5_atm_iv_7d_change smallint DEFAULT 0 NOT NULL,
    frac_d5_oi_weighted_all_div_spot_change_co double precision,
    bin20_d5_oi_weighted_all_div_spot_change_co smallint DEFAULT 0 NOT NULL,
    frac_d5_oi_weighted_all_div_spot_change_pc double precision,
    bin20_d5_oi_weighted_all_div_spot_change_pc smallint DEFAULT 0 NOT NULL,
    frac_d5_put_call_oi_ratio_change double precision,
    bin20_d5_put_call_oi_ratio_change smallint DEFAULT 0 NOT NULL,
    frac_d5_total_oi_change double precision,
    bin20_d5_total_oi_change smallint DEFAULT 0 NOT NULL,
    frac_d5_total_oi_pct_change double precision,
    bin20_d5_total_oi_pct_change smallint DEFAULT 0 NOT NULL,
    frac_donchian_pos_20d double precision,
    bin20_donchian_pos_20d smallint DEFAULT 0 NOT NULL,
    frac_iv_rv_ratio_30d double precision,
    bin20_iv_rv_ratio_30d smallint DEFAULT 0 NOT NULL,
    frac_ma20_slope_5d double precision,
    bin20_ma20_slope_5d smallint DEFAULT 0 NOT NULL,
    frac_max_oi_strike_call double precision,
    bin20_max_oi_strike_call smallint DEFAULT 0 NOT NULL,
    frac_max_oi_strike_put double precision,
    bin20_max_oi_strike_put smallint DEFAULT 0 NOT NULL,
    frac_net_new_oi_div_vol double precision,
    bin20_net_new_oi_div_vol smallint DEFAULT 0 NOT NULL,
    frac_oi_above_below_ratio_co double precision,
    bin20_oi_above_below_ratio_co smallint DEFAULT 0 NOT NULL,
    frac_oi_above_below_ratio_pc double precision,
    bin20_oi_above_below_ratio_pc smallint DEFAULT 0 NOT NULL,
    frac_oi_above_spot_co double precision,
    bin20_oi_above_spot_co smallint DEFAULT 0 NOT NULL,
    frac_oi_above_spot_pc double precision,
    bin20_oi_above_spot_pc smallint DEFAULT 0 NOT NULL,
    frac_oi_below_spot_co double precision,
    bin20_oi_below_spot_co smallint DEFAULT 0 NOT NULL,
    frac_oi_below_spot_pc double precision,
    bin20_oi_below_spot_pc smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_all double precision,
    bin20_oi_weighted_all smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_all_0_30d double precision,
    bin20_oi_weighted_all_0_30d smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_all_0_30d_div_spot_co double precision,
    bin20_oi_weighted_all_0_30d_div_spot_co smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_all_0_30d_div_spot_pc double precision,
    bin20_oi_weighted_all_0_30d_div_spot_pc smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_all_31_90d double precision,
    bin20_oi_weighted_all_31_90d smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_all_31_90d_div_spot_co double precision,
    bin20_oi_weighted_all_31_90d_div_spot_co smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_all_31_90d_div_spot_pc double precision,
    bin20_oi_weighted_all_31_90d_div_spot_pc smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_all_div_spot_co double precision,
    bin20_oi_weighted_all_div_spot_co smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_all_div_spot_pc double precision,
    bin20_oi_weighted_all_div_spot_pc smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_all_minus_spot_co double precision,
    bin20_oi_weighted_all_minus_spot_co smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_all_minus_spot_pc double precision,
    bin20_oi_weighted_all_minus_spot_pc smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_call double precision,
    bin20_oi_weighted_call smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_call_0_30d double precision,
    bin20_oi_weighted_call_0_30d smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_call_0_30d_div_spot_co double precision,
    bin20_oi_weighted_call_0_30d_div_spot_co smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_call_0_30d_div_spot_pc double precision,
    bin20_oi_weighted_call_0_30d_div_spot_pc smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_call_31_90d double precision,
    bin20_oi_weighted_call_31_90d smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_call_31_90d_div_spot_co double precision,
    bin20_oi_weighted_call_31_90d_div_spot_co smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_call_31_90d_div_spot_pc double precision,
    bin20_oi_weighted_call_31_90d_div_spot_pc smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_call_div_spot_co double precision,
    bin20_oi_weighted_call_div_spot_co smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_call_div_spot_pc double precision,
    bin20_oi_weighted_call_div_spot_pc smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_call_minus_spot_co double precision,
    bin20_oi_weighted_call_minus_spot_co smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_call_minus_spot_pc double precision,
    bin20_oi_weighted_call_minus_spot_pc smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_next_monthly_div_spot_co double precision,
    bin20_oi_weighted_next_monthly_div_spot_co smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_next_monthly_div_spot_pc double precision,
    bin20_oi_weighted_next_monthly_div_spot_pc smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_put double precision,
    bin20_oi_weighted_put smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_put_0_30d double precision,
    bin20_oi_weighted_put_0_30d smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_put_0_30d_div_spot_co double precision,
    bin20_oi_weighted_put_0_30d_div_spot_co smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_put_0_30d_div_spot_pc double precision,
    bin20_oi_weighted_put_0_30d_div_spot_pc smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_put_31_90d double precision,
    bin20_oi_weighted_put_31_90d smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_put_31_90d_div_spot_co double precision,
    bin20_oi_weighted_put_31_90d_div_spot_co smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_put_31_90d_div_spot_pc double precision,
    bin20_oi_weighted_put_31_90d_div_spot_pc smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_put_div_spot_co double precision,
    bin20_oi_weighted_put_div_spot_co smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_put_div_spot_pc double precision,
    bin20_oi_weighted_put_div_spot_pc smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_put_minus_spot_co double precision,
    bin20_oi_weighted_put_minus_spot_co smallint DEFAULT 0 NOT NULL,
    frac_oi_weighted_put_minus_spot_pc double precision,
    bin20_oi_weighted_put_minus_spot_pc smallint DEFAULT 0 NOT NULL,
    frac_oi_within_10pct_co double precision,
    bin20_oi_within_10pct_co smallint DEFAULT 0 NOT NULL,
    frac_oi_within_10pct_pc double precision,
    bin20_oi_within_10pct_pc smallint DEFAULT 0 NOT NULL,
    frac_oi_within_5pct_co double precision,
    bin20_oi_within_5pct_co smallint DEFAULT 0 NOT NULL,
    frac_oi_within_5pct_pc double precision,
    bin20_oi_within_5pct_pc smallint DEFAULT 0 NOT NULL,
    frac_pct_from_52w_high double precision,
    bin20_pct_from_52w_high smallint DEFAULT 0 NOT NULL,
    frac_pct_from_52w_low double precision,
    bin20_pct_from_52w_low smallint DEFAULT 0 NOT NULL,
    frac_pct_from_ma20 double precision,
    bin20_pct_from_ma20 smallint DEFAULT 0 NOT NULL,
    frac_pct_from_ma50 double precision,
    bin20_pct_from_ma50 smallint DEFAULT 0 NOT NULL,
    frac_pct_oi_0_30d double precision,
    bin20_pct_oi_0_30d smallint DEFAULT 0 NOT NULL,
    frac_pct_oi_31_90d double precision,
    bin20_pct_oi_31_90d smallint DEFAULT 0 NOT NULL,
    frac_pct_oi_91_365d double precision,
    bin20_pct_oi_91_365d smallint DEFAULT 0 NOT NULL,
    frac_pct_oi_above_spot_co double precision,
    bin20_pct_oi_above_spot_co smallint DEFAULT 0 NOT NULL,
    frac_pct_oi_above_spot_pc double precision,
    bin20_pct_oi_above_spot_pc smallint DEFAULT 0 NOT NULL,
    frac_pct_oi_below_spot_co double precision,
    bin20_pct_oi_below_spot_co smallint DEFAULT 0 NOT NULL,
    frac_pct_oi_below_spot_pc double precision,
    bin20_pct_oi_below_spot_pc smallint DEFAULT 0 NOT NULL,
    frac_pct_oi_in_front_expiry double precision,
    bin20_pct_oi_in_front_expiry smallint DEFAULT 0 NOT NULL,
    frac_pct_oi_next_monthly double precision,
    bin20_pct_oi_next_monthly smallint DEFAULT 0 NOT NULL,
    frac_pct_oi_within_10pct_co double precision,
    bin20_pct_oi_within_10pct_co smallint DEFAULT 0 NOT NULL,
    frac_pct_oi_within_10pct_pc double precision,
    bin20_pct_oi_within_10pct_pc smallint DEFAULT 0 NOT NULL,
    frac_pct_oi_within_5pct_co double precision,
    bin20_pct_oi_within_5pct_co smallint DEFAULT 0 NOT NULL,
    frac_pct_oi_within_5pct_pc double precision,
    bin20_pct_oi_within_5pct_pc smallint DEFAULT 0 NOT NULL,
    frac_pct_up_days_20d double precision,
    bin20_pct_up_days_20d smallint DEFAULT 0 NOT NULL,
    frac_pct_vol_0_30d double precision,
    bin20_pct_vol_0_30d smallint DEFAULT 0 NOT NULL,
    frac_pct_vol_31_90d double precision,
    bin20_pct_vol_31_90d smallint DEFAULT 0 NOT NULL,
    frac_pct_vol_within_10pct_pc double precision,
    bin20_pct_vol_within_10pct_pc smallint DEFAULT 0 NOT NULL,
    frac_pct_vol_within_5pct_pc double precision,
    bin20_pct_vol_within_5pct_pc smallint DEFAULT 0 NOT NULL,
    frac_put_call_oi_ratio double precision,
    bin20_put_call_oi_ratio smallint DEFAULT 0 NOT NULL,
    frac_put_call_ratio_vol double precision,
    bin20_put_call_ratio_vol smallint DEFAULT 0 NOT NULL,
    frac_put_oi double precision,
    bin20_put_oi smallint DEFAULT 0 NOT NULL,
    frac_relative_strength_vs_spy_20d double precision,
    bin20_relative_strength_vs_spy_20d smallint DEFAULT 0 NOT NULL,
    frac_ret_10d double precision,
    bin20_ret_10d smallint DEFAULT 0 NOT NULL,
    frac_ret_20d double precision,
    bin20_ret_20d smallint DEFAULT 0 NOT NULL,
    frac_ret_5d double precision,
    bin20_ret_5d smallint DEFAULT 0 NOT NULL,
    frac_rv_20d double precision,
    bin20_rv_20d smallint DEFAULT 0 NOT NULL,
    frac_rv_5d double precision,
    bin20_rv_5d smallint DEFAULT 0 NOT NULL,
    frac_rv_ratio_5d_20d double precision,
    bin20_rv_ratio_5d_20d smallint DEFAULT 0 NOT NULL,
    frac_spot_co double precision,
    bin20_spot_co smallint DEFAULT 0 NOT NULL,
    frac_spot_pc double precision,
    bin20_spot_pc smallint DEFAULT 0 NOT NULL,
    frac_term_30d_90d double precision,
    bin20_term_30d_90d smallint DEFAULT 0 NOT NULL,
    frac_term_7d_30d double precision,
    bin20_term_7d_30d smallint DEFAULT 0 NOT NULL,
    frac_top10_strikes_pct_total_oi double precision,
    bin20_top10_strikes_pct_total_oi smallint DEFAULT 0 NOT NULL,
    frac_top5_strikes_pct_total_oi double precision,
    bin20_top5_strikes_pct_total_oi smallint DEFAULT 0 NOT NULL,
    frac_total_oi double precision,
    bin20_total_oi smallint DEFAULT 0 NOT NULL,
    frac_vol_above_below_ratio_pc double precision,
    bin20_vol_above_below_ratio_pc smallint DEFAULT 0 NOT NULL,
    frac_vol_oi_ratio_all double precision,
    bin20_vol_oi_ratio_all smallint DEFAULT 0 NOT NULL,
    frac_vol_oi_ratio_call double precision,
    bin20_vol_oi_ratio_call smallint DEFAULT 0 NOT NULL,
    frac_vol_oi_ratio_put double precision,
    bin20_vol_oi_ratio_put smallint DEFAULT 0 NOT NULL,
    frac_vol_weighted_all_div_spot_pc double precision,
    bin20_vol_weighted_all_div_spot_pc smallint DEFAULT 0 NOT NULL,
    frac_vol_weighted_call_div_spot_pc double precision,
    bin20_vol_weighted_call_div_spot_pc smallint DEFAULT 0 NOT NULL,
    frac_vol_weighted_put_div_spot_pc double precision,
    bin20_vol_weighted_put_div_spot_pc smallint DEFAULT 0 NOT NULL,
    frac_vrp_30d double precision,
    bin20_vrp_30d smallint DEFAULT 0 NOT NULL,
    frac_weighted_avg_dte double precision,
    bin20_weighted_avg_dte smallint DEFAULT 0 NOT NULL,
    frac_weighted_avg_dte_vol double precision,
    bin20_weighted_avg_dte_vol smallint DEFAULT 0 NOT NULL,
    frac_zscore_d1_oi_change_3m double precision,
    bin20_zscore_d1_oi_change_3m smallint DEFAULT 0 NOT NULL,
    frac_zscore_d5_oi_change_3m double precision,
    bin20_zscore_d5_oi_change_3m smallint DEFAULT 0 NOT NULL,
    frac_zscore_iv_30d double precision,
    bin20_zscore_iv_30d smallint DEFAULT 0 NOT NULL,
    frac_zscore_iv_7d double precision,
    bin20_zscore_iv_7d smallint DEFAULT 0 NOT NULL,
    frac_zscore_iv_90d double precision,
    bin20_zscore_iv_90d smallint DEFAULT 0 NOT NULL,
    frac_zscore_iv_rv_ratio_30d double precision,
    bin20_zscore_iv_rv_ratio_30d smallint DEFAULT 0 NOT NULL,
    frac_zscore_oi_above_below_ratio_3m_co double precision,
    bin20_zscore_oi_above_below_ratio_3m_co smallint DEFAULT 0 NOT NULL,
    frac_zscore_oi_above_below_ratio_3m_pc double precision,
    bin20_zscore_oi_above_below_ratio_3m_pc smallint DEFAULT 0 NOT NULL,
    frac_zscore_oi_weighted_all_div_spot_3m_co double precision,
    bin20_zscore_oi_weighted_all_div_spot_3m_co smallint DEFAULT 0 NOT NULL,
    frac_zscore_oi_weighted_all_div_spot_3m_pc double precision,
    bin20_zscore_oi_weighted_all_div_spot_3m_pc smallint DEFAULT 0 NOT NULL,
    frac_zscore_price_vs_ma20 double precision,
    bin20_zscore_price_vs_ma20 smallint DEFAULT 0 NOT NULL,
    frac_zscore_price_vs_ma50 double precision,
    bin20_zscore_price_vs_ma50 smallint DEFAULT 0 NOT NULL,
    frac_zscore_put_call_oi_ratio_3m double precision,
    bin20_zscore_put_call_oi_ratio_3m smallint DEFAULT 0 NOT NULL,
    frac_zscore_put_call_ratio_vol double precision,
    bin20_zscore_put_call_ratio_vol smallint DEFAULT 0 NOT NULL,
    frac_zscore_term_30d_90d double precision,
    bin20_zscore_term_30d_90d smallint DEFAULT 0 NOT NULL,
    frac_zscore_term_7d_30d double precision,
    bin20_zscore_term_7d_30d smallint DEFAULT 0 NOT NULL,
    frac_zscore_underlying_vol_20d double precision,
    bin20_zscore_underlying_vol_20d smallint DEFAULT 0 NOT NULL,
    frac_zscore_vol_above_below_ratio_pc double precision,
    bin20_zscore_vol_above_below_ratio_pc smallint DEFAULT 0 NOT NULL,
    frac_zscore_vol_oi_ratio_all double precision,
    bin20_zscore_vol_oi_ratio_all smallint DEFAULT 0 NOT NULL,
    frac_zscore_vol_oi_ratio_call double precision,
    bin20_zscore_vol_oi_ratio_call smallint DEFAULT 0 NOT NULL,
    frac_zscore_vol_oi_ratio_put double precision,
    bin20_zscore_vol_oi_ratio_put smallint DEFAULT 0 NOT NULL,
    frac_zscore_vrp_30d double precision,
    bin20_zscore_vrp_30d smallint DEFAULT 0 NOT NULL,
    frac_bf_25d_30d double precision,
    bin20_bf_25d_30d smallint DEFAULT 0 NOT NULL,
    frac_bf_25d_7d double precision,
    bin20_bf_25d_7d smallint DEFAULT 0 NOT NULL,
    frac_iv_25d_call_30d double precision,
    bin20_iv_25d_call_30d smallint DEFAULT 0 NOT NULL,
    frac_iv_25d_call_7d double precision,
    bin20_iv_25d_call_7d smallint DEFAULT 0 NOT NULL,
    frac_iv_25d_put_30d double precision,
    bin20_iv_25d_put_30d smallint DEFAULT 0 NOT NULL,
    frac_iv_25d_put_7d double precision,
    bin20_iv_25d_put_7d smallint DEFAULT 0 NOT NULL,
    frac_rr_25d_30d double precision,
    bin20_rr_25d_30d smallint DEFAULT 0 NOT NULL,
    frac_rr_25d_7d double precision,
    bin20_rr_25d_7d smallint DEFAULT 0 NOT NULL,
    frac_skew_25p_atm_30d double precision,
    bin20_skew_25p_atm_30d smallint DEFAULT 0 NOT NULL,
    frac_skew_25p_atm_7d double precision,
    bin20_skew_25p_atm_7d smallint DEFAULT 0 NOT NULL,
    frac_skew_atm_25c_30d double precision,
    bin20_skew_atm_25c_30d smallint DEFAULT 0 NOT NULL,
    frac_skew_atm_25c_7d double precision,
    bin20_skew_atm_25c_7d smallint DEFAULT 0 NOT NULL,
    frac_zscore_rr_25d_30d double precision,
    bin20_zscore_rr_25d_30d smallint DEFAULT 0 NOT NULL,
    frac_zscore_rr_25d_7d double precision,
    bin20_zscore_rr_25d_7d smallint DEFAULT 0 NOT NULL
);


ALTER TABLE public.wf_bins OWNER TO portfolio;

--
-- Name: backtest_call_spread id; Type: DEFAULT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.backtest_call_spread ALTER COLUMN id SET DEFAULT nextval('public.backtest_call_spread_id_seq'::regclass);


--
-- Name: equity_structure_presets id; Type: DEFAULT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.equity_structure_presets ALTER COLUMN id SET DEFAULT nextval('public.equity_structure_presets_id_seq'::regclass);


--
-- Name: signals id; Type: DEFAULT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.signals ALTER COLUMN id SET DEFAULT nextval('public.signals_id_seq'::regclass);


--
-- Name: ticker_analysis_layouts id; Type: DEFAULT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.ticker_analysis_layouts ALTER COLUMN id SET DEFAULT nextval('public.ticker_analysis_layouts_id_seq'::regclass);


--
-- Name: analyze_cache_outcome analyze_cache_outcome_pkey; Type: CONSTRAINT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.analyze_cache_outcome
    ADD CONSTRAINT analyze_cache_outcome_pkey PRIMARY KEY (cache_key, outcome);


--
-- Name: analyze_cache_slim analyze_cache_slim_pkey; Type: CONSTRAINT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.analyze_cache_slim
    ADD CONSTRAINT analyze_cache_slim_pkey PRIMARY KEY (cache_key);


--
-- Name: analyze_cache_trade_meta analyze_cache_trade_meta_pkey; Type: CONSTRAINT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.analyze_cache_trade_meta
    ADD CONSTRAINT analyze_cache_trade_meta_pkey PRIMARY KEY (cache_key);


--
-- Name: analyze_primary_cache analyze_primary_cache_pkey; Type: CONSTRAINT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.analyze_primary_cache
    ADD CONSTRAINT analyze_primary_cache_pkey PRIMARY KEY (cache_key);


--
-- Name: backtest_call_spread backtest_call_spread_pkey; Type: CONSTRAINT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.backtest_call_spread
    ADD CONSTRAINT backtest_call_spread_pkey PRIMARY KEY (id);


--
-- Name: backtest_call_spread backtest_call_spread_ticker_trade_date_exit_date_key; Type: CONSTRAINT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.backtest_call_spread
    ADD CONSTRAINT backtest_call_spread_ticker_trade_date_exit_date_key UNIQUE (ticker, trade_date, exit_date);


--
-- Name: corner_scan_1f corner_scan_1f_pkey; Type: CONSTRAINT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.corner_scan_1f
    ADD CONSTRAINT corner_scan_1f_pkey PRIMARY KEY (metric, extreme, outcome, mode);


--
-- Name: corner_scan_2f corner_scan_2f_pkey; Type: CONSTRAINT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.corner_scan_2f
    ADD CONSTRAINT corner_scan_2f_pkey PRIMARY KEY (primary_metric, secondary_metric, corner_direction, outcome, mode);


--
-- Name: corner_scan_notes corner_scan_notes_pkey; Type: CONSTRAINT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.corner_scan_notes
    ADD CONSTRAINT corner_scan_notes_pkey PRIMARY KEY (primary_metric, secondary_metric, corner_direction, outcome);


--
-- Name: daily_features daily_features_pkey; Type: CONSTRAINT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.daily_features
    ADD CONSTRAINT daily_features_pkey PRIMARY KEY (ticker, trade_date);


--
-- Name: earnings_calendar earnings_calendar_pkey; Type: CONSTRAINT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.earnings_calendar
    ADD CONSTRAINT earnings_calendar_pkey PRIMARY KEY (ticker, earnings_date);


--
-- Name: earnings_coverage earnings_coverage_pkey; Type: CONSTRAINT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.earnings_coverage
    ADD CONSTRAINT earnings_coverage_pkey PRIMARY KEY (ticker);


--
-- Name: equity_atm equity_atm_ticker_trade_date_snapshot_dte_key; Type: CONSTRAINT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.equity_atm
    ADD CONSTRAINT equity_atm_ticker_trade_date_snapshot_dte_key UNIQUE (ticker, trade_date, snapshot, dte);


--
-- Name: equity_metrics_catalog equity_metrics_catalog_pkey; Type: CONSTRAINT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.equity_metrics_catalog
    ADD CONSTRAINT equity_metrics_catalog_pkey PRIMARY KEY (column_name);


--
-- Name: equity_metrics equity_metrics_pkey; Type: CONSTRAINT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.equity_metrics
    ADD CONSTRAINT equity_metrics_pkey PRIMARY KEY (ticker, trade_date, snapshot);


--
-- Name: equity_metrics_z equity_metrics_z_pkey; Type: CONSTRAINT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.equity_metrics_z
    ADD CONSTRAINT equity_metrics_z_pkey PRIMARY KEY (ticker, trade_date, snapshot);


--
-- Name: equity_structure_presets equity_structure_presets_name_key; Type: CONSTRAINT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.equity_structure_presets
    ADD CONSTRAINT equity_structure_presets_name_key UNIQUE (name);


--
-- Name: equity_structure_presets equity_structure_presets_pkey; Type: CONSTRAINT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.equity_structure_presets
    ADD CONSTRAINT equity_structure_presets_pkey PRIMARY KEY (id);


--
-- Name: equity_surface_diagnostics equity_surface_diagnostics_pkey; Type: CONSTRAINT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.equity_surface_diagnostics
    ADD CONSTRAINT equity_surface_diagnostics_pkey PRIMARY KEY (ticker, trade_date, snapshot, expiry);


--
-- Name: equity_surface equity_surface_ticker_trade_date_snapshot_dte_put_delta_key; Type: CONSTRAINT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.equity_surface
    ADD CONSTRAINT equity_surface_ticker_trade_date_snapshot_dte_put_delta_key UNIQUE (ticker, trade_date, snapshot, dte, put_delta);


--
-- Name: global_bins_cache global_bins_cache_pkey; Type: CONSTRAINT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.global_bins_cache
    ADD CONSTRAINT global_bins_cache_pkey PRIMARY KEY (cache_key);


--
-- Name: ic_batch_cache ic_batch_cache_pkey; Type: CONSTRAINT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.ic_batch_cache
    ADD CONSTRAINT ic_batch_cache_pkey PRIMARY KEY (cache_key);


--
-- Name: is_bins is_bins_pkey; Type: CONSTRAINT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.is_bins
    ADD CONSTRAINT is_bins_pkey PRIMARY KEY (ticker, trade_date);


--
-- Name: metric_classification metric_classification_pkey; Type: CONSTRAINT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.metric_classification
    ADD CONSTRAINT metric_classification_pkey PRIMARY KEY (metric);


--
-- Name: option_iv_daily option_iv_daily_pkey; Type: CONSTRAINT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.option_iv_daily
    ADD CONSTRAINT option_iv_daily_pkey PRIMARY KEY (ticker, trade_date);


--
-- Name: option_volume_daily option_volume_daily_pkey; Type: CONSTRAINT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.option_volume_daily
    ADD CONSTRAINT option_volume_daily_pkey PRIMARY KEY (ticker, trade_date);


--
-- Name: research_charts research_charts_pkey; Type: CONSTRAINT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.research_charts
    ADD CONSTRAINT research_charts_pkey PRIMARY KEY (id);


--
-- Name: research_followups research_followups_pkey; Type: CONSTRAINT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.research_followups
    ADD CONSTRAINT research_followups_pkey PRIMARY KEY (id);


--
-- Name: research_results research_results_pkey; Type: CONSTRAINT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.research_results
    ADD CONSTRAINT research_results_pkey PRIMARY KEY (id);


--
-- Name: research_runs research_runs_pkey; Type: CONSTRAINT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.research_runs
    ADD CONSTRAINT research_runs_pkey PRIMARY KEY (id);


--
-- Name: research_series research_series_pkey; Type: CONSTRAINT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.research_series
    ADD CONSTRAINT research_series_pkey PRIMARY KEY (id);


--
-- Name: sec_scan_cache sec_scan_cache_pkey; Type: CONSTRAINT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.sec_scan_cache
    ADD CONSTRAINT sec_scan_cache_pkey PRIMARY KEY (structural_key);


--
-- Name: signals signals_pkey; Type: CONSTRAINT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.signals
    ADD CONSTRAINT signals_pkey PRIMARY KEY (id);


--
-- Name: ticker_analysis_chain_cache ticker_analysis_chain_cache_pkey; Type: CONSTRAINT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.ticker_analysis_chain_cache
    ADD CONSTRAINT ticker_analysis_chain_cache_pkey PRIMARY KEY (cache_key);


--
-- Name: ticker_analysis_layouts ticker_analysis_layouts_name_key; Type: CONSTRAINT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.ticker_analysis_layouts
    ADD CONSTRAINT ticker_analysis_layouts_name_key UNIQUE (name);


--
-- Name: ticker_analysis_layouts ticker_analysis_layouts_pkey; Type: CONSTRAINT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.ticker_analysis_layouts
    ADD CONSTRAINT ticker_analysis_layouts_pkey PRIMARY KEY (id);


--
-- Name: trade_path_rules trade_path_rules_pkey; Type: CONSTRAINT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.trade_path_rules
    ADD CONSTRAINT trade_path_rules_pkey PRIMARY KEY (rule_key);


--
-- Name: trade_paths_manifest trade_paths_manifest_pkey; Type: CONSTRAINT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.trade_paths_manifest
    ADD CONSTRAINT trade_paths_manifest_pkey PRIMARY KEY (ticker, entry_anchor);


--
-- Name: trade_paths trade_paths_pkey; Type: CONSTRAINT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.trade_paths
    ADD CONSTRAINT trade_paths_pkey PRIMARY KEY (ticker, trade_date, entry_anchor);


--
-- Name: tt_bins tt_bins_pkey; Type: CONSTRAINT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.tt_bins
    ADD CONSTRAINT tt_bins_pkey PRIMARY KEY (ticker, trade_date);


--
-- Name: tt_thresholds tt_thresholds_pkey; Type: CONSTRAINT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.tt_thresholds
    ADD CONSTRAINT tt_thresholds_pkey PRIMARY KEY (metric, ticker, cutoff_date);


--
-- Name: underlying_ohlc underlying_ohlc_pkey; Type: CONSTRAINT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.underlying_ohlc
    ADD CONSTRAINT underlying_ohlc_pkey PRIMARY KEY (ticker, trade_date);


--
-- Name: wf_bins wf_bins_pkey; Type: CONSTRAINT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.wf_bins
    ADD CONSTRAINT wf_bins_pkey PRIMARY KEY (ticker, trade_date);


--
-- Name: analyze_cache_slim_last_accessed; Type: INDEX; Schema: public; Owner: portfolio
--

CREATE INDEX analyze_cache_slim_last_accessed ON public.analyze_cache_slim USING btree (last_accessed);


--
-- Name: analyze_primary_cache_last_accessed; Type: INDEX; Schema: public; Owner: portfolio
--

CREATE INDEX analyze_primary_cache_last_accessed ON public.analyze_primary_cache USING btree (last_accessed);


--
-- Name: daily_features_date_idx; Type: INDEX; Schema: public; Owner: portfolio
--

CREATE INDEX daily_features_date_idx ON public.daily_features USING btree (trade_date);


--
-- Name: idx_rc_run; Type: INDEX; Schema: public; Owner: portfolio
--

CREATE INDEX idx_rc_run ON public.research_charts USING btree (run_id);


--
-- Name: idx_rr_run; Type: INDEX; Schema: public; Owner: portfolio
--

CREATE INDEX idx_rr_run ON public.research_results USING btree (run_id);


--
-- Name: idx_rr_tick; Type: INDEX; Schema: public; Owner: portfolio
--

CREATE INDEX idx_rr_tick ON public.research_results USING btree (ticker);


--
-- Name: idx_rr_type; Type: INDEX; Schema: public; Owner: portfolio
--

CREATE INDEX idx_rr_type ON public.research_results USING btree (analysis_type);


--
-- Name: idx_rs_run; Type: INDEX; Schema: public; Owner: portfolio
--

CREATE INDEX idx_rs_run ON public.research_series USING btree (run_id);


--
-- Name: ix_earnings_calendar_lookup; Type: INDEX; Schema: public; Owner: portfolio
--

CREATE INDEX ix_earnings_calendar_lookup ON public.earnings_calendar USING btree (ticker, earnings_date);


--
-- Name: ix_equity_metrics_catalog_family; Type: INDEX; Schema: public; Owner: portfolio
--

CREATE INDEX ix_equity_metrics_catalog_family ON public.equity_metrics_catalog USING btree (family, tenor);


--
-- Name: ix_equity_metrics_scan; Type: INDEX; Schema: public; Owner: portfolio
--

CREATE INDEX ix_equity_metrics_scan ON ONLY public.equity_metrics USING btree (trade_date, snapshot);


--
-- Name: ix_equity_metrics_z_scan; Type: INDEX; Schema: public; Owner: portfolio
--

CREATE INDEX ix_equity_metrics_z_scan ON ONLY public.equity_metrics_z USING btree (trade_date, snapshot);


--
-- Name: ix_equity_surface_diag_date; Type: INDEX; Schema: public; Owner: portfolio
--

CREATE INDEX ix_equity_surface_diag_date ON public.equity_surface_diagnostics USING btree (trade_date, snapshot);


--
-- Name: ix_equity_surface_latest; Type: INDEX; Schema: public; Owner: portfolio
--

CREATE INDEX ix_equity_surface_latest ON ONLY public.equity_surface USING btree (ticker, trade_date, snapshot DESC);


--
-- Name: ix_signals_match; Type: INDEX; Schema: public; Owner: portfolio
--

CREATE INDEX ix_signals_match ON public.signals USING btree (primary_metric, secondary_metric, corner, outcome) WHERE (corner IS NOT NULL);


--
-- Name: ix_trade_paths_date; Type: INDEX; Schema: public; Owner: portfolio
--

CREATE INDEX ix_trade_paths_date ON public.trade_paths USING btree (trade_date, entry_anchor);


--
-- Name: ix_trade_paths_status; Type: INDEX; Schema: public; Owner: portfolio
--

CREATE INDEX ix_trade_paths_status ON public.trade_paths USING btree (path_status);


--
-- Name: ix_tt_thresholds_ticker; Type: INDEX; Schema: public; Owner: portfolio
--

CREATE INDEX ix_tt_thresholds_ticker ON public.tt_thresholds USING btree (ticker, metric, cutoff_date);


--
-- Name: sec_scan_cache_cached_at; Type: INDEX; Schema: public; Owner: portfolio
--

CREATE INDEX sec_scan_cache_cached_at ON public.sec_scan_cache USING btree (cached_at);


--
-- Name: underlying_ohlc_date_idx; Type: INDEX; Schema: public; Owner: portfolio
--

CREATE INDEX underlying_ohlc_date_idx ON public.underlying_ohlc USING btree (trade_date);


--
-- Name: research_charts research_charts_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.research_charts
    ADD CONSTRAINT research_charts_run_id_fkey FOREIGN KEY (run_id) REFERENCES public.research_runs(id) ON DELETE CASCADE;


--
-- Name: research_followups research_followups_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.research_followups
    ADD CONSTRAINT research_followups_run_id_fkey FOREIGN KEY (run_id) REFERENCES public.research_runs(id) ON DELETE CASCADE;


--
-- Name: research_results research_results_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.research_results
    ADD CONSTRAINT research_results_run_id_fkey FOREIGN KEY (run_id) REFERENCES public.research_runs(id) ON DELETE CASCADE;


--
-- Name: research_series research_series_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: portfolio
--

ALTER TABLE ONLY public.research_series
    ADD CONSTRAINT research_series_run_id_fkey FOREIGN KEY (run_id) REFERENCES public.research_runs(id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

\unrestrict eCgUI3D9eZ34cZaLTbbfsjgQpew7mptovpPXwL1LZLYbFotwjAygkUO9emoQQdE

