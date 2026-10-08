-- =====================================================================
-- gold.payment_summary
-- Grain : one row per (order date, channel, payment_method)
-- Source: silver.fact_payments joined to ONE row per order taken from
--         silver.fact_order_items (supplies order date and channel).
--
-- Payments are reported AS RECORDED. There is NO cancelled/returned
-- filter here: this table describes money movement, so a SUCCESS payment
-- on a CANCELLED order must still appear.
--
-- Date used: the ORDER date, not paid_at. In the data only SUCCESS
-- payments have paid_at; PENDING, FAILED and REFUNDED have none, so a
-- paid_at grain would silently drop 43,742 payments.
--
-- How to read the measures:
--   * Each payment has exactly one status, so the four status counts add
--     up to total_payments (and the four status amounts to total_amount).
--   * amount is always the FULL order value, also on FAILED / PENDING /
--     REFUNDED payments. So failed_amount and pending_amount are the value
--     of those attempts, NOT money that moved.
--   * total_amount therefore is NOT revenue. Do not use it as such; revenue
--     lives in gold.daily_sales_summary.
--   * Refunds are stored as positive amounts, not negatives.
--
-- All columns are additive. Rates are NOT stored; compute in Power BI:
--   success rate = SUM(success_payments) / SUM(total_payments)
--   failure rate = SUM(failed_payments)  / SUM(total_payments)
--
-- Join assumption (verified: 0 violations): exactly one payment per order
-- and no payment without an order. Payments without an order would be
-- dropped by the INNER JOIN; the reconciliation check catches that.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.payment_summary (
    date_key            INT            NOT NULL REFERENCES gold.dim_date (date_key),
    order_date          DATE           NOT NULL,
    channel             VARCHAR(50)    NOT NULL,
    payment_method      VARCHAR(50)    NOT NULL,

    -- payment counts
    total_payments      INT            NOT NULL,
    success_payments    INT            NOT NULL,
    failed_payments     INT            NOT NULL,
    pending_payments    INT            NOT NULL,
    refunded_payments   INT            NOT NULL,

    -- payment amounts (see notes above: amount = full order value)
    total_amount        DECIMAL(18,2)  NOT NULL,
    success_amount      DECIMAL(18,2)  NOT NULL,
    failed_amount       DECIMAL(18,2)  NOT NULL,
    pending_amount      DECIMAL(18,2)  NOT NULL,
    refunded_amount     DECIMAL(18,2)  NOT NULL,

    _loaded_at          TIMESTAMP      NOT NULL DEFAULT now(),
    PRIMARY KEY (date_key, channel, payment_method)
);

CREATE INDEX IF NOT EXISTS idx_payment_summary_date
    ON gold.payment_summary (order_date);