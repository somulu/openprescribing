WITH practice AS (
  SELECT
    month AS practice_month,
    SUM(IF(cost_savings_10 > 0, cost_savings_10, 0)) AS practice_cost_savings_10,
    SUM(IF(cost_savings_20 > 0, cost_savings_20, 0)) AS practice_cost_savings_20,
    SUM(IF(cost_savings_30 > 0, cost_savings_30, 0)) AS practice_cost_savings_30,
    SUM(IF(cost_savings_40 > 0, cost_savings_40, 0)) AS practice_cost_savings_40,
    SUM(IF(cost_savings_50 > 0, cost_savings_50, 0)) AS practice_cost_savings_50,
    SUM(IF(cost_savings_60 > 0, cost_savings_60, 0)) AS practice_cost_savings_60,
    SUM(IF(cost_savings_70 > 0, cost_savings_70, 0)) AS practice_cost_savings_70,
    SUM(IF(cost_savings_80 > 0, cost_savings_80, 0)) AS practice_cost_savings_80,
    SUM(IF(cost_savings_90 > 0, cost_savings_90, 0)) AS practice_cost_savings_90
  FROM
    {measures}.practice_data_{measure_id} GROUP BY month
),

pcn AS (
  SELECT
    month AS pcn_month,
    SUM(IF(cost_savings_10 > 0, cost_savings_10, 0)) AS pcn_cost_savings_10,
    SUM(IF(cost_savings_20 > 0, cost_savings_20, 0)) AS pcn_cost_savings_20,
    SUM(IF(cost_savings_30 > 0, cost_savings_30, 0)) AS pcn_cost_savings_30,
    SUM(IF(cost_savings_40 > 0, cost_savings_40, 0)) AS pcn_cost_savings_40,
    SUM(IF(cost_savings_50 > 0, cost_savings_50, 0)) AS pcn_cost_savings_50,
    SUM(IF(cost_savings_60 > 0, cost_savings_60, 0)) AS pcn_cost_savings_60,
    SUM(IF(cost_savings_70 > 0, cost_savings_70, 0)) AS pcn_cost_savings_70,
    SUM(IF(cost_savings_80 > 0, cost_savings_80, 0)) AS pcn_cost_savings_80,
    SUM(IF(cost_savings_90 > 0, cost_savings_90, 0)) AS pcn_cost_savings_90
  FROM
    {measures}.pcn_data_{measure_id} GROUP BY month
),

ccg AS (
  SELECT
    month AS ccg_month,
    SUM(IF(cost_savings_10 > 0, cost_savings_10, 0)) AS ccg_cost_savings_10,
    SUM(IF(cost_savings_20 > 0, cost_savings_20, 0)) AS ccg_cost_savings_20,
    SUM(IF(cost_savings_30 > 0, cost_savings_30, 0)) AS ccg_cost_savings_30,
    SUM(IF(cost_savings_40 > 0, cost_savings_40, 0)) AS ccg_cost_savings_40,
    SUM(IF(cost_savings_50 > 0, cost_savings_50, 0)) AS ccg_cost_savings_50,
    SUM(IF(cost_savings_60 > 0, cost_savings_60, 0)) AS ccg_cost_savings_60,
    SUM(IF(cost_savings_70 > 0, cost_savings_70, 0)) AS ccg_cost_savings_70,
    SUM(IF(cost_savings_80 > 0, cost_savings_80, 0)) AS ccg_cost_savings_80,
    SUM(IF(cost_savings_90 > 0, cost_savings_90, 0)) AS ccg_cost_savings_90
  FROM
    {measures}.ccg_data_{measure_id} GROUP BY month
),

stp AS (
  SELECT
    month AS stp_month,
    SUM(IF(cost_savings_10 > 0, cost_savings_10, 0)) AS stp_cost_savings_10,
    SUM(IF(cost_savings_20 > 0, cost_savings_20, 0)) AS stp_cost_savings_20,
    SUM(IF(cost_savings_30 > 0, cost_savings_30, 0)) AS stp_cost_savings_30,
    SUM(IF(cost_savings_40 > 0, cost_savings_40, 0)) AS stp_cost_savings_40,
    SUM(IF(cost_savings_50 > 0, cost_savings_50, 0)) AS stp_cost_savings_50,
    SUM(IF(cost_savings_60 > 0, cost_savings_60, 0)) AS stp_cost_savings_60,
    SUM(IF(cost_savings_70 > 0, cost_savings_70, 0)) AS stp_cost_savings_70,
    SUM(IF(cost_savings_80 > 0, cost_savings_80, 0)) AS stp_cost_savings_80,
    SUM(IF(cost_savings_90 > 0, cost_savings_90, 0)) AS stp_cost_savings_90
  FROM
    {measures}.stp_data_{measure_id} GROUP BY month
),

regtm AS (
  SELECT
    month AS regtm_month,
    SUM(IF(cost_savings_10 > 0, cost_savings_10, 0)) AS regtm_cost_savings_10,
    SUM(IF(cost_savings_20 > 0, cost_savings_20, 0)) AS regtm_cost_savings_20,
    SUM(IF(cost_savings_30 > 0, cost_savings_30, 0)) AS regtm_cost_savings_30,
    SUM(IF(cost_savings_40 > 0, cost_savings_40, 0)) AS regtm_cost_savings_40,
    SUM(IF(cost_savings_50 > 0, cost_savings_50, 0)) AS regtm_cost_savings_50,
    SUM(IF(cost_savings_60 > 0, cost_savings_60, 0)) AS regtm_cost_savings_60,
    SUM(IF(cost_savings_70 > 0, cost_savings_70, 0)) AS regtm_cost_savings_70,
    SUM(IF(cost_savings_80 > 0, cost_savings_80, 0)) AS regtm_cost_savings_80,
    SUM(IF(cost_savings_90 > 0, cost_savings_90, 0)) AS regtm_cost_savings_90
  FROM
    {measures}.regtm_data_{measure_id} GROUP BY month
),

global AS (
  SELECT
    month AS global_month,
    denominator AS global_denominator,
    numerator AS global_numerator,
    regtm_10th AS global_regtm_10th,
    regtm_20th AS global_regtm_20th,
    regtm_30th AS global_regtm_30th,
    regtm_40th AS global_regtm_40th,
    regtm_50th AS global_regtm_50th,
    regtm_60th AS global_regtm_60th,
    regtm_70th AS global_regtm_70th,
    regtm_80th AS global_regtm_80th,
    regtm_90th AS global_regtm_90th,
    stp_10th AS global_stp_10th,
    stp_20th AS global_stp_20th,
    stp_30th AS global_stp_30th,
    stp_40th AS global_stp_40th,
    stp_50th AS global_stp_50th,
    stp_60th AS global_stp_60th,
    stp_70th AS global_stp_70th,
    stp_80th AS global_stp_80th,
    stp_90th AS global_stp_90th,
    ccg_10th AS global_ccg_10th,
    ccg_20th AS global_ccg_20th,
    ccg_30th AS global_ccg_30th,
    ccg_40th AS global_ccg_40th,
    ccg_50th AS global_ccg_50th,
    ccg_60th AS global_ccg_60th,
    ccg_70th AS global_ccg_70th,
    ccg_80th AS global_ccg_80th,
    ccg_90th AS global_ccg_90th,
    pcn_10th AS global_pcn_10th,
    pcn_20th AS global_pcn_20th,
    pcn_30th AS global_pcn_30th,
    pcn_40th AS global_pcn_40th,
    pcn_50th AS global_pcn_50th,
    pcn_60th AS global_pcn_60th,
    pcn_70th AS global_pcn_70th,
    pcn_80th AS global_pcn_80th,
    pcn_90th AS global_pcn_90th,
    practice_10th AS global_practice_10th,
    practice_20th AS global_practice_20th,
    practice_30th AS global_practice_30th,
    practice_40th AS global_practice_40th,
    practice_50th AS global_practice_50th,
    practice_60th AS global_practice_60th,
    practice_70th AS global_practice_70th,
    practice_80th AS global_practice_80th,
    practice_90th AS global_practice_90th
  FROM
    {measures}.global_data_{measure_id}
)

SELECT *
FROM practice
INNER JOIN pcn
  ON practice_month = pcn_month
INNER JOIN ccg
  ON practice_month = ccg_month
INNER JOIN stp
  ON practice_month = stp_month
INNER JOIN regtm
  ON practice_month = regtm_month
INNER JOIN global
  ON practice_month = global_month
