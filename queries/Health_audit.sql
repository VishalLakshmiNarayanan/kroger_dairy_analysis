-- Health accessibility audit 
SELECT
  p.product_description,
  p.wellness_score,
  f.sale_price

FROM
  `kroger-analysis.Kroger_Products.dim_product` AS p
INNER JOIN
  `kroger-analysis.Kroger_Products.fact_pricing_inventory` AS f
  ON p.sku_id_pk = f.sku_id_fk
WHERE
  is_snap_eligible is TRUE
  AND
  p.wellness_score > 60
ORDER BY
  f.sale_price ASC
