-- Margin Erosion 

SELECT
  p.product_description,
  f.base_price,
  f.sale_price,
  ROUND(((f.base_price - f.sale_price) / f.base_price) * 100,2) as discount_depth_percentage

FROM
  `kroger-analysis.Kroger_Products.dim_product` AS p
INNER JOIN
  `kroger-analysis.Kroger_Products.fact_pricing_inventory` AS f
  ON p.sku_id_pk = f.sku_id_fk
WHERE
  f.inventory_risk_code = 1
  AND
  ((f.base_price - f.sale_price) / f.base_price) * 100 > 20
ORDER BY
  discount_depth_percentage DESC