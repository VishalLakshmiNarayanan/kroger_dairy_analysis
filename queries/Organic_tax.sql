-- Organic tax calculation brand_premium vs brand_private

SELECT
  p.brand_tier,
  ROUND(AVG(f.sale_price),2) as avg_sale_price

FROM
  `kroger-analysis.Kroger_Products.dim_product` AS p
INNER JOIN
  `kroger-analysis.Kroger_Products.fact_pricing_inventory` AS f
  ON p.sku_id_pk = f.sku_id_fk
WHERE
  p.size_uom = '1 gal'
GROUP BY
  p.brand_tier