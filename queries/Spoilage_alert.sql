-- spoilage alert for products with HIGH stock level and there is no sale
SELECT
  p.product_description,
  f.base_price,
  f.total_shelf_facings
FROM
  `kroger-analysis.Kroger_Products.dim_product` AS p
INNER JOIN
  `kroger-analysis.Kroger_Products.fact_pricing_inventory` AS f
  ON p.sku_id_pk = f.sku_id_fk
WHERE
  f.inventory_risk_code = 0
  AND f.base_price = f.sale_price
ORDER BY 
  f.total_shelf_facings DESC;