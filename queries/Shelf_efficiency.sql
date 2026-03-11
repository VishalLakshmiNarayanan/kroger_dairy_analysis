-- Shelf Real Estate Efficiency , products with more shelf facings but the stock is low 

SELECT
  p.product_description,
  f.total_shelf_facings
FROM
  `kroger-analysis.Kroger_Products.dim_product` AS p
INNER JOIN
  `kroger-analysis.Kroger_Products.fact_pricing_inventory` AS f
  ON p.sku_id_pk = f.sku_id_fk
WHERE
  (f.inventory_risk_code = 1 
  OR
  f.inventory_risk_code = 2)
  AND
  f.total_shelf_facings > 5
ORDER BY 
  f.total_shelf_facings ASC;

