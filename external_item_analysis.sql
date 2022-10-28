--/// Query for analyzing External Order Categorization - showcasing the data to stakeholders ///


WITH main AS
(
	WITH blueprints AS
	(
		SELECT  blueprint_id
		       ,DESCRIPTION
		       ,MONGO_ID
		       ,name
		       ,CASE WHEN lower(type_label) = 'hoodie' THEN 'hoodie'
		             WHEN lower(type_label) = 'long-sleeve' or lower(name) like '%hoodie%' AND lower(name) not like '%hoodie%dress%' AND lower(TYPE_LABEL) != 'kids clothes' or lower(name) like '%sweatshirt%' AND lower(TYPE_LABEL) != 'kids clothes' THEN 'hoodie'
		             WHEN lower(type_label) = 'sweatshirt' THEN 'hoodie'
		             WHEN lower(type_label) = 't-shirt' AND not lower(name) like any ('%kid%','%youth%','%infant%','%toddler%','%baby%') THEN 't-shirt'
		             WHEN lower(type_label) = 'tank top' AND not lower(name) like any ('%kid%','%youth%','%infant%','%toddler%','%baby%') or lower(name) like any ('%jersey%','%tee','%shirt%') THEN 't-shirt'
		             WHEN lower(type_label) = 'v-neck' THEN 't-shirt'
		             WHEN lower(type_label) = 'phone case' THEN 'phone-case'
		             WHEN lower(type_label) = 'mug' AND lower(name) not like '%bottle%' THEN 'mug'
		             WHEN lower(type_label) = 'hats' THEN 'hats'
		             WHEN lower(type_label) = 'poster' or lower(name) like '%tapestry%' THEN 'poster'
		             WHEN lower(name) like any ('%poster%','%prints%','%picture%') THEN 'poster'
		             WHEN lower(type_label) = 'shoes' THEN 'shoes'
		             WHEN lower(type_label) = 'canvas' or lower(name) like any ('%canva %','%canvas') THEN 'canvas'
		             WHEN lower(type_label) = 'puzzle' THEN 'other'
		             WHEN lower(type_label) = 'kids clothes' or lower(name) like '%baby%' THEN 'kids-clothes'
		             WHEN lower(type_label) IN ('paper products','wall decal') AND lower(name) like any ('%sticker%','%decal%') THEN 'stickers'
		             WHEN lower(type_label) = 'paper products' AND lower(name) like '%journal%' or lower(name) like '%notebook%' THEN 'journal'
		             WHEN lower(name) like '%magnet%' THEN 'magnet'
		             WHEN lower(name) like '%clock%' THEN 'wall-clock'
		             WHEN lower(name) like '%shower%' AND lower(name) like '%curtain%' THEN 'shower-curtain'
		             WHEN lower(name) like '%bath%' AND lower(name) like '%mat%' THEN 'bath-mat'
		             WHEN lower(name) like '%blanket%' THEN 'blanket'
		             WHEN lower(name) like '%bottle%' THEN 'bottle'
		             WHEN lower(name) like '%dress%' AND not lower(name) like any ('%shirt%dress%','%skirt%') THEN 'dress'
		             WHEN lower(name) like '%face%' THEN 'face-mask'
		             WHEN lower(name) like any ('%mouse pad%','%mousepad%') THEN 'mouse-pad'
		             WHEN lower(name) like '%ornament%' THEN 'ornament'
		             WHEN lower(name) like '%pillow%' THEN 'pillow'
		             WHEN lower(name) like any ('%floor-mat%','%rug%','%mat %') AND not lower(name) like any ('%mat%towel%','%mat','%car%mats%','%pet%mat%','%mat%towel%') THEN 'rug'
		             WHEN lower(name) like '%socks%' THEN 'socks'
		             WHEN lower(name) like any ('%swim%','%swimwear%') THEN 'swim-wear'
		             WHEN lower(name) like '%towel%' THEN 'towel'
		             WHEN lower(name) like any ('%underwear%','%bra %','%bra','%boxer%','%briefs%') THEN 'underwear'
		             WHEN lower(type_label) = 'bags' or lower(name) like '%bag%' THEN 'bag'
		             WHEN lower(type_label) = 'trousers' or lower(name) like any ('%leggings%','%shorts%','%joggers%') THEN 'trousers'
		             WHEN lower(type_label) = 'pets' or lower(name) like any ('%pet%','%dog%','%cat%') THEN 'pets'  ELSE 'other' END AS label
		FROM junk12
	)
	SELECT  oo.MERCHANT_ID
	       ,GMV_USD                                                   AS gmv
	       ,oo.MONGO_ID
	       ,oo.CREATED_DT
	       ,oo.SHOP_ID
	       ,true                                                      AS IS_PRINTIFY_item
	       ,o.QUANTITY
	       ,blueprints.label
	       ,CASE WHEN not label = 'unknown' THEN true  ELSE false END AS IS_POD_PRODUCT
	       ,blueprints.name
	       ,LINE_ITEM_ID
	       ,null                                                      AS EXTERNAL_LINE_ITEM_ID
	FROM junk11 o
	LEFT JOIN blueprints
	ON o.BLUEPRINT_ID = blueprints.BLUEPRINT_ID
	LEFT JOIN junk10 oo
	ON o.MONGO_ID = oo.MONGO_ID
	WHERE oo.CREATED_TS::date BETWEEN '2021-12-01' AND current_date() 
	UNION ALL
	SELECT  oooo.MERCHANT_ID
	       ,null                                                           AS gmv
	       ,o.EXTERNAL_ORDER_ID
	       ,oo.CREATED_AT
	       ,shop_id_recent                                                 AS shop_id
	       ,oo.IS_PRINTIFY_order                                           AS printify_item
	       ,o.QUANTITY                                                     AS item_quantity
	       ,ooo.class                                                      AS item_label
	       ,CASE WHEN not item_label = 'unknown' THEN true  ELSE false END AS IS_POD_PRODUCT
	       ,o.TITLE
	       ,null                                                           AS LINE_ITEM_ID
	       ,o.EXTERNAL_LINE_ITEM_ID
	FROM junk7 o
	LEFT JOIN junk9 oo
	ON o.MONGO_ID = oo.MONGO_ID
	RIGHT JOIN
	(
		SELECT  pred.external_line_item_id
		       ,pred.PREDICTION_ID
		       ,pred.output_data:predicted_class::string class
		FROM junk8 pred QUALIFY row_number
		(
		) OVER (PARTITION BY EXTERNAL_LINE_ITEM_ID ORDER BY PREDICTION_ID DESC) = 1
	) ooo
	ON o.external_line_item_id = ooo.EXTERNAL_LINE_ITEM_ID
	LEFT JOIN
	(
		SELECT  SHOP_ID                              AS shop_id_recent
		       ,MAX(try_cast(MERCHANT_ID AS number)) AS MERCHANT_ID
		FROM junk1
		GROUP BY  1
	) oooo
	ON oo.SHOP_ID = oooo.shop_id_recent
	WHERE not oo.IS_PRINTIFY_ORDER = true
	AND oo.CREATED_AT::date BETWEEN '2021-12-01' AND current_date() 
), non_Printify_POD_2nd AS
(
	SELECT  distinct merchant_id
	       ,max_lb_2
	FROM
	(
		SELECT  MERCHANT_ID
		       ,label
		       ,COUNT(distinct MONGO_ID)                                                       AS order_count
		       ,SUM(quantity)
		       ,nth_value(label,2) over (partition by MERCHANT_ID ORDER BY SUM(quantity) desc) AS max_lb_2
		FROM main
		WHERE IS_PRINTIFY_item = false
		AND IS_POD_PRODUCT = 1
		GROUP BY  1
		         ,2
	)
), orders_maxes AS
(
	SELECT  MERCHANT_ID
	       ,COUNT(distinct NAME)                            AS distinct_products
	       ,SUM(quantity)                                   AS quantity
	       ,COUNT(distinct MONGO_ID)                        AS order_count
	       ,any_value(MAX_category_POD_ORDERS)              AS MAX_category_POD_ORDERS_real
	       ,any_value(MAX_category_Printify_ORDERS)         AS MAX_category_Printify_ORDERS_real
	       ,any_value(MAX_category_non_Printify_POD_orders) AS MAX_category_non_Printify_POD_orders_real
	       ,mode(shop_id)                                   AS MOST_USED_SHOP_ID
	FROM main
	LEFT JOIN
	(
		SELECT  MERCHANT_ID                                                                  AS mch
		       ,label                                                                        AS lbb
		       ,first_value(lbb) over (partition by MERCHANT_ID ORDER BY SUM(quantity) desc) AS MAX_category_non_Printify_POD_orders
		FROM main
		WHERE IS_PRINTIFY_item = false
		AND IS_POD_PRODUCT = 1
		GROUP BY  1
		         ,2
	) non_Printify_POD_orders
	ON main.MERCHANT_ID = mch
	LEFT JOIN
	(
		SELECT  MERCHANT_ID                                                                  AS mch_2
		       ,label                                                                        AS lbb
		       ,first_value(lbb) over (partition by MERCHANT_ID ORDER BY SUM(quantity) desc) AS MAX_category_Printify_ORDERS
		FROM main
		WHERE IS_PRINTIFY_item = true
		GROUP BY  1
		         ,2
	) Printify_ORDERS
	ON main.MERCHANT_ID = mch_2
	LEFT JOIN
	(
		SELECT  MERCHANT_ID                                                                  AS mch_3
		       ,label                                                                        AS lbb
		       ,first_value(lbb) over (partition by MERCHANT_ID ORDER BY SUM(quantity) desc) AS MAX_category_POD_ORDERS
		FROM main
		WHERE IS_POD_PRODUCT = 1
		GROUP BY  1
		         ,2
	) POD_ORDERS
	ON main.MERCHANT_ID = mch_3
	WHERE MERCHANT_ID is not null
	GROUP BY  1
), orders AS
(
	SELECT  main.MERCHANT_ID
	       ,gmv
	       ,main.CREATED_DT
	       ,main.MONGO_ID
	       ,MAX(ifnull(IS_POD_PRODUCT,0)) AS is_pod_order
	       ,IS_PRINTIFY_item
	       ,COUNT(distinct main.MONGO_ID) AS order_count
	       ,SUM(quantity)                 AS quantity
	FROM main
	GROUP BY  1
	         ,2
	         ,3
	         ,4
	         ,6
)
SELECT  o.MERCHANT_ID
       ,any_value(MERCHANT_SELLER_STATE)                                                      AS seller_state
       ,any_value(man.LEGAL_NAME)                                                             AS legal_name
       ,any_value(cont.fullname)                                                              AS full_name
       ,any_value(cont.email)                                                                 AS email
       ,any_value(cont.phone)                                                                 AS phone
       ,any_value(man.tag)                                                                    AS tag
       ,any_value(man.SUCCESS_MANAGER)                                                        AS suc_manager
       ,any_value(sal.sales_person)                                                           AS sales_manager
       ,any_value(sh.domain)                                                                  AS store_URL
       ,any_value(shop.store_name)                                                            AS store_name
       ,SUM(gmv)                                                                              AS gmv
       ,COUNT(o.CREATED_DT)                                                                   AS all_orders
       ,COUNT(case WHEN o.is_pod_order = 1 THEN 1 else null end)                              AS pod_order_count
       ,COUNT(case WHEN o.is_pod_order = 0 THEN 1 else null end)                              AS not_pod_order_count
       ,COUNT(case WHEN o.IS_PRINTIFY_item = true THEN 1 else null end)                       AS Printify_orders
       ,COUNT(case WHEN o.IS_PRINTIFY_item = false THEN 1 else null end)                      AS NON_Printify_orders
       ,COUNT(case WHEN o.IS_PRINTIFY_item = false AND is_pod_order = 1 THEN 1 else null end) AS non_Printify_POD_orders
       ,any_value(om.MAX_category_POD_ORDERS_real)                                            AS MAX_category_POD_ORDERS_fromBP
       ,any_value(om.MAX_category_Printify_ORDERS_real)                                       AS MAX_category_Printify_ORDERS_fromBP
       ,any_value(om.MAX_category_non_Printify_POD_orders_real)                               AS MAX_category_non_Printify_POD_orders_fromBP
       ,any_value(second.max_lb_2)                                                            AS second_max_nonprintify_pod
       ,1 - (not_pod_order_count / all_orders)                                                AS pod_index
       ,CASE WHEN pod_index > 0.80 THEN true  ELSE false END                                  AS POD_index_flag
       ,(printify_orders / ifnull(pod_order_count,0))                                         AS Printify_wallet_share
       ,CASE WHEN pod_order_count > 250 THEN true  ELSE false END                             AS POD_relevance
FROM orders o
LEFT JOIN orders_maxes om
ON o.MERCHANT_ID = om.MERCHANT_ID
LEFT JOIN non_Printify_POD_2nd second
ON o.MERCHANT_ID = second.MERCHANT_ID
LEFT JOIN
(
	SELECT  MERCHANT_ID
	       ,MERCHANT_P1M_STATUS AS merchant_seller_state
	       ,STATUS_DATE
	FROM junk6 QUALIFY row_number
	(
	) OVER (PARTITION BY MERCHANT_ID ORDER BY STATUS_DATE DESC) = 1
) ooooo
ON o.MERCHANT_ID = ooooo.MERCHANT_ID
LEFT JOIN
(
	SELECT  distinct MERCHANT_ID AS user
	       ,MAX(BRANDING_BRAND)  AS store_name
	FROM junk5
	WHERE MERCHANT_ID is not null
	GROUP BY  user
) shop
ON o.MERCHANT_ID = shop.user
LEFT JOIN
(
	SELECT  distinct user_id AS user
	       ,MAX(domain)      AS domain
	FROM junk4
	WHERE user_id is not null
	GROUP BY  user
) sh
ON o.MERCHANT_ID = sh.user
LEFT JOIN junk3 man
ON o.MERCHANT_ID = man.MERCHANT_ID
LEFT JOIN junk2 cont
ON o.MERCHANT_ID = cont.user_id
LEFT JOIN junk1 sal
ON o.MERCHANT_ID = sal.sales_merchant_id
WHERE o.MERCHANT_ID is not null
AND CREATED_DT >= DATEADD(day, -30, current_date())
GROUP BY  o.MERCHANT_ID
ORDER BY non_Printify_POD_orders desc
         ,o.MERCHANT_ID desc