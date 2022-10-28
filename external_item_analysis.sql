//LAST 30 DAYS!!!!!!//

;
with main as (
    with blueprints as (
        select blueprint_id,
               DESCRIPTION,
               MONGO_ID,
               name,
               case
                   when lower(type_label) = 'hoodie' then 'hoodie'
                   when lower(type_label) = 'long-sleeve' or
                        lower(name) like '%hoodie%'
                            and lower(name) not like '%hoodie%dress%'
                            and lower(TYPE_LABEL) != 'kids clothes' or
                        lower(name) like '%sweatshirt%'
                            and lower(TYPE_LABEL) != 'kids clothes' then 'hoodie'
                   when lower(type_label) = 'sweatshirt' then 'hoodie'
                   when lower(type_label) = 't-shirt'
                            and not lower(name) like any ('%kid%', '%youth%', '%infant%', '%toddler%', '%baby%') then 't-shirt'
                   when lower(type_label) = 'tank top'
                       and not lower(name) like any ('%kid%', '%youth%', '%infant%', '%toddler%', '%baby%') or
                        lower(name) like any ('%jersey%', '%tee',  '%shirt%') then 't-shirt'
                   when lower(type_label) = 'v-neck' then 't-shirt'
                   when lower(type_label) = 'phone case' then 'phone-case'
                   when lower(type_label) = 'mug'
                            and lower(name) not like '%bottle%' then 'mug'
                   when lower(type_label) = 'hats' then 'hats'
                   when lower(type_label) = 'poster' or
                        lower(name) like '%tapestry%' then 'poster'
                   when lower(name) like any ('%poster%', '%prints%', '%picture%') then 'poster'
                   when lower(type_label) = 'shoes' then 'shoes'
                   when lower(type_label) = 'canvas' or
                        lower(name) like any ('%canva %', '%canvas') then 'canvas'
                   when lower(type_label) = 'puzzle' then 'other'
                   when lower(type_label) = 'kids clothes' or
                        lower(name) like '%baby%' then 'kids-clothes'
                   when lower(type_label) in ('paper products', 'wall decal')
                            and lower(name) like any ('%sticker%', '%decal%') then 'stickers'
                   when lower(type_label) = 'paper products'
                            and lower(name) like '%journal%' or
                        lower(name) like '%notebook%' then 'journal'
                   when lower(name) like '%magnet%' then 'magnet'
                   when lower(name) like '%clock%' then 'wall-clock'
                   when lower(name) like '%shower%'
                            and lower(name) like '%curtain%' then 'shower-curtain'
                   when lower(name) like '%bath%'
                            and lower(name) like '%mat%' then 'bath-mat'
                   when lower(name) like '%blanket%' then 'blanket'
                   when lower(name) like '%bottle%' then 'bottle'
                   when lower(name) like '%dress%'
                            and not lower(name) like any ('%shirt%dress%', '%skirt%') then 'dress'
                   when lower(name) like '%face%' then 'face-mask'
                   when lower(name) like any ('%mouse pad%', '%mousepad%') then 'mouse-pad'
                   when lower(name) like '%ornament%' then 'ornament'
                   when lower(name) like '%pillow%' then 'pillow'
                   when lower(name) like any ('%floor-mat%', '%rug%', '%mat %')
                            and not lower(name) like any ('%mat%towel%', '%mat', '%car%mats%', '%pet%mat%', '%mat%towel%') then 'rug'
                   when lower(name) like '%socks%' then 'socks'
                   when lower(name) like any ('%swim%', '%swimwear%') then 'swim-wear'
                   when lower(name) like '%towel%' then 'towel'
                   when lower(name) like any ('%underwear%', '%bra %', '%bra', '%boxer%', '%briefs%') then 'underwear'
                   when lower(type_label) = 'bags' or
                        lower(name) like '%bag%' then 'bag'
                   when lower(type_label) = 'trousers' or
                        lower(name) like any ('%leggings%', '%shorts%', '%joggers%') then 'trousers'
                   when lower(type_label) = 'pets' or
                        lower(name) like any ('%pet%', '%dog%', '%cat%') then 'pets'
                   else 'other' end as label
        from junk12
    )
    select oo.MERCHANT_ID,
           GMV_USD as gmv,
           oo.MONGO_ID,
           oo.CREATED_DT,
           oo.SHOP_ID,
           true                                                     as IS_PRINTIFY_item,
           o.QUANTITY,
           blueprints.label,
           case when not label = 'unknown' then true else false end as IS_POD_PRODUCT,
           blueprints.name,
           LINE_ITEM_ID,
           null                                                     as EXTERNAL_LINE_ITEM_ID
    from junk11 o
             left JOIN blueprints
                       ON o.BLUEPRINT_ID = blueprints.BLUEPRINT_ID
             left join junk10 oo on
        o.MONGO_ID = oo.MONGO_ID
    where oo.CREATED_TS::date between '2021-12-01' and current_date()
    union all
    select oooo.MERCHANT_ID,
        null as gmv,
           o.EXTERNAL_ORDER_ID,
           oo.CREATED_AT,
           shop_id_recent       as shop_id,
           oo.IS_PRINTIFY_order as printify_item,
           o.QUANTITY as item_quantity,
           ooo.class                                                                   as item_label,
           case when not item_label = 'unknown' then true else false end as IS_POD_PRODUCT,
           o.TITLE,
           null as LINE_ITEM_ID,
           o.EXTERNAL_LINE_ITEM_ID
    from junk7 o
             left join junk9 oo
                       on o.MONGO_ID = oo.MONGO_ID
             right join
         (
             select pred.external_line_item_id,
                    pred.PREDICTION_ID,
                    pred.output_data:predicted_class::string class
             from junk8 pred
            QUALIFY row_number() OVER (PARTITION BY EXTERNAL_LINE_ITEM_ID ORDER BY PREDICTION_ID DESC) = 1
         ) ooo
         on o.external_line_item_id = ooo.EXTERNAL_LINE_ITEM_ID
             left join
         (
             select SHOP_ID                              as shop_id_recent,
                    MAX(try_cast(MERCHANT_ID as number)) as MERCHANT_ID
             from junk1
             group by 1
         ) oooo
         on oo.SHOP_ID = oooo.shop_id_recent
    where not oo.IS_PRINTIFY_ORDER = true
      and oo.CREATED_AT::date between '2021-12-01' and current_date()
      ),
     non_Printify_POD_2nd as (
            select distinct merchant_id,
                            max_lb_2
            from (
                   select MERCHANT_ID,
                          label,
                          count(distinct MONGO_ID)                        as order_count,
                          sum(quantity),
                          nth_value(label, 2) over (partition by MERCHANT_ID order by sum(quantity) desc) as max_lb_2
                   from main
                   where IS_PRINTIFY_item = false
                     and IS_POD_PRODUCT = 1
                   group by 1, 2 )
     ),
     orders_maxes as (
         select MERCHANT_ID,
                count(distinct NAME)                     as distinct_products,
                sum(quantity)                                   as quantity,
                count(distinct MONGO_ID)                        as order_count,
                any_value(MAX_category_POD_ORDERS)              as MAX_category_POD_ORDERS_real,
                any_value(MAX_category_Printify_ORDERS)         as MAX_category_Printify_ORDERS_real,
                any_value(MAX_category_non_Printify_POD_orders) as MAX_category_non_Printify_POD_orders_real,
                mode(shop_id)                                   as MOST_USED_SHOP_ID

         from main
                  left join (
             select MERCHANT_ID                                                             as mch,
                    label                                                                   as lbb,
                    first_value(lbb)
                                over (partition by MERCHANT_ID order by sum(quantity) desc) as MAX_category_non_Printify_POD_orders
             from main
             where IS_PRINTIFY_item = false
               and IS_POD_PRODUCT = 1
             group by 1, 2
         ) non_Printify_POD_orders on
             main.MERCHANT_ID = mch
                  left join (
             select MERCHANT_ID                                                             as mch_2,
                    label                                                                   as lbb,
                    first_value(lbb)
                                over (partition by MERCHANT_ID order by sum(quantity) desc) as MAX_category_Printify_ORDERS
             from main
             where IS_PRINTIFY_item = true
             group by 1, 2
         ) Printify_ORDERS on
             main.MERCHANT_ID = mch_2
                  left join (
             select MERCHANT_ID                                                             as mch_3,
                    label                                                                   as lbb,
                    first_value(lbb)
                                over (partition by MERCHANT_ID order by sum(quantity) desc) as MAX_category_POD_ORDERS
             from main
             where IS_POD_PRODUCT = 1
             group by 1, 2
         ) POD_ORDERS on
             main.MERCHANT_ID = mch_3
         where MERCHANT_ID is not null
         group by 1
     ),



     orders as (
         select main.MERCHANT_ID,
               gmv,
                main.CREATED_DT,
                main.MONGO_ID,
                max(ifnull(IS_POD_PRODUCT, 0)) as is_pod_order,
                IS_PRINTIFY_item,
                count(distinct main.MONGO_ID)       as order_count,
                sum(quantity)                  as quantity

         from main

         group by 1, 2, 3, 4, 6
     )



select o.MERCHANT_ID,
       any_value(MERCHANT_SELLER_STATE)                                                            as seller_state,
       any_value(man.LEGAL_NAME)                                                                   as legal_name,
       any_value(cont.fullname)                                                                    as full_name,
       any_value(cont.email)                                                                       as email,
       any_value(cont.phone)                                                                       as phone,
       any_value(man.tag)                                                                          as tag,
       any_value(man.SUCCESS_MANAGER)                                                              as suc_manager,
       any_value(sal.sales_person)                                                                 as sales_manager,
       any_value(sh.domain)                                                                        as store_URL,
       any_value(shop.store_name)                                                            as store_name,
       sum(gmv)                                                                              as gmv,
       count(o.CREATED_DT)                                                                   as all_orders,
       count(case when o.is_pod_order = 1 then 1 else null end)                              as pod_order_count,
       count(case when o.is_pod_order = 0 then 1 else null end)                              as not_pod_order_count,
       count(case WHEN o.IS_PRINTIFY_item = true then 1 else null end)                       as Printify_orders,
       count(case WHEN o.IS_PRINTIFY_item = false then 1 else null end)                      as NON_Printify_orders,
       count(case WHEN o.IS_PRINTIFY_item = false and is_pod_order = 1 then 1 else null end) as non_Printify_POD_orders,

       any_value(om.MAX_category_POD_ORDERS_real)                                            as MAX_category_POD_ORDERS_fromBP,
       any_value(om.MAX_category_Printify_ORDERS_real)                                       as MAX_category_Printify_ORDERS_fromBP,
       any_value(om.MAX_category_non_Printify_POD_orders_real)                               as MAX_category_non_Printify_POD_orders_fromBP,

       any_value(second.max_lb_2)                                                            as second_max_nonprintify_pod,
       1 - (not_pod_order_count / all_orders)                                                as pod_index,
       case when pod_index > 0.80 then true else false end                                   as POD_index_flag,
       (printify_orders / ifnull(pod_order_count, 0))                                        as Printify_wallet_share,
       case when pod_order_count > 250 then true else false end                              as POD_relevance
from orders o

         left join orders_maxes om on
    o.MERCHANT_ID = om.MERCHANT_ID
         left join non_Printify_POD_2nd second on
    o.MERCHANT_ID = second.MERCHANT_ID

         left join
     (select MERCHANT_ID,
             MERCHANT_P1M_STATUS as merchant_seller_state,
             STATUS_DATE
      from junk6
          QUALIFY row_number() OVER (PARTITION BY MERCHANT_ID ORDER BY STATUS_DATE DESC) = 1
     ) ooooo on
         o.MERCHANT_ID = ooooo.MERCHANT_ID
       left join (
             select distinct MERCHANT_ID as user,
                             max(BRANDING_BRAND) as store_name
             from junk5
             where MERCHANT_ID is not null
             group by user
    ) shop on
    o.MERCHANT_ID = shop.user
         left join (
             select distinct user_id as user,
                             max(domain) as domain
             from junk4
             where user_id is not null
             group by user
    ) sh on
    o.MERCHANT_ID = sh.user
         left join junk3 man on
    o.MERCHANT_ID = man.MERCHANT_ID
         left join junk2 cont on
    o.MERCHANT_ID = cont.user_id
         left join junk1 sal on
    o.MERCHANT_ID = sal.sales_merchant_id

where o.MERCHANT_ID is not null and CREATED_DT >= DATEADD(day, -30, current_date())
group by o.MERCHANT_ID
order by non_Printify_POD_orders desc, o.MERCHANT_ID desc