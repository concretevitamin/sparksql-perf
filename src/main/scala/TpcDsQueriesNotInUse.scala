import java.io.File

import org.apache.spark.sql.hive.HiveContext

/**
 * Currently not targeted queries that are included in TPC-DS.
 */
class TpcDsQueriesNotInUse(
    hiveContext: HiveContext,
    location: String  = new File(".", "tpcds").getCanonicalPath) {

  import hiveContext._

  // No window support
  val query12partitioned = hql("""
    select i_item_desc
    ,i_category
    ,i_class
    ,i_current_price
    ,i_item_id
    ,itemrevenue
    ,itemrevenue*100/sum(itemrevenue) over
      (partition by i_class) as revenueratio
      from (select
      i_item_desc
      ,i_category
      ,i_class
      ,i_current_price
      ,i_item_id
      ,sum(ss_ext_sales_price) as itemrevenue
      from store_sales
      join item on (store_sales.ss_item_sk = item.i_item_sk)
      join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
      where
      i_category in ('Sports', 'Shoes', 'Books') and year(d_date) = 2001 and month(d_date) = 10 and
    ss_sold_date between '2001-10-01' and '2001-10-31'
  group by
    i_item_id
  ,i_item_desc
  ,i_category
  ,i_class
  ,i_current_price) tmp
  order by
    i_category
  ,i_class
  ,i_item_id
  ,i_item_desc
  ,revenueratio

                                    """)


  val query12 = hql("""
    select i_item_desc
    ,i_category
    ,i_class
    ,i_current_price
    ,i_item_id
    ,itemrevenue
    ,itemrevenue*100/sum(itemrevenue) over
      (partition by i_class) as revenueratio
      from (select
      i_item_desc
      ,i_category
      ,i_class
      ,i_current_price
      ,i_item_id
      ,sum(ss_ext_sales_price) as itemrevenue
      from store_sales
      join item on (store_sales.ss_item_sk = item.i_item_sk)
      join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
      where
      i_category in ('Sports', 'Shoes', 'Books') and year(d_date) = 2001 and month(d_date) = 10
  group by
    i_item_id
  ,i_item_desc
  ,i_category
  ,i_class
  ,i_current_price) tmp
  order by
    i_category
  ,i_class
  ,i_item_id
  ,i_item_desc
  ,revenueratio

                         """)

  val query13 = hql("""
   -- shive.mapred.local.mem=3072

  select avg(ss_quantity) ,avg(ss_ext_sales_price) ,avg(ss_ext_wholesale_cost) ,sum(ss_ext_wholesale_cost)
  from store_sales
    JOIN store ON store.s_store_sk = store_sales.ss_store_sk
  JOIN customer_demographics ON customer_demographics.cd_demo_sk = store_sales.ss_cdemo_sk
  JOIN household_demographics ON store_sales.ss_hdemo_sk=household_demographics.hd_demo_sk
  JOIN customer_address ON store_sales.ss_addr_sk = customer_address.ca_address_sk
  JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
  where
  d_year = 2001
  and((
    cd_marital_status = 'M'
      and cd_education_status = '4 yr Degree'
  and ss_sales_price between 100.00 and 150.00
  and hd_dep_count = 3
  )or
  (
    cd_marital_status = 'D'
      and cd_education_status = 'Primary'
  and ss_sales_price between 50.00 and 100.00
  and hd_dep_count = 1
  ) or
  (
    cd_marital_status = 'U'
      and cd_education_status = 'Advanced Degree'
  and ss_sales_price between 150.00 and 200.00
  and hd_dep_count = 1
  ))
  and((
    ca_country = 'United States'
  and ca_state in ('KY', 'GA', 'NM')
  and ss_net_profit between 100 and 200
  ) or
  (
    ca_country = 'United States'
  and ca_state in ('MT', 'OR', 'IN')
  and ss_net_profit between 150 and 300
  ) or
  (
    ca_country = 'United States'
  and ca_state in ('WI', 'MO', 'WV')
  and ss_net_profit between 50 and 250
  ))


                         """)


  val query15 = hql("""

    select  ca_zip
    ,sum(cs_sales_price)
      from catalog_sales
      JOIN customer ON catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
      JOIN customer_address ON customer.c_current_addr_sk = customer_address.ca_address_sk
      JOIN date_dim ON catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
      where
      ( substr(ca_zip,1,5) in ('85669', '86197','88274','83405','86475',
  '85392', '85460', '80348', '81792')
  or ca_state in ('CA','WA','GA')
  or cs_sales_price > 500)
  and d_qoy = 2 and d_year = 2000
  group by ca_zip
  order by ca_zip
  limit 100



                    """)


  val query17 = hql("""

    select  i_item_id ,i_item_desc ,s_state
    ,count(ss_quantity) as store_sales_quantitycount
    ,avg(ss_quantity) as store_sales_quantityave
    ,stddev_samp(ss_quantity) as store_sales_quantitystdev
    ,stddev_samp(ss_quantity)/avg(ss_quantity) as store_sales_quantitycov
    ,count(sr_return_quantity) as_store_returns_quantitycount
    ,avg(sr_return_quantity) as_store_returns_quantityave
    ,stddev_samp(sr_return_quantity) as_store_returns_quantitystdev
    ,stddev_samp(sr_return_quantity)/avg(sr_return_quantity) as store_returns_quantitycov
    ,count(cs_quantity) as catalog_sales_quantitycount ,avg(cs_quantity) as catalog_sales_quantityave
    ,stddev_samp(cs_quantity)/avg(cs_quantity) as catalog_sales_quantitystdev
    ,stddev_samp(cs_quantity)/avg(cs_quantity) as catalog_sales_quantitycov
      from store_sales
      JOIN store_returns ON store_sales.ss_customer_sk = store_returns.sr_customer_sk
      and store_sales.ss_item_sk = store_returns.sr_item_sk
      and store_sales.ss_ticket_number = store_returns.sr_ticket_number
      JOIN catalog_sales ON store_returns.sr_customer_sk = catalog_sales.cs_bill_customer_sk
      and store_returns.sr_item_sk = catalog_sales.cs_item_sk
      JOIN date_dim d1 ON d1.d_date_sk = store_sales.ss_sold_date_sk
  JOIN date_dim d2 ON store_returns.sr_returned_date_sk = d2.d_date_sk
  JOIN date_dim d3 ON catalog_sales.cs_sold_date_sk = d3.d_date_sk
  JOIN store ON store.s_store_sk = store_sales.ss_store_sk
  JOIN item ON item.i_item_sk = store_sales.ss_item_sk
  where d1.d_quarter_name = '2000Q1'
  and d2.d_quarter_name in ('2000Q1','2000Q2','2000Q3')
  and d3.d_quarter_name in ('2000Q1','2000Q2','2000Q3')
  group by i_item_id ,i_item_desc ,s_state
  order by i_item_id ,i_item_desc ,s_state
  limit 100

                    """)


  val query18 = hql("""

    select  i_item_id,
    ca_country,
    ca_state,
    ca_county,
    avg( cast(cs_quantity as decimal)) agg1,
    avg( cast(cs_list_price as decimal)) agg2,
    avg( cast(cs_coupon_amt as decimal)) agg3,
    avg( cast(cs_sales_price as decimal)) agg4,
    avg( cast(cs_net_profit as decimal)) agg5,
    avg( cast(c_birth_year as decimal)) agg6,
    avg( cast(cd1.cd_dep_count as decimal)) agg7
      from catalog_sales
      JOIN customer_demographics cd1 ON catalog_sales.cs_bill_cdemo_sk = cd1.cd_demo_sk
      JOIN customer ON catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
      JOIN customer_demographics cd2 ON customer.c_current_cdemo_sk = cd2.cd_demo_sk
  JOIN customer_address ON customer.c_current_addr_sk = customer_address.ca_address_sk
  JOIN date_dim ON catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
  JOIN item ON catalog_sales.cs_item_sk = item.i_item_sk
  where
  cd1.cd_gender = 'M' and
    cd1.cd_education_status = 'College' and
  c_birth_month in (9,5,12,4,1,10) and
    d_year = 2001 and
    ca_state in ('ND','WI','AL'
  ,'NC','OK','MS','TN')
  group by i_item_id, ca_country, ca_state, ca_county
  order by ca_country,
  ca_state,
  ca_county,
  i_item_id
  limit 100



                    """)

  val query20 = hql("""
    select  i_item_desc
    ,i_category
    ,i_class
    ,i_current_price
    ,i_item_id
    ,sum(cs_ext_sales_price) as itemrevenue
    ,sum(cs_ext_sales_price)*100/sum(sum(cs_ext_sales_price)) over
      (partition by i_class) as revenueratio
      from	catalog_sales
      JOIN item ON catalog_sales.cs_item_sk = item.i_item_sk
      JOIN date_dim ON catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
      where
      i_category in ('Jewelry', 'Sports', 'Books')
  and d_date between '2001-01-12' and '2001-02-11'
  group by i_item_id
  ,i_item_desc
  ,i_category
  ,i_class
  ,i_current_price
  ,cs_ext_sales_price
  order by i_category
  ,i_class
  ,i_item_id
  ,i_item_desc
  ,revenueratio
  limit 100



                         """)


  val query21 = hql("""

    select  *
    from(select w_warehouse_name
    ,i_item_id
    ,sum(case when (d_date < '1998-04-08')
  then inv_quantity_on_hand
  else 0 end) as inv_before
  ,sum(case when (d_date >= '1998-04-08')
  then inv_quantity_on_hand
  else 0 end) as inv_after
    from inventory
    JOIN warehouse ON inventory.inv_warehouse_sk   = warehouse.w_warehouse_sk
  JOIN item ON item.i_item_sk          = inventory.inv_item_sk
  JOIN date_dim ON inventory.inv_date_sk    = date_dim.d_date_sk
  where i_current_price between 0.99 and 1.49
  and d_date between '1998-03-09' and '1998-05-08'
  group by w_warehouse_name, i_item_id) x
  where (case when inv_before > 0
  then inv_after / inv_before
  else null
  end) between 2.0/3.0 and 3.0/2.0
  order by w_warehouse_name
  ,i_item_id
  limit 100



                         """)


  val query22 = hql("""

    select  i_product_name
    ,i_brand
    ,i_class
    ,i_category
    ,avg(inv_quantity_on_hand) qoh
      from inventory
      JOIN date_dim ON inventory.inv_date_sk=date_dim.d_date_sk
  JOIN item ON inventory.inv_item_sk=item.i_item_sk
  JOIN warehouse ON inventory.inv_warehouse_sk = warehouse.w_warehouse_sk
  where
  d_month_seq between 1193 and 1193 + 11
  group by i_product_name ,i_brand ,i_class ,i_category with rollup
  order by qoh, i_product_name, i_brand, i_class, i_category
  limit 100


                         """)


  val query26 = hql("""

    select  i_item_id,
    avg(cs_quantity) agg1,
    avg(cs_list_price) agg2,
    avg(cs_coupon_amt) agg3,
    avg(cs_sales_price) agg4
      from catalog_sales
      JOIN customer_demographics ON catalog_sales.cs_bill_cdemo_sk = customer_demographics.cd_demo_sk
  JOIN date_dim ON catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
  JOIN item ON catalog_sales.cs_item_sk = item.i_item_sk
  JOIN promotion ON catalog_sales.cs_promo_sk = promotion.p_promo_sk
  where
  cd_gender = 'F' and
    cd_marital_status = 'W' and
    cd_education_status = 'Primary' and
  (p_channel_email = 'N' or p_channel_event = 'N') and
    d_year = 1998
  group by i_item_id
  order by i_item_id
  limit 100


                    """)


  val query28 = hql("""

    -- following settings are required to generate appropriate number of mappers
    -- for ORC scale factor 200:
  -- orc 200 split side
  ---- s mapreduce.input.fileinputformat.split.minsize=55421376
  ---- s mapreduce.input.fileinputformat.split.maxsize=55421376
 -- s mapreduce.input.fileinputformat.split.minsize=200000000
 -- s mapreduce.input.fileinputformat.split.maxsize=200000000
  -- for ORC scale factor 1000:
  ---- s mapreduce.input.fileinputformat.split.maxsize=1000089600
  ---- s mapreduce.input.fileinputformat.split.minsize=1000089600

 -- shive.auto.convert.sortmerge.join=false
 -- shive.auto.convert.sortmerge.join.noconditionaltask=false
 -- shive.use.tez.natively=true
 -- shive.enable.mrr=true
 -- shive.vectorized.execution.enabled=true

  select avg(ss_list_price) B1_LP ,count(ss_list_price) B1_CNT from store_sales where ss_quantity  > 0 and ss_quantity < 5 and ( (ss_list_price > 145 and ss_list_price  < 155) or (ss_coupon_amt > 9000 and ss_coupon_amt < 10000) or (ss_wholesale_cost > 50 and ss_wholesale_cost < 71))

                    """)


  val query32 = hql("""

    SELECT sum(cs1.cs_ext_discount_amt) as excess_discount_amount
    FROM (SELECT cs.cs_item_sk as cs_item_sk,
    cs.cs_ext_discount_amt as cs_ext_discount_amt
      FROM catalog_sales cs
      JOIN date_dim d ON (d.d_date_sk = cs.cs_sold_date_sk)
      WHERE d.d_date between '2000-01-27' and '2000-04-27') cs1
  JOIN item i ON (i.i_item_sk = cs1.cs_item_sk)
  JOIN (SELECT cs2.cs_item_sk as cs_item_sk,
  1.3 * avg(cs_ext_discount_amt) as avg_cs_ext_discount_amt
  FROM (SELECT cs.cs_item_sk as cs_item_sk,
  cs.cs_ext_discount_amt as cs_ext_discount_amt
  FROM catalog_sales cs
  JOIN date_dim d ON (d.d_date_sk = cs.cs_sold_date_sk)
  WHERE d.d_date between '2000-01-27' and '2000-04-27') cs2
  GROUP BY cs2.cs_item_sk) tmp1
  ON (i.i_item_sk = tmp1.cs_item_sk)
  WHERE i.i_manufact_id = 436 and
    cs1.cs_ext_discount_amt > tmp1.avg_cs_ext_discount_amt

                         """)

  val query39 = hql("""

    select inv1.w_warehouse_sk,inv1.i_item_sk,inv1.d_moy,inv1.mean, inv1.cov
    ,inv2.w_warehouse_sk,inv2.i_item_sk,inv2.d_moy,inv2.mean, inv2.cov
      from
      (select w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy
        ,stdev,mean, case mean when cast (0 as double) then null else stdev/mean end cov
  from(select w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy
    ,stddev_samp(inv_quantity_on_hand) stdev,avg(inv_quantity_on_hand) mean
      from inventory
      JOIN item ON inventory.inv_item_sk = item.i_item_sk
  JOIN warehouse ON inventory.inv_warehouse_sk = warehouse.w_warehouse_sk
  JOIN date_dim ON inventory.inv_date_sk = date_dim.d_date_sk
  where
  d_year =2000
  group by w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy) foo
  where case mean when cast (0 as double) then cast (0 as double) else stdev/mean end > 1) inv1
  JOIN
  (select w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy
    ,stdev,mean, case mean when cast (0 as double) then null else stdev/mean end cov
  from(select w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy
    ,stddev_samp(inv_quantity_on_hand) stdev,avg(inv_quantity_on_hand) mean
      from inventory
      JOIN item ON inventory.inv_item_sk = item.i_item_sk
  JOIN warehouse ON inventory.inv_warehouse_sk = warehouse.w_warehouse_sk
  JOIN date_dim ON inventory.inv_date_sk = date_dim.d_date_sk
  where
  d_year =2000
  group by w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy) foo
  where case mean when cast (0 as double) then cast (0 as double) else stdev/mean end > 1) inv2
  ON
  inv1.i_item_sk = inv2.i_item_sk
  and inv1.w_warehouse_sk =  inv2.w_warehouse_sk
  where
  inv1.d_moy=1
  and inv2.d_moy=2
  order by inv1.w_warehouse_sk,inv1.i_item_sk,inv1.d_moy,inv1.mean,inv1.cov
  ,inv2.d_moy,inv2.mean, inv2.cov

                    """)


  val query40 = hql("""

    select
    w_state
    ,i_item_id
    ,sum(case when (d_date < '1998-04-08')
  then cs_sales_price - coalesce(cr_refunded_cash,0) else cast(0.0 as float) end) as sales_before
  ,sum(case when (d_date >= '1998-04-08')
  then cs_sales_price - coalesce(cr_refunded_cash,0) else cast(0.0 as float) end) as sales_after
    from
  catalog_sales left outer join catalog_returns on
    (catalog_sales.cs_order_number = catalog_returns.cr_order_number
      and catalog_sales.cs_item_sk = catalog_returns.cr_item_sk)
  JOIN warehouse ON catalog_sales.cs_warehouse_sk    = warehouse.w_warehouse_sk
  JOIN item ON item.i_item_sk          = catalog_sales.cs_item_sk
  JOIN date_dim ON catalog_sales.cs_sold_date_sk    = date_dim.d_date_sk
  where
  i_current_price between 0.99 and 1.49
  and d_date between '1998-03-09' and '1998-05-08'
  group by
    w_state,i_item_id
  order by w_state,i_item_id
  limit 100



                         """)

  val query45 = hql("""
    select  ca_zip, ca_county, sum(ws_sales_price)
    from
    web_sales
    JOIN customer ON web_sales.ws_bill_customer_sk = customer.c_customer_sk
    JOIN customer_address ON customer.c_current_addr_sk = customer_address.ca_address_sk
    JOIN date_dim ON web_sales.ws_sold_date_sk = date_dim.d_date_sk
    JOIN item ON web_sales.ws_item_sk = item.i_item_sk
    where
    ( item.i_item_id in (select i_item_id
      from item
      where i_item_sk in (2, 3, 5, 7, 11, 13, 17, 19, 23, 29)
      )
      )
    and d_qoy = 2 and d_year = 2000
    group by ca_zip, ca_county
    order by ca_zip, ca_county
    limit 100

                         """)


  val query48 = hql("""

    select sum (ss_quantity)
    from store_sales
    JOIN store ON store.s_store_sk = store_sales.ss_store_sk
    JOIN customer_demographics ON customer_demographics.cd_demo_sk = store_sales.ss_cdemo_sk
    JOIN customer_address ON store_sales.ss_addr_sk = customer_address.ca_address_sk
    JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
    where
    d_year = 1998
    and
    (
      (
        cd_marital_status = 'M'
          and
          cd_education_status = '4 yr Degree'
  and
  ss_sales_price between 100.00 and 150.00
  )
  or
  (
    cd_marital_status = 'M'
      and
      cd_education_status = '4 yr Degree'
  and
  ss_sales_price between 50.00 and 100.00
  )
  or
  (
    cd_marital_status = 'M'
      and
      cd_education_status = '4 yr Degree'
  and
  ss_sales_price between 150.00 and 200.00
  )
  )
  and
  (
    (
      ca_country = 'United States'
  and
  ca_state in ('KY', 'GA', 'NM')
  and ss_net_profit between 0 and 2000
  )
  or
  (
    ca_country = 'United States'
  and
  ca_state in ('MT', 'OR', 'IN')
  and ss_net_profit between 150 and 3000
  )
  or
  (
    ca_country = 'United States'
  and
  ca_state in ('WI', 'MO', 'WV')
  and ss_net_profit between 50 and 25000
  )
  )



                         """)


  val query49 = hql("""

    select channel, item, return_ratio, return_rank, currency_rank from
    (
      select
  'web' as channel ,web.item ,web.return_ratio ,web.return_rank ,web.currency_rank from (
    select item ,return_ratio ,currency_ratio
    ,rank() over (order by return_ratio) as return_rank
    ,rank() over (order by currency_ratio) as currency_rank
    from (	select ws.ws_item_sk as item
    ,(cast(sum(coalesce(wr.wr_return_quantity,0)) as decimal)/
    cast(sum(coalesce(ws.ws_quantity,0)) as decimal )) as return_ratio
    ,(cast(sum(coalesce(wr.wr_return_amt,0)) as decimal)/
    cast(sum(coalesce(ws.ws_net_paid,0)) as decimal )) as currency_ratio
    from
    web_sales ws left outer join web_returns wr
    on (ws.ws_order_number = wr.wr_order_number and
    ws.ws_item_sk = wr.wr_item_sk)
    JOIN date_dim ON ws.ws_sold_date_sk = date_dim.d_date_sk
    where
    wr.wr_return_amt > 10000 and ws.ws_net_profit > 1 and ws.ws_net_paid > 0
    and ws.ws_quantity > 0 and d_year = 2000 and d_moy = 12
    group by ws.ws_item_sk
  ) in_web
  ) web
  where ( web.return_rank <= 10 or web.currency_rank <= 10)
  union all
    select 'catalog' as channel ,catalog.item ,catalog.return_ratio ,catalog.return_rank ,catalog.currency_rank from (
    select item ,return_ratio ,currency_ratio
    ,rank() over (order by return_ratio) as return_rank
    ,rank() over (order by currency_ratio) as currency_rank
    from (	select
    cs.cs_item_sk as item
    ,(cast(sum(coalesce(cr.cr_return_quantity,0)) as decimal)/
    cast(sum(coalesce(cs.cs_quantity,0)) as decimal )) as return_ratio
    ,(cast(sum(coalesce(cr.cr_return_amount,0)) as decimal)/
    cast(sum(coalesce(cs.cs_net_paid,0)) as decimal )) as currency_ratio
    from
    catalog_sales cs left outer join catalog_returns cr
    on (cs.cs_order_number = cr.cr_order_number and
    cs.cs_item_sk = cr.cr_item_sk)
    JOIN date_dim ON cs.cs_sold_date_sk = date_dim.d_date_sk
    where
    cr.cr_return_amount > 10000 and cs.cs_net_profit > 1 and cs.cs_net_paid > 0
    and cs.cs_quantity > 0 and d_year = 2000 and d_moy = 12
    group by cs.cs_item_sk
  ) in_cat
  ) catalog
  where ( catalog.return_rank <= 10 or catalog.currency_rank <=10)
  union all
    select
  'store' as channel ,store.item ,store.return_ratio ,store.return_rank ,store.currency_rank from (
    select item ,return_ratio ,currency_ratio
    ,rank() over (order by return_ratio) as return_rank
    ,rank() over (order by currency_ratio) as currency_rank
    from (	select sts.ss_item_sk as item
    ,(cast(sum(coalesce(sr.sr_return_quantity,0)) as decimal)/cast(sum(coalesce(sts.ss_quantity,0)) as decimal )) as return_ratio
    ,(cast(sum(coalesce(sr.sr_return_amt,0)) as decimal)/cast(sum(coalesce(sts.ss_net_paid,0)) as decimal )) as currency_ratio
    from
    store_sales sts left outer join store_returns sr
    on (sts.ss_ticket_number = sr.sr_ticket_number and sts.ss_item_sk = sr.sr_item_sk)
    JOIN date_dim ON sts.ss_sold_date_sk = date_dim.d_date_sk
    where
    sr.sr_return_amt > 10000 and sts.ss_net_profit > 1 and sts.ss_net_paid > 0
    and sts.ss_quantity > 0 and d_year = 2000 and d_moy = 12
    group by sts.ss_item_sk
  ) in_store
  ) store
  where  ( store.return_rank <= 10 or store.currency_rank <= 10)
  ) sub
  order by channel, return_rank, currency_rank
  limit 100


                         """)


  val query50 = hql("""

    select  s_store_name ,s_company_id ,s_street_number ,s_street_name ,s_street_type
    ,s_suite_number ,s_city ,s_county ,s_state ,s_zip
    ,sum(case when (sr_returned_date_sk - ss_sold_date_sk <= 30 ) then 1 else 0 end)  as 30days
  ,sum(case when (sr_returned_date_sk - ss_sold_date_sk > 30) and
    (sr_returned_date_sk - ss_sold_date_sk <= 60) then 1 else 0 end )  as 3160days
  ,sum(case when (sr_returned_date_sk - ss_sold_date_sk > 60) and
    (sr_returned_date_sk - ss_sold_date_sk <= 90) then 1 else 0 end)  as 6190days
  ,sum(case when (sr_returned_date_sk - ss_sold_date_sk > 90) and
    (sr_returned_date_sk - ss_sold_date_sk <= 120) then 1 else 0 end)  as 91120days
  ,sum(case when (sr_returned_date_sk - ss_sold_date_sk  > 120) then 1 else 0 end)  as 120days
  from
  store_sales
  JOIN store_returns ON
  store_sales.ss_ticket_number = store_returns.sr_ticket_number
  and store_sales.ss_item_sk = store_returns.sr_item_sk
  and store_sales.ss_customer_sk = store_returns.sr_customer_sk
  JOIN store ON store_sales.ss_store_sk = store.s_store_sk
  JOIN date_dim d1 ON store_sales.ss_sold_date_sk   = d1.d_date_sk
  JOIN date_dim d2 ON store_returns.sr_returned_date_sk   = d2.d_date_sk
  where
  d2.d_year = 2000
  and d2.d_moy  = 9
  group by
    s_store_name ,s_company_id ,s_street_number ,s_street_name ,s_street_type ,s_suite_number
  ,s_city ,s_county ,s_state ,s_zip
  order by s_store_name ,s_company_id ,s_street_number ,s_street_name ,s_street_type
  ,s_suite_number ,s_city ,s_county ,s_state ,s_zip
  limit 100


                    """)

  val query58 = hql("""

    select  ss_items.item_id
    ,ss_item_rev
    ,ss_item_rev/(ss_item_rev+cs_item_rev+ws_item_rev)/3 * 100 ss_dev
    ,cs_item_rev
    ,cs_item_rev/(ss_item_rev+cs_item_rev+ws_item_rev)/3 * 100 cs_dev
    ,ws_item_rev
    ,ws_item_rev/(ss_item_rev+cs_item_rev+ws_item_rev)/3 * 100 ws_dev
    ,(ss_item_rev+cs_item_rev+ws_item_rev)/3 average
      FROM
        ( select i_item_id item_id ,sum(ss_ext_sales_price) as ss_item_rev
          from store_sales
          JOIN item ON store_sales.ss_item_sk = item.i_item_sk
          JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
          JOIN (select d1.d_date
          from date_dim d1 JOIN date_dim d2 ON d1.d_week_seq = d2.d_week_seq
          where d2.d_date = '1998-08-04') sub ON date_dim.d_date = sub.d_date
  group by i_item_id ) ss_items
  JOIN
  ( select i_item_id item_id ,sum(cs_ext_sales_price) as cs_item_rev
    from catalog_sales
    JOIN item ON catalog_sales.cs_item_sk = item.i_item_sk
    JOIN date_dim ON catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
    JOIN (select d1.d_date
    from date_dim d1 JOIN date_dim d2 ON d1.d_week_seq = d2.d_week_seq
    where d2.d_date = '1998-08-04') sub ON date_dim.d_date = sub.d_date
  group by i_item_id ) cs_items
  ON ss_items.item_id=cs_items.item_id
  JOIN
  ( select i_item_id item_id ,sum(ws_ext_sales_price) as ws_item_rev
    from web_sales
    JOIN item ON web_sales.ws_item_sk = item.i_item_sk
    JOIN date_dim ON web_sales.ws_sold_date_sk = date_dim.d_date_sk
    JOIN (select d1.d_date
    from date_dim d1 JOIN date_dim d2 ON d1.d_week_seq = d2.d_week_seq
    where d2.d_date = '1998-08-04') sub ON date_dim.d_date = sub.d_date
  group by i_item_id ) ws_items
  ON ss_items.item_id=ws_items.item_id
  where
  ss_item_rev between 0.9 * cs_item_rev and 1.1 * cs_item_rev
  and ss_item_rev between 0.9 * ws_item_rev and 1.1 * ws_item_rev
    and cs_item_rev between 0.9 * ss_item_rev and 1.1 * ss_item_rev
    and cs_item_rev between 0.9 * ws_item_rev and 1.1 * ws_item_rev
    and ws_item_rev between 0.9 * ss_item_rev and 1.1 * ss_item_rev
    and ws_item_rev between 0.9 * cs_item_rev and 1.1 * cs_item_rev
    order by item_id ,ss_item_rev
  limit 100


                         """)


  val query64 = hql("""

    select cs1.product_name ,cs1.store_name ,cs1.store_zip ,cs1.b_street_number ,cs1.b_streen_name ,cs1.b_city
    ,cs1.b_zip ,cs1.c_street_number ,cs1.c_street_name ,cs1.c_city ,cs1.c_zip ,cs1.syear ,cs1.cnt
    ,cs1.s1 ,cs1.s2 ,cs1.s3
    ,cs2.s1 ,cs2.s2 ,cs2.s3 ,cs2.syear ,cs2.cnt
      from
      (select i_product_name as product_name ,i_item_sk as item_sk ,s_store_name as store_name
        ,s_zip as store_zip ,ad1.ca_street_number as b_street_number ,ad1.ca_street_name as b_streen_name
        ,ad1.ca_city as b_city ,ad1.ca_zip as b_zip ,ad2.ca_street_number as c_street_number
        ,ad2.ca_street_name as c_street_name ,ad2.ca_city as c_city ,ad2.ca_zip as c_zip
        ,d1.d_year as syear ,d2.d_year as fsyear ,d3.d_year as s2year ,count(*) as cnt
        ,sum(ss_wholesale_cost) as s1 ,sum(ss_list_price) as s2 ,sum(ss_coupon_amt) as s3
        FROM   store_sales
        JOIN store_returns ON store_sales.ss_item_sk = store_returns.sr_item_sk and store_sales.ss_ticket_number = store_returns.sr_ticket_number
        JOIN customer ON store_sales.ss_customer_sk = customer.c_customer_sk
        JOIN date_dim d1 ON store_sales.ss_sold_date_sk = d1.d_date_sk
      JOIN date_dim d2 ON customer.c_first_sales_date_sk = d2.d_date_sk
  JOIN date_dim d3 ON customer.c_first_shipto_date_sk = d3.d_date_sk
  JOIN store ON store_sales.ss_store_sk = store.s_store_sk
  JOIN customer_demographics cd1 ON store_sales.ss_cdemo_sk= cd1.cd_demo_sk
  JOIN customer_demographics cd2 ON customer.c_current_cdemo_sk = cd2.cd_demo_sk
  JOIN promotion ON store_sales.ss_promo_sk = promotion.p_promo_sk
  JOIN household_demographics hd1 ON store_sales.ss_hdemo_sk = hd1.hd_demo_sk
  JOIN household_demographics hd2 ON customer.c_current_hdemo_sk = hd2.hd_demo_sk
  JOIN customer_address ad1 ON store_sales.ss_addr_sk = ad1.ca_address_sk
  JOIN customer_address ad2 ON customer.c_current_addr_sk = ad2.ca_address_sk
  JOIN income_band ib1 ON hd1.hd_income_band_sk = ib1.ib_income_band_sk
  JOIN income_band ib2 ON hd2.hd_income_band_sk = ib2.ib_income_band_sk
  JOIN item ON store_sales.ss_item_sk = item.i_item_sk
  JOIN
  (select cs_item_sk
    ,sum(cs_ext_list_price) as sale,sum(cr_refunded_cash+cr_reversed_charge+cr_store_credit) as refund
    from catalog_sales JOIN catalog_returns
    ON catalog_sales.cs_item_sk = catalog_returns.cr_item_sk
    and catalog_sales.cs_order_number = catalog_returns.cr_order_number
    group by cs_item_sk
    having sum(cs_ext_list_price)>2*sum(cr_refunded_cash+cr_reversed_charge+cr_store_credit)) cs_ui
    ON store_sales.ss_item_sk = cs_ui.cs_item_sk
  WHERE
  cd1.cd_marital_status <> cd2.cd_marital_status and
    i_color in ('maroon','burnished','dim','steel','navajo','chocolate') and
  i_current_price between 35 and 35 + 10 and
    i_current_price between 35 + 1 and 35 + 15
  group by i_product_name ,i_item_sk ,s_store_name ,s_zip ,ad1.ca_street_number
  ,ad1.ca_street_name ,ad1.ca_city ,ad1.ca_zip ,ad2.ca_street_number
  ,ad2.ca_street_name ,ad2.ca_city ,ad2.ca_zip ,d1.d_year ,d2.d_year ,d3.d_year
  ) cs1
  JOIN
  (select i_product_name as product_name ,i_item_sk as item_sk ,s_store_name as store_name
    ,s_zip as store_zip ,ad1.ca_street_number as b_street_number ,ad1.ca_street_name as b_streen_name
    ,ad1.ca_city as b_city ,ad1.ca_zip as b_zip ,ad2.ca_street_number as c_street_number
    ,ad2.ca_street_name as c_street_name ,ad2.ca_city as c_city ,ad2.ca_zip as c_zip
    ,d1.d_year as syear ,d2.d_year as fsyear ,d3.d_year as s2year ,count(*) as cnt
    ,sum(ss_wholesale_cost) as s1 ,sum(ss_list_price) as s2 ,sum(ss_coupon_amt) as s3
    FROM   store_sales
    JOIN store_returns ON store_sales.ss_item_sk = store_returns.sr_item_sk and store_sales.ss_ticket_number = store_returns.sr_ticket_number
    JOIN customer ON store_sales.ss_customer_sk = customer.c_customer_sk
    JOIN date_dim d1 ON store_sales.ss_sold_date_sk = d1.d_date_sk
  JOIN date_dim d2 ON customer.c_first_sales_date_sk = d2.d_date_sk
  JOIN date_dim d3 ON customer.c_first_shipto_date_sk = d3.d_date_sk
  JOIN store ON store_sales.ss_store_sk = store.s_store_sk
  JOIN customer_demographics cd1 ON store_sales.ss_cdemo_sk= cd1.cd_demo_sk
  JOIN customer_demographics cd2 ON customer.c_current_cdemo_sk = cd2.cd_demo_sk
  JOIN promotion ON store_sales.ss_promo_sk = promotion.p_promo_sk
  JOIN household_demographics hd1 ON store_sales.ss_hdemo_sk = hd1.hd_demo_sk
  JOIN household_demographics hd2 ON customer.c_current_hdemo_sk = hd2.hd_demo_sk
  JOIN customer_address ad1 ON store_sales.ss_addr_sk = ad1.ca_address_sk
  JOIN customer_address ad2 ON customer.c_current_addr_sk = ad2.ca_address_sk
  JOIN income_band ib1 ON hd1.hd_income_band_sk = ib1.ib_income_band_sk
  JOIN income_band ib2 ON hd2.hd_income_band_sk = ib2.ib_income_band_sk
  JOIN item ON store_sales.ss_item_sk = item.i_item_sk
  JOIN
  (select cs_item_sk
    ,sum(cs_ext_list_price) as sale,sum(cr_refunded_cash+cr_reversed_charge+cr_store_credit) as refund
    from catalog_sales JOIN catalog_returns
    ON catalog_sales.cs_item_sk = catalog_returns.cr_item_sk
    and catalog_sales.cs_order_number = catalog_returns.cr_order_number
    group by cs_item_sk
    having sum(cs_ext_list_price)>2*sum(cr_refunded_cash+cr_reversed_charge+cr_store_credit)) cs_ui
    ON store_sales.ss_item_sk = cs_ui.cs_item_sk
  WHERE
  cd1.cd_marital_status <> cd2.cd_marital_status and
    i_color in ('maroon','burnished','dim','steel','navajo','chocolate') and
  i_current_price between 35 and 35 + 10 and
    i_current_price between 35 + 1 and 35 + 15
  group by i_product_name ,i_item_sk ,s_store_name ,s_zip ,ad1.ca_street_number
  ,ad1.ca_street_name ,ad1.ca_city ,ad1.ca_zip ,ad2.ca_street_number
  ,ad2.ca_street_name ,ad2.ca_city ,ad2.ca_zip ,d1.d_year ,d2.d_year ,d3.d_year
  ) cs2
  ON cs1.item_sk=cs2.item_sk
  where
  cs1.syear = 2000 and
    cs2.syear = 2000 + 1 and
    cs2.cnt <= cs1.cnt and
    cs1.store_name = cs2.store_name and
    cs1.store_zip = cs2.store_zip
  order by cs1.product_name ,cs1.store_name ,cs2.cnt

                         """)


  val query66 = hql("""

    select
    w_warehouse_name ,w_warehouse_sq_ft ,w_city ,w_county ,w_state ,w_country
    ,ship_carriers ,year
    ,sum(jan_sales) as jan_sales ,sum(feb_sales) as feb_sales
    ,sum(mar_sales) as mar_sales ,sum(apr_sales) as apr_sales
    ,sum(may_sales) as may_sales ,sum(jun_sales) as jun_sales
    ,sum(jul_sales) as jul_sales ,sum(aug_sales) as aug_sales
    ,sum(sep_sales) as sep_sales ,sum(oct_sales) as oct_sales
    ,sum(nov_sales) as nov_sales ,sum(dec_sales) as dec_sales
    ,sum(jan_sales/w_warehouse_sq_ft) as jan_sales_per_sq_foot
    ,sum(feb_sales/w_warehouse_sq_ft) as feb_sales_per_sq_foot
    ,sum(mar_sales/w_warehouse_sq_ft) as mar_sales_per_sq_foot
    ,sum(apr_sales/w_warehouse_sq_ft) as apr_sales_per_sq_foot
    ,sum(may_sales/w_warehouse_sq_ft) as may_sales_per_sq_foot
    ,sum(jun_sales/w_warehouse_sq_ft) as jun_sales_per_sq_foot
    ,sum(jul_sales/w_warehouse_sq_ft) as jul_sales_per_sq_foot
    ,sum(aug_sales/w_warehouse_sq_ft) as aug_sales_per_sq_foot
    ,sum(sep_sales/w_warehouse_sq_ft) as sep_sales_per_sq_foot
    ,sum(oct_sales/w_warehouse_sq_ft) as oct_sales_per_sq_foot
    ,sum(nov_sales/w_warehouse_sq_ft) as nov_sales_per_sq_foot
    ,sum(dec_sales/w_warehouse_sq_ft) as dec_sales_per_sq_foot
    ,sum(jan_net) as jan_net ,sum(feb_net) as feb_net
    ,sum(mar_net) as mar_net ,sum(apr_net) as apr_net
    ,sum(may_net) as may_net ,sum(jun_net) as jun_net
    ,sum(jul_net) as jul_net ,sum(aug_net) as aug_net
    ,sum(sep_net) as sep_net ,sum(oct_net) as oct_net
    ,sum(nov_net) as nov_net ,sum(dec_net) as dec_net
      from (
      select
        w_warehouse_name ,w_warehouse_sq_ft ,w_city ,w_county ,w_state
      ,w_country , concat('DIAMOND', ',', 'AIRBORNE') as ship_carriers
  ,d_year as year
  ,sum(case when d_moy = 1 then ws_sales_price* ws_quantity else cast(0 as float) end) as jan_sales
  ,sum(case when d_moy = 2 then ws_sales_price* ws_quantity else cast(0 as float) end) as feb_sales
  ,sum(case when d_moy = 3 then ws_sales_price* ws_quantity else cast(0 as float) end) as mar_sales
  ,sum(case when d_moy = 4 then ws_sales_price* ws_quantity else cast(0 as float) end) as apr_sales
  ,sum(case when d_moy = 5 then ws_sales_price* ws_quantity else cast(0 as float) end) as may_sales
  ,sum(case when d_moy = 6 then ws_sales_price* ws_quantity else cast(0 as float) end) as jun_sales
  ,sum(case when d_moy = 7 then ws_sales_price* ws_quantity else cast(0 as float) end) as jul_sales
  ,sum(case when d_moy = 8 then ws_sales_price* ws_quantity else cast(0 as float) end) as aug_sales
  ,sum(case when d_moy = 9 then ws_sales_price* ws_quantity else cast(0 as float) end) as sep_sales
  ,sum(case when d_moy = 10 then ws_sales_price* ws_quantity else cast(0 as float) end) as oct_sales
  ,sum(case when d_moy = 11 then ws_sales_price* ws_quantity else cast(0 as float) end) as nov_sales
  ,sum(case when d_moy = 12 then ws_sales_price* ws_quantity else cast(0 as float) end) as dec_sales
  ,sum(case when d_moy = 1 then ws_net_paid_inc_tax * ws_quantity else cast(0 as float) end) as jan_net
  ,sum(case when d_moy = 2 then ws_net_paid_inc_tax * ws_quantity else cast(0 as float) end) as feb_net
  ,sum(case when d_moy = 3 then ws_net_paid_inc_tax * ws_quantity else cast(0 as float) end) as mar_net
  ,sum(case when d_moy = 4 then ws_net_paid_inc_tax * ws_quantity else cast(0 as float) end) as apr_net
  ,sum(case when d_moy = 5 then ws_net_paid_inc_tax * ws_quantity else cast(0 as float) end) as may_net
  ,sum(case when d_moy = 6 then ws_net_paid_inc_tax * ws_quantity else cast(0 as float) end) as jun_net
  ,sum(case when d_moy = 7 then ws_net_paid_inc_tax * ws_quantity else cast(0 as float) end) as jul_net
  ,sum(case when d_moy = 8 then ws_net_paid_inc_tax * ws_quantity else cast(0 as float) end) as aug_net
  ,sum(case when d_moy = 9 then ws_net_paid_inc_tax * ws_quantity else cast(0 as float) end) as sep_net
  ,sum(case when d_moy = 10 then ws_net_paid_inc_tax * ws_quantity else cast(0 as float) end) as oct_net
  ,sum(case when d_moy = 11 then ws_net_paid_inc_tax * ws_quantity else cast(0 as float) end) as nov_net
  ,sum(case when d_moy = 12 then ws_net_paid_inc_tax * ws_quantity else cast(0 as float) end) as dec_net
    from
  web_sales
  JOIN warehouse ON web_sales.ws_warehouse_sk =  warehouse.w_warehouse_sk
  JOIN date_dim ON web_sales.ws_sold_date_sk = date_dim.d_date_sk
  JOIN time_dim ON web_sales.ws_sold_time_sk = time_dim.t_time_sk
  JOIN ship_mode ON web_sales.ws_ship_mode_sk = ship_mode.sm_ship_mode_sk
  where
  d_year = 2002
  and t_time between 49530 and 49530+28800
  and sm_carrier in ('DIAMOND','AIRBORNE')
  group by
    w_warehouse_name ,w_warehouse_sq_ft ,w_city
  ,w_county ,w_state ,w_country ,d_year
  union all
    select
  w_warehouse_name ,w_warehouse_sq_ft ,w_city ,w_county
  ,w_state ,w_country , concat('DIAMOND', ',', 'AIRBORNE') as ship_carriers
  ,d_year as year
  ,sum(case when d_moy = 1 then cs_ext_sales_price* cs_quantity else cast(0 as float) end) as jan_sales
  ,sum(case when d_moy = 2 then cs_ext_sales_price* cs_quantity else cast(0 as float) end) as feb_sales
  ,sum(case when d_moy = 3 then cs_ext_sales_price* cs_quantity else cast(0 as float) end) as mar_sales
  ,sum(case when d_moy = 4 then cs_ext_sales_price* cs_quantity else cast(0 as float) end) as apr_sales
  ,sum(case when d_moy = 5 then cs_ext_sales_price* cs_quantity else cast(0 as float) end) as may_sales
  ,sum(case when d_moy = 6 then cs_ext_sales_price* cs_quantity else cast(0 as float) end) as jun_sales
  ,sum(case when d_moy = 7 then cs_ext_sales_price* cs_quantity else cast(0 as float) end) as jul_sales
  ,sum(case when d_moy = 8 then cs_ext_sales_price* cs_quantity else cast(0 as float) end) as aug_sales
  ,sum(case when d_moy = 9 then cs_ext_sales_price* cs_quantity else cast(0 as float) end) as sep_sales
  ,sum(case when d_moy = 10 then cs_ext_sales_price* cs_quantity else cast(0 as float) end) as oct_sales
  ,sum(case when d_moy = 11 then cs_ext_sales_price* cs_quantity else cast(0 as float) end) as nov_sales
  ,sum(case when d_moy = 12 then cs_ext_sales_price* cs_quantity else cast(0 as float) end) as dec_sales
  ,sum(case when d_moy = 1 then cs_net_paid_inc_ship_tax * cs_quantity else cast(0 as float) end) as jan_net
  ,sum(case when d_moy = 2 then cs_net_paid_inc_ship_tax * cs_quantity else cast(0 as float) end) as feb_net
  ,sum(case when d_moy = 3 then cs_net_paid_inc_ship_tax * cs_quantity else cast(0 as float) end) as mar_net
  ,sum(case when d_moy = 4 then cs_net_paid_inc_ship_tax * cs_quantity else cast(0 as float) end) as apr_net
  ,sum(case when d_moy = 5 then cs_net_paid_inc_ship_tax * cs_quantity else cast(0 as float) end) as may_net
  ,sum(case when d_moy = 6 then cs_net_paid_inc_ship_tax * cs_quantity else cast(0 as float) end) as jun_net
  ,sum(case when d_moy = 7 then cs_net_paid_inc_ship_tax * cs_quantity else cast(0 as float) end) as jul_net
  ,sum(case when d_moy = 8 then cs_net_paid_inc_ship_tax * cs_quantity else cast(0 as float) end) as aug_net
  ,sum(case when d_moy = 9 then cs_net_paid_inc_ship_tax * cs_quantity else cast(0 as float) end) as sep_net
  ,sum(case when d_moy = 10 then cs_net_paid_inc_ship_tax * cs_quantity else cast(0 as float) end) as oct_net
  ,sum(case when d_moy = 11 then cs_net_paid_inc_ship_tax * cs_quantity else cast(0 as float) end) as nov_net
  ,sum(case when d_moy = 12 then cs_net_paid_inc_ship_tax * cs_quantity else cast(0 as float) end) as dec_net
    from
  catalog_sales
  JOIN warehouse ON catalog_sales.cs_warehouse_sk =  warehouse.w_warehouse_sk
  JOIN date_dim ON catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
  JOIN time_dim ON catalog_sales.cs_sold_time_sk = time_dim.t_time_sk
  JOIN ship_mode ON catalog_sales.cs_ship_mode_sk = ship_mode.sm_ship_mode_sk
  where
  d_year = 2002
  and t_time between 49530 AND 49530+28800
  and sm_carrier in ('DIAMOND','AIRBORNE')
  group by
    w_warehouse_name ,w_warehouse_sq_ft ,w_city ,w_county
  ,w_state ,w_country ,d_year
  ) x
  group by
    w_warehouse_name ,w_warehouse_sq_ft ,w_city ,w_county
  ,w_state ,w_country ,ship_carriers ,year
  order by w_warehouse_name
  limit 100


                         """)


  val query67 = hql("""

    select  *
    from (select i_category ,i_class ,i_brand ,i_product_name ,d_year ,d_qoy
    ,d_moy ,s_store_id ,sumsales
    ,rank() over (partition by i_category order by sumsales desc) rk
    from (select i_category ,i_class ,i_brand ,i_product_name ,d_year ,d_qoy
      ,d_moy ,s_store_id
      ,sum(coalesce(ss_sales_price*ss_quantity,0)) sumsales
        from store_sales
        JOIN date_dim ON store_sales.ss_sold_date_sk=date_dim.d_date_sk
    JOIN store ON store_sales.ss_store_sk = store.s_store_sk
    JOIN item ON store_sales.ss_item_sk=item.i_item_sk
    where
    d_month_seq between 1193 and 1193+11
    group by i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy,s_store_id with rollup)dw1) dw2
  where rk <= 100
  order by i_category ,i_class ,i_brand ,i_product_name ,d_year
  ,d_qoy ,d_moy ,s_store_id ,sumsales ,rk
  limit 100


                         """)


  val query70 = hql("""
    select
    sum(ss_net_profit) as total_sum
    ,s_state
    ,s_county
    ,grouping__id as lochierarchy
    , rank() over(partition by grouping__id, case when grouping__id == 2 then s_state end order by sum(ss_net_profit)) as rank_within_parent
    from
  store_sales ss join date_dim d1 on d1.d_date_sk = ss.ss_sold_date_sk
  join store s on s.s_store_sk  = ss.ss_store_sk
  where
  d1.d_month_seq between 1193 and 1193+11
  and s.s_state in
    ( select s_state
      from  (select s_state as s_state, sum(ss_net_profit),
        rank() over ( partition by s_state order by sum(ss_net_profit) desc) as ranking
          from   store_sales ss join store s on s.s_store_sk  = ss.ss_store_sk
          join  date_dim d1 on d1.d_date_sk = ss.ss_sold_date_sk
      where  d_month_seq between 1193 and 1193+11
      group by s_state
      ) tmp1
    where ranking <= 5
  )
  group by s_state,s_county with rollup
  order by
    lochierarchy desc
  ,case when lochierarchy = 0 then s_state end
  ,rank_within_parent
  limit 100

                         """)


  val query71 = hql("""

    select i_brand_id brand_id, i_brand brand,t_hour,t_minute,
    sum(ext_price) ext_price
      from item
      JOIN (select ws_ext_sales_price as ext_price,
        ws_sold_date_sk as sold_date_sk,
        ws_item_sk as sold_item_sk,
        ws_sold_time_sk as time_sk
          from web_sales
          JOIN date_dim ON date_dim.d_date_sk = web_sales.ws_sold_date_sk
          where
          d_moy=12
          and d_year=2001
          union all
          select cs_ext_sales_price as ext_price,
        cs_sold_date_sk as sold_date_sk,
        cs_item_sk as sold_item_sk,
        cs_sold_time_sk as time_sk
          from catalog_sales
          JOIN date_dim ON date_dim.d_date_sk = catalog_sales.cs_sold_date_sk
          where
          d_moy=12
          and d_year=2001
          union all
          select ss_ext_sales_price as ext_price,
        ss_sold_date_sk as sold_date_sk,
        ss_item_sk as sold_item_sk,
        ss_sold_time_sk as time_sk
          from store_sales
          JOIN date_dim ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
          where
          d_moy=12
          and d_year=2001
      ) tmp ON tmp.sold_item_sk = item.i_item_sk
  JOIN time_dim ON tmp.time_sk = time_dim.t_time_sk
  where
  i_manager_id=1
  and (t_meal_time = 'breakfast' or t_meal_time = 'dinner')
  group by i_brand, i_brand_id,t_hour,t_minute
  order by ext_price desc, brand_id




                    """)


  val query72 = hql("""

    select  i_item_desc
    ,w_warehouse_name
    ,d1.d_week_seq
    ,count(case when p_promo_sk is null then 1 else 0 end) no_promo
  ,count(case when p_promo_sk is not null then 1 else 0 end) promo
  ,count(*) total_cnt
    from catalog_sales
    join inventory on (catalog_sales.cs_item_sk = inventory.inv_item_sk)
  join warehouse on (warehouse.w_warehouse_sk=inventory.inv_warehouse_sk)
  join item on (item.i_item_sk = catalog_sales.cs_item_sk)
  join customer_demographics on (catalog_sales.cs_bill_cdemo_sk = customer_demographics.cd_demo_sk)
  join household_demographics on (catalog_sales.cs_bill_hdemo_sk = household_demographics.hd_demo_sk)
  join date_dim d1 on (catalog_sales.cs_sold_date_sk = d1.d_date_sk)
  join date_dim d2 on (inventory.inv_date_sk = d2.d_date_sk)
  join date_dim d3 on (catalog_sales.cs_ship_date_sk = d3.d_date_sk)
  left outer join promotion on (catalog_sales.cs_promo_sk=promotion.p_promo_sk)
  left outer join catalog_returns on (catalog_returns.cr_item_sk = catalog_sales.cs_item_sk and
    catalog_returns.cr_order_number = catalog_sales.cs_order_number)
  where d1.d_week_seq = d2.d_week_seq
  and inv_quantity_on_hand < cs_quantity
    and d3.d_date > d1.d_date + 5
  and hd_buy_potential = '1001-5000'
  and d1.d_year = 2001
  and hd_buy_potential = '1001-5000'
  and cd_marital_status = 'M'
  and d1.d_year = 2001
  group by i_item_desc,w_warehouse_name,d1.d_week_seq
  order by total_cnt desc, i_item_desc, w_warehouse_name, d_week_seq
  limit 100



                    """)


  val query76 = hql("""

    select  channel, col_name, d_year, d_qoy, i_category, COUNT(*) sales_cnt, SUM(ext_sales_price) sales_amt FROM (
    SELECT 'store' as channel, 'ss_addr_sk' col_name, d_year, d_qoy, i_category, ss_ext_sales_price ext_sales_price
    FROM store_sales
    JOIN item ON store_sales.ss_item_sk=item.i_item_sk
  JOIN date_dim ON store_sales.ss_sold_date_sk=date_dim.d_date_sk
  WHERE ss_addr_sk IS NULL
    UNION ALL
    SELECT 'web' as channel, 'ws_web_page_sk' col_name, d_year, d_qoy, i_category, ws_ext_sales_price ext_sales_price
    FROM web_sales
    JOIN item ON web_sales.ws_item_sk=item.i_item_sk
  JOIN date_dim ON web_sales.ws_sold_date_sk=date_dim.d_date_sk
  WHERE ws_web_page_sk IS NULL
    UNION ALL
    SELECT 'catalog' as channel, 'cs_warehouse_sk' col_name, d_year, d_qoy, i_category, cs_ext_sales_price ext_sales_price
    FROM catalog_sales
    JOIN item ON catalog_sales.cs_item_sk=item.i_item_sk
  JOIN date_dim ON catalog_sales.cs_sold_date_sk=date_dim.d_date_sk
  WHERE cs_warehouse_sk IS NULL ) sub
  GROUP BY channel, col_name, d_year, d_qoy, i_category
  ORDER BY channel, col_name, d_year, d_qoy, i_category
  limit 100



                    """)


  val query82 = hql("""
    select i_item_id
    ,i_item_desc
    ,i_current_price
      from item i
      join inventory inv on (inv.inv_item_sk = i.i_item_sk)
      join store_sales ss on (ss.ss_item_sk = i.i_item_sk)
      where i_current_price between 0.0 and 0.0+30.0
      and inv_date between '1998-01-01' and '1998-03-02'
  and i_manufact_id in (11,22,33,44)
  and inv_quantity_on_hand between 100 and 500
  group by i_item_id,i_item_desc,i_current_price
  order by i_item_id
  limit 100

                         """)


  val query84 = hql("""

    select  c_customer_id as customer_id
    ,concat(c_last_name, ', ', c_first_name) as customername
    from customer
    JOIN customer_address ON customer.c_current_addr_sk = customer_address.ca_address_sk
  JOIN customer_demographics ON customer_demographics.cd_demo_sk = customer.c_current_cdemo_sk
  JOIN household_demographics ON household_demographics.hd_demo_sk = customer.c_current_hdemo_sk
  JOIN income_band ON household_demographics.hd_income_band_sk = income_band.ib_income_band_sk
  JOIN store_returns ON customer_demographics.cd_demo_sk = store_returns.sr_cdemo_sk
  where ca_city	        =  'Hopewell'
  and ib_lower_bound   >=  32287
  and ib_upper_bound   <=  32287 + 50000
  order by customer_id
  limit 100


                    """)


  val query85 = hql("""

    select  substr(r_reason_desc,1,20) as r, avg(ws_quantity) as wq, avg(wr_refunded_cash) ref, avg(wr_fee) as fee
    from web_sales
    JOIN web_returns ON web_sales.ws_item_sk = web_returns.wr_item_sk and web_sales.ws_order_number = web_returns.wr_order_number
    JOIN web_page ON web_sales.ws_web_page_sk = web_page.wp_web_page_sk
    JOIN customer_demographics cd1 ON cd1.cd_demo_sk = web_returns.wr_refunded_cdemo_sk
  JOIN customer_demographics cd2 ON cd2.cd_demo_sk = web_returns.wr_returning_cdemo_sk
  JOIN customer_address ON customer_address.ca_address_sk = web_returns.wr_refunded_addr_sk
  JOIN date_dim ON web_sales.ws_sold_date_sk = date_dim.d_date_sk
  JOIN reason ON reason.r_reason_sk = web_returns.wr_reason_sk
  where
  d_year = 1998
  and
  (
    (
      cd1.cd_marital_status = 'M'
        and
        cd1.cd_marital_status = cd2.cd_marital_status
        and
        cd1.cd_education_status = '4 yr Degree'
  and
  cd1.cd_education_status = cd2.cd_education_status
  and
  ws_sales_price >= 100.00 and ws_sales_price <= 150.00
  )
  or
  (
    cd1.cd_marital_status = 'D'
      and
      cd1.cd_marital_status = cd2.cd_marital_status
      and
      cd1.cd_education_status = 'Primary'
  and
  cd1.cd_education_status = cd2.cd_education_status
  and
  ws_sales_price >= 50.00 and ws_sales_price <= 100.00
  )
  or
  (
    cd1.cd_marital_status = 'U'
      and
      cd1.cd_marital_status = cd2.cd_marital_status
      and
      cd1.cd_education_status = 'Advanced Degree'
  and
  cd1.cd_education_status = cd2.cd_education_status
  and
  ws_sales_price >= 150.00 and ws_sales_price <= 200.00
  )
  )
  and
  (
    (
      ca_country = 'United States'
  and
  ca_state in ('KY', 'GA', 'NM')
  and ws_net_profit >= 100 and ws_net_profit <= 200
  )
  or
  (
    ca_country = 'United States'
  and
  ca_state in ('MT', 'OR', 'IN')
  and ws_net_profit >= 150 and ws_net_profit <= 300
  )
  or
  (
    ca_country = 'United States'
  and
  ca_state in ('WI', 'MO', 'WV')
  and ws_net_profit >= 50 and ws_net_profit <= 250
  )
  )
  group by r_reason_desc
  order by r, wq, ref, fee
  limit 100



                    """)


  val query87 = hql("""

    select count(*)
    from (select distinct c_last_name as l1, c_first_name as f1, d_date as d1
    from store_sales
    JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
    JOIN customer ON store_sales.ss_customer_sk = customer.c_customer_sk
    where
    d_month_seq between 1193 and 1193+11) t1
    LEFT OUTER JOIN
    ( select distinct c_last_name as l2, c_first_name as f2, d_date as d2
      from catalog_sales
      JOIN date_dim ON catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
      JOIN customer ON catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
      where
      d_month_seq between 1193 and 1193+11) t2
    ON t1.l1 = t2.l2 and
    t1.f1 = t2.f2 and
    t1.d1 = t2.d2
  LEFT OUTER JOIN
  (select distinct c_last_name as l3, c_first_name as f3, d_date as d3
    from web_sales
    JOIN date_dim ON web_sales.ws_sold_date_sk = date_dim.d_date_sk
    JOIN customer ON web_sales.ws_bill_customer_sk = customer.c_customer_sk
    where
    d_month_seq between 1193 and 1193+11) t3
    ON t1.l1 = t3.l3 and
    t1.f1 = t3.f3 and
    t1.d1 = t3.d3
  WHERE
  l2 is null and
    l3 is null

  -- Note: This query crashes in Hive 11, works in Hive 12.
  -- Note: this over-counts (slightly) the actual results due to nulls in the source tables.
  -- Difficult to fix until Hive supports "except".


                         """)


  val query88 = hql("""

    select  *
    from
    (select count(*) h8_30_to_9
      from store_sales
      JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    JOIN time_dim ON store_sales.ss_sold_time_sk = time_dim.t_time_sk
    JOIN store ON store_sales.ss_store_sk = store.s_store_sk
    where
    time_dim.t_hour = 8
    and time_dim.t_minute >= 30
    and ((household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2) or
    (household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or
    (household_demographics.hd_dep_count = 1 and household_demographics.hd_vehicle_count<=1+2))
    and store.s_store_name = 'ese') s1 JOIN
    (select count(*) h9_to_9_30
      from store_sales
      JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
  JOIN time_dim ON store_sales.ss_sold_time_sk = time_dim.t_time_sk
  JOIN store ON store_sales.ss_store_sk = store.s_store_sk
  where
  time_dim.t_hour = 9
  and time_dim.t_minute < 30
  and ((household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2) or
    (household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or
    (household_demographics.hd_dep_count = 1 and household_demographics.hd_vehicle_count<=1+2))
  and store.s_store_name = 'ese') s2 JOIN
    (select count(*) h9_30_to_10
      from store_sales
      JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
  JOIN time_dim ON store_sales.ss_sold_time_sk = time_dim.t_time_sk
  JOIN store ON store_sales.ss_store_sk = store.s_store_sk
  where
  time_dim.t_hour = 9
  and time_dim.t_minute >= 30
  and ((household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2) or
    (household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or
    (household_demographics.hd_dep_count = 1 and household_demographics.hd_vehicle_count<=1+2))
  and store.s_store_name = 'ese') s3 JOIN
    (select count(*) h10_to_10_30
      from store_sales
      JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
  JOIN time_dim ON store_sales.ss_sold_time_sk = time_dim.t_time_sk
  JOIN store ON store_sales.ss_store_sk = store.s_store_sk
  where
  time_dim.t_hour = 10
  and time_dim.t_minute < 30
  and ((household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2) or
    (household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or
    (household_demographics.hd_dep_count = 1 and household_demographics.hd_vehicle_count<=1+2))
  and store.s_store_name = 'ese') s4 JOIN
    (select count(*) h10_30_to_11
      from store_sales
      JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
  JOIN time_dim ON store_sales.ss_sold_time_sk = time_dim.t_time_sk
  JOIN store ON store_sales.ss_store_sk = store.s_store_sk
  where
  time_dim.t_hour = 10
  and time_dim.t_minute >= 30
  and ((household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2) or
    (household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or
    (household_demographics.hd_dep_count = 1 and household_demographics.hd_vehicle_count<=1+2))
  and store.s_store_name = 'ese') s5 JOIN
    (select count(*) h11_to_11_30
      from store_sales
      JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
  JOIN time_dim ON store_sales.ss_sold_time_sk = time_dim.t_time_sk
  JOIN store ON store_sales.ss_store_sk = store.s_store_sk
  where
  time_dim.t_hour = 11
  and time_dim.t_minute < 30
  and ((household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2) or
    (household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or
    (household_demographics.hd_dep_count = 1 and household_demographics.hd_vehicle_count<=1+2))
  and store.s_store_name = 'ese') s6 JOIN
    (select count(*) h11_30_to_12
      from store_sales
      JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
  JOIN time_dim ON store_sales.ss_sold_time_sk = time_dim.t_time_sk
  JOIN store ON store_sales.ss_store_sk = store.s_store_sk
  where
  time_dim.t_hour = 11
  and time_dim.t_minute >= 30
  and ((household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2) or
    (household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or
    (household_demographics.hd_dep_count = 1 and household_demographics.hd_vehicle_count<=1+2))
  and store.s_store_name = 'ese') s7 JOIN
    (select count(*) h12_to_12_30
      from store_sales
      JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
  JOIN time_dim ON store_sales.ss_sold_time_sk = time_dim.t_time_sk
  JOIN store ON store_sales.ss_store_sk = store.s_store_sk
  where
  time_dim.t_hour = 12
  and time_dim.t_minute < 30
  and ((household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2) or
    (household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or
    (household_demographics.hd_dep_count = 1 and household_demographics.hd_vehicle_count<=1+2))
  and store.s_store_name = 'ese') s8



                    """)

  val query90 = hql("""

    select  cast(amc as decimal)/cast(pmc as decimal) am_pm_ratio
    from ( select count(*) amc
      from web_sales
      JOIN household_demographics ON web_sales.ws_ship_hdemo_sk = household_demographics.hd_demo_sk
    JOIN time_dim ON web_sales.ws_sold_time_sk = time_dim.t_time_sk
    JOIN web_page ON web_sales.ws_web_page_sk = web_page.wp_web_page_sk
    where
    time_dim.t_hour between 6 and 6+1
    and household_demographics.hd_dep_count = 8
    and web_page.wp_char_count between 5000 and 5200) at JOIN
  ( select count(*) pmc
    from web_sales
    JOIN household_demographics ON web_sales.ws_ship_hdemo_sk = household_demographics.hd_demo_sk
  JOIN time_dim ON web_sales.ws_sold_time_sk = time_dim.t_time_sk
  JOIN web_page ON web_sales.ws_web_page_sk = web_page.wp_web_page_sk
  where
  time_dim.t_hour between 14 and 14+1
  and household_demographics.hd_dep_count = 8
  and web_page.wp_char_count between 5000 and 5200) pt
  order by am_pm_ratio
  limit 100



                         """)


  val query91 = hql("""

    select
    cc_call_center_id as Call_Center,
    cc_name as Call_Center_Name,
    cc_manager as Manager,
    sum(cr_net_loss) as Returns_Loss
      from
      call_center
      JOIN catalog_returns on catalog_returns.cr_call_center_sk       = call_center.cc_call_center_sk
      JOIN date_dim ON     catalog_returns.cr_returned_date_sk     = date_dim.d_date_sk
      JOIN customer ON     catalog_returns.cr_returning_customer_sk= customer.c_customer_sk
      JOIN customer_address ON     customer_address.ca_address_sk           = customer.c_current_addr_sk
      JOIN customer_demographics ON     customer_demographics.cd_demo_sk    = customer.c_current_cdemo_sk
      JOIN household_demographics ON     household_demographics.hd_demo_sk    = customer.c_current_hdemo_sk
      where
      d_year                  = 1999
      and     d_moy                   = 11
      and     ( (cd_marital_status       = 'M' and cd_education_status     = 'Unknown')
  or(cd_marital_status       = 'W' and cd_education_status     = 'Advanced Degree'))
  and     hd_buy_potential like '0-500%'
  and     ca_gmt_offset           = -7
  group by cc_call_center_id,cc_name,cc_manager,cd_marital_status,cd_education_status
  order by Returns_Loss desc



                         """)


  val query92 = hql("""

    SELECT sum(case when ssci.customer_sk is not null and csci.customer_sk is null then 1
  else 0 end) as store_only,
  sum(case when ssci.customer_sk is null and csci.customer_sk is not null then 1
  else 0 end) as catalog_only,
  sum(case when ssci.customer_sk is not null and csci.customer_sk is not null then 1
  else 0 end) as store_and_catalog
    FROM (SELECT ss.ss_customer_sk as customer_sk,
  ss.ss_item_sk as item_sk
  FROM store_sales ss
  JOIN date_dim d1 ON (ss.ss_sold_date_sk = d1.d_date_sk)
  WHERE d1.d_month_seq >= 1206 and
    d1.d_month_seq <= 1217
  GROUP BY ss.ss_customer_sk, ss.ss_item_sk) ssci
  FULL OUTER JOIN (SELECT cs.cs_bill_customer_sk as customer_sk,
  cs.cs_item_sk as item_sk
  FROM catalog_sales cs
  JOIN date_dim d2 ON (cs.cs_sold_date_sk = d2.d_date_sk)
  WHERE d2.d_month_seq >= 1206 and
    d2.d_month_seq <= 1217
  GROUP BY cs.cs_bill_customer_sk, cs.cs_item_sk) csci
  ON (ssci.customer_sk=csci.customer_sk and
    ssci.item_sk = csci.item_sk)

                         """)


  val query93 = hql("""

    select  ss_customer_sk
    ,sum(act_sales) sumsales
      from (select ss_item_sk
        ,ss_ticket_number
        ,ss_customer_sk
        ,case when sr_return_quantity is not null then (ss_quantity-sr_return_quantity)*ss_sales_price
  else (ss_quantity*ss_sales_price) end act_sales
  from store_sales left outer join store_returns on
  (store_returns.sr_item_sk = store_sales.ss_item_sk
    and store_returns.sr_ticket_number = store_sales.ss_ticket_number)
  JOIN reason ON store_returns.sr_reason_sk = reason.r_reason_sk
  where
  r_reason_desc = 'Did not like the warranty' ) sub
  group by ss_customer_sk
  order by sumsales, ss_customer_sk
  limit 100


                    """)


  val query94 = hql("""

    SELECT count(distinct ws_order_number) as order_count,
    sum(ws_ext_ship_cost) as total_shipping_cost,
    sum(ws_net_profit) as total_net_profit
      FROM web_sales ws1
      JOIN customer_address ca ON (ws1.ws_ship_addr_sk = ca.ca_address_sk)
      JOIN web_site s ON (ws1.ws_web_site_sk = s.web_site_sk)
      JOIN date_dim d ON (ws1.ws_ship_date_sk = d.d_date_sk)
      LEFT SEMI JOIN (SELECT ws2.ws_order_number as ws_order_number
      FROM web_sales ws2 JOIN web_sales ws3
      ON (ws2.ws_order_number = ws3.ws_order_number)
      WHERE ws2.ws_warehouse_sk <> ws3.ws_warehouse_sk) ws_wh1
    ON (ws1.ws_order_number = ws_wh1.ws_order_number)
  LEFT OUTER JOIN web_returns wr1 ON (ws1.ws_order_number = wr1.wr_order_number)
  WHERE d.d_date between '2000-05-01' and '2000-07-01' and
  ca.ca_state = 'TX' and
  s.web_company_name = 'pri' and
  wr1.wr_order_number is null
  limit 100

                         """)


  val query95 = hql("""

    SELECT count(distinct ws1.ws_order_number) as order_count,
  sum(ws1.ws_ext_ship_cost) as total_shipping_cost,
  sum(ws1.ws_net_profit) as total_net_profit
  FROM web_sales ws1
  JOIN customer_address ca ON (ws1.ws_ship_addr_sk = ca.ca_address_sk)
  JOIN web_site s ON (ws1.ws_web_site_sk = s.web_site_sk)
  JOIN date_dim d ON (ws1.ws_ship_date_sk = d.d_date_sk)
  LEFT SEMI JOIN (SELECT ws2.ws_order_number as ws_order_number
  FROM web_sales ws2 JOIN web_sales ws3
    ON (ws2.ws_order_number = ws3.ws_order_number)
  WHERE ws2.ws_warehouse_sk <> ws3.ws_warehouse_sk) ws_wh1
  ON (ws1.ws_order_number = ws_wh1.ws_order_number)
  LEFT SEMI JOIN (SELECT wr_order_number
    FROM web_returns wr
    JOIN (SELECT ws4.ws_order_number as ws_order_number
    FROM web_sales ws4 JOIN web_sales ws5
    ON (ws4.ws_order_number = ws5.ws_order_number)
    WHERE ws4.ws_warehouse_sk <> ws5.ws_warehouse_sk) ws_wh2
    ON (wr.wr_order_number = ws_wh2.ws_order_number)) tmp1
  ON (ws1.ws_order_number = tmp1.wr_order_number)
  WHERE d.d_date between '1999-02-01' and '1999-04-01' and
  ca.ca_state = 'TN' and
  s.web_company_name = 'pri'

                         """)


  val query96 = hql("""

    select  count(*) as ct
    from store_sales
    JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    JOIN time_dim ON store_sales.ss_sold_time_sk = time_dim.t_time_sk
    JOIN store ON store_sales.ss_store_sk = store.s_store_sk
    where
    time_dim.t_hour = 8
    and time_dim.t_minute >= 30
    and household_demographics.hd_dep_count = 5
    and store.s_store_name = 'ese'
  order by ct
  limit 100


                    """)


  val query97 = hql("""

    select sum(case when ssci.customer_sk is not null and csci.customer_sk is null then 1 else 0 end) store_only
  ,sum(case when ssci.customer_sk is null and csci.customer_sk is not null then 1 else 0 end) catalog_only
  ,sum(case when ssci.customer_sk is not null and csci.customer_sk is not null then 1 else 0 end) store_and_catalog
  from
  ( select ss_customer_sk customer_sk
    ,ss_item_sk item_sk
    from store_sales
    JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
  where
  d_month_seq between 1193 and 1193 + 11
  group by ss_customer_sk ,ss_item_sk) ssci
  full outer join
  ( select cs_bill_customer_sk customer_sk
    ,cs_item_sk item_sk
    from catalog_sales
    JOIN date_dim ON catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
  where
  d_month_seq between 1193 and 1193 + 11
  group by cs_bill_customer_sk ,cs_item_sk) csci
  on (ssci.customer_sk=csci.customer_sk and ssci.item_sk = csci.item_sk)
  limit 100
                         """)


}
