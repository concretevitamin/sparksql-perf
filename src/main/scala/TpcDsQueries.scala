import java.io.File

import org.apache.spark.sql.hive.HiveContext

/**
 * Currently targeted queries (in Impala benchmark).
 *
 * Interactive: q19 q42 q52 q55 q63 q68 q73 q98
 * Reporting: q3 q7 q27 q43 q53 q89
 * Deep Analytic: q34 q46 q59 q79 ss_max
 */
class TpcDsQueries(
    hiveContext: HiveContext,
    queryNames: Seq[String] = Seq(),
    location: String  = new File(".", "tpcds").getCanonicalPath) {

  import hiveContext._

  // TODO: commented out some queries that involve window functions.

  lazy val queryNameToObject = Map(
    "q19" -> q19,
    "q42" -> q42,
    "q52" -> q52,
    "q55" -> q55,
//    "q63" -> q63,
    "q68" -> q68,
    "q73" -> q73,
//    "q98" -> q98,
    "q3" -> q3,
    "q7" -> q7,
    "q27" -> q27,
    "q43" -> q43,
//    "q53" -> q53,
//    "q89" -> q89,
    "q34" -> q34,
    "q46" -> q46,
    "q59" -> q59,
    "q79" -> q79,
    "ss_max" -> qSsMax
  )

  lazy val interactiveQueries = Seq(
    q19,
    q42,
    q52,
    q55,
//    q63,
    q68,
    q73
//    q98
  )
  lazy val reportingQueries = Seq(
    q3,
    q7,
    q27,
    q43
//    q53
//    q89
  )
  lazy val deepAnalyticQueries = Seq(
    q34,
    q46,
    q59,
    q79,
    qSsMax
  )

  lazy val allQueries = queryNames.map(queryNameToObject(_))

  lazy val warmUpQuery = hql(
    """
      |select count(*) from store_sales
    """.stripMargin)

  /*********************** Real Queries ************************/

  val q3 = hql("""
    select  d_year
    ,item.i_brand_id brand_id
    ,item.i_brand brand
    ,sum(ss_ext_sales_price) sum_agg
      from  date_dim dt
      JOIN store_sales on dt.d_date_sk = store_sales.ss_sold_date_sk
      JOIN item on store_sales.ss_item_sk = item.i_item_sk
      where
      item.i_manufact_id = 436
      and dt.d_moy=12
      group by d_year
    ,item.i_brand
    ,item.i_brand_id
      order by d_year
    ,sum_agg desc
    ,brand_id
      limit 100""")

  val q7 = hql("""

    select  i_item_id,
    avg(ss_quantity) agg1,
    avg(ss_list_price) agg2,
    avg(ss_coupon_amt) agg3,
    avg(ss_sales_price) agg4
      from store_sales
      JOIN customer_demographics ON store_sales.ss_cdemo_sk = customer_demographics.cd_demo_sk
  JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
  JOIN item ON store_sales.ss_item_sk = item.i_item_sk
  JOIN promotion ON store_sales.ss_promo_sk = promotion.p_promo_sk
  where
  cd_gender = 'F' and
    cd_marital_status = 'W' and
    cd_education_status = 'Primary' and
  (p_channel_email = 'N' or p_channel_event = 'N') and
    d_year = 1998
  group by i_item_id
  order by i_item_id
  limit 100""")

  val q19 = hql("""

    select  i_brand_id, i_brand, i_manufact_id, i_manufact,
    sum(ss_ext_sales_price) as ext_price
      from date_dim
      JOIN store_sales ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
      JOIN item ON store_sales.ss_item_sk = item.i_item_sk
      JOIN customer ON store_sales.ss_customer_sk = customer.c_customer_sk
      JOIN customer_address ON customer.c_current_addr_sk = customer_address.ca_address_sk
      JOIN store ON store_sales.ss_store_sk = store.s_store_sk
      where
      i_manager_id=7
      and d_moy=11
      and d_year=1999
      and substr(ca_zip,1,5) <> substr(s_zip,1,5)
      group by i_brand
    ,i_brand_id
    ,i_manufact_id
    ,i_manufact
      order by ext_price desc
    ,i_brand
    ,i_brand_id
    ,i_manufact_id
    ,i_manufact
      limit 100""")

  val q27partitioned = hql("""
    select  i_item_id,
    s_state,
    avg(ss_quantity) agg1,
    avg(ss_list_price) agg2,
    avg(ss_coupon_amt) agg3,
    avg(ss_sales_price) agg4
      from store_sales
      JOIN customer_demographics ON store_sales.ss_cdemo_sk = customer_demographics.cd_demo_sk
  JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
  JOIN store ON store_sales.ss_store_sk = store.s_store_sk
  JOIN item ON store_sales.ss_item_sk = item.i_item_sk
  where
  cd_gender = 'F' and
    cd_marital_status = 'W' and
    cd_education_status = 'Primary' and
  d_year = 1998 and
    s_state = 'TN' and
  ss_sold_date between '1998-01-01' and '1998-12-31'
  group by i_item_id, s_state
  order by i_item_id
  ,s_state
  limit 100""")

  val q27 = hql("""
    select  i_item_id,
    s_state,
    avg(ss_quantity) agg1,
    avg(ss_list_price) agg2,
    avg(ss_coupon_amt) agg3,
    avg(ss_sales_price) agg4
      from store_sales
      JOIN customer_demographics ON store_sales.ss_cdemo_sk = customer_demographics.cd_demo_sk
  JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
  JOIN store ON store_sales.ss_store_sk = store.s_store_sk
  JOIN item ON store_sales.ss_item_sk = item.i_item_sk
  where
  cd_gender = 'F' and
    cd_marital_status = 'W' and
    cd_education_status = 'Primary' and
  d_year = 1998 and
    s_state = 'TN'
  group by i_item_id, s_state
  order by i_item_id
  ,s_state
  limit 100""")

  val q34 = hql("""
    select c_last_name
    ,c_first_name
    ,c_salutation
    ,c_preferred_cust_flag
    ,ss_ticket_number
    ,cnt from
      (select ss_ticket_number
        ,ss_customer_sk
        ,count(*) cnt
        from store_sales
        JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
      JOIN store ON store_sales.ss_store_sk = store.s_store_sk
      JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
      where
      (date_dim.d_dom between 1 and 3 or date_dim.d_dom between 25 and 28)
      and (household_demographics.hd_buy_potential = '>10000' or
  household_demographics.hd_buy_potential = 'unknown')
  and household_demographics.hd_vehicle_count > 0
  and (case when household_demographics.hd_vehicle_count > 0
  then household_demographics.hd_dep_count/ household_demographics.hd_vehicle_count
  else null
  end)  > 1.2
  and date_dim.d_year in (1998,1998+1,1998+2)
  and store.s_county in ('Williamson County','Williamson County','Williamson County','Williamson County',
  'Williamson County','Williamson County','Williamson County','Williamson County')
  group by ss_ticket_number,ss_customer_sk) dn
  JOIN customer ON dn.ss_customer_sk = customer.c_customer_sk
  WHERE
  cnt between 15 and 20
  order by c_last_name,c_first_name,c_salutation,c_preferred_cust_flag desc""")

  val q42 = hql("""
    select  d_year
    ,item.i_category_id
    ,item.i_category
    ,sum(ss_ext_sales_price) as s
      from 	date_dim dt
      JOIN store_sales ON dt.d_date_sk = store_sales.ss_sold_date_sk
  JOIN item ON store_sales.ss_item_sk = item.i_item_sk
  where
  item.i_manager_id = 1
  and dt.d_moy=12
  and dt.d_year=1998
  group by 	d_year
  ,item.i_category_id
  ,item.i_category
  order by       s desc,d_year
  ,i_category_id
  ,i_category
  limit 100""")

  val q43 = hql("""
    select  s_store_name, s_store_id,
    sum(case when (d_day_name='Sunday') then ss_sales_price else null end) sun_sales,
  sum(case when (d_day_name='Monday') then ss_sales_price else null end) mon_sales,
  sum(case when (d_day_name='Tuesday') then ss_sales_price else  null end) tue_sales,
  sum(case when (d_day_name='Wednesday') then ss_sales_price else null end) wed_sales,
  sum(case when (d_day_name='Thursday') then ss_sales_price else null end) thu_sales,
  sum(case when (d_day_name='Friday') then ss_sales_price else null end) fri_sales,
  sum(case when (d_day_name='Saturday') then ss_sales_price else null end) sat_sales
  from date_dim
    JOIN store_sales ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
  JOIN store ON store.s_store_sk = store_sales.ss_store_sk
  where
  s_gmt_offset = -5 and
    d_year = 1998
  group by s_store_name, s_store_id
  order by s_store_name, s_store_id,sun_sales,mon_sales,tue_sales,wed_sales,thu_sales,fri_sales,sat_sales
  limit 100""")

  val q46 = hql("""
    select  c_last_name
    ,c_first_name
    ,ca_city
    ,bought_city
    ,ss_ticket_number
    ,amt,profit
      from
      (select ss_ticket_number
        ,ss_customer_sk
        ,ca_city as bought_city
        ,sum(ss_coupon_amt) as amt
        ,sum(ss_net_profit) as profit
        from store_sales
        JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
        JOIN store ON store_sales.ss_store_sk = store.s_store_sk
        JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
        JOIN customer_address ON store_sales.ss_addr_sk = customer_address.ca_address_sk
        where
        (household_demographics.hd_dep_count = 5 or
          household_demographics.hd_vehicle_count= 3)
        and date_dim.d_dow in (6,0)
        and date_dim.d_year in (1999,1999+1,1999+2)
        and store.s_city in ('Midway','Fairview','Fairview','Fairview','Fairview')
  group by ss_ticket_number,ss_customer_sk,ss_addr_sk,ca_city) dn
  JOIN customer ON dn.ss_customer_sk = customer.c_customer_sk
  JOIN customer_address ON customer.c_current_addr_sk = customer_address.ca_address_sk
  where
  customer_address.ca_city <> dn.bought_city
  order by c_last_name
  ,c_first_name
  ,ca_city
  ,bought_city
  ,ss_ticket_number
  limit 100""")

  val q52 = hql("""
    select  d_year
    ,item.i_brand_id brand_id
    ,item.i_brand brand
    ,sum(ss_ext_sales_price) as ext_price
      from date_dim
      JOIN store_sales ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
      JOIN item ON store_sales.ss_item_sk = item.i_item_sk
      where
      item.i_manager_id = 1
      and date_dim.d_moy=12
      and date_dim.d_year=1998
      group by d_year
    ,item.i_brand
    ,item.i_brand_id
      order by d_year
    ,ext_price desc
    ,brand_id
      limit 100""")

//  val q53 = hql(
//    """
//      |select *
//      |from
//      |  (select
//      |    /*+ MAPJOIN(item, store, date_dim) */
//      |    i_manufact_id,
//      |    sum(ss_sales_price) sum_sales
//      |    -- avg(sum(ss_sales_price)) over(partition by i_manufact_id) avg_quarterly_sales
//      |  from
//      |    store_sales
//      |    join item on (store_sales.ss_item_sk = item.i_item_sk)
//      |    join store on (store_sales.ss_store_sk = store.s_store_sk)
//      |    join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
//      |  where
//      |    ss_sold_date_sk between 2451911 and 2452275 -- partition key filter
//      |    -- ss_date between '2001-01-01' and '2001-12-31'
//      |    and d_month_seq in(1212, 1212 + 1, 1212 + 2, 1212 + 3, 1212 + 4, 1212 + 5, 1212 + 6, 1212 + 7, 1212 + 8, 1212 + 9, 1212 + 10, 1212 + 11)
//      |    and (
//      |          (i_category in('Books', 'Children', 'Electronics')
//      |            and i_class in('personal', 'portable', 'reference', 'self-help')
//      |            and i_brand in('scholaramalgamalg #14', 'scholaramalgamalg #7', 'exportiunivamalg #9', 'scholaramalgamalg #9')
//      |          )
//      |          or
//      |          (i_category in('Women', 'Music', 'Men')
//      |            and i_class in('accessories', 'classical', 'fragrances', 'pants')
//      |            and i_brand in('amalgimporto #1', 'edu packscholar #1', 'exportiimporto #1', 'importoamalg #1')
//      |          )
//      |        )
//      |  group by
//      |    i_manufact_id,
//      |    d_qoy
//      |  ) tmp1
//      |-- where
//      |--   case when avg_quarterly_sales > 0 then abs(sum_sales - avg_quarterly_sales) / avg_quarterly_sales else null end > 0.1
//      |order by
//      |  -- avg_quarterly_sales,
//      |  sum_sales,
//      |  i_manufact_id
//      |limit 100
//    """.stripMargin)

  val q55 = hql("""
    select  i_brand_id as brand_id, i_brand as brand,
    sum(store_sales.ss_ext_sales_price) ext_price
      from date_dim
      JOIN store_sales ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
  JOIN item ON store_sales.ss_item_sk = item.i_item_sk
  where
  i_manager_id=36
  and d_moy=12
  and d_year=2001
  group by i_brand, i_brand_id
  order by ext_price desc, brand_id
  limit 100 """)

  val q59 = hql(
    """
      |select
      |  s_store_name1,
      |  s_store_id1,
      |  d_week_seq1,
      |  sun_sales1 / sun_sales2,
      |  mon_sales1 / mon_sales2,
      |  tue_sales1 / tue_sales2,
      |  wed_sales1 / wed_sales2,
      |  thu_sales1 / thu_sales2,
      |  fri_sales1 / fri_sales2,
      |  sat_sales1 / sat_sales2
      |from
      |  (select
      |    /*+ MAPJOIN(store, date_dim) */
      |    s_store_name s_store_name1,
      |    wss.d_week_seq d_week_seq1,
      |    s_store_id s_store_id1,
      |    sun_sales sun_sales1,
      |    mon_sales mon_sales1,
      |    tue_sales tue_sales1,
      |    wed_sales wed_sales1,
      |    thu_sales thu_sales1,
      |    fri_sales fri_sales1,
      |    sat_sales sat_sales1
      |  from
      |    (select
      |      /*+ MAPJOIN(date_dim) */
      |      d_week_seq,
      |      ss_store_sk,
      |      sum(case when(d_day_name = 'Sunday') then ss_sales_price else null end) sun_sales,
      |      sum(case when(d_day_name = 'Monday') then ss_sales_price else null end) mon_sales,
      |      sum(case when(d_day_name = 'Tuesday') then ss_sales_price else null end) tue_sales,
      |      sum(case when(d_day_name = 'Wednesday') then ss_sales_price else null end) wed_sales,
      |      sum(case when(d_day_name = 'Thursday') then ss_sales_price else null end) thu_sales,
      |      sum(case when(d_day_name = 'Friday') then ss_sales_price else null end) fri_sales,
      |      sum(case when(d_day_name = 'Saturday') then ss_sales_price else null end) sat_sales
      |    from
      |      store_sales
      |      join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
      |    where
      |      -- ss_date between '1998-10-01' and '1999-09-30'
      |      ss_sold_date_sk between 2451088 and 2451452
      |    group by
      |      d_week_seq,
      |      ss_store_sk
      |    ) wss
      |    join store on (wss.ss_store_sk = store.s_store_sk)
      |    join date_dim d on (wss.d_week_seq = d.d_week_seq)
      |  where
      |    d_month_seq between 1185 and 1185 + 11
      |  ) y
      |  join
      |  (select
      |    /*+ MAPJOIN(store, date_dim) */
      |    s_store_name s_store_name2,
      |    wss.d_week_seq d_week_seq2,
      |    s_store_id s_store_id2,
      |    sun_sales sun_sales2,
      |    mon_sales mon_sales2,
      |    tue_sales tue_sales2,
      |    wed_sales wed_sales2,
      |    thu_sales thu_sales2,
      |    fri_sales fri_sales2,
      |    sat_sales sat_sales2
      |  from
      |    (select
      |      /*+ MAPJOIN(date_dim) */
      |      d_week_seq,
      |      ss_store_sk,
      |      sum(case when(d_day_name = 'Sunday') then ss_sales_price else null end) sun_sales,
      |      sum(case when(d_day_name = 'Monday') then ss_sales_price else null end) mon_sales,
      |      sum(case when(d_day_name = 'Tuesday') then ss_sales_price else null end) tue_sales,
      |      sum(case when(d_day_name = 'Wednesday') then ss_sales_price else null end) wed_sales,
      |      sum(case when(d_day_name = 'Thursday') then ss_sales_price else null end) thu_sales,
      |      sum(case when(d_day_name = 'Friday') then ss_sales_price else null end) fri_sales,
      |      sum(case when(d_day_name = 'Saturday') then ss_sales_price else null end) sat_sales
      |    from
      |      store_sales
      |      join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
      |    where
      |      -- ss_date between '1999-10-01' and '2000-09-30'
      |      ss_sold_date_sk between 2451088 and 2451452
      |    group by
      |      d_week_seq,
      |      ss_store_sk
      |    ) wss
      |    join store on (wss.ss_store_sk = store.s_store_sk)
      |    join date_dim d on (wss.d_week_seq = d.d_week_seq)
      |  where
      |    d_month_seq between 1185 + 12 and 1185 + 23
      |  ) x
      |  on (y.s_store_id1 = x.s_store_id2)
      |where
      |  d_week_seq1 = d_week_seq2 - 52
      |order by
      |  s_store_name1,
      |  s_store_id1,
      |  d_week_seq1
      |limit 100
    """.stripMargin)

//  val q63 = hql(
//    """
//      |select
//      |  *
//      |from
//      |  (select
//      |    /*+ MAPJOIN(item, store, date_dim) */
//      |    i_manager_id,
//      |    sum(ss_sales_price) sum_sales
//      |    -- avg(sum(ss_sales_price)) over(partition by i_manager_id) avg_monthly_sales
//      |  from
//      |    store_sales
//      |    join item on (store_sales.ss_item_sk = item.i_item_sk)
//      |    join store on (store_sales.ss_store_sk = store.s_store_sk)
//      |    join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
//      |  where
//      |    ss_sold_date_sk between 2451911 and 2452275  -- partition key filter
//      |    -- ss_date between '2001-01-01' and '2001-12-31'
//      |    and d_month_seq in (1212, 1212 + 1, 1212 + 2, 1212 + 3, 1212 + 4, 1212 + 5, 1212 + 6, 1212 + 7, 1212 + 8, 1212 + 9, 1212 + 10, 1212 + 11)
//      |    and (
//      |          (i_category in('Books', 'Children', 'Electronics')
//      |            and i_class in('personal', 'portable', 'refernece', 'self-help')
//      |            and i_brand in('scholaramalgamalg #14', 'scholaramalgamalg #7', 'exportiunivamalg #9', 'scholaramalgamalg #9')
//      |          )
//      |          or
//      |          (i_category in('Women', 'Music', 'Men')
//      |            and i_class in('accessories', 'classical', 'fragrances', 'pants')
//      |            and i_brand in('amalgimporto #1', 'edu packscholar #1', 'exportiimporto #1', 'importoamalg #1')
//      |          )
//      |        )
//      |  group by
//      |    i_manager_id,
//      |    d_moy
//      |  ) tmp1
//      |-- where
//      |--   case when avg_monthly_sales > 0 then abs(sum_sales - avg_monthly_sales) / avg_monthly_sales else null end > 0.1
//      |order by
//      |  i_manager_id,
//      |  -- avg_monthly_sales,
//      |  sum_sales
//      |limit 100
//    """.stripMargin)

  val q68 = hql("""
    select  c_last_name ,c_first_name ,ca_city
    ,bought_city ,ss_ticket_number ,extended_price
    ,extended_tax ,list_price
      from (select ss_ticket_number
      ,ss_customer_sk
      ,ca_city as bought_city
      ,sum(ss_ext_sales_price) as extended_price
      ,sum(ss_ext_list_price) as list_price
      ,sum(ss_ext_tax) as extended_tax
      from store_sales
      JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
      JOIN store ON store_sales.ss_store_sk = store.s_store_sk
      JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
      JOIN customer_address ON store_sales.ss_addr_sk = customer_address.ca_address_sk
      where
      date_dim.d_dom between 1 and 2
      and (household_demographics.hd_dep_count = 5 or
      household_demographics.hd_vehicle_count= 3)
      and date_dim.d_year in (1999,1999+1,1999+2)
      and store.s_city in ('Midway','Fairview')
  group by ss_ticket_number
  ,ss_customer_sk
  ,ss_addr_sk,ca_city) dn
  JOIN customer ON dn.ss_customer_sk = customer.c_customer_sk
  JOIN customer_address ON customer.c_current_addr_sk = customer_address.ca_address_sk
  where
  customer_address.ca_city <> dn.bought_city
  order by c_last_name
  ,ss_ticket_number
  limit 100""")

  val q73 = hql("""
    select c_last_name
    ,c_first_name
    ,c_salutation
    ,c_preferred_cust_flag
    ,ss_ticket_number
    ,cnt from
      (select ss_ticket_number
        ,ss_customer_sk
        ,count(*) cnt
        from store_sales
        JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
      JOIN store ON store_sales.ss_store_sk = store.s_store_sk
      JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
      where
      date_dim.d_dom between 1 and 2
      and (household_demographics.hd_buy_potential = '>10000' or
  household_demographics.hd_buy_potential = 'unknown')
  and household_demographics.hd_vehicle_count > 0
  and case when household_demographics.hd_vehicle_count > 0 then
    household_demographics.hd_dep_count/ household_demographics.hd_vehicle_count else null end > 1
  and date_dim.d_year in (1998,1998+1,1998+2)
  and store.s_county in ('Williamson County','Williamson County','Williamson County','Williamson County')
  group by ss_ticket_number,ss_customer_sk) dj
  JOIN customer ON dj.ss_customer_sk = customer.c_customer_sk
  where
  cnt between 5 and 10
  order by cnt desc""")

  val q79 = hql("""
    select
    c_last_name,c_first_name,substr(s_city,1,30) as s_city,ss_ticket_number,amt,profit
    from
    (select ss_ticket_number
      ,ss_customer_sk
      ,store.s_city
      ,sum(ss_coupon_amt) amt
      ,sum(ss_net_profit) profit
      from store_sales
      JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
    JOIN store ON store_sales.ss_store_sk = store.s_store_sk
    JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    where
    (household_demographics.hd_dep_count = 8 or household_demographics.hd_vehicle_count > 0)
    and date_dim.d_dow = 1
    and date_dim.d_year in (1998,1998+1,1998+2)
    and store.s_number_employees between 200 and 295
    group by ss_ticket_number,ss_customer_sk,ss_addr_sk,store.s_city) ms
    JOIN customer on ms.ss_customer_sk = customer.c_customer_sk
  order by c_last_name,c_first_name,s_city, profit
  limit 100""")

//  val q89 = hql("""
//    select  *
//    from(
//    select i_category, i_class, i_brand,
//    s_store_name, s_company_name,
//    d_moy,
//    sum(ss_sales_price) sum_sales,
//    avg(sum(ss_sales_price)) over
//      (partition by i_category, i_brand, s_store_name, s_company_name)
//      avg_monthly_sales
//      from item
//      JOIN store_sales ON store_sales.ss_item_sk = item.i_item_sk
//    JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
//    JOIN store ON store_sales.ss_store_sk = store.s_store_sk
//    where
//    d_year in (2000) and
//    ((i_category in ('Home','Books','Electronics') and
//  i_class in ('wallpaper','parenting','musical')
//  )
//  or (i_category in ('Shoes','Jewelry','Men') and
//  i_class in ('womens','birdal','pants')
//  ))
//  group by i_category, i_class, i_brand,
//  s_store_name, s_company_name, d_moy) tmp1
//  where case when (avg_monthly_sales <> 0) then (abs(sum_sales - avg_monthly_sales) / avg_monthly_sales) else null end > 0.1
//  order by sum_sales - avg_monthly_sales, s_store_name
//  limit 100""")

//  val q98 = hql("""
//    select i_item_desc
//    ,i_category
//    ,i_class
//    ,i_current_price
//    ,i_item_id
//    ,sum(ss_ext_sales_price) as itemrevenue
//    ,sum(ss_ext_sales_price)*100/sum(sum(ss_ext_sales_price)) over
//      (partition by i_class) as revenueratio
//      from
//      store_sales
//      JOIN item ON store_sales.ss_item_sk = item.i_item_sk
//      JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
//      where
//      i_category in ('Jewelry', 'Sports', 'Books')
//  and d_date between '2001-01-12' and '2001-02-11'
//  group by
//    i_item_id
//  ,i_item_desc
//  ,i_category
//  ,i_class
//  ,i_current_price
//  ,ss_ext_sales_price
//  order by
//    i_category
//  ,i_class
//  ,i_item_id
//  ,i_item_desc
//  ,revenueratio""")

  val qSsMax = hql(
    """
      |select
      |  count(*) as total,
      |  count(ss_sold_date_sk) as not_null_total,
      |  count(distinct ss_sold_date_sk) as unique_days,
      |  max(ss_sold_date_sk) as max_ss_sold_date_sk,
      |  max(ss_sold_time_sk) as max_ss_sold_time_sk,
      |  max(ss_item_sk) as max_ss_item_sk,
      |  max(ss_customer_sk) as max_ss_customer_sk,
      |  max(ss_cdemo_sk) as max_ss_cdemo_sk,
      |  max(ss_hdemo_sk) as max_ss_hdemo_sk,
      |  max(ss_addr_sk) as max_ss_addr_sk,
      |  max(ss_store_sk) as max_ss_store_sk,
      |  max(ss_promo_sk) as max_ss_promo_sk
      |from store_sales
    """.stripMargin)

}
