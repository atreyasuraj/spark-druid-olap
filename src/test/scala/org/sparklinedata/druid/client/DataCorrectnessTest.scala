/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.sparklinedata.druid.client

import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.sparklinedata.spark.dateTime.dsl.expressions._
import scala.language.postfixOps

class DataCorrectnessTest extends BaseTest {

  test("baseTableCorrectnessTest") { td =>

    val sqlQry = "select * from orderLineItemPartSupplier limit 100"

    correctnessTest(sqlQry, "orderLineItemPartSupplier")
  }

  test("baseCubeCorrectnessTest") { td =>

    val sqlQry = "select l_returnflag, l_linestatus, " +
      "count(*), sum(l_extendedprice) as s " +
      "from orderLineItemPartSupplier " +
      "group by l_returnflag, l_linestatus with cube limit 5"

    correctnessTest(sqlQry, "orderLineItemPartSupplier")
  }


  test("unionCorrectnessTest") { td =>

    val sqlQry = "select l_returnflag, l_linestatus, " +
      "count(*), sum(l_extendedprice) as s " +
      "from orderLineItemPartSupplier " +
      "group by l_returnflag, l_linestatus " +
      "union all " +
      "select l_returnflag, null, " +
      "count(*), sum(l_extendedprice) as s " +
      "from orderLineItemPartSupplier " +
      "group by l_returnflag limit 5"

    correctnessTest(sqlQry, "orderLineItemPartSupplier")

  }

  test("basicFilterCubeCorrectnessTest") { td =>
    val sqlQry = "select s_nation, l_returnflag, l_linestatus, " +
      "count(*), sum(l_extendedprice) as s " +
      "from orderLineItemPartSupplier " +
      "where s_nation = 'FRANCE' " +
      "group by s_nation, l_returnflag, l_linestatus with cube"

    correctnessTest(sqlQry, "orderLineItemPartSupplier")
  }

  test("basicFilterRollUp") { td =>
    val sqlQry = "select l_returnflag, l_linestatus, grouping__id, " +
      "count(*), sum(l_extendedprice) as s " +
      "from orderLineItemPartSupplier " +
      "where s_nation = 'FRANCE' " +
      "group by l_returnflag, l_linestatus with rollup"

    correctnessTest(sqlQry, "orderLineItemPartSupplier")
  }

  test("basicFilterGroupingSetCorrectnessTest") { td =>
    val sqlQry = "select l_returnflag, l_linestatus, grouping__id, " +
      "count(*), sum(l_extendedprice) as s " +
      "from orderLineItemPartSupplier " +
      "where s_nation = 'FRANCE' " +
      "group by l_returnflag, l_linestatus grouping sets(l_returnflag, l_linestatus, ())"

    correctnessTest(sqlQry, "orderLineItemPartSupplier")
  }

  test("basicCubeWithExprCorrectnessTest") { td =>
    val sqlQry = "select lower(l_returnflag), l_linestatus, " +
      "count(*), sum(l_extendedprice) as s " +
      "from orderLineItemPartSupplier " +
      "group by lower(l_returnflag), l_linestatus with cube"

    correctnessTest(sqlQry, "orderLineItemPartSupplier")
  }

  test("basicAggOrderByDimensionCorrectnessTest") { td =>
    val sqlQry = "select l_returnflag, l_linestatus, " +
      "count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a  " +
      "from orderLineItemPartSupplier " +
      "group by l_returnflag, l_linestatus " +
      "order by l_linestatus"

    correctnessTest(sqlQry, "orderLineItemPartSupplier")
  }

  test("basicAggOrderByDimensionLimitCorrectnessTest") { td =>
    val sqlQry = "select l_returnflag, l_linestatus, " +
      "count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a," +
      "count(distinct o_orderkey)  " +
      "from orderLineItemPartSupplier " +
      "group by l_returnflag, l_linestatus " +
      "order by l_returnflag " +
      "limit 2"

    correctnessTest(sqlQry, "orderLineItemPartSupplier")
  }

  test("basicAggOrderByMetricCorrectnessTest") { td =>
    val sqlQry = "select l_returnflag, l_linestatus, " +
      "count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a  " +
      "from orderLineItemPartSupplier " +
      "group by l_returnflag, l_linestatus " +
      "order by count(*)"

    correctnessTest(sqlQry, "orderLineItemPartSupplier")
  }

  test("basicAggOrderByMetric2CorrectnessTest") { td =>
    val sqlQry = "select l_returnflag, l_linestatus, " +
      "count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a  " +
      "from orderLineItemPartSupplier " +
      "group by l_returnflag, l_linestatus " +
      "order by s"

    correctnessTest(sqlQry, "orderLineItemPartSupplier")
  }

  test("basicAggOrderByLimitFullCorrectnessTest") { td =>
    val sqlQry = "select l_returnflag as r, l_linestatus as ls, " +
      "count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a  " +
      "from orderLineItemPartSupplier " +
      "group by l_returnflag, l_linestatus " +
      "order by s, ls, r " +
      "limit 3"

    correctnessTest(sqlQry, "orderLineItemPartSupplier")
  }

  test("basicAggOrderByLimitFull2CorrectnessTest"){ td =>
    val sqlQry = "select l_returnflag as r, l_linestatus as ls, " +
      "count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a  " +
      "from orderLineItemPartSupplier " +
      "group by l_returnflag, l_linestatus " +
      "order by m desc, s, r " +
      "limit 3"

    correctnessTest(sqlQry, "orderLineItemPartSupplier")
  }

  test("sortNotPushedCorrectnessTest") { td =>
    val sqlQry = "select l_returnflag as r, l_linestatus as ls, " +
      "count(*) + 1 as c, sum(l_extendedprice) as s, max(ps_supplycost) as m, " +
      "avg(ps_availqty) as a  " +
      "from orderLineItemPartSupplier " +
      "group by l_returnflag, l_linestatus " +
      "order by c " +
      "limit 3"

    correctnessTest(sqlQry, "orderLineItemPartSupplier")
  }

  test("basicAggCorrectnessTest") { td =>
    val sqlQry = "select l_returnflag, l_linestatus, " +
      "count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a," +
      "count(distinct o_orderkey)  " +
      "from orderLineItemPartSupplier group by l_returnflag, l_linestatus"

    correctnessTest(sqlQry, "orderLineItemPartSupplier")
  }

  test("noAggsCorrectnessTest") { td =>
    val sqlQry = "select l_returnflag, l_linestatus " +
      "from orderLineItemPartSupplier " +
      "group by l_returnflag, l_linestatus"

    correctnessTest(sqlQry, "orderLineItemPartSupplier")
  }

  test("basicAggWithProjectCorrectnessTest") { td =>
    val sqlQry = "select f, s, " +
      "count(*)  " +
      "from (select l_returnflag f, l_linestatus s " +
      "from orderLineItemPartSupplier) t group by f, s"

    correctnessTest(sqlQry, "orderLineItemPartSupplier")
  }

  test("dateFilterCorrectnessTest") { td =>
    val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)
    val sqlQry =
      date"""
      select f, s, count(*) as count_order
      from
      (
         select l_returnflag as f, l_linestatus as s, l_shipdate, s_region
         from orderLineItemPartSupplier
      ) t
      where $shipDtPredicate
      group by f,s
      order by f,s
      """
    correctnessTest(sqlQry, "orderLineItemPartSupplier")
  }

  test("intervalFilterCorrectnessTest") { td =>
    val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)
    val sqlQry = date"""
      select f, s, count(*) as count_order
      from
      (
         select l_returnflag as f, l_linestatus as s, l_shipdate, s_region, s_nation, c_nation
         from orderLineItemPartSupplier
      ) t
      where $shipDtPredicate and ((s_nation = 'FRANCE' and c_nation = 'GERMANY') or
                                  (c_nation = 'FRANCE' and s_nation = 'GERMANY')
                                 )
      group by f,s
      order by f,s
    """
    correctnessTest(sqlQry, "orderLineItemPartSupplier")
  }

  test("intervalFilter2CorrectnessTest") { td =>
    val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)
    val shipDtPredicate2 = dateTime('l_shipdate) > (dateTime("1995-12-01"))

    val sqlQry =
      date"""
      select f, s, count(*) as count_order
      from
      (
         select l_returnflag as f, l_linestatus as s, l_shipdate, s_region, s_nation, c_nation
         from orderLineItemPartSupplier
      ) t
      where $shipDtPredicate and $shipDtPredicate2
      group by f,s
      order by f,s
      """

    correctnessTest(sqlQry, "orderLineItemPartSupplier")
  }

  test("intervalFilter3CorrectnessTest") { td =>
    val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)
    val shipDtPredicate2 = dateTime('l_shipdate) < (dateTime("1995-12-01"))

    val sqlQry = date"""
      select f, s, count(*) as count_order
      from
      (
         select l_returnflag as f, l_linestatus as s, l_shipdate, s_region, s_nation, c_nation
         from orderLineItemPartSupplier
      ) t
      where $shipDtPredicate and $shipDtPredicate2
      group by f,s
      order by f,s
    """

    correctnessTest(sqlQry, "orderLineItemPartSupplier")
  }

  test("intervalFilter4CorrectnessTest") { td =>
    val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)
    val shipDtPredicate2 = dateTime('l_shipdate) > (dateTime("1997-12-02"))

    val sqlQry = date"""
      select f, s, count(*) as count_order
      from
      (
         select l_returnflag as f, l_linestatus as s, l_shipdate, s_region, s_nation, c_nation
         from orderLineItemPartSupplier
      ) t
      where $shipDtPredicate and $shipDtPredicate2
      group by f,s
      order by f,s
    """

    correctnessTest(sqlQry, "orderLineItemPartSupplier")
  }

  test("dimFilter2CorrectnessTest") { td =>
    val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)

    val sqlQry = date"""
      select f, s, count(*) as count_order
      from
      (
         select l_returnflag as f, l_linestatus as s, l_shipdate,
         s_region, s_nation, c_nation, p_type
         from orderLineItemPartSupplier
      ) t
      where $shipDtPredicate and ((s_nation = 'FRANCE' and c_nation = 'GERMANY') or
                                  (c_nation = 'FRANCE' and s_nation = 'GERMANY')
                                 ) and p_type = 'ECONOMY ANODIZED STEEL'
      group by f,s
      order by f,s
    """

    correctnessTest(sqlQry, "orderLineItemPartSupplier")
  }

  test("dimFilter3CorrectnessTest") { td =>
    val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)

    val sqlQry = date"""
      select s_nation, count(*) as count_order
      from
      (
         select l_returnflag as f, l_linestatus as s, l_shipdate,
         s_region, s_nation, c_nation, p_type
         from orderLineItemPartSupplier
      ) t
      where $shipDtPredicate and ((s_nation = 'FRANCE' and c_nation = 'GERMANY') or
                                  (c_nation = 'FRANCE' and s_nation = 'GERMANY')
                                 )
      group by s_nation
      order by s_nation
    """

    correctnessTest(sqlQry, "orderLineItemPartSupplier")
  }

  test("dimFilter4CorrectnessTest") { td =>
    val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)

    val sqlQry = date"""
      select s_nation, count(*) as count_order
      from
      (
         select l_returnflag as f, l_linestatus as s, l_shipdate,
         s_region, s_nation, c_nation, p_type
         from orderLineItemPartSupplier
      ) t
      where $shipDtPredicate and s_nation >= 'FRANCE'
      group by s_nation
      order by s_nation
    """

    correctnessTest(sqlQry, "orderLineItemPartSupplier")
  }

  test("projFilterAggCorrectnessTest") { td =>
    val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)
    val shipDtPredicate2 = dateTime('l_shipdate) > (dateTime("1995-12-01"))

    val sqlQry = date"""
      select s_nation,
      count(*) as count_order,
      sum(l_extendedprice) as s,
      max(ps_supplycost) as m,
      avg(ps_availqty) as a,
      count(distinct o_orderkey)
      from
      (
         select l_returnflag as f, l_linestatus as s, l_shipdate,
         s_region, s_nation, c_nation, p_type,
         l_extendedprice, ps_supplycost, ps_availqty, o_orderkey
         from orderLineItemPartSupplier
         where p_type = 'ECONOMY ANODIZED STEEL'
      ) t
      where $shipDtPredicate and
            $shipDtPredicate2 and ((s_nation = 'FRANCE' and c_nation = 'GERMANY') or
                                  (c_nation = 'FRANCE' and s_nation = 'GERMANY')
                                 )
      group by s_nation
      order by s_nation
    """

    correctnessTest(sqlQry, "orderLineItemPartSupplier")
  }

  test("ShipDateYearAggCorrectnessTest") { td =>

    val shipDtYrGroup = dateTime('l_shipdate) year

    val sqlQry = date"""select l_returnflag, l_linestatus, $shipDtYrGroup, count(*),
      sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a,
      count(distinct o_orderkey)
      from orderLineItemPartSupplier group by l_returnflag, l_linestatus, $shipDtYrGroup"""

    correctnessTest(sqlQry, "orderLineItemPartSupplier")
  }

  test("OrderDateYearAggCorrectnessTest") { td =>

    val orderDtYrGroup = dateTime('o_orderdate) year

    val sqlQry = date"""select l_returnflag, l_linestatus, $orderDtYrGroup, count(*),
      sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a,
      count(distinct o_orderkey)
      from orderLineItemPartSupplier group by l_returnflag, l_linestatus, $orderDtYrGroup"""

    correctnessTest(sqlQry, "orderLineItemPartSupplier")
  }

  test("noRewriteCorrectnessTest") { td =>
    val sqlQry = "select *  from orderLineItemPartSupplier  limit 3"

    correctnessTest(sqlQry, "orderLineItemPartSupplier")
  }

  test("inclauseTest1CorrectnessTest") { td =>
    val sqlQry = "select c_name, sum(c_acctbal) as bal " +
      "from orderLineItemPartSupplier " +
      "where c_mktsegment in ('MACHINERY', 'HOUSEHOLD') " +
      "group by c_name"

    correctnessTest(sqlQry, "orderLineItemPartSupplier")
  }

  test("notInclauseTest1CorrectnessTest") { td =>
    val sqlQry = "select c_name, sum(c_acctbal) as bal " +
      "from orderLineItemPartSupplier " +
      "where c_mktsegment not in ('MACHINERY', 'HOUSEHOLD') " +
      "group by c_name"

    correctnessTest(sqlQry, "orderLineItemPartSupplier")
  }

  test("notEqTest1CorrectnessTest") { td =>
    val sqlQry = "select c_name, sum(c_acctbal) as bal " +
      "from orderLineItemPartSupplier " +
      "where c_mktsegment !=  'MACHINERY' " +
      "group by c_name"

    correctnessTest(sqlQry, "orderLineItemPartSupplier")
  }

}
