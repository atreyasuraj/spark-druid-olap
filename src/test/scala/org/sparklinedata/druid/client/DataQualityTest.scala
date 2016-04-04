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

import org.apache.spark.sql.hive.test.TestHive._

class DataQualityTest extends BaseTest {

  test("baseTableDataQuality") { td =>
    val resultDF = sql("select * from orderLineItemPartSupplierBase limit 10")

    val refDF = loadRefDataFrame(td)

    checkEqualDataFrames(resultDF, refDF)
  }

  test("indexedDataQuality") { td =>
    val resultDF = sql("select * from orderLineItemPartSupplier limit 10")

    val refDF = loadRefDataFrame(td)

    checkEqualDataFrames(resultDF, refDF)
  }

  test("indexedBasicCubeTest") { td =>
    val resultDF = sql("select l_returnflag, l_linestatus, count(*), sum(l_extendedprice) as s " +
      "from orderLineItemPartSupplier " +
      "group by l_returnflag, l_linestatus with cube limit 10")

    val refDF = loadRefDataFrame(td)

    checkEqualDataFrames(resultDF, refDF)
  }
}
