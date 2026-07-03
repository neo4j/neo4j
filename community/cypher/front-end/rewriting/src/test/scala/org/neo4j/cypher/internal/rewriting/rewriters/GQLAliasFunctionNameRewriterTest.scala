/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.rewriting.rewriters

import org.neo4j.cypher.internal.rewriting.RewriteTest
import org.neo4j.cypher.internal.rewriting.rewriters.astRewriters.GQLAliasFunctionNameRewriter
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.TestName

class GQLAliasFunctionNameRewriterTest extends CypherFunSuite with RewriteTest with TestName {

  override val rewriterUnderTest: Rewriter = GQLAliasFunctionNameRewriter.instance

  override protected def assertRewrite(originalQuery: String, expectedQuery: String): Unit =
    super.assertRewrite(originalQuery, expectedQuery)

  override protected def assertIsNotRewritten(query: String): Unit =
    super.assertIsNotRewritten(query)

  test("RETURN size('abc') AS result") {
    assertIsNotRewritten(testName)
  }

  test("RETURN character_length('abc') AS result") {
    assertIsNotRewritten(testName)
  }

  List("char_length", "CHAR_LENGTH", "char_LENGTH", "ChAr_LeNgTH").foreach(functionName =>
    test(s"RETURN $functionName('abc') AS result") {
      assertRewrite(testName, "RETURN character_length('abc') AS result")
    }
  )

  List("UPPER", "uPpER", "upper", "upPER").foreach(functionName =>
    test(s"RETURN $functionName('abc') AS result") {
      assertRewrite(testName, "RETURN toUpper('abc') AS result")
    }
  )

  List("LOWER", "loWEr", "LoWeR", "LOWer").foreach(functionName =>
    test(s"RETURN $functionName('abc') AS result") {
      assertRewrite(testName, "RETURN toLower('abc') AS result")
    }
  )

  List("CEILING", "ceiling", "CeIlInG", "ceilING").foreach(functionName =>
    test(s"RETURN $functionName(1.2) AS result") {
      assertRewrite(testName, "RETURN ceil(1.2) AS result")
    }
  )

  List("LOCAL_TIME", "local_time", "LoCaL_tImE", "LOCAL_tiME").foreach(functionName =>
    test(s"RETURN $functionName() AS result") {
      assertRewrite(testName, "RETURN localtime() AS result")
    }
  )

  List("LOCAL_DATETIME", "local_datetime", "LoCaL_dAtEtImE", "LOCAL_dateTIME").foreach(functionName =>
    test(s"RETURN $functionName() AS result") {
      assertRewrite(testName, "RETURN localdatetime() AS result")
    }
  )

  List("ZONED_TIME", "zoned_time", "ZoNeD_tImE", "ZONED_time").foreach(functionName =>
    test(s"RETURN $functionName() AS result") {
      assertRewrite(testName, "RETURN time() AS result")
    }
  )

  List("ZONED_DATETIME", "zoned_datetime", "ZoNeD_dAtEtImE", "ZONED_dateTIME").foreach(functionName =>
    test(s"RETURN $functionName() AS result") {
      assertRewrite(testName, "RETURN datetime() AS result")
    }
  )

  List("PATH_LENGTH", "path_length", "PaTh_LeNgTh", "PATH_lenGTH").foreach(functionName =>
    test(s"RETURN $functionName(p) AS result") {
      assertRewrite(testName, "RETURN length(p) AS result")
    }
  )

  List("LN", "ln", "Ln", "lN").foreach(functionName =>
    test(s"RETURN $functionName(1.2) AS result") {
      assertRewrite(testName, "RETURN log(1.2) AS result")
    }
  )

  List("COLLECT_LIST", "collect_list", "CoLlEcT_lIsT", "COLLECT_lisT").foreach(functionName =>
    test(s"RETURN $functionName(x) AS result") {
      assertRewrite(testName, "RETURN collect(x) AS result")
    }
  )

  List("PERCENTILE_DISC", "percentile_disc", "PeRcEnTiLe_DiSc", "PERCENTILE_diSC").foreach(functionName =>
    test(s"RETURN $functionName(x, 0.5) AS result") {
      assertRewrite(testName, "RETURN percentileDisc(x, 0.5) AS result")
    }
  )

  List("PERCENTILE_CONT", "percentile_cont", "PeRcEnTiLe_CoNt", "PERCENTILE_coNT").foreach(functionName =>
    test(s"RETURN $functionName(x, 0.5) AS result") {
      assertRewrite(testName, "RETURN percentileCont(x, 0.5) AS result")
    }
  )

  List("STDEV_POP", "stdev_pop", "StDeV_PoP", "STDEV_poP").foreach(functionName =>
    test(s"RETURN $functionName(x) AS result") {
      assertRewrite(testName, "RETURN stDevP(x) AS result")
    }
  )

  List("STDEV_SAMP", "stdev_samp", "StDeV_SaMp", "STDEV_saMP").foreach(functionName =>
    test(s"RETURN $functionName(x) AS result") {
      assertRewrite(testName, "RETURN stDev(x) AS result")
    }
  )

  List("DURATION_BETWEEN", "duration_between", "DuRaTiOn_BeTwEeN", "DURATION_beTWeen").foreach(functionName =>
    test(s"RETURN $functionName(date('2020-01-01'), date('2020-01-02')) AS result") {
      assertRewrite(testName, "RETURN duration.between(date('2020-01-01'), date('2020-01-02')) AS result")
    }
  )
}
