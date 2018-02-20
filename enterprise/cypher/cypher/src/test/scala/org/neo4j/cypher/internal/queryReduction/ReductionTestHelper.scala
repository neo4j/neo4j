/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.queryReduction

import org.neo4j.cypher.internal.queryReduction.DDmin.Oracle
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite

trait TestExhausted {
  def assertExhausted(): Unit
}

trait ReductionTestHelper extends CypherFunSuite {

  def getOracle(expectedInvocationsAndResults: Seq[(Array[Int], OracleResult)]): Oracle[Array[Int]] with TestExhausted = {
    var i = 0
    new Oracle[Array[Int]] with TestExhausted {
      override def apply(a: Array[Int]): OracleResult = {
        if (i >= expectedInvocationsAndResults.length) {
          fail(s"Oracle invoked too often. Argument was: ${a.toSeq}")
        }
        a should equal(expectedInvocationsAndResults(i)._1)
        val res = expectedInvocationsAndResults(i)._2
        i = i + 1
        res
      }

      def assertExhausted(): Unit = {
        if (i != expectedInvocationsAndResults.length) {
          fail(s"Oracle not invoked often enough. Next expected call: ${expectedInvocationsAndResults(i)._1.toSeq}")
        }
      }
    }
  }

}
