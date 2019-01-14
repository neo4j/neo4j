/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
