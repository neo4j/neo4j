/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher

import internal.helpers.CollectionSupport
import org.scalatest.Assertions
import org.neo4j.graphdb.Node
import org.scalautils.LegacyTripleEquals

class KeysAcceptanceTest extends ExecutionEngineFunSuite
with QueryStatisticsTestSupport with Assertions with CollectionSupport with LegacyTripleEquals {

  test("Using_keys_function_with_Empty_result") {
    createNode()
    assertThat("match (n) RETURN keys(n)", List())
  }

  test("Using_keys_function_with_Not_Empty_result") {
    createNode()
    assertThat("match (n) SET n.FOO='potato' RETURN keys(n)", List("FOO"))
  }

  private def assertThat(q: String, expectedProperties: List[String]) {
    val result = execute(q).toList

    graph.inTx {
      if (result.isEmpty) {
        val n = graph.getNodeById(0)
        assert(n.getPropertyKeys() === expectedProperties.toIterable)
      } else {
        result.foreach {
          map => map.get("node") match {
            case None =>
              assert(makeTraversable(map.head._2).toList === expectedProperties)

            case Some(n: Node) =>
              assert(n.getPropertyKeys() === expectedProperties)

            case _ =>
              throw new AssertionError("assertThat used with result that is not a node")
          }
        }
      }
    }

    insertNewCleanDatabase()
  }


  private def insertNewCleanDatabase() {
    stopTest()
    initTest()
  }

}
