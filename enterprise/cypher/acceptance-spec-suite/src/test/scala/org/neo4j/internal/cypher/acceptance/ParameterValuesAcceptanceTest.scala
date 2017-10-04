/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.{ExecutionEngineFunSuite, QueryStatisticsTestSupport}
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport.Configs

class ParameterValuesAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport
  with QueryStatisticsTestSupport {

  test("should be able to send in an array of nodes via parameter") {
    // given
    val node = createLabeledNode("Person")
    val result = executeWith(Configs.All, "WITH {param} as p RETURN p", params = Map("param" -> Array(node)))
    val outputP = result.next.get("p").get
    outputP should equal(Array(node))
  }

  // Not TCK material below; sending graph types or characters as parameters is not supported

  test("ANY should be able to use varibels from the horizon") {

    val query =
      """ WITH 1 AS node, [] AS nodes1
        | RETURN ANY(n IN collect(distinct node) WHERE n IN nodes1) as exists """.stripMargin

    val r = executeWith(Configs.CommunityInterpreted - Configs.Version2_3, query)
    r.next().apply("exists") should equal(false)
  }

  test("should not erase the type of an empty array sent as parameter") {
    import Array._

    Seq(emptyLongArray, emptyShortArray, emptyByteArray, emptyIntArray,
      emptyDoubleArray, emptyFloatArray,
      emptyBooleanArray, Array[String]()).foreach { array =>

      val q = "CREATE (n) SET n.prop = $param RETURN n.prop AS p"
      val r = executeWith(Configs.CommunityInterpreted - Configs.Version2_3, q, params = Map("param" -> array))

      assertStats(r, nodesCreated = 1, propertiesWritten = 1)
      val returned = r.columnAs[Array[_]]("p").next()
      returned should equal(array)
      returned.getClass.getComponentType should equal(array.getClass.getComponentType)
    }
  }

  test("should not erase the type of nonempty arrays sent as parameter") {
    Seq(Array[Long](1l), Array[Short](2), Array[Byte](3), Array[Int](4),
      Array[Double](3.14), Array[Float](5.56f),
      Array[Boolean](false, true), Array[String]("", " ")).foreach { array =>

      val q = "CREATE (n) SET n.prop = $param RETURN n.prop AS p"
      val r = executeWith(Configs.CommunityInterpreted - Configs.Version2_3, q, params = Map("param" -> array))

      assertStats(r, nodesCreated = 1, propertiesWritten = 1)
      val returned = r.columnAs[Array[_]]("p").next()
      returned should equal(array)
      returned.getClass.getComponentType should equal(array.getClass.getComponentType)
    }
  }

  test("should be able to send in node via parameter") {
    // given
    val node = createLabeledNode("Person")

    val result = executeWith(Configs.All, "MATCH (b) WHERE b = {param} RETURN b", params = Map("param" -> node))
    result.toList should equal(List(Map("b" -> node)))
  }

  test("should be able to send in relationship via parameter") {
    // given
    val rel = relate(createLabeledNode("Person"), createLabeledNode("Person"))

    val result = executeWith(Configs.All, "MATCH (:Person)-[r]->(:Person) WHERE r = {param} RETURN r", params = Map("param" -> rel))
    result.toList should equal(List(Map("r" -> rel)))
  }

  test("should treat chars as strings in equality") {
    executeScalar[Boolean]("RETURN 'a' = {param}", "param" -> 'a') shouldBe true
    executeScalar[Boolean]("RETURN {param} = 'a'", "param" -> 'a') shouldBe true
  }

  test("removing property when not sure if it is a node or relationship should still work - NODE") {
    val n = createNode("name" -> "Anders")

    executeWith(Configs.CommunityInterpreted - Configs.Cost2_3, "WITH {p} as p SET p.lastname = p.name REMOVE p.name", params = Map("p" -> n))

    graph.inTx {
      n.getProperty("lastname") should equal("Anders")
      n.hasProperty("name") should equal(false)
    }
  }

  test("removing property when not sure if it is a node or relationship should still work - REL") {
    val r = relate(createNode(), createNode(), "name" -> "Anders")

    executeWith(Configs.CommunityInterpreted - Configs.Cost2_3, "WITH {p} as p SET p.lastname = p.name REMOVE p.name", params = Map("p" -> r))

    graph.inTx {
      r.getProperty("lastname") should equal("Anders")
      r.hasProperty("name") should equal(false)
    }
  }
}
