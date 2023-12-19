/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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

  test("ANY should be able to use variables from the horizon") {

    val query =
      """ WITH 1 AS node, [] AS nodes1
        | RETURN ANY(n IN collect(distinct node) WHERE n IN nodes1) as exists """.stripMargin

    val r = executeWith(Configs.Interpreted - Configs.Version2_3, query)
    r.next().apply("exists") should equal(false)
  }

  test("should not erase the type of an empty array sent as parameter") {
    import Array._

    Seq(emptyLongArray, emptyShortArray, emptyByteArray, emptyIntArray,
      emptyDoubleArray, emptyFloatArray,
      emptyBooleanArray, Array[String]()).foreach { array =>

      val q = "CREATE (n) SET n.prop = $param RETURN n.prop AS p"
      val r = executeWith(Configs.Interpreted - Configs.Version2_3, q, params = Map("param" -> array))

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
      val r = executeWith(Configs.Interpreted - Configs.Version2_3, q, params = Map("param" -> array))

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

    executeWith(Configs.Interpreted - Configs.Cost2_3, "WITH {p} as p SET p.lastname = p.name REMOVE p.name", params = Map("p" -> n))

    graph.inTx {
      n.getProperty("lastname") should equal("Anders")
      n.hasProperty("name") should equal(false)
    }
  }

  test("removing property when not sure if it is a node or relationship should still work - REL") {
    val r = relate(createNode(), createNode(), "name" -> "Anders")

    executeWith(Configs.Interpreted - Configs.Cost2_3, "WITH {p} as p SET p.lastname = p.name REMOVE p.name", params = Map("p" -> r))

    graph.inTx {
      r.getProperty("lastname") should equal("Anders")
      r.hasProperty("name") should equal(false)
    }
  }

  test("match with missing parameter should return error for empty db") {
    // all versions of 3.3 and 3.4
    val config = Configs.Version3_4 + Configs.Version3_3 + Configs.Procs - Configs.AllRulePlanners
    failWithError(config, "MATCH (n:Person {name:{name}}) RETURN n", Seq("Expected parameter(s): name"))
  }

  test("match with missing parameter should return error for non-empty db") {
    // all versions of 3.3 and 3.4
    val config = Configs.Version3_4 + Configs.Version3_3 + Configs.Procs - Configs.AllRulePlanners - Configs.Compiled
    failWithError(config, "CREATE (n:Person) WITH n MATCH (n:Person {name:{name}}) RETURN n", Seq("Expected parameter(s): name"))
  }

  test("match with multiple missing parameters should return error for empty db") {
    // all versions of 3.3 and 3.4
    val config = Configs.Version3_4 + Configs.Version3_3 + Configs.Procs - Configs.AllRulePlanners
    failWithError(config, "MATCH (n:Person {name:{name}, age:{age}}) RETURN n", Seq("Expected parameter(s): name, age"))
  }

  test("match with multiple missing parameters should return error for non-empty db") {
    // all versions of 3.3 and 3.4
    val config = Configs.Version3_4 + Configs.Version3_3 + Configs.Procs - Configs.AllRulePlanners - Configs.Compiled
    failWithError(config, "CREATE (n:Person) WITH n MATCH (n:Person {name:{name}, age:{age}}) RETURN n", Seq("Expected parameter(s): name, age"))
  }

  test("match with misspelled parameter should return error for empty db") {
    // all versions of 3.3 and 3.4
    val config = Configs.Version3_4 + Configs.Version3_3 + Configs.Procs - Configs.AllRulePlanners
    failWithError(config, "MATCH (n:Person {name:{name}}) RETURN n", Seq("Expected parameter(s): name"), params = Map("nam" -> "Neo"))
  }

  test("match with misspelled parameter should return error for non-empty db") {
    // all versions of 3.3 and 3.4
    val config = Configs.Version3_4 + Configs.Version3_3 + Configs.Procs - Configs.AllRulePlanners - Configs.Compiled
    failWithError(config, "CREATE (n:Person) WITH n MATCH (n:Person {name:{name}}) RETURN n", Seq("Expected parameter(s): name"), params = Map("nam" -> "Neo"))
  }

  test("explain with missing parameter should NOT return error for empty db") {
    val config = Configs.All
    executeWith(config, "EXPLAIN MATCH (n:Person {name:{name}}) RETURN n")
  }

  test("explain with missing parameter should NOT return error for non-empty db") {
    val config = Configs.Interpreted - Configs.Cost2_3
    executeWith(config, "EXPLAIN CREATE (n:Person) WITH n MATCH (n:Person {name:{name}}) RETURN n")
  }
}
