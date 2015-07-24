/*
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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription.Arguments.KeyNames
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.compiler.v2_3.{GreedyPlannerName, IDPPlannerName}
import org.neo4j.cypher.{ExecutionEngineFunSuite, HintException, IndexHintException, NewPlannerTestSupport, SyntaxException}
import org.scalatest.matchers.{MatchResult, Matcher}

class UsingAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  test("fail if using index with start clause") {
    // GIVEN
    graph.createIndex("Person", "name")

    // WHEN & THEN
    intercept[SyntaxException](
      executeWithAllPlanners("start n=node(*) using index n:Person(name) where n:Person and n.name = 'kabam' return n"))
  }

  test("fail if using an identifier with label not used in match") {
    // GIVEN
    graph.createIndex("Person", "name")

    // WHEN
    intercept[SyntaxException](
      executeWithAllPlanners("match n-->() using index n:Person(name) where n.name = 'kabam' return n"))
  }

  test("fail if using an hint for a non existing index") {
    // GIVEN: NO INDEX

    // WHEN
    intercept[IndexHintException](
      executeWithAllPlanners("match (n:Person)-->() using index n:Person(name) where n.name = 'kabam' return n"))
  }

  test("fail if using hints with unusable equality predicate") {
    // GIVEN
    graph.createIndex("Person", "name")

    // WHEN
    intercept[SyntaxException](
      executeWithAllPlanners("match (n:Person)-->() using index n:Person(name) where n.name <> 'kabam' return n"))
  }

  test("fail if joining index hints in equality predicates") {
    // GIVEN
    graph.createIndex("Person", "name")
    graph.createIndex("Food", "name")

    // WHEN
    intercept[SyntaxException](
      executeWithAllPlanners("match (n:Person)-->(m:Food) using index n:Person(name) using index m:Food(name) where n.name = m.name return n"))
  }

  test("scan hints are handled by ronja") {
    executeWithAllPlannersAndRuntimes("match (n:Person) using scan n:Person return n").toList
  }

  test("fail when equality checks are done with OR") {
    // GIVEN
    graph.createIndex("Person", "name")

    // WHEN
    intercept[SyntaxException](
      executeWithAllPlanners("match n-->() using index n:Person(name) where n.name = 'kabam' OR n.name = 'kaboom' return n"))
  }

  test("should be able to use index hints on IN expressions") {
    //GIVEN
    val andres = createLabeledNode(Map("name" -> "Andres"), "Person")
    val jake = createLabeledNode(Map("name" -> "Jacob"), "Person")
    relate(andres, createNode())
    relate(jake, createNode())

    graph.createIndex("Person", "name")

    //WHEN
    val result = executeWithAllPlannersAndRuntimes("MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name IN ['Jacob'] RETURN n")

    //THEN
    result.toList should equal (List(Map("n" -> jake)))
  }

  test("should be able to use index hints on IN collections with duplicates") {
    //GIVEN
    val andres = createLabeledNode(Map("name" -> "Andres"), "Person")
    val jake = createLabeledNode(Map("name" -> "Jacob"), "Person")
    relate(andres, createNode())
    relate(jake, createNode())

    graph.createIndex("Person", "name")

    //WHEN
    val result = executeWithAllPlannersAndRuntimes("MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name IN ['Jacob','Jacob'] RETURN n")

    //THEN
    result.toList should equal (List(Map("n" -> jake)))
  }

  test("should be able to use index hints on IN an empty collections") {
    //GIVEN
    val andres = createLabeledNode(Map("name" -> "Andres"), "Person")
    val jake = createLabeledNode(Map("name" -> "Jacob"), "Person")
    relate(andres, createNode())
    relate(jake, createNode())

    graph.createIndex("Person", "name")

    //WHEN
    val result = executeWithAllPlannersAndRuntimes("MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name IN [] RETURN n")

    //THEN
    result.toList should equal (List())
  }

  test("should be able to use index hints on IN a null value") {
    //GIVEN
    val andres = createLabeledNode(Map("name" -> "Andres"), "Person")
    val jake = createLabeledNode(Map("name" -> "Jacob"), "Person")
    relate(andres, createNode())
    relate(jake, createNode())

    graph.createIndex("Person", "name")

    //WHEN
    val result = executeWithAllPlannersAndRuntimes("MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name IN null RETURN n")

    //THEN
    result.toList should equal (List())
  }

  test("should be able to use index hints on IN a collection parameter") {
    //GIVEN
    val andres = createLabeledNode(Map("name" -> "Andres"), "Person")
    val jake = createLabeledNode(Map("name" -> "Jacob"), "Person")
    relate(andres, createNode())
    relate(jake, createNode())

    graph.createIndex("Person", "name")

    //WHEN
    val result = executeWithAllPlannersAndRuntimes("MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name IN {coll} RETURN n","coll"->List("Jacob"))

    //THEN
    result.toList should equal (List(Map("n" -> jake)))
  }

  test("does not accept multiple index hints for the same identifier") {
    // GIVEN
    graph.createIndex("Entity", "source")
    graph.createIndex("Person", "first_name")
    createNode("source" -> "form1")
    createNode("first_name" -> "John")

    // WHEN THEN
    val e = intercept[SyntaxException] {
      executeWithAllPlanners(
        "MATCH (n:Entity:Person) " +
          "USING INDEX n:Person(first_name) " +
          "USING INDEX n:Entity(source) " +
          "WHERE n.first_name = \"John\" AND n.source = \"form1\" " +
          "RETURN n;"
      )
    }

    e.getMessage should startWith("Multiple hints for same identifier are not supported")
  }

  test("does not accept multiple scan hints for the same identifier") {
    val e = intercept[SyntaxException] {
      executeWithAllPlanners(
        "MATCH (n:Entity:Person) " +
          "USING SCAN n:Person " +
          "USING SCAN n:Entity " +
          "WHERE n.first_name = \"John\" AND n.source = \"form1\" " +
          "RETURN n;"
      )
    }

    e.getMessage should startWith("Multiple hints for same identifier are not supported")
  }

  test("does not accept multiple mixed hints for the same identifier") {
    val e = intercept[SyntaxException] {
      executeWithAllPlanners(
        "MATCH (n:Entity:Person) " +
          "USING SCAN n:Person " +
          "USING INDEX n:Entity(first_name) " +
          "WHERE n.first_name = \"John\" AND n.source = \"form1\" " +
          "RETURN n;"
      )
    }

    e.getMessage should startWith("Multiple hints for same identifier are not supported")
  }

  test("scan hint must fail if using an identifier not used in the query") {
    // GIVEN

    // WHEN
    intercept[SyntaxException](
      executeWithAllPlanners("MATCH (n:Person)-->() USING SCAN x:Person return n"))
  }

  test("scan hint must fail if using label not used in the query") {
    // GIVEN

    // WHEN
    intercept[SyntaxException](
      executeWithAllPlanners("MATCH n-->() USING SCAN n:Person return n"))
  }

  test("when failing to support all hints we should provide an understandable error message") {
    // GIVEN
    graph.createIndex("LocTag", "id")

    // WHEN
    val query = """CYPHER planner=greedy MATCH (t1:LocTag {id:1642})-[:Child*0..]->(:LocTag)
                  |     <-[:Tagged]-(s1:Startup)<-[r1:Role]-(u:User)
                  |     -[r2:Role]->(s2:Startup)-[:Tagged]->(:LocTag)
                  |     <-[:Child*0..]-(t2:LocTag {id:1642})
                  |USING INDEX t1:LocTag(id)
                  |USING INDEX t2:LocTag(id)
                  |RETURN count(u)""".stripMargin


    val error = intercept[HintException](innerExecute(query))

    error.getMessage should equal("The current planner cannot satisfy all hints in the query, please try removing hints or try with another planner")
  }

  val plannersThatSupportJoinHints = Seq(GreedyPlannerName, IDPPlannerName)

  plannersThatSupportJoinHints.foreach { planner =>

    val plannerName = planner.name.toLowerCase

    test(s"$plannerName should fail when join hint is applied to an undefined node") {
      val error = intercept[SyntaxException](
        executeWithCostPlannerOnly(
          s"""
             |CYPHER planner=$plannerName
              |MATCH (a:A)-->(b:B)<--(c:C)
              |USING JOIN ON d
              |RETURN a.prop
          """.stripMargin))

      error.getMessage should include("d not defined")
    }

    test(s"$plannerName should fail when join hint is applied to a single node") {
      val error = intercept[SyntaxException](
        executeWithCostPlannerOnly(
          s"""
             |CYPHER planner=$plannerName
              |MATCH (a:A)
              |USING JOIN ON a
              |RETURN a.prop
          """.stripMargin))

      error.getMessage should include("Cannot use join hint")
    }

    test(s"$plannerName should fail when join hint is applied to a relationship") {
      val error = intercept[SyntaxException](
        executeWithCostPlannerOnly(
          s"""
             |CYPHER planner=$plannerName
              |MATCH (a:A)-[r1]->(b:B)-[r2]->(c:C)
              |USING JOIN ON r1
              |RETURN a.prop
          """.stripMargin))

      error.getMessage should include("Type mismatch: expected Node but was Relationship")
    }

    test(s"$plannerName should fail when join hint is applied to a path") {
      val error = intercept[SyntaxException](
        executeWithCostPlannerOnly(
          s"""
             |CYPHER planner=$plannerName
              |MATCH p=(a:A)-->(b:B)-->(c:C)
              |USING JOIN ON p
              |RETURN a.prop
          """.stripMargin))

      error.getMessage should include("Type mismatch: expected Node but was Path")
    }

    test(s"$plannerName should fail when join hint is applied to a pattern of length 1") {
      val error = intercept[SyntaxException](
        executeWithCostPlannerOnly(
          s"""
             |CYPHER planner=$plannerName
              |MATCH (a:A)-->(b:B)
              |USING JOIN ON b
              |RETURN a.prop
          """.stripMargin))

      error.getMessage should include("Cannot use join hint")
    }

    test(s"$plannerName should be able to use join hints for multiple hop pattern") {
      val a = createNode(("prop", "foo"))
      val b = createNode()
      val c = createNode()
      val d = createNode()
      val e = createNode(("prop", "foo"))

      relate(a, b, "X")
      relate(b, c, "X")
      relate(c, d, "X")
      relate(d, e, "X")

      val result = executeWithCostPlannerOnly(
        s"""
           |CYPHER planner=$plannerName
            |MATCH (a)-[:X]->(b)-[:X]->(c)-[:X]->(d)-[:X]->(e)
            |USING JOIN ON c
            |WHERE a.prop = e.prop
            |RETURN c""".stripMargin)

      result.toList should equal(List(Map("c" -> c)))
      result.executionPlanDescription() should includeHashJoinOn("c")
    }

    test(s"$plannerName should be able to use join hints for queries with var length pattern") {
      val a = createLabeledNode(Map("prop" -> "foo"), "Foo")
      val b = createNode()
      val c = createNode()
      val d = createNode()
      val e = createLabeledNode(Map("prop" -> "foo"), "Bar")

      relate(a, b, "X")
      relate(b, c, "X")
      relate(c, d, "X")
      relate(e, d, "Y")

      val result = executeWithCostPlannerOnly(
        s"""
           |CYPHER planner=$plannerName
            |MATCH (a:Foo)-[:X*]->(b)<-[:Y]->(c:Bar)
            |USING JOIN ON b
            |WHERE a.prop = c.prop
            |RETURN c""".stripMargin)

      result.toList should equal(List(Map("c" -> e)))
      result.executionPlanDescription() should includeHashJoinOn("b")
    }

    test(s"$plannerName should be able to use multiple join hints") {
      val a = createNode(("prop", "foo"))
      val b = createNode()
      val c = createNode()
      val d = createNode()
      val e = createNode(("prop", "foo"))

      relate(a, b, "X")
      relate(b, c, "X")
      relate(c, d, "X")
      relate(d, e, "X")

      val result = executeWithCostPlannerOnly(
        s"""
           |CYPHER planner=$plannerName
            |MATCH (a)-[:X]->(b)-[:X]->(c)-[:X]->(d)-[:X]->(e)
            |USING JOIN ON b
            |USING JOIN ON c
            |USING JOIN ON d
            |WHERE a.prop = e.prop
            |RETURN b, d""".stripMargin)

      result.toList should equal(List(Map("b" -> b, "d" -> d)))
      result.executionPlanDescription() should includeHashJoinOn("b")
      result.executionPlanDescription() should includeHashJoinOn("c")
      result.executionPlanDescription() should includeHashJoinOn("d")
    }

  }

  test("rule planner should ignore join hint") {
    val a = createNode()
    val m = createNode()
    val z = createNode()
    relate(a, m)
    relate(z, m)

    val result = innerExecute(
      """
        |CYPHER planner=rule
        |MATCH (a)-->(m)<--(z)
        |USING JOIN ON m
        |RETURN DISTINCT m""".stripMargin).toList

    result should equal(List(Map("m" -> m)))
  }

  case class includeHashJoinOn(nodeIdentifier: String) extends Matcher[InternalPlanDescription] {

    private val hashJoinStr = classOf[NodeHashJoin].getSimpleName

    override def apply(result: InternalPlanDescription): MatchResult = {
      val hashJoinExists = result.flatten.exists { description =>
        description.name == hashJoinStr && description.arguments.contains(KeyNames(Seq(nodeIdentifier)))
      }

      MatchResult(hashJoinExists, matchResultMsg(negated = false, result), matchResultMsg(negated = true, result))
    }

    private def matchResultMsg(negated: Boolean, result: InternalPlanDescription) =
      s"$hashJoinStr on node '$nodeIdentifier' ${if (negated) "" else "not"}found in plan description\n $result"
  }

}
