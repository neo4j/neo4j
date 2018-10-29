/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher._
import org.neo4j.cypher.internal.RewindableExecutionResult
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport
import org.neo4j.kernel.impl.proc.Procedures

class IndexWithValuesAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with CypherComparisonSupport {

  // Need to override so that graph.execute will not throw an exception
  override def databaseConfig(): collection.Map[Setting[_], String] = super.databaseConfig() ++ Map(
    GraphDatabaseSettings.default_schema_provider -> GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10.providerName
  )

  override def beforeEach(): Unit = {
    super.beforeEach()
    createSomeNodes()
    graph.createIndex("Awesome", "prop1")
    graph.createIndex("Awesome", "prop2")
    graph.createIndex("Awesome", "prop1", "prop2")
    graph.createIndex("Awesome", "prop3")
    graph.createIndex("Awesome", "prop4")
    graph.createIndex("Awesome", "emptyProp")
    graph.createIndex("DateString", "ds")
    graph.createIndex("DateDate", "d")
  }

  // Invoked once before the Tx and once in the same Tx
  def createSomeNodes(): Unit = {
    graph.execute(
      """
      CREATE (:Awesome {prop1: 40, prop2: 5})-[:R]->()
      CREATE (:Awesome {prop1: 41, prop2: 2})-[:R]->()
      CREATE (:Awesome {prop1: 42, prop2: 3})-[:R]->()
      CREATE (:Awesome {prop1: 43, prop2: 1})-[:R]->()
      CREATE (:Awesome {prop1: 44, prop2: 3})-[:R]->()
      CREATE (:Awesome {prop3: 'footurama', prop4:'bar'})-[:R]->()
      CREATE (:Awesome {prop3: 'fooism', prop4:'rab'})-[:R]->()
      CREATE (:Awesome {prop3: 'ismfama', prop4:'rab'})-[:R]->()

      CREATE (:DateString {ds: '2018-01-01'})
      CREATE (:DateString {ds: '2018-02-01'})
      CREATE (:DateString {ds: '2018-04-01'})
      CREATE (:DateString {ds: '2017-03-01'})

      CREATE (:DateDate {d: date('2018-02-10')})
      CREATE (:DateDate {d: date('2018-01-10')})
      """)
  }

  test("should plan index seek with GetValue when the property is projected") {
    val result = executeWith(Configs.All, "MATCH (n:Awesome) WHERE n.prop1 = 42 RETURN n.prop1", executeBefore = createSomeNodes)

    result.executionPlanDescription() should (
      not(includeSomewhere.aPlan("Projection").withDBHits()) and
        includeSomewhere.aPlan("NodeIndexSeek")
          .withExactVariables("n", "cached[n.prop1]"))
    result.toList should equal(List(Map("n.prop1" -> 42), Map("n.prop1" -> 42)))
  }

  test("should plan projection and index seek with GetValue when two properties are projected") {
    val result = executeWith(Configs.All, "PROFILE MATCH (n:Awesome) WHERE n.prop1 = 42 RETURN n.prop1, n.prop2", executeBefore = createSomeNodes)

    result.executionPlanDescription() should includeSomewhere.aPlan("Projection")
      .containingArgument("{n.prop1 : cached[n.prop1], n.prop2 : n.prop2}")
      // just for n.prop2, not for n.prop1
      .withDBHits(2)
      .onTopOf(aPlan("NodeIndexSeek")
        .withExactVariables("n", "cached[n.prop1]"))
    result.toList should equal(List(Map("n.prop1" -> 42, "n.prop2" -> 3), Map("n.prop1" -> 42, "n.prop2" -> 3)))
  }

  test("should plan index seek with GetValue when the property is projected and renamed in a RETURN") {
    val result = executeWith(Configs.All, "PROFILE MATCH (n:Awesome) WHERE n.prop1 = 42 RETURN n.prop1 AS foo", executeBefore = createSomeNodes)

    result.executionPlanDescription() should includeSomewhere.aPlan("Projection")
      .containingArgument("{foo : cached[n.prop1]}")
      .withDBHits(0)
      .onTopOf(aPlan("NodeIndexSeek")
        .withExactVariables("n", "cached[n.prop1]"))
    result.toList should equal(List(Map("foo" -> 42), Map("foo" -> 42)))
  }

  test("compiled creates an extra dbhit because it can't get values from indexes") {
    val result = executeSingle("CYPHER runtime=compiled PROFILE MATCH (n:Awesome) WHERE n.prop1 = 42 RETURN n.prop1 AS foo")

    result.executionPlanDescription() should includeSomewhere.aPlan("Projection")
      .containingArgument("{foo : cached[n.prop1]}")
      .withDBHits(0)
      .onTopOf(aPlan("NodeIndexSeek")
        .withDBHits(3)
        .withExactVariables("n", "cached[n.prop1]"))

    result.toList should equal(List(Map("foo" -> 42)))
  }

  test("should plan index seek with GetValue when the property is projected before the property access") {
    val result = executeWith(Configs.All, "MATCH (n:Awesome) WHERE n.prop1 = 42 WITH n MATCH (m)-[r]-(n) RETURN n.prop1", executeBefore = createSomeNodes)

    result.executionPlanDescription() should (
      not(includeSomewhere.aPlan("Projection").withDBHits()) and
        includeSomewhere.aPlan("Expand(All)")
          .onTopOf(aPlan("NodeIndexSeek")
            .withExactVariables("n", "cached[n.prop1]")))
    result.toList should equal(List(Map("n.prop1" -> 42), Map("n.prop1" -> 42)))
  }

  test("should plan projection and index seek with GetValue when the property is projected inside of a expression") {
    val result = executeWith(Configs.All, "PROFILE MATCH (n:Awesome) WHERE n.prop1 = 42 RETURN n.prop1 * 2", executeBefore = createSomeNodes)

    result.executionPlanDescription() should includeSomewhere.aPlan("Projection")
      .containingArgument("{n.prop1 * 2 : cached[n.prop1] * $`  AUTOINT1`}")
      .withDBHits(0)
      .onTopOf(aPlan("NodeIndexSeek")
        .withExactVariables("n", "cached[n.prop1]"))
    result.toList should equal(List(Map("n.prop1 * 2" -> 84), Map("n.prop1 * 2" -> 84)))
  }

  test("should plan projection and index seek with GetValue when the property is used in another predicate") {
    val result = executeWith(Configs.InterpretedAndSlotted, "PROFILE MATCH (n:Awesome) WHERE n.prop1 <= 42 AND n.prop1 % 2 = 0 RETURN n.prop2", executeBefore = createSomeNodes)

    result.executionPlanDescription() should includeSomewhere.aPlan("Filter")
      .withDBHits(0)
      .onTopOf(aPlan("NodeIndexSeekByRange")
        .withExactVariables("n", "cached[n.prop1]"))
    result.toList should equal(List(
      Map("n.prop2" -> 5), Map("n.prop2" -> 5),
      Map("n.prop2" -> 3), Map("n.prop2" -> 3)
    ))
  }

  test("should plan projection and index seek with GetValue when the property is used in ORDER BY") {
    val result = executeWith(Configs.InterpretedAndSlotted, "PROFILE MATCH (n:Awesome) WHERE n.prop1 > 41 RETURN n.prop2 ORDER BY n.prop1", executeBefore = createSomeNodes)

    result.executionPlanDescription() should includeSomewhere.aPlan("Projection")
      .containingArgument("{n.prop2 : n.prop2}")
      // just for n.prop2, not for n.prop1
      .withDBHits(6)
      .onTopOf(aPlan("NodeIndexSeekByRange")
        .withExactVariables("n", "cached[n.prop1]"))
    result.toList should equal(List(
      Map("n.prop2" -> 3), Map("n.prop2" -> 3),
      Map("n.prop2" -> 1), Map("n.prop2" -> 1),
      Map("n.prop2" -> 3), Map("n.prop2" -> 3)))
  }

  test("should correctly project cached node property through ORDER BY") {
    val result = executeWith(Configs.InterpretedAndSlotted - Configs.Version3_1 - Configs.Version2_3,
      "MATCH (a:DateString), (b:DateDate) WHERE a.ds STARTS WITH '2018' AND b.d > date(a.ds) RETURN a.ds ORDER BY a.ds",
      executeBefore = createSomeNodes)

    result.executionPlanDescription() should
      includeSomewhere.aPlan("Apply")
        .withLHS(aPlan("NodeIndexSeekByRange"))
        .withRHS(aPlan("NodeIndexSeekByRange"))

    result.toList should equal(List(
      Map("a.ds" -> "2018-01-01"), Map("a.ds" -> "2018-01-01"),
      Map("a.ds" -> "2018-01-01"), Map("a.ds" -> "2018-01-01"),
      Map("a.ds" -> "2018-01-01"), Map("a.ds" -> "2018-01-01"),
      Map("a.ds" -> "2018-01-01"), Map("a.ds" -> "2018-01-01"),
      Map("a.ds" -> "2018-02-01"), Map("a.ds" -> "2018-02-01"),
      Map("a.ds" -> "2018-02-01"), Map("a.ds" -> "2018-02-01")
    ))
  }

  test("should plan index seek with GetValue when the property is part of an aggregating column") {
    val result = executeWith(Configs.InterpretedAndSlotted, "PROFILE MATCH (n:Awesome) WHERE n.prop1 > 41 RETURN sum(n.prop1), n.prop2 AS nums", executeBefore = createSomeNodes)

    result.executionPlanDescription() should includeSomewhere.aPlan("EagerAggregation")
      // just for n.prop2, not for n.prop1
      .withDBHits(6)
      .onTopOf(aPlan("NodeIndexSeekByRange")
        .withExactVariables("n", "cached[n.prop1]"))
    result.toList.toSet should equal(Set(
      Map("sum(n.prop1)" -> 43 * 2, "nums" -> 1), Map("sum(n.prop1)" -> (42 * 2 + 44 * 2), "nums" -> 3)))
  }

  test("should plan projection and index seek with GetValue when the property is used in key column of an aggregation") {
    val result = executeWith(Configs.InterpretedAndSlotted, "PROFILE MATCH (n:Awesome) WHERE n.prop1 > 41 RETURN sum(n.prop2), n.prop1 AS nums", executeBefore = createSomeNodes)

    result.executionPlanDescription() should includeSomewhere.aPlan("EagerAggregation")
      // just for n.prop2, not for n.prop1
      .withDBHits(6)
      .onTopOf(aPlan("NodeIndexSeekByRange")
        .withExactVariables("n", "cached[n.prop1]"))
    result.toList.toSet should equal(Set(
      Map("sum(n.prop2)" -> 3 * 2, "nums" -> 42), Map("sum(n.prop2)" -> 1 * 2, "nums" -> 43), Map("sum(n.prop2)" -> 3 * 2, "nums" -> 44)))
  }

  test("should plan index seek with GetValue when the property is part of a distinct column") {
    val result = executeWith(Configs.InterpretedAndSlotted, "PROFILE MATCH (n:Awesome) WHERE n.prop1 > 41 AND n.prop1 < 44 RETURN DISTINCT n.prop1", executeBefore = createSomeNodes)

    result.executionPlanDescription() should includeSomewhere.aPlan("Distinct")
      .withDBHits(0)
      .onTopOf(aPlan("NodeIndexSeekByRange")
        .withExactVariables("n", "cached[n.prop1]"))
    result.toList should equal(List(Map("n.prop1" -> 42), Map("n.prop1" -> 43)))
  }

  test("should plan exists with GetValue when the property is projected") {
    val result = executeWith(Configs.InterpretedAndSlotted, "PROFILE MATCH (n:Awesome) WHERE exists(n.prop3) RETURN n.prop3", executeBefore = createSomeNodes)

    result.executionPlanDescription() should (
      not(includeSomewhere.aPlan("Projection").withDBHits()) and
        includeSomewhere.aPlan("NodeIndexScan")
          .withExactVariables("n", "cached[n.prop3]"))
    result.toList.toSet should equal(Set(Map("n.prop3" -> "footurama"), Map("n.prop3" -> "fooism"), Map("n.prop3" -> "ismfama")))
  }

  test("should plan starts with seek with GetValue when the property is projected") {
    val result = executeWith(Configs.InterpretedAndSlotted, "PROFILE MATCH (n:Awesome) WHERE n.prop3 STARTS WITH 'foo' RETURN n.prop3", executeBefore = createSomeNodes)

    result.executionPlanDescription() should (
      not(includeSomewhere.aPlan("Projection").withDBHits()) and
        includeSomewhere.aPlan("NodeIndexSeekByRange")
          .withExactVariables("n", "cached[n.prop3]"))
    result.toList.toSet should equal(Set(Map("n.prop3" -> "footurama"), Map("n.prop3" -> "fooism")))
  }

  test("should plan ends with seek with GetValue when the property is projected") {
    val result = executeWith(Configs.InterpretedAndSlotted, "PROFILE MATCH (n:Awesome) WHERE n.prop3 ENDS WITH 'ama' RETURN n.prop3", executeBefore = createSomeNodes)

    result.executionPlanDescription() should (
      not(includeSomewhere.aPlan("Projection").withDBHits()) and
        includeSomewhere.aPlan("NodeIndexEndsWithScan")
          .withExactVariables("n", "cached[n.prop3]"))
    result.toList.toSet should equal(Set(Map("n.prop3" -> "footurama"), Map("n.prop3" -> "ismfama")))
  }

  test("should plan contains seek with GetValue when the property is projected") {
    val result = executeWith(Configs.InterpretedAndSlotted, "PROFILE MATCH (n:Awesome) WHERE n.prop3 CONTAINS 'ism' RETURN n.prop3", executeBefore = createSomeNodes)

    result.executionPlanDescription() should (
      not(includeSomewhere.aPlan("Projection").withDBHits()) and
        includeSomewhere.aPlan("NodeIndexContainsScan")
          .withExactVariables("n", "cached[n.prop3]"))
    result.toList.toSet should equal(Set(Map("n.prop3" -> "fooism"), Map("n.prop3" -> "ismfama")))
  }

  test("should plan index seek with GetValue when the property is projected (composite index)") {
    val result = executeWith(Configs.InterpretedAndSlotted, "PROFILE MATCH (n:Awesome) WHERE n.prop1 = 42 AND n.prop2 = 3 RETURN n.prop1, n.prop2", executeBefore = createSomeNodes)

    result.executionPlanDescription() should (
      not(includeSomewhere.aPlan("Projection").withDBHits()) and
        includeSomewhere.aPlan("NodeIndexSeek")
          .withExactVariables("n", "cached[n.prop1]", "cached[n.prop2]"))
    result.toList should equal(List(Map("n.prop1" -> 42, "n.prop2" -> 3), Map("n.prop1" -> 42, "n.prop2" -> 3)))
  }

  test("should plan index seek with GetValue and DoNotGetValue when only one property is projected (composite index)") {
    val result = executeWith(Configs.InterpretedAndSlotted, "MATCH (n:Awesome) WHERE n.prop1 = 42 AND n.prop2 = 3 RETURN n.prop1", executeBefore = createSomeNodes)

    result.executionPlanDescription() should (
      not(includeSomewhere.aPlan("Projection").withDBHits()) and
        includeSomewhere.aPlan("NodeIndexSeek")
          .withExactVariables("n", "cached[n.prop1]"))
    result.toList should equal(List(Map("n.prop1" -> 42), Map("n.prop1" -> 42)))
  }

  test("should plan index seek with GetValue when the property is projected after a renaming projection") {
    val result = executeWith(Configs.All, "PROFILE MATCH (n:Awesome) WHERE n.prop1 = 42 WITH n as m MATCH (m)-[r]-(o) RETURN m.prop1", executeBefore = createSomeNodes)

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Projection")
        .containingArgument("{m.prop1 : cached[n.prop1]}")
        .withDBHits(0)
        .withLHS(includeSomewhere
        .aPlan("NodeIndexSeek")
        .withExactVariables("n", "cached[n.prop1]"))
    result.toList should equal(List(Map("m.prop1" -> 42), Map("m.prop1" -> 42)))
  }

  test("should not get confused by variable named as index-backed property I") {

    val query =
      """MATCH (n:Awesome) WHERE n.prop1 = 42
        |WITH n.prop1 AS projected, 'Whoops!' AS `n.prop1`, n
        |RETURN n.prop1, projected""".stripMargin

    val result = executeWith(Configs.All, query)
    assertIndexSeekWithValues(result)
    result.toList should equal(List(Map("n.prop1" -> 42, "projected" -> 42)))
  }

  test("should not get confused by variable named as index-backed property II") {

    val query =
      """WITH 'Whoops!' AS `n.prop1`
        |MATCH (n:Awesome) WHERE n.prop1 = 42
        |RETURN n.prop1, `n.prop1` AS trap""".stripMargin

    val result = executeWith(Configs.All, query)
    assertIndexSeekWithValues(result)
    result.toList should equal(List(Map("n.prop1" -> 42, "trap" -> "Whoops!")))
  }

  test("index-backed property values should be updated on property write") {
    val query = "MATCH (n:Awesome) WHERE n.prop1 = 42 SET n.prop1 = 'newValue' RETURN n.prop1"
    val result = executeWith(Configs.InterpretedAndSlotted - Configs.Cost2_3, query)
    assertIndexSeekWithValues(result)
    result.toList should equal(List(Map("n.prop1" -> "newValue")))
  }

  test("index-backed property values should be removed on property remove") {
    val query = "MATCH (n:Awesome) WHERE n.prop1 = 42 REMOVE n.prop1 RETURN n.prop1"
    val result = executeWith(Configs.InterpretedAndSlotted - Configs.Cost2_3, query)
    assertIndexSeekWithValues(result)
    result.toList should equal(List(Map("n.prop1" -> null)))
  }

  test("index-backed property values should be removed on node delete") {
    val query = "MATCH (n:Awesome) WHERE n.prop1 = 42 DETACH DELETE n RETURN n.prop1"
    failWithError(Configs.InterpretedAndSlotted - Configs.Cost2_3, query, Seq(/* Node with id 4 */ "has been deleted in this transaction"))
  }

  test("index-backed property values should not exist after node deleted") {
    val query = "MATCH (n:Awesome) WHERE n.prop1 = 42 DETACH DELETE n RETURN exists(n.prop1)"
    val result = executeWith(Configs.InterpretedAndSlotted - Configs.Cost2_3, query)
    assertIndexSeekWithValues(result)
    result.toList should equal(List(Map("exists(n.prop1)" -> false)))
  }

  test("index-backed property values should not exist after node deleted - optional match case") {
    val query = "OPTIONAL MATCH (n:Awesome) WHERE n.prop1 = 42 DETACH DELETE n RETURN exists(n.prop1)"
    val result = executeWith(Configs.InterpretedAndSlotted - Configs.Cost2_3, query)
    assertIndexSeekWithValues(result)
    result.toList should equal(List(Map("exists(n.prop1)" -> false)))
  }

  test("existance of index-backed property values of optional node from an empty index, where the node is deleted") {
    val query = "OPTIONAL MATCH (n:Awesome) WHERE n.emptyProp = 42 DETACH DELETE n RETURN exists(n.emptyProp)"
    val result = executeWith(Configs.InterpretedAndSlotted - Configs.Cost2_3, query)
    assertIndexSeekWithValues(result, "n.emptyProp")
    result.toList should equal(List(Map("exists(n.emptyProp)" -> null)))
  }

  test("index-backed property values should be updated on map property write") {
    val query = "MATCH (n:Awesome) WHERE n.prop1 = 42 SET n = {decoy1: 1, prop1: 'newValue', decoy2: 2} RETURN n.prop1"
    val result = executeWith(Configs.InterpretedAndSlotted - Configs.Cost2_3, query)
    assertIndexSeekWithValues(result)
    result.toList should equal(List(Map("n.prop1" -> "newValue")))
  }

  test("index-backed property values should be removed on map property remove") {
    val query = "MATCH (n:Awesome) WHERE n.prop1 = 42 SET n = {decoy1: 1, decoy2: 2} RETURN n.prop1"
    val result = executeWith(Configs.InterpretedAndSlotted - Configs.Cost2_3, query)
    assertIndexSeekWithValues(result)
    result.toList should equal(List(Map("n.prop1" -> null)))
  }

  test("index-backed property values should be updated on procedure property write") {
    registerTestProcedures()
    val query = "MATCH (n:Awesome) WHERE n.prop1 = 42 CALL org.neo4j.setProperty(n, 'prop1', 'newValue') YIELD node RETURN n.prop1"
    val result = executeWith(Configs.InterpretedAndSlotted - Configs.Version2_3 - Configs.RulePlanner, query)
    assertIndexSeek(result)
    result.toList should equal(List(Map("n.prop1" -> "newValue")))
  }

  test("index-backed property values should be updated on procedure property remove") {
    registerTestProcedures()
    val query = "MATCH (n:Awesome) WHERE n.prop1 = 42 CALL org.neo4j.setProperty(n, 'prop1', null) YIELD node RETURN n.prop1"
    val result = executeWith(Configs.InterpretedAndSlotted - Configs.Version2_3 - Configs.RulePlanner, query)
    assertIndexSeek(result)
    result.toList should equal(List(Map("n.prop1" -> null)))
  }

  test("should use cached properties after projection") {
    val result = executeWith(Configs.InterpretedAndSlotted, "MATCH (n:Awesome) WHERE n.prop1 < 42 RETURN n.prop1 ORDER BY n.prop2", executeBefore = createSomeNodes)

    result.executionPlanDescription() should
      includeSomewhere.aPlan("Projection").containingArgument("{n.prop1 : cached[n.prop1]}")
        .onTopOf(aPlan("Sort")
                   .onTopOf(aPlan("Projection")
                              .onTopOf(aPlan("NodeIndexSeekByRange").withExactVariables("cached[n.prop1]", "n"))))
    result.toList should equal(
      List(Map("n.prop1" -> 41), Map("n.prop1" -> 41), Map("n.prop1" -> 40), Map("n.prop1" -> 40)))
  }

  private def assertIndexSeek(result: RewindableExecutionResult): Unit = {
    result.executionPlanDescription() should
      includeSomewhere.aPlan("NodeIndexSeek")
        .containingVariables("n")
  }

  private def assertIndexSeekWithValues(result: RewindableExecutionResult, propName: String = "n.prop1"): Unit = {
    result.executionPlanDescription() should
      includeSomewhere.aPlan("NodeIndexSeek")
        .containingVariables("n", s"cached[$propName]")
  }

  private def registerTestProcedures(): Unit = {
    graph.getDependencyResolver.resolveDependency(classOf[Procedures]).registerProcedure(classOf[TestProcedure])
  }
}
