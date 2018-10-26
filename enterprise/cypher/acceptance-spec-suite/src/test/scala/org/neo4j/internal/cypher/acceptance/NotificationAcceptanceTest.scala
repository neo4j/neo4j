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

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.internal.compiler.v3_5.PlannerUnsupportedNotification
import org.neo4j.cypher.internal.javacompat.DeprecationAcceptanceTest.ChangedResults
import org.neo4j.graphdb
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.graphdb.impl.notification.NotificationCode._
import org.neo4j.graphdb.impl.notification.NotificationDetail.Factory._
import org.neo4j.graphdb.impl.notification.{NotificationCode, NotificationDetail}
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.kernel.impl.proc.Procedures
import org.neo4j.procedure.Procedure

import scala.collection.JavaConverters._

class NotificationAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  // Need to override so that graph.execute will not throw an exception
  override def databaseConfig(): collection.Map[Setting[_], String] = super.databaseConfig() ++ Map(
    GraphDatabaseSettings.cypher_hints_error -> "false",
    GraphDatabaseSettings.query_non_indexed_label_warning_threshold -> "10"
  )

  override def initTest(): Unit = {
    super.initTest()
    val procedures = this.graph.getDependencyResolver.resolveDependency(classOf[Procedures])
    procedures.registerProcedure(classOf[NotificationAcceptanceTest.TestProcedures])
  }

  test("Warn on future ambiguous separator between alternative relationship types") {
    val res1 = executeSingle("explain MATCH (a)-[:A|:B|:C {foo:'bar'}]-(b) RETURN a,b", Map.empty)

    res1.notifications should contain(
      DEPRECATED_RELATIONSHIP_TYPE_SEPARATOR.notification(new graphdb.InputPosition(17, 1, 18)))

    val res2 = executeSingle("explain MATCH (a)-[:A|B|C {foo:'bar'}]-(b) RETURN a,b", Map.empty)

    res2.notifications.map(_.getTitle) should not contain "Neo.ClientNotification.Statement.FeatureDeprecationWarning."

    val res3 = executeSingle("explain MATCH (a)-[:A|:B|:C]-(b) RETURN a,b", Map.empty)

    res3.notifications.map(_.getTitle) should not contain "Neo.ClientNotification.Statement.FeatureDeprecationWarning."

    val res4 = executeSingle("explain MATCH (a)-[:A|B|C]-(b) RETURN a,b", Map.empty)

    res4.notifications.map(_.getTitle) should not contain "Neo.ClientNotification.Statement.FeatureDeprecationWarning."

    val res5 = executeSingle("explain MATCH (a)-[x:A|:B|:C]-() RETURN a", Map.empty)

    res5.notifications should contain(
      DEPRECATED_RELATIONSHIP_TYPE_SEPARATOR.notification(new graphdb.InputPosition(17, 1, 18)))

    val res6 = executeSingle("explain MATCH (a)-[:A|:B|:C*]-() RETURN a", Map.empty)

    res6.notifications should contain(
      DEPRECATED_RELATIONSHIP_TYPE_SEPARATOR.notification(new graphdb.InputPosition(17, 1, 18)))
  }

  test("Warn on binding variable length relationships") {
    val res1 = executeSingle("explain MATCH ()-[rs*]-() RETURN rs", Map.empty)

    res1.notifications should contain(
      DEPRECATED_BINDING_VAR_LENGTH_RELATIONSHIP.notification(new graphdb.InputPosition(16, 1, 17),
                                                              bindingVarLengthRelationship("rs")))

    val res2 = executeSingle("explain MATCH p = ()-[*]-() RETURN relationships(p) AS rs", Map.empty)

    res2.notifications.map(_.getCode) should not contain "Neo.ClientNotification.Statement.FeatureDeprecationWarning."
  }

  test("Warn on deprecated standalone procedure calls") {
    val result = executeSingle("explain CALL oldProc()", Map.empty)

    result.notifications.toList should equal(
      List(
        DEPRECATED_PROCEDURE.notification(new graphdb.InputPosition(8, 1, 9), deprecatedName("oldProc", "newProc"))))
  }

  test("Warn on deprecated in-query procedure calls") {
    val result = executeSingle("explain CALL oldProc() RETURN 1", Map.empty)

    result.notifications.toList should equal(
      List(DEPRECATED_PROCEDURE.notification(new graphdb.InputPosition(8, 1, 9), deprecatedName("oldProc", "newProc"))))
  }

  test("Warn on deprecated procedure result field") {
    val result = executeSingle("explain CALL changedProc() YIELD oldField RETURN oldField", Map.empty)

    result.notifications.toList should equal(
      List(
        DEPRECATED_PROCEDURE_RETURN_FIELD.notification(new graphdb.InputPosition(33, 1, 34),
                                                       deprecatedField("changedProc", "oldField"))))
  }

  test("Warn for cartesian product") {
    val result = executeSingle("explain match (a)-->(b), (c)-->(d) return *", Map.empty)

    result.notifications.toList should equal(List(
      CARTESIAN_PRODUCT.notification(new graphdb.InputPosition(8, 1, 9), cartesianProduct(Set("c", "d").asJava))))
  }

  test("Warn for cartesian product when running 3.4") {
    val result = executeSingle("explain cypher 3.4 match (a)-->(b), (c)-->(d) return *", Map.empty)

    result.notifications.toList should equal(List(
      CARTESIAN_PRODUCT.notification(new graphdb.InputPosition(19, 1, 20), cartesianProduct(Set("c", "d").asJava))))
  }

  test("Warn for cartesian product with runtime=compiled") {
    val result = executeSingle("explain cypher runtime=compiled match (a)-->(b), (c)-->(d) return count(*)", Map.empty)

    result.notifications.toList should equal(List(
      DEPRECATED_COMPILED_RUNTIME.notification(graphdb.InputPosition.empty),
      CARTESIAN_PRODUCT.notification(new graphdb.InputPosition(32, 1, 33), cartesianProduct(Set("c", "d").asJava)),
      RUNTIME_UNSUPPORTED.notification(graphdb.InputPosition.empty)))
  }

  test("Warn unsupported runtime with explain and runtime=compiled") {
    val result = executeSingle(
      """explain cypher runtime=compiled
         RETURN reduce(y=0, x IN [0] | x) AS z""", Map.empty)

    result.notifications.toList should equal(List(
      DEPRECATED_COMPILED_RUNTIME.notification(graphdb.InputPosition.empty),
      RUNTIME_UNSUPPORTED.notification(graphdb.InputPosition.empty)))
  }

  test("Warn for cartesian product with runtime=interpreted") {
    val result = executeSingle("explain cypher runtime=interpreted match (a)-->(b), (c)-->(d) return *", Map.empty)

    result.notifications.toList should equal(List(
      CARTESIAN_PRODUCT.notification(new graphdb.InputPosition(35, 1, 36), cartesianProduct(Set("c", "d").asJava))))
  }

  test("Don't warn for cartesian product when not using explain") {
    val result = executeWith(Configs.All, "match (a)-->(b), (c)-->(d) return *")

    result.notifications shouldBe empty
  }

  test("warn when using length on collection") {
    val result = executeSingle("explain return length([1, 2, 3])", Map.empty)

    result.notifications should equal(Set(
      LENGTH_ON_NON_PATH.notification(new graphdb.InputPosition(22, 1, 23))))
  }

  test("do not warn when using length on a path") {
    val result = executeSingle("explain match p=(a)-[*]->(b) return length(p)", Map.empty)

    result.notifications shouldBe empty
  }

  test("do warn when using length on a pattern expression") {
    val result = executeWith(Configs.InterpretedAndSlotted,
      "explain match (a) where a.name='Alice' return length((a)-->()-->())")

    result.notifications should contain(LENGTH_ON_NON_PATH.notification(new graphdb.InputPosition(97, 1, 98)))
  }

  test("do warn when using length on a string") {
    val result = executeSingle("explain return length('a string')", Map.empty)

    result.notifications should equal(Set(LENGTH_ON_NON_PATH.notification(new graphdb.InputPosition(22, 1, 23))))
  }

  test("do not warn when using size on a collection") {
    val result = executeSingle("explain return size([1, 2, 3])", Map.empty)
    result.notifications shouldBe empty
  }

  test("do not warn when using size on a string") {
    val result = executeSingle("explain return size('a string')", Map.empty)
    result.notifications shouldBe empty
  }

  test("do not warn for cost unsupported on update query if planner not explicitly requested") {
    val result = executeSingle("EXPLAIN MATCH (n:Movie) SET n.title = 'The Movie'", Map.empty)
    result.notifications should not contain PlannerUnsupportedNotification
  }

  test("do not warn for cost unsupported when requesting COST on a supported update query") {
    val result = executeSingle("EXPLAIN CYPHER planner=cost MATCH (n:Movie) SET n:Seen", Map.empty)
    result.notifications should not contain PlannerUnsupportedNotification
  }

  test("do not warn for cost unsupported when requesting IDP on a supported update query") {
    val result = executeSingle("EXPLAIN CYPHER planner=idp MATCH (n:Movie) SET n:Seen", Map.empty)
    result.notifications should not contain PlannerUnsupportedNotification
  }

  test("do not warn for cost unsupported when requesting DP on a supported update query") {
    val result = executeSingle("EXPLAIN CYPHER planner=dp MATCH (n:Movie) SET n:Seen", Map.empty)
    result.notifications should not contain PlannerUnsupportedNotification
  }

  test("warn when requesting runtime=compiled on an unsupported query") {
    val result = executeSingle("EXPLAIN CYPHER runtime=compiled MATCH (a)-->(b), (c)-->(d) RETURN count(*)", Map.empty)
    result.notifications should contain(RUNTIME_UNSUPPORTED.notification(graphdb.InputPosition.empty))
  }

  test("warn once when a single index hint cannot be fulfilled") {
    val result = executeSingle("EXPLAIN MATCH (n:Person) USING INDEX n:Person(name) WHERE n.name = 'John' RETURN n", Map.empty)
    result.notifications.toSet should contain(
      INDEX_HINT_UNFULFILLABLE.notification(graphdb.InputPosition.empty, index("Person", "name")))
  }

  test("warn for each unfulfillable index hint") {
    val result = executeSingle(
      """EXPLAIN MATCH (n:Person), (m:Party), (k:Animal)
        |USING INDEX n:Person(name)
        |USING INDEX m:Party(city)
        |USING INDEX k:Animal(species)
        |WHERE n.name = 'John' AND m.city = 'Reykjavik' AND k.species = 'Sloth'
        |RETURN n""".stripMargin, Map.empty)

    result.notifications should contain(
      INDEX_HINT_UNFULFILLABLE.notification(graphdb.InputPosition.empty, index("Person", "name")))
    result.notifications should contain(
      INDEX_HINT_UNFULFILLABLE.notification(graphdb.InputPosition.empty, index("Party", "city")))
    result.notifications should contain(
      INDEX_HINT_UNFULFILLABLE.notification(graphdb.InputPosition.empty, index("Animal", "species")))
  }

  test("should not warn when join hint is used with COST planner") {
    val result = executeSingle( """CYPHER planner=cost EXPLAIN MATCH (a)-->(b) USING JOIN ON b RETURN a, b""", Map.empty)

    result.notifications should not contain "Neo.Status.Statement.JoinHintUnsupportedWarning"
  }

  test("should not warn when join hint is used with COST planner with EXPLAIN") {
    val result = executeSingle( """CYPHER planner=cost EXPLAIN MATCH (a)-->(x)<--(b) USING JOIN ON x RETURN a, b""", Map.empty)

    result.notifications.map(_.getCode) should not contain "Neo.Status.Statement.JoinHintUnsupportedWarning"
  }

  test("Warnings should work on potentially cached queries") {
    val resultWithoutExplain = executeWith(Configs.All,
      "match (a)-->(b), (c)-->(d) return *")
    val resultWithExplain = executeWith(Configs.All,
      "explain match (a)-->(b), (c)-->(d) return *")

    resultWithoutExplain.notifications.toList shouldBe empty
    resultWithExplain.notifications.toList should equal(
      List(CARTESIAN_PRODUCT.notification(new graphdb.InputPosition(52, 1, 53), cartesianProduct(Set("c", "d").asJava))))
  }

  test("warn for unfulfillable index seek when using dynamic property lookup with a single label") {
    graph.createIndex("Person", "name")

    val result = executeSingle("EXPLAIN MATCH (n:Person) WHERE n['key-' + n.name] = 'value' RETURN n", Map.empty)

    result.notifications should equal(Set(INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY.notification(graphdb.InputPosition.empty,
                                                                                         indexSeekOrScan(
                                                                                           Set("Person").asJava))))
  }

  test("warn for unfulfillable index seek when using dynamic property lookup with explicit label check") {
    graph.createIndex("Person", "name")

    val result = executeSingle("EXPLAIN MATCH (n) WHERE n['key-' + n.name] = 'value' AND (n:Person) RETURN n", Map.empty)

    result.notifications should equal(Set(INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY.notification(graphdb.InputPosition.empty,
                                                                                         indexSeekOrScan(
                                                                                           Set("Person").asJava))
    ))
  }

  test(
    "warn for unfulfillable index seek when using dynamic property lookup with a single label and negative predicate") {
    graph.createIndex("Person", "name")

    val result = executeSingle("EXPLAIN MATCH (n:Person) WHERE n['key-' + n.name] <> 'value' RETURN n", Map.empty)

    result.notifications shouldBe empty
  }

  test("warn for unfulfillable index seek when using dynamic property lookup with range seek") {
    graph.createIndex("Person", "name")

    val result = executeSingle("EXPLAIN MATCH (n:Person) WHERE n['key-' + n.name] > 10 RETURN n", Map.empty)

    result.notifications should equal(Set(INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY.notification(graphdb.InputPosition.empty,
                                                                                         indexSeekOrScan(
                                                                                           Set("Person").asJava))))
  }

  test("warn for unfulfillable index seek when using dynamic property lookup with range seek (reverse)") {
    graph.createIndex("Person", "name")

    val result = executeSingle("EXPLAIN MATCH (n:Person) WHERE 10 > n['key-' + n.name] RETURN n", Map.empty)

    result.notifications should equal(Set(INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY.notification(graphdb.InputPosition.empty,
                                                                                         indexSeekOrScan(
                                                                                           Set("Person").asJava))))
  }

  test(
    "warn for unfulfillable index seek when using dynamic property lookup with a single label and property existence check with exists")
  {
    graph.createIndex("Person", "name")

    val result = executeSingle("EXPLAIN MATCH (n:Person) WHERE exists(n['na' + 'me']) RETURN n", Map.empty)

    result.notifications should equal(Set(INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY.notification(graphdb.InputPosition.empty,
                                                                                         indexSeekOrScan(
                                                                                           Set("Person").asJava))))
  }

  test("warn for unfulfillable index seek when using dynamic property lookup with a single label and starts with") {
    graph.createIndex("Person", "name")

    val result = executeSingle("EXPLAIN MATCH (n:Person) WHERE n['key-' + n.name] STARTS WITH 'Foo' RETURN n", Map.empty)

    result.notifications should equal(Set(INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY.notification(graphdb.InputPosition.empty,
                                                                                         indexSeekOrScan(
                                                                                           Set("Person").asJava))))
  }

  test("warn for unfulfillable index seek when using dynamic property lookup with a single label and regex") {
    graph.createIndex("Person", "name")

    val result = executeSingle("EXPLAIN MATCH (n:Person) WHERE n['key-' + n.name] =~ 'Foo*' RETURN n", Map.empty)

    result.notifications should equal(Set(INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY.notification(graphdb.InputPosition.empty,
                                                                                         indexSeekOrScan(
                                                                                           Set("Person").asJava))))
  }

  test("warn for unfulfillable index seek when using dynamic property lookup with a single label and IN") {
    graph.createIndex("Person", "name")

    val result = executeSingle("EXPLAIN MATCH (n:Person) WHERE n['key-' + n.name] IN ['Foo', 'Bar'] RETURN n", Map.empty)

    result.notifications should equal(Set(INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY.notification(graphdb.InputPosition.empty,
                                                                                         indexSeekOrScan(
                                                                                           Set("Person").asJava))))
  }

  test("warn for unfulfillable index seek when using dynamic property lookup with multiple labels") {
    graph.createIndex("Person", "name")

    val result = executeSingle("EXPLAIN MATCH (n:Person:Foo) WHERE n['key-' + n.name] = 'value' RETURN n", Map.empty)

    result.notifications should contain(INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY.notification(graphdb.InputPosition.empty,
                                                                                       indexSeekOrScan(
                                                                                         Set("Person").asJava)))
  }

  test("warn for unfulfillable index seek when using dynamic property lookup with multiple indexed labels") {
    graph.createIndex("Person", "name")
    graph.createIndex("Jedi", "weapon")

    val result = executeSingle("EXPLAIN MATCH (n:Person:Jedi) WHERE n['key-' + n.name] = 'value' RETURN n", Map.empty)

    result.notifications should equal(Set(INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY.notification(graphdb.InputPosition.empty,
                                                                                         indexSeekOrScan(
                                                                                           Set("Person", "Jedi").asJava))))
  }

  test("should not warn when using dynamic property lookup with no labels") {
    graph.createIndex("Person", "name")

    val result = executeSingle("EXPLAIN MATCH (n) WHERE n['key-' + n.name] = 'value' RETURN n", Map.empty)

    result.notifications shouldBe empty
  }

  test("should warn when using dynamic property lookup with both a static and a dynamic property") {
    graph.createIndex("Person", "name")

    val result = executeSingle(
      "EXPLAIN MATCH (n:Person) WHERE n.name = 'Tobias' AND n['key-' + n.name] = 'value' RETURN n", Map.empty)

    result.notifications should equal(Set(INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY.notification(graphdb.InputPosition.empty,
                                                                                         indexSeekOrScan(
                                                                                           Set("Person").asJava))))
  }

  test("should not warn when using dynamic property lookup with a label having no index") {
    graph.createIndex("Person", "name")
    createLabeledNode("Foo")

    val result = executeSingle("EXPLAIN MATCH (n:Foo) WHERE n['key-' + n.name] = 'value' RETURN n", Map.empty)

    result.notifications shouldBe empty
  }

  test("should not warn for eager before load csv") {
    val result = executeSingle(
      "EXPLAIN MATCH (n) DELETE n WITH * LOAD CSV FROM 'file:///ignore/ignore.csv' AS line MERGE () RETURN line", Map.empty)

    result.executionPlanDescription() should includeSomewhere.aPlan("LoadCSV").withLHS(aPlan("Eager"))
    result.notifications.map(_.getCode) should not contain "Neo.ClientNotification.Statement.EagerOperatorWarning"
  }

  test("should warn for eager after load csv") {
    val result = executeSingle(
      "EXPLAIN MATCH (n) LOAD CSV FROM 'file:///ignore/ignore.csv' AS line WITH * DELETE n MERGE () RETURN line", Map.empty)

    result.executionPlanDescription() should includeSomewhere.aPlan("Eager").withLHS(includeSomewhere.aPlan("LoadCSV"))
    result.notifications.map(_.getCode) should contain("Neo.ClientNotification.Statement.EagerOperatorWarning")
  }

  test("should warn for eager after load csv in 3.4") {
    val result = executeSingle(
      "EXPLAIN CYPHER 3.4 MATCH (n) LOAD CSV FROM 'file:///ignore/ignore.csv' AS line WITH * DELETE n MERGE () RETURN line", Map.empty)

    result.executionPlanDescription() should includeSomewhere.aPlan("Eager").withLHS(includeSomewhere.aPlan("LoadCSV"))
    result.notifications.map(_.getCode) should contain("Neo.ClientNotification.Statement.EagerOperatorWarning")
  }

  test("should not warn for load csv without eager") {
    val result = executeSingle(
      "EXPLAIN LOAD CSV FROM 'file:///ignore/ignore.csv' AS line MATCH (:A) CREATE (:B) RETURN line", Map.empty)

    result.executionPlanDescription() should includeSomewhere.aPlan("LoadCSV")
    result.notifications.map(_.getCode) should not contain "Neo.ClientNotification.Statement.EagerOperatorWarning"
  }

  test("should not warn for eager without load csv") {
    val result = executeSingle("EXPLAIN MATCH (a), (b) CREATE (c) RETURN *", Map.empty)

    result.executionPlanDescription() should includeSomewhere.aPlan("Eager")
    result.notifications.map(_.getCode) should not contain "Neo.ClientNotification.Statement.EagerOperatorWarning"
  }

  test("should not warn for eager that precedes load csv") {
    val result = executeSingle(
      "EXPLAIN MATCH (a), (b) CREATE (c) WITH c LOAD CSV FROM 'file:///ignore/ignore.csv' AS line RETURN *", Map.empty)

    result.executionPlanDescription() should includeSomewhere.aPlan("LoadCSV").withLHS(includeSomewhere.aPlan("Eager"))
    result.notifications.map(_.getCode) should not contain "Neo.ClientNotification.Statement.EagerOperatorWarning"
  }

  test("should warn for large label scans combined with load csv") {
    1 to 11 foreach { _ => createLabeledNode("A") }
    val result = executeSingle("EXPLAIN LOAD CSV FROM 'file:///ignore/ignore.csv' AS line MATCH (a:A) RETURN *", Map.empty)
    result.executionPlanDescription() should includeSomewhere.aPlan.withLHS(aPlan("LoadCSV")).withRHS(aPlan("NodeByLabelScan"))
    result.notifications.map(_.getCode) should contain("Neo.ClientNotification.Statement.NoApplicableIndexWarning")
  }

  test("should warn for large label scans with merge combined with load csv") {
    1 to 11 foreach { _ => createLabeledNode("A") }
    val result = executeSingle("EXPLAIN LOAD CSV FROM 'file:///ignore/ignore.csv' AS line MERGE (a:A) RETURN *", Map.empty)
    result.executionPlanDescription() should includeSomewhere.aPlan.withLHS(aPlan("LoadCSV")).withRHS(aPlan("AntiConditionalApply"))
    result.notifications.map(_.getCode) should contain("Neo.ClientNotification.Statement.NoApplicableIndexWarning")
  }

  test("should not warn for small label scans combined with load csv") {
    createLabeledNode("A")
    val result = executeSingle("EXPLAIN LOAD CSV FROM 'file:///ignore/ignore.csv' AS line MATCH (a:A) RETURN *", Map.empty)
    result.executionPlanDescription() should includeSomewhere.aPlan.withLHS(aPlan("LoadCSV")).withRHS(aPlan("NodeByLabelScan"))
    result.notifications.map(_.getCode) should not contain "Neo.ClientNotification.Statement.NoApplicableIndexWarning"
  }

  test("should not warn for small label scans with merge combined with load csv") {
    createLabeledNode("A")
    val result = executeSingle("EXPLAIN LOAD CSV FROM 'file:///ignore/ignore.csv' AS line MERGE (a:A) RETURN *", Map.empty)
    result.executionPlanDescription() should includeSomewhere.aPlan.withLHS(aPlan("LoadCSV")).withRHS(aPlan("AntiConditionalApply"))
    result.notifications.map(_.getCode) should not contain "Neo.ClientNotification.Statement.NoApplicableIndexWarning"
  }

  test("should warn for misspelled/missing label") {
    //given
    createLabeledNode("Person")

    //when
    val resultMisspelled = executeSingle("EXPLAIN MATCH (n:Preson) RETURN *", Map.empty)
    val resultCorrectlySpelled = executeSingle("EXPLAIN MATCH (n:Person) RETURN *", Map.empty)

    //then
    resultMisspelled.notifications should contain(
      MISSING_LABEL.notification(new graphdb.InputPosition(17, 1, 18), label("Preson")))

    resultCorrectlySpelled.notifications shouldBe empty
  }

  test("should not warn for missing label on update") {

    //when
    val result = executeSingle("EXPLAIN CREATE (n:Person)", Map.empty)

    //then
    result.notifications shouldBe empty
  }

  test("should warn for misspelled/missing relationship type") {
    //given
    relate(createNode(), createNode(), "R")

    //when
    val resultMisspelled = executeSingle("EXPLAIN MATCH ()-[r:r]->() RETURN *", Map.empty)
    val resultCorrectlySpelled = executeSingle("EXPLAIN MATCH ()-[r:R]->() RETURN *", Map.empty)

    resultMisspelled.notifications should contain(
      MISSING_REL_TYPE
        .notification(new graphdb.InputPosition(20, 1, 21), NotificationDetail.Factory.relationshipType("r")))

    resultCorrectlySpelled.notifications shouldBe empty
  }

  test("should warn for misspelled/missing property names") {
    //given
    createNode(Map("prop" -> 42))
    //when
    val resultMisspelled = executeSingle("EXPLAIN MATCH (n) WHERE n.propp = 43 RETURN n", Map.empty)
    val resultCorrectlySpelled = executeSingle("EXPLAIN MATCH (n) WHERE n.prop = 43 RETURN n", Map.empty)

    resultMisspelled.notifications should contain(
      NotificationCode.MISSING_PROPERTY_NAME.notification(new graphdb.InputPosition(26, 1, 27), propertyName("propp")))

    resultCorrectlySpelled.notifications shouldBe empty
  }

  test("should not warn for missing properties on update") {
    val result = executeSingle("EXPLAIN CREATE (n {prop: 42})", Map.empty)

    result.notifications shouldBe empty
  }

  test("should warn about unbounded shortest path") {
    val res = executeSingle("EXPLAIN MATCH p = shortestPath((n)-[*]->(m)) RETURN m", Map.empty)

    res.notifications should contain(
      UNBOUNDED_SHORTEST_PATH.notification(new graphdb.InputPosition(34, 1, 35)))
  }

  test("2.3 can warn about bare nodes") {
    val res = executeSingle("EXPLAIN CYPHER 2.3 MATCH n RETURN n", Map.empty)

    res.notifications should not be empty
  }

  test("should not warn about literal maps") {
    val res = executeSingle("explain return { id: 42 } ", Map.empty)

    res.notifications should be(empty)
  }

  test("do not warn when creating a node with non-existent label when using load csv") {
    val result = executeSingle(
      "EXPLAIN LOAD CSV WITH HEADERS FROM 'file:///fake.csv' AS row CREATE (n:Category)", Map.empty)

    result.notifications shouldBe empty
  }

  test("do not warn when merging a node with non-existent label when using load csv") {
    val result = executeSingle(
      "EXPLAIN LOAD CSV WITH HEADERS FROM 'file:///fake.csv' AS row MERGE (n:Category)", Map.empty)

    result.notifications shouldBe empty
  }

  test("do not warn when setting on a node a non-existent label when using load csv") {
    val result = executeSingle(
      "EXPLAIN LOAD CSV WITH HEADERS FROM 'file:///fake.csv' AS row CREATE (n) SET n:Category", Map.empty)

    result.notifications shouldBe empty
  }

  test("do not warn when creating a rel with non-existent type when using load csv") {
    val result = executeSingle(
      "EXPLAIN LOAD CSV WITH HEADERS FROM 'file:///fake.csv' AS row CREATE ()-[:T]->()", Map.empty)

    result.notifications shouldBe empty
  }

  test("do not warn when merging a rel with non-existent type when using load csv") {
    val result = executeSingle(
      "EXPLAIN LOAD CSV WITH HEADERS FROM 'file:///fake.csv' AS row MERGE ()-[:T]->()", Map.empty)

    result.notifications shouldBe empty
  }

  test("do not warn when creating a node with non-existent prop key id when using load csv") {
    val result = executeSingle(
      "EXPLAIN LOAD CSV WITH HEADERS FROM 'file:///fake.csv' AS row CREATE (n) SET n.p = 'a'", Map.empty)

    result.notifications shouldBe empty
  }

  test("do not warn when merging a node with non-existent prop key id when using load csv") {
    val result = executeSingle(
      "EXPLAIN LOAD CSV WITH HEADERS FROM 'file:///fake.csv' AS row MERGE (n) ON CREATE SET n.p = 'a'", Map.empty)

    result.notifications shouldBe empty
  }

  test("warn for use of deprecated toInt") {
    val result = executeSingle("EXPLAIN RETURN toInt('1') AS one", Map.empty)

    result.notifications should contain(DEPRECATED_FUNCTION.notification(new graphdb.InputPosition(15, 1, 16),
                                                                         deprecatedName("toInt", "toInteger"))
    )
  }

  test("warn for use of deprecated upper") {
    val result = executeSingle("EXPLAIN RETURN upper('foo') AS one", Map.empty)

    result.notifications should contain(DEPRECATED_FUNCTION.notification(new graphdb.InputPosition(15, 1, 16),
                                                                          deprecatedName("upper", "toUpper")))
  }

  test("warn for use of deprecated lower") {
    val result = executeSingle("EXPLAIN RETURN lower('BAR') AS one", Map.empty)

    result.notifications should contain(DEPRECATED_FUNCTION.notification(new graphdb.InputPosition(15, 1, 16),
                                                                         deprecatedName("lower", "toLower")))
  }

  test("warn for use of deprecated rels") {
    val result = executeSingle("EXPLAIN MATCH p = ()-->() RETURN rels(p) AS r", Map.empty)

    result.notifications should contain(
      DEPRECATED_FUNCTION.notification(new graphdb.InputPosition(33, 1, 34),
                                       deprecatedName("rels", "relationships")))
  }

  test("should warn when using START in newer runtimes") {
    createNode()
    val query = "EXPLAIN CYPHER runtime=slotted START n=node(0) RETURN n"
    val result = executeSingle(query, Map.empty)
    val notifications = result.notifications
    notifications should contain(RUNTIME_UNSUPPORTED.notification(graphdb.InputPosition.empty))
  }

  test("should warn when using CREATE UNIQUE in newer runtimes") {
    val query = "EXPLAIN CYPHER runtime=slotted MATCH (root { name: 'root' }) CREATE UNIQUE (root)-[:LOVES]-(someone) RETURN someone"
    val result = executeSingle(query, Map.empty)
    val notifications = result.notifications
    notifications should contain(RUNTIME_UNSUPPORTED.notification(graphdb.InputPosition.empty))
  }

  test("should warn when using contains on an index with SLOW_CONTAINS limitation") {
    graph.createIndex("Person", "name")
    val query = "EXPLAIN MATCH (a:Person) WHERE a.name CONTAINS 'er' RETURN a"
    val result = executeSingle(query, Map.empty)
    result.notifications should contain(SUBOPTIMAL_INDEX_FOR_CONTAINS_QUERY.notification(graphdb.InputPosition.empty, suboptimalIndex("Person", "name")))
  }

  test("should warn when using ends with on an index with SLOW_CONTAINS limitation") {
    graph.createIndex("Person", "name")
    val query = "EXPLAIN MATCH (a:Person) WHERE a.name ENDS WITH 'son' RETURN a"
    val result = executeSingle(query, Map.empty)
    result.notifications should contain(SUBOPTIMAL_INDEX_FOR_ENDS_WITH_QUERY.notification(graphdb.InputPosition.empty, suboptimalIndex("Person", "name")))
  }

  test("should warn when using contains on a unique index with SLOW_CONTAINS limitation") {
    graph.createConstraint("Person", "name")
    val query = "EXPLAIN MATCH (a:Person) WHERE a.name CONTAINS 'er' RETURN a"
    val result = executeSingle(query, Map.empty)
    result.notifications should contain(SUBOPTIMAL_INDEX_FOR_CONTAINS_QUERY.notification(graphdb.InputPosition.empty, suboptimalIndex("Person", "name")))
  }

  test("should warn when using ends with on a unique index with SLOW_CONTAINS limitation") {
    graph.createConstraint("Person", "name")
    val query = "EXPLAIN MATCH (a:Person) WHERE a.name ENDS WITH 'son' RETURN a"
    val result = executeSingle(query, Map.empty)
    result.notifications should contain(SUBOPTIMAL_INDEX_FOR_ENDS_WITH_QUERY.notification(graphdb.InputPosition.empty, suboptimalIndex("Person", "name")))
  }

  test("should not warn when using starts with on an index with SLOW_CONTAINS limitation") {
    graph.createIndex("Person", "name")
    val query = "EXPLAIN MATCH (a:Person) WHERE a.name STARTS WITH 'er' RETURN a"
    val result = executeSingle(query, Map.empty)
    result.notifications should not contain SUBOPTIMAL_INDEX_FOR_CONTAINS_QUERY.notification(graphdb.InputPosition.empty, suboptimalIndex("Person", "name"))
    result.notifications should not contain SUBOPTIMAL_INDEX_FOR_ENDS_WITH_QUERY.notification(graphdb.InputPosition.empty, suboptimalIndex("Person", "name"))
  }

  test("should not warn when using starts with on a unique index with SLOW_CONTAINS limitation") {
    graph.createConstraint("Person", "name")
    val query = "EXPLAIN MATCH (a:Person) WHERE a.name STARTS WITH 'er' RETURN a"
    val result = executeSingle(query, Map.empty)
    result.notifications should not contain SUBOPTIMAL_INDEX_FOR_CONTAINS_QUERY.notification(graphdb.InputPosition.empty, suboptimalIndex("Person", "name"))
    result.notifications should not contain SUBOPTIMAL_INDEX_FOR_ENDS_WITH_QUERY.notification(graphdb.InputPosition.empty, suboptimalIndex("Person", "name"))
  }
}

class LuceneIndexNotificationAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  // Need to override so that graph.execute will not throw an exception
  override def databaseConfig(): collection.Map[Setting[_], String] = super.databaseConfig() ++ Map(
    GraphDatabaseSettings.cypher_hints_error -> "false",
    GraphDatabaseSettings.query_non_indexed_label_warning_threshold -> "10",
    GraphDatabaseSettings.default_schema_provider -> "lucene-1.0"
  )

  test("should not warn when using contains on an index with no limitations") {
    graph.createIndex("Person", "name")
    val query = "EXPLAIN MATCH (a:Person) WHERE a.name CONTAINS 'er' RETURN a"
    val result = executeSingle(query, Map.empty)
    result.notifications should not contain SUBOPTIMAL_INDEX_FOR_CONTAINS_QUERY.notification(graphdb.InputPosition.empty, suboptimalIndex("Person", "name"))
  }

  test("should not warn when using ends with on an index with no limitations") {
    graph.createIndex("Person", "name")
    val query = "EXPLAIN MATCH (a:Person) WHERE a.name ENDS WITH 'son' RETURN a"
    val result = executeSingle(query, Map.empty)
    result.notifications should not contain SUBOPTIMAL_INDEX_FOR_ENDS_WITH_QUERY.notification(graphdb.InputPosition.empty, suboptimalIndex("Person", "name"))
  }

  test("should not warn when using contains on a unique index with no limitations") {
    graph.createConstraint("Person", "name")
    val query = "EXPLAIN MATCH (a:Person) WHERE a.name CONTAINS 'er' RETURN a"
    val result = executeSingle(query, Map.empty)
    result.notifications should not contain SUBOPTIMAL_INDEX_FOR_CONTAINS_QUERY.notification(graphdb.InputPosition.empty, suboptimalIndex("Person", "name"))
  }

  test("should not warn when using ends with on a unique index with no limitations") {
    graph.createConstraint("Person", "name")
    val query = "EXPLAIN MATCH (a:Person) WHERE a.name ENDS WITH 'son' RETURN a"
    val result = executeSingle(query, Map.empty)
    result.notifications should not contain SUBOPTIMAL_INDEX_FOR_ENDS_WITH_QUERY.notification(graphdb.InputPosition.empty, suboptimalIndex("Person", "name"))
  }
}

object NotificationAcceptanceTest {

  class TestProcedures {

    @Procedure("newProc")
    def newProc(): Unit = {}

    @Deprecated
    @Procedure(name = "oldProc", deprecatedBy = "newProc")
    def oldProc(): Unit = {}

    @Procedure("changedProc")
    def changedProc(): java.util.stream.Stream[ChangedResults] =
      java.util.stream.Stream.builder().add(new ChangedResults).build()
  }

}
