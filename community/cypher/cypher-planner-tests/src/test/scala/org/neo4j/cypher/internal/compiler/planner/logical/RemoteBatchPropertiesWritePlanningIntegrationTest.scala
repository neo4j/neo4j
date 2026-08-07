/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseInternalSettings.RemoteBatchPropertiesImplementation
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.compiler.ExecutionModel
import org.neo4j.cypher.internal.compiler.ExecutionModel.BatchedParallel
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanConstructionTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.ir.EagernessReason
import org.neo4j.cypher.internal.ir.HasHeaders
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeFull
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createPattern
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationship
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setNodeProperty
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.planner.spi.DatabaseMode
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.collection.immutable.ListSet
import org.neo4j.cypher.internal.util.symbols.CTAny

class DefaultRuntimeRemoteBatchPropertiesWritePlanningIntegrationTest
    extends AbstractRemoteBatchPropertiesWritePlanningIntegrationTest(ExecutionModel.default) {
  override protected val orderPreserving: Boolean = true
}

class ParallelRuntimeRemoteBatchPropertiesWritePlanningIntegrationTest
    extends AbstractRemoteBatchPropertiesWritePlanningIntegrationTest(BatchedParallel(1, 2)) {
  override protected val orderPreserving: Boolean = false
}

abstract class AbstractRemoteBatchPropertiesWritePlanningIntegrationTest(executionModel: ExecutionModel)
    extends CypherPlannerTestSuite
    with LogicalPlanningIntegrationTestSupport
    with AstConstructionTestSupport
    with LogicalPlanConstructionTestSupport {

  protected val orderPreserving: Boolean

  // Graph counts based on a subset of LDBC SF 1
  final protected val plannerBase = plannerBuilder()
    .setDatabaseMode(DatabaseMode.SHARDED)
    .withSetting(
      GraphDatabaseInternalSettings.cypher_remote_batch_properties_implementation,
      RemoteBatchPropertiesImplementation.PLANNER
    )
    .withSetting(
      GraphDatabaseInternalSettings.remote_leaf_operators,
      true
    )
    .setExecutionModel(executionModel)
    .setAllNodesCardinality(3181725)
    .setLabelCardinality("Person", 9892)
    .setLabelCardinality("Message", 3055774)
    .setLabelCardinality("Post", 1003605)
    .setLabelCardinality("Comment", 2052169)
    .setLabelCardinality("Country", 111)
    .setLabelCardinality("City", 1343)
    .setLabelCardinality("Tag", 16080)
    .setLabelCardinality("TagClass", 71)
    .setRelationshipCardinality("()-[:KNOWS]->()", 180623)
    .setRelationshipCardinality("(:Person)-[:KNOWS]->()", 180623)
    .setRelationshipCardinality("()-[:KNOWS]->(:Person)", 180623)
    .setRelationshipCardinality("(:Person)-[:KNOWS]->(:Person)", 180623)
    .setRelationshipCardinality("()-[:POST_HAS_CREATOR]->()", 1003605)
    .setRelationshipCardinality("(:Message)-[:POST_HAS_CREATOR]->()", 1003605)
    .setRelationshipCardinality("(:Post)-[:POST_HAS_CREATOR]->()", 1003605)
    .setRelationshipCardinality("()-[:POST_HAS_CREATOR]->(:Person)", 1003605)
    .setRelationshipCardinality("(:Message)-[:POST_HAS_CREATOR]->(:Person)", 1003605)
    .setRelationshipCardinality("(:Post)-[:POST_HAS_CREATOR]->(:Person)", 1003605)
    .setRelationshipCardinality("(:Person)-[:POST_HAS_CREATOR]->()", 0)
    .setRelationshipCardinality("()-[:POST_HAS_CREATOR]->(:Message)", 0)
    .setRelationshipCardinality("()-[:POST_HAS_CREATOR]->(:Post)", 0)
    .setRelationshipCardinality("(:Person)-[:POST_HAS_CREATOR]->(:Person)", 0)
    .setRelationshipCardinality("()-[:COMMENT_HAS_CREATOR]->()", 2052169)
    .setRelationshipCardinality("(:Message)-[:COMMENT_HAS_CREATOR]->()", 2052169)
    .setRelationshipCardinality("(:Comment)-[:COMMENT_HAS_CREATOR]->()", 2052169)
    .setRelationshipCardinality("()-[:REPLY_OF]->()", 2052169)
    .setRelationshipCardinality("()-[:REPLY_OF]->(:Message)", 2052169)
    .setRelationshipCardinality("(:Message)-[:REPLY_OF]->()", 0)
    .setRelationshipCardinality("(:Message)-[:REPLY_OF]->(:Message)", 0)
    .setRelationshipCardinality("()-[:COMMENT_HAS_CREATOR]->(:Person)", 2052169)
    .setRelationshipCardinality("(:Message)-[:COMMENT_HAS_CREATOR]->(:Person)", 2052169)
    .setRelationshipCardinality("(:Comment)-[:COMMENT_HAS_CREATOR]->(:Person)", 2052169)
    .setRelationshipCardinality("(:Person)-[:COMMENT_HAS_CREATOR]->()", 0)
    .setRelationshipCardinality("()-[:COMMENT_HAS_CREATOR]->(:Message)", 0)
    .setRelationshipCardinality("()-[:COMMENT_HAS_CREATOR]->(:Comment)", 0)
    .setRelationshipCardinality("()-[:MESSAGE_HAS_TAG]->()", 2928064)
    .setRelationshipCardinality("(:Message)-[:MESSAGE_HAS_TAG]->(:Tag)", 2928064)
    .setRelationshipCardinality("(:Message)-[:MESSAGE_HAS_TAG]->()", 2928064)
    .setRelationshipCardinality("()-[:MESSAGE_HAS_TAG]->(:Tag)", 2928064)
    .setRelationshipCardinality("(:Tag)-[:MESSAGE_HAS_TAG]->()", 0)
    .setRelationshipCardinality("(:Tag)-[:MESSAGE_HAS_TAG]->(:Message)", 0)
    .setRelationshipCardinality("()-[:MESSAGE_HAS_TAG]->(:Message)", 0)
    .setRelationshipCardinality("(:Post)-[:MESSAGE_HAS_TAG]->(:Tag)", 0)
    .setRelationshipCardinality("()-[:IS_LOCATED_IN]->()", 9892)
    .setRelationshipCardinality("(:Person)-[:IS_LOCATED_IN]->()", 9892)
    .setRelationshipCardinality("()-[:IS_LOCATED_IN]->(:City)", 9892)
    .setRelationshipCardinality("()-[:IS_LOCATED_IN]->(:Person)", 0)
    .setRelationshipCardinality("(:City)-[:IS_LOCATED_IN]->()", 0)
    .setRelationshipCardinality("(:Person)-[:IS_LOCATED_IN]->(:City)", 9892)
    .setRelationshipCardinality("()-[:IS_PART_OF]->()", 1454)
    .setRelationshipCardinality("(:City)-[:IS_PART_OF]->()", 1454)
    .setRelationshipCardinality("()-[:IS_PART_OF]->(:Country)", 1454)
    .setRelationshipCardinality("(:City)-[:IS_PART_OF]->(:Country)", 1454)
    .setRelationshipCardinality("()-[:IS_PART_OF]->(:City)", 0)
    .setRelationshipCardinality("(:Country)-[:IS_PART_OF]->()", 0)
    .setRelationshipCardinality("()-[:HAS_TYPE]->()", 16080)
    .setRelationshipCardinality("(:Tag)-[:HAS_TYPE]->(:TagClass)", 16080)
    .setRelationshipCardinality("(:Tag)-[:HAS_TYPE]->()", 16080)
    .setRelationshipCardinality("()-[:HAS_TYPE]->(:TagClass)", 16080)
    .setRelationshipCardinality("()-[:HAS_TYPE]->(:Tag)", 0)
    .setRelationshipCardinality("(:TagClass)-[:HAS_TYPE]->()", 0)
    .setRelationshipCardinality("(:Message)-[]->(:Person)", 2052169 + 1003605)
    .setRelationshipCardinality("(:Message)-[]->()", 2052169 + 1003605)
    .setRelationshipCardinality("()-[]->()", 2052169 + 1003605 + 180623)
    .setRelationshipCardinality("()-[]->(:Person)", 2052169 + 1003605 + 180623)
    .addNodeIndex("Person", List("id"), existsSelectivity = 1.0, uniqueSelectivity = 1.0 / 9892.0, isUnique = true)
    .addNodeIndex("Person", List("firstName"), existsSelectivity = 1.0, uniqueSelectivity = 1 / 1323.0)
    .addNodeIndex("Message", List("creationDate"), existsSelectivity = 1.0, uniqueSelectivity = 10 / 3055774.0)
    .addNodeIndex("City", List("name"), existsSelectivity = 1.0, uniqueSelectivity = 1.0 / 1343.0)
    .addNodeIndex("Country", List("name"), existsSelectivity = 1.0, uniqueSelectivity = 1.0 / 111.0)
    .addNodeIndex("Country", List("id"), existsSelectivity = 1.0, uniqueSelectivity = 1.0 / 111.0)
    .addNodeIndex("Tag", List("id"), existsSelectivity = 1.0, uniqueSelectivity = 1.0 / 16080.0)
    .addNodeIndex("Tag", List("name"), existsSelectivity = 1.0, uniqueSelectivity = 1.0 / 16080.0)
    .addNodeIndex("TagClass", List("name"), existsSelectivity = 1.0, uniqueSelectivity = 1.0 / 71.0)
    .addRelationshipIndex("COMMENT_HAS_CREATOR", List("location"), existsSelectivity = 1.0, uniqueSelectivity = 0.1)
    .addRelationshipIndex(
      "COMMENT_HAS_CREATOR",
      List("id"),
      existsSelectivity = 1.0,
      uniqueSelectivity = 1.0 / 2052169,
      isUnique = true
    )
    .setLabelCardinality("Dog", 10)
    .setRelationshipCardinality("()-[:HAS_DOG]->()", 10)
    .setRelationshipCardinality("(:Person)-[:HAS_DOG]->()", 10)
    .setRelationshipCardinality("()-[:HAS_DOG]->(:Person)", 10)
    .setRelationshipCardinality("(:Person)-[:HAS_DOG]->(:Person)", 10)
    .setRelationshipCardinality("(:Person)-[:HAS_DOG]->(:Dog)", 10)
    .setRelationshipCardinality("()-[:FRIEND]->()", 10)
    .setRelationshipCardinality("(:Person)-[:FRIEND]->()", 10)
    .setRelationshipCardinality("()-[:FRIEND]->(:Person)", 10)
    .setRelationshipCardinality("(:Person)-[:FRIEND]->(:Person)", 10)

  val planner = plannerBase.build()

  test("should fetch properties again after properties have been set") {
    val query =
      """
        |MATCH (n:Person {name: 'Alice', age: 30})
        |SET n.age = 31
        |RETURN n.name, n.age
        |""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("`n.name`", "`n.age`")
      .projection("cacheN[n.name] AS `n.name`", "cacheN[n.age] AS `n.age`")
      .remoteBatchProperties("cacheNFromStore[n.name]", "cacheNFromStore[n.age]")
      .setNodeProperty("n", "age", "31")
      .filter("cacheN[n.name] = 'Alice'", "cacheN[n.age] = 30")
      .remoteBatchProperties("cacheNFromStore[n.name]", "cacheNFromStore[n.age]")
      .nodeByLabelScan("n", "Person")
      .build()
  }

  test("should set remote batch properties after a create") {
    val query =
      """
        |CREATE
        |  (max:Person {name: 'Max'}),
        |  (chris:Person {name: 'Chris'})
        |CREATE (max)-[:KNOWS {prop: 42}]->(chris)
        |WITH *
        |MATCH (a)-[r:KNOWS]->(b)
        |WHERE r.prop = 42
        |RETURN a.name, b.name
        |""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner.planBuilder()
      .produceResults("`a.name`", "`b.name`")
      .projection("cacheN[a.name] AS `a.name`", "cacheN[b.name] AS `b.name`")
      .remoteBatchProperties("cacheNFromStore[a.name]", "cacheNFromStore[b.name]")
      .filter("cacheR[r.prop] = 42")
      .remoteBatchProperties("cacheRFromStore[r.prop]")
      .apply()
      .|.relationshipTypeScan("(a)-[r:KNOWS]->(b)")
      .eager(ListSet(
        EagernessReason.TypeReadSetConflict(RelTypeName("KNOWS")(InputPosition.NONE)).withConflict(
          EagernessReason.Conflict(Id(8), Id(6))
        ),
        EagernessReason.PropertyReadSetConflict(PropertyKeyName("prop")(InputPosition.NONE)).withConflict(
          EagernessReason.Conflict(Id(8), Id(6))
        ),
        EagernessReason.LabelReadSetConflict(LabelName("Person")(InputPosition.NONE)).withConflict(
          EagernessReason.Conflict(Id(8), Id(6))
        )
      ))
      .create(
        createNodeFull("max", labels = Seq("Person"), properties = Some("{name: 'Max'}")),
        createNodeFull("chris", labels = Seq("Person"), properties = Some("{name: 'Chris'}")),
        createRelationship("anon_0", "max", "KNOWS", "chris", OUTGOING, Some("{prop: 42}"))
      )
      .argument()
      .build()
  }

  test("should plan remoteBatchProperties on sort after transaction") {
    val plan = planner.plan(
      """LOAD CSV WITH HEADERS from $param AS row
        |CALL {
        |  WITH row
        |  CREATE (n {name: row.name, age: toInteger(row.age)})
        |  RETURN n
        |} IN TRANSACTIONS
        |RETURN n.name, n.age ORDER BY n.age ASC""".stripMargin
    )

    plan shouldEqual
      planner.planBuilder()
        .produceResults("`n.name`", "`n.age`")
        .projection("cacheN[n.name] AS `n.name`")
        .___CONDITION_BEGIN___(orderPreserving)
        .remoteBatchProperties("cacheNFromStore[n.name]")
        .___CONDITION_END___()
        .sort("`n.age` ASC")
        .projection("cacheN[n.age] AS `n.age`")
        .___CONDITION_BEGIN___(!orderPreserving)
        .remoteBatchProperties("cacheNFromStore[n.age]", "cacheNFromStore[n.name]")
        .___CONDITION_BEGIN___(orderPreserving)
        .remoteBatchProperties("cacheNFromStore[n.age]")
        .___CONDITION_END___()
        .transactionApply()
        .|.create(createNodeFull("n", properties = Some("{name: row.name, age: toInteger(row.age)}")))
        .|.argument("row")
        .loadCSV("$param", "row", HasHeaders, None)
        .argument()
        .build()
  }

  test("should fetch properties when using dynamic labels") {
    val query = """
                  |LOAD CSV WITH HEADERS FROM $param AS line
                  |CREATE (n {name: line.name})
                  |SET n:$(line.role)
                  |RETURN n.name""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("`n.name`")
        .projection("cacheN[n.name] AS `n.name`")
        .remoteBatchProperties("cacheNFromStore[n.name]")
        .setLabels("n", Seq(), Seq("line.role"))
        .create(createNodeFull("n", properties = Some("{name: line.name}")))
        .loadCSV("$param", "line", HasHeaders, None)
        .argument()
        .build()
  }

  test("should fetch properties on merge queries") {
    val query =
      """
        |MERGE (p:Person {name: 'Andy'})
        |ON MATCH SET p.existed = true
        |ON CREATE SET p.existed = false
        |RETURN p.name, p.existed
        |""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("`p.name`", "`p.existed`")
        .projection("cacheN[p.name] AS `p.name`", "cacheN[p.existed] AS `p.existed`")
        .remoteBatchProperties("cacheNFromStore[p.name]", "cacheNFromStore[p.existed]")
        .merge(
          Seq(createNodeFull("p", labels = Seq("Person"), properties = Some("{name: 'Andy'}"))),
          Seq.empty,
          Seq(setNodeProperty("p", "existed", "true")),
          Seq(setNodeProperty("p", "existed", "false")),
          Set.empty
        )
        .filter("cacheN[p.name] = 'Andy'")
        .remoteBatchProperties("cacheNFromStore[p.name]")
        .nodeByLabelScan("p", "Person")
        .build()
  }

  test("should fetch properties on locking merge queries") {
    val query =
      """
        |MATCH  (andy:Person {name: 'Andy'}),
        |       (lou:Person {name: 'Lou'})
        |MERGE (andy)-[r:KNOWS {from: 'Factory'}]->(lou)
        |RETURN r.name, r.existed
        |""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("`r.name`", "`r.existed`")
        .projection("cacheR[r.name] AS `r.name`", "cacheR[r.existed] AS `r.existed`")
        .apply()
        .|.merge(
          Seq(),
          Seq(createRelationship("r", "andy", "KNOWS", "lou", OUTGOING, Some("{from: 'Factory'}"))),
          Seq(),
          Seq(),
          Set("andy", "lou")
        )
        .|.filter("cacheR[r.from] = 'Factory'")
        .|.remoteBatchProperties("cacheRFromStore[r.from]", "cacheRFromStore[r.name]", "cacheRFromStore[r.existed]")
        .|.expandInto("(andy)-[r:KNOWS]->(lou)")
        .|.argument("andy", "lou")
        .cartesianProduct()
        .|.filter("cacheN[lou.name] = 'Lou'")
        .|.remoteBatchProperties("cacheNFromStore[lou.name]")
        .|.nodeByLabelScan("lou", "Person")
        .filter("cacheN[andy.name] = 'Andy'")
        .remoteBatchProperties("cacheNFromStore[andy.name]")
        .nodeByLabelScan("andy", "Person")
        .build()
  }

  test("should set eager when filters use cached properties") {
    val query =
      """MATCH (person:Person)
        |WHERE person.id IN [1, 2] AND person.firstName IS NOT NULL
        |CALL {
        |  WITH person
        |  SET person.firstName = 'Unknown'
        |  RETURN person AS x
        |}
        |RETURN x.firstName AS personFirstName""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("personFirstName")
      .projection("cacheN[x.firstName] AS personFirstName")
      .remoteBatchProperties("cacheNFromStore[x.firstName]")
      .eager(ListSet(
        EagernessReason
          .PropertyReadSetConflict(PropertyKeyName("firstName")(InputPosition.NONE))
          .withConflict(EagernessReason.Conflict(Id(6), Id(1)))
      ))
      .apply()
      .|.projection("person AS x")
      .|.setNodeProperty("person", "firstName", "'Unknown'")
      .|.argument("person")
      .eager(ListSet(
        EagernessReason
          .PropertyReadSetConflict(PropertyKeyName("firstName")(InputPosition.NONE))
          .withConflict(EagernessReason.Conflict(Id(6), Id(9)))
      ))
      .filter("cacheN[person.firstName] IS NOT NULL")
      .remoteBatchProperties("cacheNFromStore[person.firstName]")
      .nodeIndexOperator(
        "person:Person(id = 1 OR 2)",
        getValue = Map("id" -> DoNotGetValue),
        unique = true,
        supportPartitionedScan = false
      )
      .build()
  }

  test("SET relationship property should use previously cached node property") {
    // `lastName` will be fetched for `p1` in the outer query graph
    // within the CALL subquery the cached version can then be used to set the property for the newly created relationship
    val query = """MATCH (p1:Person), (p2:Person)
                  |WHERE p1.lastName = p2.lastName
                  |CALL (p1, p2) {
                  |  CREATE (p1)-[x:MAYBE_RELATED]->(p2)
                  |  SET x.lastName = p1.lastName
                  |} IN TRANSACTIONS OF 1000 ROWS;""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .transactionForeach(1000)
      .|.setRelationshipProperty("x", "lastName", "cacheN[p1.lastName]") // use cached property `lastName`
      .|.create(createRelationship("x", "p1", "MAYBE_RELATED", "p2"))
      .|.argument("p1", "p2")
      .valueHashJoin("cacheN[p1.lastName] = cacheN[p2.lastName]")
      .|.remoteBatchProperties("cacheNFromStore[p2.lastName]")
      .|.nodeByLabelScan("p2", "Person")
      .remoteBatchProperties("cacheNFromStore[p1.lastName]") // cache property `lastName`
      .nodeByLabelScan("p1", "Person")
      .build()
  }

  test("SET node property should use previously cached node property") {
    val query = """MATCH (p1:Person)
                  |WHERE p1.lastName = "J"
                  |CALL (p1) {
                  |  CREATE (x:Entity)
                  |  SET x.name = p1.lastName
                  |} IN TRANSACTIONS OF 1000 ROWS;""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .transactionForeach(1000)
      .|.setNodeProperty("x", "name", "cacheN[p1.lastName]") // use property `lastName`
      .|.create(createNodeFull("x", labels = Seq("Entity")))
      .|.argument("p1")
      .filter("cacheN[p1.lastName] = 'J'")
      .remoteBatchProperties("cacheNFromStore[p1.lastName]") // cache property `lastName`
      .nodeByLabelScan("p1", "Person")
      .build()
  }

  test("SET relationship property should use previously cached relationship property") {
    // Fetch properties `since` and `value` from k,
    // use the cached property when setting the property `confidence` for the newly created relationship.
    val query = """MATCH (p1:Person)-[k:KNOWS]->(p2:Person)
                  |WHERE p1.firstName = "J" AND k.since > 2020
                  |CALL (p1, k, p2) {
                  |  CREATE (p1)-[x:MAYBE_RELATED]->(p2)
                  |  SET x.confidence = k.value
                  |} IN TRANSACTIONS OF 1000 ROWS;""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .transactionForeach(1000)
      .|.setRelationshipProperty("x", "confidence", "cacheR[k.value]") // use cached property `value``
      .|.create(createRelationship("x", "p1", "MAYBE_RELATED", "p2"))
      .|.argument("p1", "k", "p2")
      .filter("cacheR[k.since] > 2020", "p2:Person")
      .remoteBatchProperties("cacheRFromStore[k.since]", "cacheRFromStore[k.value]") // cache property `value`
      .expandAll("(p1)-[k:KNOWS]->(p2)")
      .nodeIndexOperator("p1:Person(firstName = 'J')", getValue = Map("firstName" -> DoNotGetValue))
      .build()
  }

  test("SET node property should use previously cached relationship property") {
    // Fetch properties `since` and `value` from k,
    // use the cached property when setting the property `confidence` for the newly created relationship.
    val query = """MATCH (p1:Person)-[k:KNOWS]->(p2:Person)
                  |WHERE p1.firstName = "J" AND k.since > 2020
                  |CALL (p1, k, p2) {
                  |  CREATE (p1)-[x:MAYBE_RELATED]->(p2)
                  |  SET p1.confidence = k.value
                  |} IN TRANSACTIONS OF 1000 ROWS;""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .transactionForeach(1000)
      .|.setNodeProperty("p1", "confidence", "cacheR[k.value]") // use cached property `value``
      .|.create(createRelationship("x", "p1", "MAYBE_RELATED", "p2"))
      .|.argument("p1", "k", "p2")
      .filter("cacheR[k.since] > 2020", "p2:Person")
      .remoteBatchProperties("cacheRFromStore[k.since]", "cacheRFromStore[k.value]") // cache property `value`
      .expandAll("(p1)-[k:KNOWS]->(p2)")
      .nodeIndexOperator(
        "p1:Person(firstName = 'J')",
        getValue = Map("firstName" -> DoNotGetValue)
      ) // should NOT plan a remoteNodeIndexSeek operator since it is not supported for write queries.
      .build()
  }

  test("SET relationship properties should use previously cached node and relationship properties") {
    val query = """MATCH (p1:Person)-[k:KNOWS]->(p2:Person)
                  |WHERE p1.firstName = "J" AND p1.age > 20 AND k.since > 2020
                  |CALL (p1, k, p2) {
                  |  CREATE (p1)-[x:MAYBE_RELATED]->(p2)
                  |  SET x.confidence = k.value, x.fromP1 = p1.prop
                  |} IN TRANSACTIONS OF 1000 ROWS;""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .transactionForeach(1000)
      .|.setRelationshipProperties("x", ("confidence", "cacheR[k.value]"), ("fromP1", "cacheN[p1.prop]"))
      .|.create(createRelationship("x", "p1", "MAYBE_RELATED", "p2"))
      .|.argument("p1", "k", "p2")
      .filter("cacheR[k.since] > 2020", "p2:Person")
      .remoteBatchProperties("cacheRFromStore[k.since]", "cacheRFromStore[k.value]")
      .expandAll("(p1)-[k:KNOWS]->(p2)")
      .filter("cacheN[p1.age] > 20")
      .remoteBatchProperties("cacheNFromStore[p1.age]", "cacheNFromStore[p1.prop]")
      .nodeIndexOperator("p1:Person(firstName = 'J')", getValue = Map("firstName" -> DoNotGetValue))
      .build()
  }

  test("SET node properties should use previously cached node and relationship properties") {
    val query = """MATCH (p1:Person)-[k:KNOWS]->(p2:Person)
                  |WHERE p1.firstName = "J" AND p1.age > 20 AND k.since > 2020
                  |CALL (p1, k) {
                  |  CREATE (x:Entity)
                  |  SET x.confidence = k.value, x.fromJ = p1.prop
                  |} IN TRANSACTIONS OF 1000 ROWS;""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .transactionForeach(1000)
      .|.setNodeProperties(
        "x",
        ("confidence", "cacheR[k.value]"),
        ("fromJ", "cacheN[p1.prop]")
      ) // use cached `k.value` and `p1.prop`
      .|.create(createNodeFull("x", labels = Seq("Entity")))
      .|.argument("p1", "k")
      .filter("cacheR[k.since] > 2020", "p2:Person")
      .remoteBatchProperties("cacheRFromStore[k.since]", "cacheRFromStore[k.value]") // cache `k.value`
      .expandAll("(p1)-[k:KNOWS]->(p2)")
      .filter("cacheN[p1.age] > 20")
      .remoteBatchProperties("cacheNFromStore[p1.age]", "cacheNFromStore[p1.prop]") // cache `p1.prop`
      .nodeIndexOperator("p1:Person(firstName = 'J')", getValue = Map("firstName" -> DoNotGetValue))
      .build()
  }

  test("SET relationship properties from MAP should use previously cached node and relationship properties") {
    val query = """MATCH (p1:Person)-[k:KNOWS]->(p2:Person)
                  |WHERE p1.firstName = "J" AND p1.age > 20 AND k.since > 2020
                  |CALL (p1, k, p2) {
                  |  CREATE (p1)-[x:MAYBE_RELATED]->(p2)
                  |  SET x += {confidence: k.value, fromP1: p1.prop}
                  |} IN TRANSACTIONS OF 1000 ROWS;""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .transactionForeach(1000)
      .|.setRelationshipPropertiesFromMap(
        "x",
        "{confidence: cacheR[k.value], fromP1: cacheN[p1.prop]}",
        removeOtherProps = false
      )
      .|.create(createRelationship("x", "p1", "MAYBE_RELATED", "p2"))
      .|.argument("p1", "k", "p2")
      .filter("cacheR[k.since] > 2020", "p2:Person")
      .remoteBatchProperties("cacheRFromStore[k.since]", "cacheRFromStore[k.value]")
      .expandAll("(p1)-[k:KNOWS]->(p2)")
      .filter("cacheN[p1.age] > 20")
      .remoteBatchProperties("cacheNFromStore[p1.age]", "cacheNFromStore[p1.prop]")
      .nodeIndexOperator("p1:Person(firstName = 'J')", getValue = Map("firstName" -> DoNotGetValue))
      .build()
  }

  test("SET node properties from MAP should use previously cached node and relationship properties") {
    val query = """MATCH (p1:Person)-[k:KNOWS]->(p2:Person)
                  |WHERE p1.firstName = "J" AND p1.age > 20 AND k.since > 2020
                  |CALL (p1, k) {
                  |  CREATE (x:Entity)
                  |  SET x += {confidence: k.value, fromJ: p1.prop}
                  |} IN TRANSACTIONS OF 1000 ROWS;""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .transactionForeach(1000)
      .|.setNodePropertiesFromMap(
        "x",
        "{confidence: cacheR[k.value], fromJ: cacheN[p1.prop]}",
        removeOtherProps = false
      ) // use cached `k.value` and `p1.prop`
      .|.create(createNodeFull("x", labels = Seq("Entity")))
      .|.argument("p1", "k")
      .filter("cacheR[k.since] > 2020", "p2:Person")
      .remoteBatchProperties("cacheRFromStore[k.since]", "cacheRFromStore[k.value]") // cache `k.value`
      .expandAll("(p1)-[k:KNOWS]->(p2)")
      .filter("cacheN[p1.age] > 20")
      .remoteBatchProperties("cacheNFromStore[p1.age]", "cacheNFromStore[p1.prop]") // cache `p1.prop`
      .nodeIndexOperator("p1:Person(firstName = 'J')", getValue = Map("firstName" -> DoNotGetValue))
      .build()
  }

  test("SET dynamic property should use previously cached node property") {
    val query = """MATCH (p1:Person)-[k:KNOWS]->(p2:Person)
                  |WHERE p1.firstName = "J" AND p1.age > 20 AND k.since > 2020
                  |CALL (p1, k, p2) {
                  |  CREATE (p1)-[x:MAYBE_RELATED]->(p2)
                  |  SET x[$newPropName] = k.since
                  |} IN TRANSACTIONS OF 1000 ROWS;""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .transactionForeach(1000)
      .|.setDynamicProperty("x", "$newPropName", "cacheR[k.since]")
      .|.create(createRelationship("x", "p1", "MAYBE_RELATED", "p2"))
      .|.argument("p1", "k", "p2")
      .eager(ListSet(
        EagernessReason.UnknownPropertyReadSetConflict.withConflict(EagernessReason.Conflict(Id(3), Id(7))),
        EagernessReason.UnknownPropertyReadSetConflict.withConflict(EagernessReason.Conflict(Id(3), Id(10))),
        EagernessReason.UnknownPropertyReadSetConflict.withConflict(EagernessReason.Conflict(Id(3), Id(12)))
      ))
      .filter("cacheR[k.since] > 2020", "p2:Person")
      .remoteBatchProperties("cacheRFromStore[k.since]")
      .expandAll("(p1)-[k:KNOWS]->(p2)")
      .filter("cacheN[p1.age] > 20")
      .remoteBatchProperties("cacheNFromStore[p1.age]")
      .nodeIndexOperator("p1:Person(firstName = 'J')", getValue = Map("firstName" -> DoNotGetValue))
      .build()
  }

  test("SET properties in CREATE should use previously cached node property") {
    val query = """MATCH (p1:Person)-[k:KNOWS]->(p2:Person)
                  |WHERE p1.firstName = "J" AND p1.age > 20 AND k.since > 2020
                  |CALL (p1, k, p2) {
                  |  CREATE (p1)-[x:MAYBE_RELATED {newProp: k.since}]->(p2)
                  |} IN TRANSACTIONS OF 1000 ROWS;""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .transactionForeach(1000)
      .|.create(
        createRelationship("x", "p1", "MAYBE_RELATED", "p2", OUTGOING, Some("{newProp: cacheR[k.since]}"))
        // Use cached property
      )
      .|.argument("p1", "k", "p2")
      .filter("cacheR[k.since] > 2020", "p2:Person")
      .remoteBatchProperties("cacheRFromStore[k.since]") // Cache property
      .expandAll("(p1)-[k:KNOWS]->(p2)")
      .filter("cacheN[p1.age] > 20")
      .remoteBatchProperties("cacheNFromStore[p1.age]")
      .nodeIndexOperator("p1:Person(firstName = 'J')", getValue = Map("firstName" -> DoNotGetValue))
      .build()
  }

  test("Should get value from index and use cached value in SET to set firstName and _key") {
    // Variant on Pokec_write WQ14
    val query = """MATCH (p:Person { firstName: $name })
                  |SET p = { firstName:p.firstName, _key:p.firstName }""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .setNodePropertiesFromMap(
        "p",
        "{firstName: cacheN[p.firstName], _key: cacheN[p.firstName]}",
        removeOtherProps = true
      )
      .nodeIndexOperator(
        "p:Person(firstName = ???)",
        paramExpr = Seq(parameter("name", CTAny)),
        getValue = Map("firstName" -> GetValue)
      )
      .build()
  }

  test(
    "Should get value from index and use cached value in SET to set p.firstName - should do a remoteBatchProperties for p.name and use the cached value in the SET to set p.description"
  ) {
    // Variant on QMUL write WQ13
    val query = """MATCH (p:Person { firstName: $fn })
                  |SET p = { firstName: p.firstName, description: 'Person: ' + p.name }
                  |RETURN p.description""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("cacheN[p.description] AS `p.description`")
      .remoteBatchProperties("cacheNFromStore[p.description]")
      .eager(ListSet(
        EagernessReason.UnknownPropertyReadSetConflict.withConflict(EagernessReason.Conflict(Id(4), Id(1)))
      ))
      .setNodePropertiesFromMap(
        "p",
        "{firstName: cacheN[p.firstName], description: 'Person: ' + cacheN[p.name]}",
        removeOtherProps = true
      ) // Use cached p.firstName and p.name
      .remoteBatchProperties("cacheNFromStore[p.name]") // Cache p.name
      .nodeIndexOperator(
        "p:Person(firstName = ???)",
        paramExpr = Seq(parameter("fn", CTAny)),
        getValue = Map("firstName" -> GetValue)
      ) // Cache p.firstName
      .build()
  }

  test("Should use cached value in SET to set p.hometown and p.description") {
    // Variant on QMUL write WQ13
    val query = """MATCH (p:Person { hometown: $hometown })
                  |SET p = { hometown: p.hometown, description: 'Person: ' + p.name }
                  |RETURN p.description""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("cacheN[p.description] AS `p.description`")
      .remoteBatchProperties("cacheNFromStore[p.description]")
      .setNodePropertiesFromMap(
        "p",
        "{hometown: cacheN[p.hometown], description: 'Person: ' + cacheN[p.name]}",
        removeOtherProps = true
      )
      .filter("cacheN[p.hometown] = $hometown")
      .remoteBatchProperties(
        "cacheNFromStore[p.hometown]",
        "cacheNFromStore[p.name]"
      ) // p.description seems unneeded here, since it will be fetched again just before the projection because the SET invalidates it.
      .nodeByLabelScan("p", "Person")
      .build()
  }

  test("Should use remoteBatchProperties for p.name and use cached values in SET to set p.name and p.description") {
    // Variant on QMUL write WQ19
    val query = """MATCH (p:Person)
                  |SET p = { name: p.name, description: 'Person: ' + p.name }
                  |RETURN p.description""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("cacheN[p.description] AS `p.description`")
      .remoteBatchProperties("cacheNFromStore[p.description]")
      .setNodePropertiesFromMap(
        "p",
        "{name: cacheN[p.name], description: 'Person: ' + cacheN[p.name]}",
        removeOtherProps = true
      )
      .remoteBatchProperties("cacheNFromStore[p.name]")
      .nodeByLabelScan("p", "Person")
      .build()
  }

  test("Should get value from index and use cached value in SET to set f.friendId") {
    // Variant on ldbc_ish_write_sf010 wq7
    val query = """MATCH (person:Person)-[:KNOWS]-(f) WHERE person.id IN $ListOfPersons
                  |SET f.friendId = person.id""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .setNodeProperty("f", "friendId", "cacheN[person.id]")
      .expandAll("(person)-[:KNOWS]-(f)")
      .nodeIndexOperator(
        "person:Person(id IN ???)",
        paramExpr = Seq(parameter("ListOfPersons", CTAny)),
        getValue = Map("id" -> GetValue),
        unique = true,
        supportPartitionedScan = false
      )
      .build()
  }

  test("Should RBP p.pr and use cached version in SET - SetRelationshipPropertyPattern") {
    val query = """MATCH (p:Person)-[f:KNOWS]->(e)
                  |SET f.prop = p.pr""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .setRelationshipProperty("f", "prop", "cacheN[p.pr]")
      .expandAll("(p)-[f:KNOWS]->()")
      .remoteBatchProperties("cacheNFromStore[p.pr]")
      .nodeByLabelScan("p", "Person")
      .build()
  }

  test(
    "Should RBPs p.pr and p.pr2 and use cached versions in SET - SetRelationshipPropertiesPattern"
  ) {
    val query = """MATCH (p:Person)-[f:KNOWS]->(e)
                  |SET f.prop = p.pr, f.prop2 = p.pr2""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .setRelationshipProperties("f", ("prop", "cacheN[p.pr]"), ("prop2", "cacheN[p.pr2]"))
      .expandAll("(p)-[f:KNOWS]->()")
      .remoteBatchProperties("cacheNFromStore[p.pr]", "cacheNFromStore[p.pr2]")
      .nodeByLabelScan("p", "Person")
      .build()
  }

  test(
    "Should RBPs p.pr and p.pr2 and use cached versions in SET - SetRelationshipPropertiesFromMapPattern"
  ) {
    val query = """MATCH (p:Person)-[f:KNOWS]->(e)
                  |SET f = {prop: p.pr, prop2: p.pr2}""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .setRelationshipPropertiesFromMap("f", "{prop: cacheN[p.pr], prop2: cacheN[p.pr2]}", removeOtherProps = true)
      .expandAll("(p)-[f:KNOWS]->()")
      .remoteBatchProperties("cacheNFromStore[p.pr]", "cacheNFromStore[p.pr2]")
      .nodeByLabelScan("p", "Person")
      .build()
  }

  test("Should RBP p.pr and use cached version in SET - SetNodePropertyPattern") {
    val query = """MATCH (p:Person)-[f:KNOWS]->(e)
                  |SET e.prop = p.pr""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .setNodeProperty("e", "prop", "cacheN[p.pr]")
      .expandAll("(p)-[:KNOWS]->(e)")
      .remoteBatchProperties("cacheNFromStore[p.pr]")
      .nodeByLabelScan("p", "Person")
      .build()
  }

  test(
    "Should RBPs p.pr and p.pr2 and use cached versions in SET - SetNodePropertiesPattern"
  ) {
    val query = """MATCH (p:Person)-[f:KNOWS]->(e)
                  |SET e.prop = p.pr, e.prop2 = p.pr2""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .setNodeProperties("e", ("prop", "cacheN[p.pr]"), ("prop2", "cacheN[p.pr2]"))
      .expandAll("(p)-[:KNOWS]->(e)")
      .remoteBatchProperties("cacheNFromStore[p.pr]", "cacheNFromStore[p.pr2]")
      .nodeByLabelScan("p", "Person")
      .build()
  }

  test(
    "Should RBPs p.pr and p.pr2 and use cached versions in SET - SetNodePropertiesFromMapPattern"
  ) {
    val query = """MATCH (p:Person)-[f:KNOWS]->(e)
                  |SET e = {prop: p.pr, prop2: p.pr2}""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .setNodePropertiesFromMap("e", "{prop: cacheN[p.pr], prop2: cacheN[p.pr2]}", removeOtherProps = true)
      .expandAll("(p)-[:KNOWS]->(e)")
      // Check if the position of the eager makes sense w.r.t. the remoteBatchProperties
      .remoteBatchProperties("cacheNFromStore[p.pr]", "cacheNFromStore[p.pr2]")
      .nodeByLabelScan("p", "Person")
      .build()
  }

  test("Should RBP p.pr and use cached version in SET - SetDynamicPropertyPattern") {
    val query = """MATCH (p:Person)-[f:KNOWS]->(e)
                  |SET f[$prop] = p.pr""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .setDynamicProperty("f", "$prop", "cacheN[p.pr]")
      .expandAll("(p)-[f:KNOWS]->()")
      .remoteBatchProperties("cacheNFromStore[p.pr]")
      .nodeByLabelScan("p", "Person")
      .build()
  }

  test("Should RBPs e.pr and use cached version in SET - SetPropertyPattern") {
    val query = """MATCH (p:Person)-[f:KNOWS]->(e)
                  |SET (CASE WHEN p.age < 20 THEN p ELSE f END).prop = e.pr""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .setProperty("CASE WHEN p.age < 20 THEN p ELSE f END", "prop", "cacheN[e.pr]")
      .remoteBatchProperties("cacheNFromStore[e.pr]")
      .expandAll("(p)-[f:KNOWS]->(e)")
      .nodeByLabelScan("p", "Person")
      .build()
  }

  test("Should RBPs e.pr and e.pr2 and use cached versions in SET - SetPropertiesPattern") {
    val query =
      """MATCH (p:Person)-[f:KNOWS]->(e)
        |SET (CASE WHEN p.age < 20 THEN p ELSE f END).prop = e.pr, (CASE WHEN p.age < 20 THEN p ELSE f END).prop2 = e.pr2""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .setProperties("CASE WHEN p.age < 20 THEN p ELSE f END", ("prop", "cacheN[e.pr]"), ("prop2", "cacheN[e.pr2]"))
      .remoteBatchProperties("cacheNFromStore[e.pr]", "cacheNFromStore[e.pr2]")
      .expandAll("(p)-[f:KNOWS]->(e)")
      .nodeByLabelScan("p", "Person")
      .build()
  }

  test("Should RBPs e.pr and e.pr2 and use cached versions in SET - SetPropertiesFromMapPattern") {
    val query = """CALL () {
                  |  MATCH (p:Person)
                  |  RETURN p AS x
                  |UNION ALL
                  |  MATCH (p:Person)-[f:KNOWS]->(e)
                  |  RETURN f AS x
                  |} // x can be a node or a relationship
                  |MATCH (e:Person) WHERE e.id < 12
                  |SET x += {prop: e.pr, prop2: e.pr2}""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      // use the cached properties
      .setPropertiesFromMap("x", "{prop: cacheN[e.pr], prop2: cacheN[e.pr2]}", removeOtherProps = false)
      .remoteBatchProperties("cacheNFromStore[e.pr]", "cacheNFromStore[e.pr2]") // cache e.pr and e.pr2
      .apply()
      .|.nodeIndexOperator("e:Person(id < 12)", argumentIds = Set("x"), unique = true)
      .union()
      .|.projection("x AS x")
      .|.projection("f AS x")
      .|.expandAll("(p)-[f:KNOWS]->()")
      .|.nodeByLabelScan("p", "Person")
      .projection("x AS x")
      .projection("p AS x")
      .nodeByLabelScan("p", "Person")
      .build()
  }

  test("Should RBP p.pr and use cached version in SET - SetLabelPattern") {
    val query = """MATCH (p:Person)-[f:KNOWS]->(e)
                  |SET e:$(p.pr)""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .setLabels("e", Seq(), Seq("cacheN[p.pr]"))
      .expandAll("(p)-[:KNOWS]->(e)")
      .remoteBatchProperties("cacheNFromStore[p.pr]")
      .nodeByLabelScan("p", "Person")
      .build()
  }

  test("Should RBP p.pr and use cached version in SET - RemoveLabelPattern") {
    val query = """MATCH (p:Person)-[f:KNOWS]->(e)
                  |REMOVE e:$(p.pr)""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .removeLabels("e", Seq(), Seq("cacheN[p.pr]"))
      .expandAll("(p)-[:KNOWS]->(e)")
      .remoteBatchProperties("cacheNFromStore[p.pr]")
      .nodeByLabelScan("p", "Person")
      .build()
  }

  test("Should RBP p.pr and use cached version in CREATE") {
    val query = """MATCH (p:Person)
                  |CREATE (:Dummy {prop: p.pr})""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .create(createNodeFull("anon_0", labels = Seq("Dummy"), properties = Some("{prop: cacheN[p.pr]}")))
      .remoteBatchProperties("cacheNFromStore[p.pr]")
      .nodeByLabelScan("p", "Person")
      .build()
  }

  test("Should not plan another RBP after the CREATE") {
    val query = """MATCH (p:Person)
                  |CREATE (:Dummy {prop: p.pr})
                  |RETURN p.pr""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("cacheN[p.pr] AS `p.pr`")
      .create(createNodeFull("anon_0", labels = Seq("Dummy"), properties = Some("{prop: cacheN[p.pr]}")))
      .remoteBatchProperties("cacheNFromStore[p.pr]")
      .nodeByLabelScan("p", "Person")
      .build()
  }

  test("Should not plan another RBP after the setLabel") {
    val query = """MATCH (p:Person {someProp: 1})
                  |SET p:Dummy
                  |RETURN p.someProp""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("cacheN[p.someProp] AS `p.someProp`")
      .setLabels("p", Seq("Dummy"), Seq())
      .filter("cacheN[p.someProp] = 1")
      .remoteBatchProperties("cacheNFromStore[p.someProp]")
      .nodeByLabelScan("p", "Person")
      .build()
  }

  test("Should RBP p.pr and use cached version in CREATE - Eager") {
    val query = """MATCH (p:Person)-[:KNOWS]->()
                  |CREATE (:Person {prop: p.pr})""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .create(createNodeFull("anon_0", labels = Seq("Person"), properties = Some("{prop: cacheN[p.pr]}")))
      .eager(ListSet(
        EagernessReason
          .LabelReadSetConflict(LabelName("Person")(InputPosition.NONE))
          .withConflict(EagernessReason.Conflict(Id(2), Id(4)))
      ))
      .expandAll("(p)-[:KNOWS]->()")
      .remoteBatchProperties("cacheNFromStore[p.pr]")
      .nodeByLabelScan("p", "Person")
      .build()
  }

  test("MutatingPattern in tail - Should RBP p.pr and use cached version in CREATE - Eager") {
    val query = """UNWIND [1] as one
                  |MATCH (p:Person)
                  |CREATE (:Person {prop: p.pr})""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .create(createNodeFull("anon_0", labels = Seq("Person"), properties = Some("{prop: cacheN[p.pr]}")))
      .remoteBatchProperties("cacheNFromStore[p.pr]")
      .eager(ListSet(
        EagernessReason
          .LabelReadSetConflict(LabelName("Person")(InputPosition.NONE))
          .withConflict(EagernessReason.Conflict(Id(2), Id(6)))
      ))
      .apply()
      .|.nodeByLabelScan("p", "Person")
      .unwind("[1] AS _")
      .argument()
      .build()
  }

  test("Should rbps p.age and p.propList and use cached version - ForeachPattern") {
    val query = """MATCH (p:Person)
                  |FOREACH (value IN p.propList | CREATE (:Item {name: value, price: 100 - p.age}))
                  |""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .foreach(
        "value",
        "cacheN[p.propList]", // Use cached p.propList
        Seq(createPattern(Seq(createNodeFull(
          "anon_0",
          labels = Seq("Item"),
          properties = Some("{name: value, price: 100 - cacheN[p.age]}") // Use cached p.age
        ))))
      )
      .remoteBatchProperties("cacheNFromStore[p.propList]", "cacheNFromStore[p.age]") // Cache p.age and p.propList
      .nodeByLabelScan("p", "Person")
      .build()
  }

  test("Should rbps p.prop and p.propList and use cached version - ForeachPattern - ForeachApply") {
    val query = """MATCH (p:Person)
                  |FOREACH (
                  |  value IN p.propList |
                  |  CREATE (:Item
                  |    {
                  |      name: value,
                  |      price: COUNT { MATCH (x:Person) WHERE x.prop < p.prop }
                  |    }
                  |  )
                  |)
                  |""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .foreachApply("value", "cacheN[p.propList]") // Use cached p.propList
      .|.create(createNodeFull("anon_0", labels = Seq("Item"), properties = Some("{name: value, price: anon_1}")))
      .|.apply()
      .|.|.aggregation(Seq(), Seq("count(*) AS anon_1"))
      .|.|.filter("cacheN[x.prop] < cacheN[p.prop]") // Use cached p.prop
      .|.|.remoteBatchProperties("cacheNFromStore[x.prop]")
      .|.|.nodeByLabelScan("x", "Person", "p")
      .|.argument("p", "value")
      .remoteBatchProperties("cacheNFromStore[p.propList]", "cacheNFromStore[p.prop]") // Cache p.propList and p.prop
      .nodeByLabelScan("p", "Person")
      .build()
  }

  test("subqueryForeach (MutatingPattern in horizon) - should rbp p.name and use cached version") {
    val query = """MATCH (p:Person)
                  |CALL (p) {
                  |  CREATE (a: Artist {name: p.name})
                  |}""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .subqueryForeach()
      .|.create(createNodeFull("a", labels = Seq("Artist"), properties = Some("{name: cacheN[p.name]}")))
      .|.argument("p")
      .remoteBatchProperties("cacheNFromStore[p.name]")
      .nodeByLabelScan("p", "Person")
      .build()
  }

  test("TransactionForeach (MutatingPattern in horizon) - should rbp p.name and use cached version") {
    val query = """MATCH (p:Person)
                  |CALL (p) {
                  |  CREATE (a: Artist {name: p.name})
                  |} IN TRANSACTIONS OF 100 ROWS""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .transactionForeach(100)
      .|.create(createNodeFull("a", labels = Seq("Artist"), properties = Some("{name: cacheN[p.name]}")))
      .|.argument("p")
      .remoteBatchProperties("cacheNFromStore[p.name]")
      .nodeByLabelScan("p", "Person")
      .build()
  }

  test("Keep the sort when required - even when extra properties are fetched") {
    val query = """MATCH (n)
                  |ORDER BY n
                  |SET n.prop2 = n.prop
                  |RETURN n""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .setNodeProperty("n", "prop2", "cacheN[n.prop]")
      .remoteBatchProperties("cacheNFromStore[n.prop]")
      .sort("n ASC")
      .allNodeScan("n")
      .build()
  }

  test("Do not rbp a.age when a is an argument") {
    // Variant of pokec_write WQ24
    val query = """MATCH (n:Person)
                  |WHERE n.AGE IS NOT NULL
                  |  WITH n
                  |  LIMIT 50000
                  |WITH n.AGE AS age, collect(n) AS nodes
                  |MERGE (a:Country { age: age })
                  |WITH a, nodes
                  |FOREACH (n IN nodes | MERGE (a)<-[:KNOWS]-(n))""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .foreachApply("n", "nodes")
      // .|.remoteBatchProperties("cacheNFromStore[a.age]") <- When `planRemoteBatchPropertiesWithLookahead` plans RBPs for properties on the query graph's arguments
      .|.mergeInto("(a)<-[anon_0:KNOWS]-(n)")
      .|.argument("a", "n")
      .apply()
      .|.merge(Seq(createNodeFull("a", labels = Seq("Country"), properties = Some("{age: age}"))))
      .|.filter("cacheN[a.age] = age")
      .|.remoteBatchProperties("cacheNFromStore[a.age]")
      .|.nodeByLabelScan("a", "Country", "age")
      .aggregation(Seq("cacheN[n.AGE] AS age"), Seq("collect(n) AS nodes"))
      .limit(50000)
      .filter("cacheN[n.AGE] IS NOT NULL")
      .remoteBatchProperties("cacheNFromStore[n.AGE]")
      .nodeByLabelScan("n", "Person")
      .build()
  }

  test("RBP should not be the cause of an eager") {
    val query = """UNWIND [1] AS one
                  |MATCH (p1:Person)-[KNOWS]->(:Person)
                  |SET p1.val = p1.val + 1""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .setNodeProperty("p1", "val", "cacheN[p1.val] + 1")
      .filter("anon_0:Person")
      .expandAll("(p1)-[]->(anon_0)")
      // Should not plan an eager between the SET and the RBP
      //  .eager(ListSet(EagernessReason.PropertyReadSetConflict(PropertyKeyName("val")(InputPosition.NONE)).withConflict(EagernessReason.Conflict(Id(2), Id(6)))))
      .remoteBatchProperties("cacheNFromStore[p1.val]")
      .apply()
      .|.nodeByLabelScan("p1", "Person")
      .unwind("[1] AS _")
      .argument()
      .build()
  }

  test("Should not fetch the property 'name' before SetNodeProperty when it is only needed after the SET") {
    val query = """MATCH (p:Person {age: 20})
                  |SET p.name = 1
                  |RETURN p.name""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual
      planner.subPlanBuilder()
        .projection("cacheN[p.name] AS `p.name`")
        .remoteBatchProperties("cacheNFromStore[p.name]")
        .setNodeProperty("p", "name", "1")
        .filter("cacheN[p.age] = 20")
        .remoteBatchProperties("cacheNFromStore[p.age]") // Should not fetch p.name here too
        .nodeByLabelScan("p", "Person")
        .build()
  }

  test("Should not fetch the property 'name' before FOREACH when it is only needed after the FOREACH") {
    val query = """MATCH p=(:Person)-[:KNOWS*]->(), (x:Person {age: 20})
                  |FOREACH (n IN nodes(p) | SET n.marked = true)
                  |RETURN x.name""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual
      planner.subPlanBuilder()
        .projection("cacheN[x.name] AS `x.name`")
        .remoteBatchProperties("cacheNFromStore[x.name]")
        .foreach(
          "n",
          nodes(PathExpressionBuilder.node("anon_0").outToVarLength("anon_1", "anon_2").build()),
          Seq(setNodeProperty("n", "marked", "true"))
        )
        .cartesianProduct()
        .|.expand("(anon_0)-[anon_1:KNOWS*]->(anon_2)")
        .|.nodeByLabelScan("anon_0", "Person")
        .filter("cacheN[x.age] = 20")
        .remoteBatchProperties("cacheNFromStore[x.age]") // Should not fetch x.name here too
        .nodeByLabelScan("x", "Person")
        .build()
  }

  test("Should not fetch the property 'name' before and after the MERGE when it is only needed after the MERGE") {
    val query = """MATCH (x:Person {age: 20})
                  |MERGE (x)-[:KNOWS]->(:Person {id: 5})
                  |RETURN x.name""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual
      planner.subPlanBuilder()
        .projection("cacheN[x.name] AS `x.name`")
        .apply()
        .|.merge(
          Seq(createNodeFull("anon_1", labels = Seq("Person"), properties = Some("{id: 5}"))),
          Seq(createRelationship("anon_0", "x", "KNOWS", "anon_1", OUTGOING)),
          lockNodes = Set("x")
        )
        .|.expandInto("(x)-[anon_0:KNOWS]->(anon_1)")
        .|.nodeIndexOperator(
          "anon_1:Person(id = 5)",
          argumentIds = Set("x"),
          unique = true
        )
        .filter("cacheN[x.age] = 20")
        .remoteBatchProperties("cacheNFromStore[x.age]", "cacheNFromStore[x.name]")
        .nodeByLabelScan("x", "Person")
        .build()
  }

  test(
    "Should not fetch the property 'name' before the MERGE with a SET in the onMatch when it is only needed after the MERGE"
  ) {
    val query = """MATCH (x:Person {age: 20})
                  |MERGE (x)-[:KNOWS]->(:Person {id: 5}) ON MATCH SET x.knewPerson5 = true
                  |RETURN x.name""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual
      planner.subPlanBuilder()
        .projection("cacheN[x.name] AS `x.name`")
        .remoteBatchProperties("cacheNFromStore[x.name]")
        .apply()
        .|.merge(
          Seq(createNodeFull("anon_1", labels = Seq("Person"), properties = Some("{id: 5}"))),
          Seq(createRelationship("anon_0", "x", "KNOWS", "anon_1", OUTGOING)),
          Seq(setNodeProperty("x", "knewPerson5", "true")),
          Seq(),
          Set("x")
        )
        .|.expandInto("(x)-[anon_0:KNOWS]->(anon_1)")
        .|.nodeIndexOperator(
          "anon_1:Person(id = 5)",
          argumentIds = Set("x"),
          unique = true
        )
        .filter("cacheN[x.age] = 20")
        .remoteBatchProperties(
          "cacheNFromStore[x.age]"
        ) // Should not fetch x.name here, since the merge invalidates them, and they are not needed before the merge
        .nodeByLabelScan("x", "Person")
        .build()
  }

  test("Should rbps p.age, p.birthPlace, p.cnt and c.cnt and use cached version - MergeNodePattern") {
    val query = """MATCH (p:Person {age: 25})
                  |MERGE (c:City {name: p.birthPlace})
                  |  ON CREATE SET c.cnt = p.cnt
                  |  ON MATCH SET c.cnt = c.cnt + p.cnt
                  |""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .apply()
      .|.merge(
        Seq(createNodeFull("c", labels = Seq("City"), properties = Some("{name: cacheN[p.birthPlace]}"))),
        Seq(),
        Seq(setNodeProperty("c", "cnt", "cacheN[c.cnt] + cacheN[p.cnt]")),
        Seq(setNodeProperty("c", "cnt", "cacheN[p.cnt]")),
        Set()
      )
      .|.remoteBatchProperties("cacheNFromStore[c.cnt]")
      .|.nodeIndexOperator(
        "c:City(name = cacheN[p.birthPlace])",
        argumentIds = Set("p"),
        getValue = Map("name" -> DoNotGetValue)
      )
      .filter("cacheN[p.age] = 25")
      .remoteBatchProperties("cacheNFromStore[p.age]", "cacheNFromStore[p.cnt]", "cacheNFromStore[p.birthPlace]")
      .nodeByLabelScan("p", "Person")
      .build()
  }

  test(
    "Should rbps p.age, p.birthPlace, p.cnt, c.cnt and c.nameNoIndex (no index usage) and use cached version - MergeNodePattern"
  ) {
    val query = """MATCH (p:Person {age: 25})
                  |MERGE (c:City {nameNoIndex: p.birthPlace})
                  |  ON CREATE SET c.cnt = p.cnt
                  |  ON MATCH SET c.cnt = c.cnt + p.cnt
                  |""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .apply()
      .|.merge(
        Seq(createNodeFull("c", labels = Seq("City"), properties = Some("{nameNoIndex: cacheN[p.birthPlace]}"))),
        Seq(),
        Seq(setNodeProperty("c", "cnt", "cacheN[c.cnt] + cacheN[p.cnt]")),
        Seq(setNodeProperty("c", "cnt", "cacheN[p.cnt]")),
        Set()
      )
      .|.filter("cacheN[c.nameNoIndex] = cacheN[p.birthPlace]")
      .|.remoteBatchProperties("cacheNFromStore[c.nameNoIndex]", "cacheNFromStore[c.cnt]")
      .|.nodeByLabelScan("c", "City", "p")
      .filter("cacheN[p.age] = 25")
      .remoteBatchProperties("cacheNFromStore[p.age]", "cacheNFromStore[p.cnt]", "cacheNFromStore[p.birthPlace]")
      .nodeByLabelScan("p", "Person")
      .build()
  }

  test(
    "Should invalidate cached properties due to SET in ON MATCH in a MergeNodePattern"
  ) {
    val query = """MATCH (p:Person {age: 25, cnt: 0})
                  |MERGE (c:City {name: p.birthPlace})
                  |  ON MATCH SET p.cnt = p.cnt + 1
                  |RETURN p.cnt
                  |""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("cacheN[p.cnt] AS `p.cnt`")
      .remoteBatchProperties(
        "cacheNFromStore[p.cnt]"
      ) // cached property p.cnt was invalidated by the SET, so we need to fetch them again
      .eager(ListSet(EagernessReason.PropertyReadSetConflict(
        PropertyKeyName("cnt")(InputPosition.NONE)
      ).withConflict(EagernessReason.Conflict(Id(5), Id(1)))))
      .apply()
      .|.merge(
        Seq(createNodeFull("c", labels = Seq("City"), properties = Some("{name: cacheN[p.birthPlace]}"))),
        Seq(),
        Seq(setNodeProperty("p", "cnt", "cacheN[p.cnt] + 1")),
        Seq(),
        Set()
      )
      .|.nodeIndexOperator("c:City(name = cacheN[p.birthPlace])", argumentIds = Set("p"))
      .filter("cacheN[p.age] = 25", "cacheN[p.cnt] = 0")
      .remoteBatchProperties("cacheNFromStore[p.age]", "cacheNFromStore[p.cnt]", "cacheNFromStore[p.birthPlace]")
      .nodeByLabelScan("p", "Person")
      .build()
  }

  test(
    "Should invalidate cached properties due to SET in ON CREATE in a MergeNodePattern"
  ) {
    val query = """MATCH (p:Person {age: 25, cnt: 0})
                  |MERGE (c:City {name: p.birthPlace})
                  |  ON CREATE SET p.cnt = p.cnt + 1
                  |RETURN p.cnt
                  |""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("cacheN[p.cnt] AS `p.cnt`")
      .remoteBatchProperties(
        "cacheNFromStore[p.cnt]"
      ) // cached property p.cnt was invalidated by the SET, so we need to fetch them again
      .eager(ListSet(EagernessReason.PropertyReadSetConflict(
        PropertyKeyName("cnt")(InputPosition.NONE)
      ).withConflict(EagernessReason.Conflict(Id(5), Id(1)))))
      .apply()
      .|.merge(
        Seq(createNodeFull("c", labels = Seq("City"), properties = Some("{name: cacheN[p.birthPlace]}"))),
        Seq(),
        Seq(),
        Seq(setNodeProperty("p", "cnt", "cacheN[p.cnt] + 1")),
        Set()
      )
      .|.nodeIndexOperator("c:City(name = cacheN[p.birthPlace])", argumentIds = Set("p"))
      .filter("cacheN[p.age] = 25", "cacheN[p.cnt] = 0")
      .remoteBatchProperties("cacheNFromStore[p.age]", "cacheNFromStore[p.cnt]", "cacheNFromStore[p.birthPlace]")
      .nodeByLabelScan("p", "Person")
      .build()
  }

  test(
    "Should rbps p.age, p.prop, p.birthPlace, p.cnt, r.pr and c.cnt and use cached version - MergeRelationshipPattern"
  ) {
    val query = """MATCH (p:Person {age: 25})
                  |MERGE (p)-[r:IS_LOCATED_IN {pr: p.prop}]->(c:City {name: p.birthPlace})
                  |  ON CREATE SET c.cnt = p.cnt
                  |  ON MATCH SET c.cnt = c.cnt + p.cnt
                  |""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .apply()
      .|.merge(
        Seq(createNodeFull("c", labels = Seq("City"), properties = Some("{name: cacheN[p.birthPlace]}"))),
        Seq(createRelationship("r", "p", "IS_LOCATED_IN", "c", OUTGOING, Some("{pr: cacheN[p.prop]}"))),
        Seq(setNodeProperty("c", "cnt", "cacheN[c.cnt] + cacheN[p.cnt]")),
        Seq(setNodeProperty("c", "cnt", "cacheN[p.cnt]")),
        Set("p")
      )
      .|.remoteBatchProperties("cacheNFromStore[c.cnt]")
      .|.filter("cacheR[r.pr] = cacheN[p.prop]")
      .|.remoteBatchProperties("cacheRFromStore[r.pr]")
      .|.expandInto("(p)-[r:IS_LOCATED_IN]->(c)")
      .|.nodeIndexOperator(
        "c:City(name = cacheN[p.birthPlace])",
        argumentIds = Set("p"),
        getValue = Map("name" -> DoNotGetValue)
      )
      .filter("cacheN[p.age] = 25")
      .remoteBatchProperties(
        "cacheNFromStore[p.birthPlace]",
        "cacheNFromStore[p.prop]",
        "cacheNFromStore[p.cnt]",
        "cacheNFromStore[p.age]"
      )
      .nodeByLabelScan("p", "Person")
      .build()
  }

  test("should project between set properties and sort") {
    // case: mutating pattern, followed by a sort that uses the mutated property
    // we might be tempted to push the sort below the set property,
    // but that would mean we need to project and sort the property again
    val query =
      """MATCH (u:Person)-[f:KNOWS]->(p:Person)
        |  WHERE u.start = true
        |SET u.name = 'joe'
        |RETURN u.name, u.foo
        |  ORDER BY u.name ASC, u.foo DESC""".stripMargin

    planner.plan(query) should equal(
      planner.planBuilder()
        .produceResults("`u.name`", "`u.foo`")
        .sort("`u.name` ASC", "`u.foo` DESC")
        // this projection is crucial after the setNodeProperty, so that `u.name` is up to date again and can be sorted on and be returned
        .projection("cacheN[u.name] AS `u.name`", "cacheN[u.foo] AS `u.foo`")
        .remoteBatchProperties("cacheNFromStore[u.name]", "cacheNFromStore[u.foo]")
        .eager(ListSet(
          propReadSetConflict("name", 5, 2)
        ))
        .setNodeProperty("u", "name", "'joe'")
        .filter("p:Person")
        .expandAll("(u)-[:KNOWS]->(p)")
        .filter("cacheN[u.start] = true")
        .remoteBatchProperties("cacheNFromStore[u.start]")
        .nodeByLabelScan("u", "Person", IndexOrderNone)
        .build()
    )
  }
}
