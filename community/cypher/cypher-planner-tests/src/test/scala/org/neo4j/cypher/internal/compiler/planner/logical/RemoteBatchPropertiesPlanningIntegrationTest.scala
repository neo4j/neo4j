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
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.CypherVersionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.compiler.ExecutionModel
import org.neo4j.cypher.internal.compiler.ExecutionModel.BatchedParallel
import org.neo4j.cypher.internal.compiler.planner.AttributeComparisonStrategy.ComparingProvidedAttributesOnly
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanConstructionTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningAttributesTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.CardinalityIntegrationTestSupport
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.ir.SelectivePathPattern.CountInteger
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.Predicate
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.TrailParameters
import org.neo4j.cypher.internal.logical.builder.TestNFABuilder
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandInto
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.IndexOrderDescending
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath
import org.neo4j.cypher.internal.planner.spi.DatabaseMode
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.UpperBound.Unlimited
import org.neo4j.cypher.internal.util.symbols.CTAny

class RemoteBatchPropertiesPlanningIntegrationTest
    extends AbstractRemoteBatchPropertiesPlanningIntegrationTest(ExecutionModel.default)
    with CypherVersionTestSupport
    with LogicalPlanningAttributesTestSupport {

  override protected val orderPreserving: Boolean = true

  test("ORDER BY renamed column old name in WITH and project and return that column") {
    val planner =
      spdPlanner
        .setAllNodesCardinality(1000)
        .setLabelCardinality("A", 100)
        .build()

    val query =
      """MATCH (a:A)
        |WITH a, a.name AS name
        |WITH name AS b, a.age AS age
        |ORDER BY name
        |RETURN b, age""".stripMargin
    val plan = planner.plan(query)

    plan should equal(
      planner.planBuilder().produceResults("b", "age")
        .projection("name AS b", "cacheN[a.age] AS age")
        .sort("name ASC")
        .projection("cacheN[a.name] AS name")
        .remoteBatchProperties("cacheNFromStore[a.name]", "cacheNFromStore[a.age]")
        .nodeByLabelScan("a", "A", IndexOrderNone)
        .build()
    )
  }

  test("should batch properties for ordered aggregations") {
    val query =
      """MATCH (person)
        |WITH person ORDER BY person.age
        |RETURN person.name, person.age, count(person.age)""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("`person.name`", "`person.age`", "`count(person.age)`")
      .orderedAggregation(
        Seq("cacheN[person.name] AS `person.name`", "cacheN[person.age] AS `person.age`"),
        Seq("count(cacheN[person.age]) AS `count(person.age)`"),
        Seq("cacheN[person.age]")
      )
      .sort("`person.age` ASC")
      .projection("cacheN[person.age] AS `person.age`")
      .remoteBatchProperties("cacheNFromStore[person.age]", "cacheNFromStore[person.name]")
      .allNodeScan("person")
      .build()
  }

  test("should propagate cached properties for an ordered distinct projection") {
    val query =
      """
        |MATCH (n:Person)
        |WHERE n.firstName = 'foo'
        |RETURN DISTINCT n.firstName""".stripMargin
    val plan = planner.plan(query).stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .orderedDistinct(Seq("cacheN[n.firstName]"), "cacheN[n.firstName] AS `n.firstName`")
      .nodeIndexOperator(
        "n:Person(firstName = 'foo')",
        indexOrder = IndexOrderAscending,
        getValue = Map("firstName" -> GetValue)
      )
      .build())
  }

  test("should batch properties when finding triadic selections") {
    val query =
      """
        |MATCH (n:Person {firstName: "Dave"})-[:KNOWS]-(friend:Person)-[:KNOWS]-(friendOfFriend:Person)
        |WHERE NOT (n)-[:KNOWS]-(friendOfFriend)
        |RETURN n. firstName, n.lastName, friendOfFriend.firstName, friendOfFriend.lastName""".stripMargin
    val plan = planner.plan(query).stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection(
        "cacheN[n.firstName] AS `n. firstName`",
        "cacheN[n.lastName] AS `n.lastName`",
        "cacheN[friendOfFriend.firstName] AS `friendOfFriend.firstName`",
        "cacheN[friendOfFriend.lastName] AS `friendOfFriend.lastName`"
      )
      .remoteBatchProperties(
        "cacheNFromStore[friendOfFriend.firstName]",
        "cacheNFromStore[friendOfFriend.lastName]"
      )
      .filter("NOT anon_1 = anon_0", "friendOfFriend:Person")
      .triadicSelection(positivePredicate = false, "n", "friend", "friendOfFriend")
      .|.expandAll("(friend)-[anon_1:KNOWS]-(friendOfFriend)")
      .|.argument("friend", "anon_0")
      .filter("friend:Person")
      .expandAll("(n)-[anon_0:KNOWS]-(friend)")
      .remoteBatchProperties("cacheNFromStore[n.lastName]")
      .nodeIndexOperator("n:Person(firstName = 'Dave')", getValue = Map("firstName" -> GetValue))
      .build())
  }

  test("should leverage order for max queries instead of using aggregation") { // index_backed_order_by:q23
    val query =
      """
        |MATCH (msg:Message)
        |WHERE msg.creationDate > $min_creation_date
        |RETURN max(msg.creationDate)
        |""".stripMargin

    planner.plan(query).stripProduceResults shouldEqual planner
      .subPlanBuilder()
      .optional()
      .limit(1)
      .projection("cacheN[msg.creationDate] AS `max(msg.creationDate)`")
      .nodeIndexOperator(
        "msg:Message(creationDate > ???)",
        paramExpr = Some(parameter("min_creation_date", CTAny)),
        getValue = Map("creationDate" -> GetValue),
        indexOrder = IndexOrderDescending
      )
      .build()
  }

  test("should leverage order for min queries instead of using aggregation") {
    val query =
      """
        |MATCH (msg:Message)
        |WHERE msg.creationDate > $min_creation_date
        |RETURN min(msg.creationDate)
        |""".stripMargin

    planner.plan(query).stripProduceResults shouldEqual planner
      .subPlanBuilder()
      .optional()
      .limit(1)
      .projection("cacheN[msg.creationDate] AS `min(msg.creationDate)`")
      .nodeIndexOperator(
        "msg:Message(creationDate > ???)",
        paramExpr = Some(parameter("min_creation_date", CTAny)),
        getValue = Map("creationDate" -> GetValue),
        indexOrder = IndexOrderAscending
      )
      .build()
  }

  test("should use index seek to find minimum even on renames") {
    val query =
      """
        |MATCH (n: Person)
        |WITH n AS m
        |RETURN min(m.age)
        |""".stripMargin

    val customIndexPlanner = spdPlanner
      .setAllNodesCardinality(10000)
      .setLabelCardinality("Person", 10000)
      .addNodeIndex(
        "Person",
        List("age"),
        existsSelectivity = 1.0,
        uniqueSelectivity = 0.25
      )
      .build()

    val plan = customIndexPlanner.plan(query).stripProduceResults
    plan shouldEqual customIndexPlanner.subPlanBuilder()
      .optional()
      .limit(1)
      .projection(Map("min(m.age)" -> cachedNodeProp("n", "age", "m")))
      .projection("n AS m")
      .nodeIndexOperator("n:Person(age)", indexOrder = IndexOrderAscending, getValue = Map("age" -> GetValue))
      .build()
  }

  test("Should be able to handle ExistsIRExpressions") {
    val query =
      """
        |MATCH (person:Person)
        |WITH DISTINCT person
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |  WHERE person.firstName = dog.name
        |}
        |RETURN person.firstName as name
        |""".stripMargin

    planner.plan(query) should
      equal(planner
        .planBuilder()
        .produceResults("name")
        .projection("cacheN[person.firstName] AS name")
        .semiApply()
        .|.remoteBatchPropertiesWithFilter("cacheNFromStore[dog.name]")("cacheN[person.firstName] = dog.name")
        .|.filter("dog:Dog")
        .|.expandAll("(person)-[:HAS_DOG]->(dog)")
        .|.argument("person")
        .remoteBatchProperties("cacheNFromStore[person.firstName]")
        .nodeByLabelScan("person", "Person", IndexOrderAscending)
        .build())
  }

  test("pushing limit below remoteBatchProperties should preserve interesting order from node index scan") {
    val query =
      """
        |MATCH (p:Person)-[r]->(x)
        |WHERE p.id IS NOT NULL
        |ORDER BY p.id ASC
        |LIMIT 1
        |UNWIND range(1, 100) AS increaseCardinality
        |RETURN x.prop AS prop
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("cacheN[x.prop] AS prop")
      .unwind("range(1, 100) AS _")
      .remoteBatchProperties("cacheNFromStore[x.prop]")
      .limit(1)
      .expandAll("(p)-[]->(x)")
      .nodeIndexOperator("p:Person(id)", Map("id" -> GetValue), indexOrder = IndexOrderAscending)
      .build()
  }

  test("pushing limit below remoteBatchProperties should preserve interesting order from relationship index scan") {
    val query =
      """
        |MATCH (comment)-[r:COMMENT_HAS_CREATOR]->(creator)
        |WHERE r.id IS NOT NULL
        |ORDER BY r.id ASC LIMIT 1
        |UNWIND range(1, 100) AS increaseCardinality
        |RETURN creator.prop AS prop
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("cacheN[creator.prop] AS prop")
      .unwind("range(1, 100) AS _")
      .remoteBatchProperties("cacheNFromStore[creator.prop]")
      .limit(1)
      .relationshipIndexOperator(
        "()-[:COMMENT_HAS_CREATOR(id)]->(creator)",
        getValue = Map("id" -> GetValue),
        indexOrder = IndexOrderAscending
      )
      .build()
  }

  test("should prefer merge join over hash join for node property equality") {
    val planner = spdPlanner
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 50)
      .setLabelCardinality("B", 75)
      .addNodeIndex("A", Seq("prop"), 1.0, 0.8)
      .addNodeIndex("B", Seq("prop"), 1.0, 0.8)
      .withSetting(GraphDatabaseInternalSettings.planning_merge_join_enabled, Boolean.box(true))
      .build()

    val plan = planner.plan("MATCH (a:A), (b:B) WHERE a.prop = b.prop RETURN a, b").stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .valueMergeJoin("cacheN[a.prop] = cacheN[b.prop]")
      .|.nodeIndexOperator("b:B(prop)", indexOrder = IndexOrderAscending, getValue = Map("prop" -> GetValue))
      .nodeIndexOperator("a:A(prop)", indexOrder = IndexOrderAscending, getValue = Map("prop" -> GetValue))
      .build()
  }

  test("should prefer nested index join over value hash join if the cardinality of LHS is large") {
    val planner = spdPlanner
      .setAllNodesCardinality(100_000_000)
      .setLabelCardinality("A", 50_000_000)
      .setLabelCardinality("B", 90_000_000)
      .addNodeIndex("A", Seq("prop"), 1.0, 1.0 / 5000, isUnique = true)
      .addNodeIndex("B", Seq("prop"), 1.0, 1.0)
      .build()

    val query =
      """MATCH (a:A), (b:B)
        |WHERE a.prop = b.prop
        |RETURN a, b""".stripMargin

    val plan = planner.planState(query)
    plan should haveSamePlanAndCardinalitiesAsBuilder(
      planner.subPlanBuilder()
        .produceResults("a", "b")
        .apply()
        .|.remoteNodeIndexOperator(
          "b:B(prop = cacheN[a.prop])",
          indexOrder = IndexOrderNone,
          getValue = Map("prop" -> DoNotGetValue),
          argumentIds = Set("a")
        )
        .nodeIndexOperator(
          "a:A(prop)",
          indexOrder = IndexOrderNone,
          getValue = Map("prop" -> GetValue)
        ).withCardinality(50_000_000),
      ComparingProvidedAttributesOnly
    )
  }

  test(
    "should prefer nested index join over value merge join if the cardinality of RHS is very large and LHS is small"
  ) {
    val cardA = 1_000
    val existsSelectivityA = 0.01
    val planner = spdPlanner
      .setAllNodesCardinality(100_000_000)
      // Fewer A nodes, so this will be the LHS
      .setLabelCardinality("A", cardA)
      .setLabelCardinality("B", 1_000_000)
      .addNodeIndex("A", Seq("prop"), existsSelectivityA, 0.001, isUnique = true)
      .addNodeIndex("B", Seq("prop"), 1.0, 1.0)
      .withSetting(GraphDatabaseInternalSettings.planning_merge_join_enabled, Boolean.box(true))
      .build()

    val query =
      """MATCH (a:A), (b:B)
        |WHERE a.prop = b.prop
        |RETURN a, b""".stripMargin

    planner.planState(query) should haveSamePlanAndCardinalitiesAsBuilder(
      planner.planBuilder()
        .produceResults("a", "b")
        .apply()
        .|.remoteNodeIndexOperator(
          "b:B(prop = cacheN[a.prop])",
          indexOrder = IndexOrderAscending,
          getValue = Map("prop" -> GetValue),
          argumentIds = Set("a")
        ).withCardinality(10)
        .nodeIndexOperator(
          "a:A(prop)",
          indexOrder = IndexOrderAscending,
          getValue = Map("prop" -> GetValue)
        ).withCardinality(cardA * existsSelectivityA), // = 10
      ComparingProvidedAttributesOnly
    )
  }

  test("should prefer merge join over nested applies for node property equality for very large LHS") {
    val cardA = 10_000
    val existsSelectivityA = 1.0
    val planner = spdPlanner
      .setAllNodesCardinality(100_000_000)
      .setLabelCardinality("A", cardA)
      .setLabelCardinality("B", 100_000)
      .addNodeIndex("A", Seq("prop"), existsSelectivityA, 1.0 / 5000, isUnique = true)
      .addNodeIndex("B", Seq("prop"), 1.0, 1.0)
      .withSetting(GraphDatabaseInternalSettings.planning_merge_join_enabled, Boolean.box(true))
      .build()

    val query =
      """MATCH (a:A), (b:B)
        |WHERE a.prop = b.prop
        |RETURN a, b""".stripMargin

    val plan = planner.planState(query)
    plan should haveSamePlanAndCardinalitiesAsBuilder(
      planner.planBuilder()
        .produceResults("a", "b")
        .valueMergeJoin("cacheN[a.prop] = cacheN[b.prop]")
        .|.nodeIndexOperator(
          "b:B(prop)",
          indexOrder = IndexOrderAscending,
          getValue = Map("prop" -> GetValue)
        )
        .nodeIndexOperator(
          "a:A(prop)",
          indexOrder = IndexOrderAscending,
          getValue = Map("prop" -> GetValue)
        ).withCardinality(cardA * existsSelectivityA), // = 10_000
      ComparingProvidedAttributesOnly
    )
  }

  test("should prefer merge join over hash join for node property equality for very large LHS and RHS") {
    val planner = spdPlanner
      .setAllNodesCardinality(10_000_000)
      .setLabelCardinality("A", 1_000_000)
      .setLabelCardinality("B", 9_000_000)
      .addNodeIndex("A", Seq("prop"), 1.0, 1.0 / 5000, isUnique = true)
      .addNodeIndex("B", Seq("prop"), 1.0, 1.0)
      .withSetting(GraphDatabaseInternalSettings.planning_merge_join_enabled, Boolean.box(true))
      .build()

    val query =
      """MATCH (a:A), (b:B)
        |WHERE a.prop = b.prop
        |RETURN a, b""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .valueMergeJoin("cacheN[a.prop] = cacheN[b.prop]")
      .|.nodeIndexOperator(
        "b:B(prop)",
        indexOrder = IndexOrderAscending,
        getValue = Map("prop" -> GetValue)
      )
      .nodeIndexOperator("a:A(prop)", indexOrder = IndexOrderAscending, getValue = Map("prop" -> GetValue))
      .build()
  }

  test("should prefer hash join over merge join if there is no index for the LHS") {
    val planner = spdPlanner
      .setAllNodesCardinality(10_000_000)
      .setLabelCardinality("A", 1_000_000)
      .setLabelCardinality("B", 9_000_000)
      .addNodeIndex("B", Seq("prop"), 1.0, 1.0)
      .withSetting(GraphDatabaseInternalSettings.planning_merge_join_enabled, Boolean.box(true))
      .build()

    val query =
      """MATCH (a:A), (b:B)
        |WHERE a.prop = b.prop
        |RETURN a, b""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .valueHashJoin(
        "cacheN[a.prop] = cacheN[b.prop]"
      ) // preferred over merge join where the LHS will need to be sorted in memory since there is no index.
      .|.nodeIndexOperator(
        "b:B(prop)",
        indexOrder = IndexOrderAscending,
        getValue = Map("prop" -> GetValue)
      )
      .remoteBatchProperties("cacheNFromStore[a.prop]")
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()
  }

  test("should use the best plan with extra properties on LHS of value hash join, best sorted plan on RHS") {
    val planner = spdPlanner
      .setAllNodesCardinality(10000)
      .setLabelCardinality("A", 500)
      .build()

    val query =
      """MATCH (x:A), (y:A)
        |WHERE
        |  x.prop = y.prop AND
        |  x.hello > y.world AND
        |  y.start > 123
        |ORDER BY y.name
        |RETURN y.name AS name, count(*) AS result
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .orderedAggregation(Seq("cacheN[y.name] AS name"), Seq("count(*) AS result"), Seq("cacheN[y.name]"))
      .filter("cacheN[x.hello] > cacheN[y.world]")
      .valueHashJoin("cacheN[x.prop] = cacheN[y.prop]")
      .|.sort("`y.name` ASC")
      .|.projection("cacheN[y.name] AS `y.name`")
      .|.remoteBatchPropertiesWithFilter(
        "cacheNFromStore[y.name]",
        "cacheNFromStore[y.prop]",
        "cacheNFromStore[y.world]"
      )("y.start > 123")
      .|.nodeByLabelScan("y", "A")
      .remoteBatchProperties("cacheNFromStore[x.hello]", "cacheNFromStore[x.prop]")
      .nodeByLabelScan("x", "A")
      .build()
  }
}

class ParallelRuntimeRemoteBatchPropertiesPlanningIntegrationTest
    extends AbstractRemoteBatchPropertiesPlanningIntegrationTest(BatchedParallel(1, 2)) {

  override protected val orderPreserving: Boolean = false

}

abstract class AbstractRemoteBatchPropertiesPlanningIntegrationTest(executionModel: ExecutionModel)
    extends CypherPlannerTestSuite
    with LogicalPlanningIntegrationTestSupport
    with AstConstructionTestSupport
    with LogicalPlanConstructionTestSupport
    with CardinalityIntegrationTestSupport {

  protected val orderPreserving: Boolean

  protected val spdPlanner = plannerBuilder()
    .setDatabaseMode(DatabaseMode.SHARDED)
    .registerTemporalFunctions()
    .withSetting(
      GraphDatabaseInternalSettings.cypher_remote_batch_properties_implementation,
      RemoteBatchPropertiesImplementation.PLANNER
    )
    .withSetting(
      GraphDatabaseInternalSettings.remote_leaf_operators,
      true
    )
    .setExecutionModel(executionModel)

  // Graph counts based on a subset of LDBC SF 1
  final protected val planner = spdPlanner
    .addNodeVectorIndex("messageContent", Seq("Message"), "content")
    .addRelationshipVectorIndex("knowsDescr", Seq("KNOWS"), "description")
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
    .addNodeIndex("Person", List("firstName", "lastName"), existsSelectivity = 1.0, uniqueSelectivity = 1 / 3532.0)
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
    .setRelationshipCardinality("()-[:HAS_DOG]->(:Dog)", 10)
    .setRelationshipCardinality("()-[:FRIEND]->()", 10)
    .setRelationshipCardinality("(:Person)-[:FRIEND]->()", 10)
    .setRelationshipCardinality("()-[:FRIEND]->(:Person)", 10)
    .setRelationshipCardinality("(:Person)-[:FRIEND]->(:Person)", 10)
    .build()

  test("should batch node properties") {
    val query =
      """MATCH (person:Person)
        |RETURN person.firstName AS personFirstName,
        |       person.lastName AS personLastName""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("personFirstName", "personLastName")
      .projection("cacheN[person.firstName] AS personFirstName", "cacheN[person.lastName] AS personLastName")
      .remoteBatchProperties("cacheNFromStore[person.firstName]", "cacheNFromStore[person.lastName]")
      .nodeByLabelScan("person", "Person")
      .build()
  }

  test("should handle renamed variables from within an apply") {
    val query =
      """MATCH (person:Person {id:$Person}) WHERE person.firstName IS NOT NULL
        |CALL {
        |  WITH person
        |  RETURN person AS x
        |}
        |RETURN x.firstName AS personFirstName""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("personFirstName")
      .projection(Map(
        "personFirstName" -> cachedNodeProp("person", "firstName", "x")
      ))
      .projection("person AS x")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[person.firstName]")("person.firstName IS NOT NULL")
      .nodeIndexOperator(
        "person:Person(id = ???)",
        paramExpr = Some(parameter("Person", CTAny)),
        getValue = Map("id" -> DoNotGetValue),
        unique = true
      )
      .build()
  }

  test("should batch relationship properties") {
    val query =
      """MATCH (person:Person)-[knows:KNOWS]->(friend:Person)
        |RETURN knows.creationDate AS knowsSince""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("knowsSince")
      .projection("cacheR[knows.creationDate] AS knowsSince")
      .remoteBatchProperties("cacheRFromStore[knows.creationDate]")
      .filter("person:Person")
      .expandAll("(friend)<-[knows:KNOWS]-(person)")
      .nodeByLabelScan("friend", "Person")
      .build()
  }

  test("should batch on variables from an unwind") {
    val query =
      """
      MATCH (n)
      UNWIND [null, n] AS x
      MATCH (x)
      RETURN x.name AS name
      """

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("name")
      .projection("cacheN[x.name] AS name")
      .remoteBatchProperties("cacheNFromStore[x.name]")
      .filter(assertIsNode("x"))
      .unwind("[NULL, n] AS x")
      .allNodeScan("n")
      .build()
  }

  test("should batch on standalone pattern arguments") {
    val planner = spdPlanner
      .setAllNodesCardinality(30)
      .setLabelCardinality("L0", 10)
      .setRelationshipCardinality("()-[]->()", 10000)
      .setRelationshipCardinality("(:L0)-[]->()", 20)
      .addNodeIndex("L0", List("prop"), existsSelectivity = 1.0, uniqueSelectivity = 1.0 / 10.0)
      .build()

    val query =
      """
        |OPTIONAL MATCH (n0)-[r1]->(n1) WHERE n0.prop = 42
        |MATCH (a:L0 {prop:42})-[r2]->(n1), (n0)
        |RETURN a.prop AS expandIntoProp, n0.prop AS standaloneProp
        |""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("expandIntoProp", "standaloneProp")
      .projection("cacheN[a.prop] AS expandIntoProp", "cacheN[n0.prop] AS standaloneProp")
      .expandInto("(a)-[]->(n1)")
      .filter(assertIsNode("n0"))
      .apply()
      .|.remoteNodeIndexOperator(
        "a:L0(prop = 42)",
        argumentIds = Set("n0", "n1"),
        getValue = Map("prop" -> GetValue)
      )
      .optional()
      .expandAll("(n0)-[]->(n1)")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[n0.prop]")("n0.prop = 42")
      .allNodeScan("n0")
      .build()
  }

  test("should also batch properties used in filters, even if just once") { // TODO: This should have two RBPWF
    val query =
      """MATCH (person:Person)-[knows:KNOWS]->(friend:Person)
        |  WHERE person.lastName = friend.lastName AND knows.creationDate < $max_creation_date
        |RETURN person.firstName AS personFirstName,
        |       friend.firstName AS friendFirstName,
        |       knows.creationDate AS knowsSince""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("personFirstName", "friendFirstName", "knowsSince")
      .projection(
        "cacheN[person.firstName] AS personFirstName",
        "cacheN[friend.firstName] AS friendFirstName",
        "cacheR[knows.creationDate] AS knowsSince"
      )
      .filter("cacheR[knows.creationDate] < $max_creation_date")
      .remoteBatchProperties("cacheRFromStore[knows.creationDate]")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[person.firstName]")(
        "person.lastName = cacheN[friend.lastName]"
      )
      .filter("person:Person")
      .expandAll("(friend)<-[knows:KNOWS]-(person)")
      .remoteBatchProperties("cacheNFromStore[friend.firstName]", "cacheNFromStore[friend.lastName]")
      .nodeByLabelScan("friend", "Person")
      .build()
  }

  test("Should not get value from index if the property isn't reused") {
    val query =
      """MATCH (person:Person {id:$Person})
        |RETURN person.firstName AS personFirstName""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("personFirstName")
      .projection("cacheN[person.firstName] AS personFirstName")
      .remoteBatchProperties("cacheNFromStore[person.firstName]")
      .nodeIndexOperator(
        "person:Person(id = ???)",
        paramExpr = Some(parameter("Person", CTAny)),
        getValue = Map("id" -> DoNotGetValue),
        unique = true
      )
      .build()
  }

  test("Should get value from index if the property used in another predicate") {
    val query =
      """MATCH (person:Person {id:$Person}) WHERE person.id <> 42
        |RETURN person.firstName AS personFirstName""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("personFirstName")
      .projection("cacheN[person.firstName] AS personFirstName")
      .remoteBatchProperties("cacheNFromStore[person.firstName]")
      .filter("NOT cacheN[person.id] = 42")
      .nodeIndexOperator(
        "person:Person(id = ???)",
        paramExpr = Some(parameter("Person", CTAny)),
        getValue = Map("id" -> GetValue),
        unique = true
      )
      .build()
  }

  test("should retrieve properties from indexes where applicable") {
    val query =
      """MATCH (person:Person {id:$Person})
        |RETURN person.id AS personId,
        |       person.firstName AS personFirstName""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("personId", "personFirstName")
      .projection("cacheN[person.id] AS personId", "cacheN[person.firstName] AS personFirstName")
      .remoteBatchProperties("cacheNFromStore[person.firstName]")
      .nodeIndexOperator(
        "person:Person(id = ???)",
        paramExpr = Some(parameter("Person", CTAny)),
        getValue = Map("id" -> GetValue),
        unique = true
      )
      .build()
  }

  test("should cache properties with optional match") {
    val query =
      """MATCH (person:Person {id:$Person})-[knows:KNOWS*1..2]-(friend)
        |  WHERE person.firstName = friend.firstName
        |OPTIONAL MATCH (friend)<-[has_creator:POST_HAS_CREATOR|COMMENT_HAS_CREATOR]-(message)
        |  WHERE message.creationDate IS NOT NULL
        |RETURN message.id AS messageId,
        |       message.creationDate AS messageCreationDate,
        |       friend.firstName AS friendFirstName,
        |       friend.lastName AS friendLastName""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual
      planner
        .planBuilder()
        .produceResults("messageId", "messageCreationDate", "friendFirstName", "friendLastName")
        .projection(
          "cacheN[message.id] AS messageId",
          "cacheN[message.creationDate] AS messageCreationDate",
          "cacheN[friend.firstName] AS friendFirstName",
          "cacheN[friend.lastName] AS friendLastName"
        )
        .apply()
        .|.optional("friend")
        .|.remoteBatchPropertiesWithFilter("cacheNFromStore[message.id]", "cacheNFromStore[message.creationDate]")(
          "message.creationDate IS NOT NULL"
        )
        .|.expandAll("(friend)<-[:POST_HAS_CREATOR|COMMENT_HAS_CREATOR]-(message)")
        .|.argument("friend")
        .remoteBatchPropertiesWithFilter(
          "cacheNFromStore[friend.firstName]",
          "cacheNFromStore[friend.lastName]"
        )("cacheN[person.firstName] = friend.firstName")
        .expand("(person)-[:KNOWS*1..2]-(friend)", expandMode = ExpandAll, projectedDir = OUTGOING)
        .remoteBatchProperties("cacheNFromStore[person.firstName]")
        .nodeIndexOperator(
          "person:Person(id = ???)",
          paramExpr = Some(parameter("Person", CTAny)),
          getValue =
            Map("id" -> DoNotGetValue), // context.plannerState.accessedProperties contains `person.id`, we need to list projected values in a different way
          unique = true
        )
        .build()
  }

  test("should cache properties with unions from the or-leaf planner") {
    val query =
      """MATCH (person:Person)-[knows:KNOWS*1..2]-(friend)
        |  WHERE person.id = $Person  OR person.firstName = 'Dave'
        |RETURN
        |       person.firstName AS firstName,
        |       person.lastName AS lastName""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual
      planner
        .planBuilder()
        .produceResults("firstName", "lastName")
        .projection("cacheN[person.firstName] AS firstName", "cacheN[person.lastName] AS lastName")
        .expand("(person)-[:KNOWS*1..2]-()")
        .remoteBatchProperties(
          "cacheNFromStore[person.firstName]",
          "cacheNFromStore[person.lastName]"
        ) // note how we retrieve firstName once again
        .distinct("person AS person")
        .union()
        .|.nodeIndexOperator(
          "person:Person(id = ???)",
          paramExpr = Some(parameter("Person", CTAny)),
          getValue = Map("id" -> DoNotGetValue),
          unique = true
        ) // here we get the person id, even though we don't use it later on
        .nodeIndexOperator(
          "person:Person(firstName = 'Dave')",
          getValue = Map("firstName" -> GetValue)
        ) // TODO: we shouldn't get the value only on one side of the union, we need some additional logic
        .build()
  }

  test("should batch properties only on non-conflicting variables on either side of the union") {
    val query =
      """
        |CALL {
        | MATCH (p: Person)
        | WHERE p.firstName = "Smith" AND p.creationDate > $max_creation_date
        | RETURN p
        |UNION ALL
        | MATCH (p: Message)-[:POST_HAS_CREATOR]->(:Person{firstName:"Smith"})
        | WHERE p.creationDate < $max_creation_date
        | RETURN p
        |}
        |RETURN p.creationDate
        |""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("`p.creationDate`")
      .projection("cacheN[p.creationDate] AS `p.creationDate`")
      .remoteBatchProperties("cacheNFromStore[p.creationDate]")
      .union()
      .|.projection("p AS p")
      .|.remoteBatchPropertiesWithFilter("cacheNFromStore[p.creationDate]")(
        "p.creationDate < $max_creation_date"
      )
      .|.filter("p:Message")
      .|.expandAll("(anon_0)<-[:POST_HAS_CREATOR]-(p)")
      .|.nodeIndexOperator("anon_0:Person(firstName = 'Smith')", getValue = Map("firstName" -> DoNotGetValue))
      .projection("p AS p")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[p.creationDate]")("p.creationDate > $max_creation_date")
      .nodeIndexOperator("p:Person(firstName = 'Smith')", getValue = Map("firstName" -> DoNotGetValue))
      .build()
  }

  test("should batch properties of renamed entities") {
    val query =
      """MATCH (person:Person)
        |  WHERE person.creationDate < $max_creation_date AND person.lastName IS NOT NULL
        |WITH person AS earlyAdopter, person.creationDate AS earlyAdopterSince ORDER BY earlyAdopterSince LIMIT 10
        |MATCH (earlyAdopter)-[knows:KNOWS]->(friend:Person)
        |RETURN earlyAdopter.lastName AS personLastName,
        | earlyAdopter.creationDate AS personCreationDate,
        | friend.lastName AS friendLastName""".stripMargin

    val plan = planner.plan(query)

    val expectedPlan = planner
      .planBuilder()
      .produceResults("personLastName", "personCreationDate", "friendLastName")
      .projection(Map(
        "personLastName" -> cachedNodeProp(
          // notice how `originalEntity` and `entityVariable`differ here
          variable = "person",
          propKey = "lastName",
          currentVarName = "earlyAdopter"
        ),
        "personCreationDate" -> cachedNodeProp(
          // notice how `originalEntity` and `entityVariable` differ here
          variable = "person",
          propKey = "creationDate",
          currentVarName = "earlyAdopter"
        ),
        "friendLastName" -> cachedNodeProp(variable = "friend", propKey = "lastName", currentVarName = "friend")
      ))
      .remoteBatchProperties(
        cachedNodeProp("friend", "lastName", "friend", knownToAccessStore = true)
      )
      .filter("friend:Person")
      .expandAll("(earlyAdopter)-[:KNOWS]->(friend)")
      .projection("person AS earlyAdopter")
      .top(10, "earlyAdopterSince ASC")
      .projection("cacheN[person.creationDate] AS earlyAdopterSince")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[person.creationDate]", "cacheNFromStore[person.lastName]")(
        "person.lastName IS NOT NULL",
        "person.creationDate < $max_creation_date"
      )
      .nodeByLabelScan("person", "Person")
      .build()
    plan shouldEqual expectedPlan
  }

  test("probably should but currently does not batch properties when returning entire entities") {
    val query =
      """MATCH (person:Person)-[knows:KNOWS]->(friend:Person)
        |RETURN person, friend""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("person", "friend")
      .filter("person:Person")
      .expandAll("(friend)<-[:KNOWS]-(person)")
      .nodeByLabelScan("friend", "Person")
      .build()
  }

  test("should batch accessed properties before a selection even if there are no predicates on it") {
    val query =
      """MATCH (person:Person {id:$Person})-[:KNOWS*1..2]-(friend)
        |WHERE NOT person.id = friend.id
        |RETURN friend.id AS personId,
        |       friend.firstName AS personFirstName,
        |       friend.lastName AS personLastName""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("personId", "personFirstName", "personLastName")
      .projection(
        "cacheN[friend.id] AS personId",
        "cacheN[friend.firstName] AS personFirstName",
        "cacheN[friend.lastName] AS personLastName"
      )
      .remoteBatchPropertiesWithFilter(
        "cacheNFromStore[friend.id]",
        "cacheNFromStore[friend.firstName]",
        "cacheNFromStore[friend.lastName]"
      )("NOT cacheN[person.id] = friend.id")
      .expand("(person)-[:KNOWS*1..2]-(friend)")
      .nodeIndexOperator(
        "person:Person(id = ???)",
        paramExpr = Some(ExplicitParameter("Person", CTAny)(InputPosition.NONE)),
        getValue = Map("id" -> GetValue),
        unique = true
      )
      .build()
  }

  test("should batch properties in complex enough queries (Query 9 in LDBC SF 1)") {
    val query =
      """MATCH (person:Person {id:$Person})-[:KNOWS*1..2]-(friend)
        |WHERE NOT person=friend
        |WITH DISTINCT friend
        |MATCH (friend)<-[:POST_HAS_CREATOR|COMMENT_HAS_CREATOR]-(message)
        |WHERE message.creationDate < $Date0
        |WITH friend, message
        |ORDER BY message.creationDate DESC, message.id ASC
        |LIMIT 20
        |RETURN message.id AS messageId,
        |       coalesce(message.content,message.imageFile) AS messageContent,
        |       message.creationDate AS messageCreationDate,
        |       friend.id AS personId,
        |       friend.firstName AS personFirstName,
        |       friend.lastName AS personLastName""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults(
        "messageId",
        "messageContent",
        "messageCreationDate",
        "personId",
        "personFirstName",
        "personLastName"
      )
      .projection(
        "cacheN[message.id] AS messageId",
        "cacheN[friend.lastName] AS personLastName",
        "cacheN[friend.id] AS personId",
        "cacheN[message.creationDate] AS messageCreationDate",
        "coalesce(cacheN[message.content], cacheN[message.imageFile]) AS messageContent",
        "cacheN[friend.firstName] AS personFirstName"
      )
      .remoteBatchProperties(
        "cacheNFromStore[friend.lastName]",
        "cacheNFromStore[friend.id]",
        "cacheNFromStore[friend.firstName]"
      )
      .top(20, "`message.creationDate` DESC", "`message.id` ASC")
      .projection("cacheN[message.creationDate] AS `message.creationDate`", "cacheN[message.id] AS `message.id`")
      .remoteBatchPropertiesWithFilter(
        "cacheNFromStore[message.imageFile]",
        "cacheNFromStore[message.creationDate]",
        "cacheNFromStore[message.content]",
        "cacheNFromStore[message.id]"
      )("message.creationDate < $Date0")
      .expandAll("(friend)<-[:POST_HAS_CREATOR|COMMENT_HAS_CREATOR]-(message)")
      .projection("friend AS friend")
      .filter("NOT person = friend")
      .bfsPruningVarExpand(
        "(person)-[:KNOWS*1..2]-(friend)"
      )
      .nodeIndexOperator(
        "person:Person(id = ???)",
        paramExpr = Some(parameter("Person", CTAny)),
        getValue = Map("id" -> DoNotGetValue),
        unique = true
      )
      .build()
  }

  test("should not batch properties if the index determines that values are not required - LDBC BI SF001-Read_9") {
    val query =
      """
        |MATCH (person:Person)<-[:POST_HAS_CREATOR]-(post:Message)<-[:REPLY_OF*0..]-(reply)
        |WHERE post.creationDate <= $endDate AND
        |      reply.creationDate <= $endDate
        |WITH person, count(post) AS threadCount, count(reply) AS messageCount
        |RETURN
        |  person.id,
        |  person.firstName,
        |  person.lastName,
        |  threadCount,
        |  messageCount
        |ORDER BY messageCount DESC, person.id ASC
        |LIMIT 100
        |""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("`person.id`", "`person.firstName`", "`person.lastName`", "threadCount", "messageCount")
      .___CONDITION_BEGIN___(orderPreserving)
      .projection(
        "cacheN[person.firstName] AS `person.firstName`",
        "cacheN[person.lastName] AS `person.lastName`",
        "cacheN[person.id] AS `person.id`"
      )
      .remoteBatchProperties("cacheNFromStore[person.firstName]", "cacheNFromStore[person.lastName]")
      .top(100, "messageCount DESC", "`person.id` ASC")
      .projection("cacheN[person.id] AS `person.id`")
      .remoteBatchProperties("cacheNFromStore[person.id]")
      .___CONDITION_BEGIN___(!orderPreserving)
      .projection("cacheN[person.firstName] AS `person.firstName`", "cacheN[person.lastName] AS `person.lastName`")
      .top(100, "messageCount DESC", "`person.id` ASC")
      .projection("cacheN[person.id] AS `person.id`")
      .remoteBatchProperties(
        "cacheNFromStore[person.lastName]",
        "cacheNFromStore[person.firstName]",
        "cacheNFromStore[person.id]"
      )
      .___CONDITION_END___()
      .aggregation(Seq("person AS person"), Seq("count(post) AS threadCount", "count(reply) AS messageCount"))
      .remoteBatchPropertiesWithFilter("cacheNFromStore[reply.creationDate]")("reply.creationDate <= $endDate")
      .expand("(post)<-[:REPLY_OF*0..]-(reply)", expandMode = ExpandAll, projectedDir = INCOMING)
      .filter("person:Person")
      .expandAll("(post)-[:POST_HAS_CREATOR]->(person)")
      .nodeIndexOperator(
        "post:Message(creationDate <= ???)",
        paramExpr = Some(parameter("endDate", CTAny)),
        getValue = Map("creationDate" -> DoNotGetValue)
      )
      .build()
  }

  test("should batch properties in relationship indexes") {
    val query =
      """MATCH (person:Person)<-[r:COMMENT_HAS_CREATOR]-(message)
        |WHERE r.location = 'London' AND person.firstName IS NOT NULL
        |RETURN r.location AS posterLocation,
        |       coalesce(message.content,message.imageFile) AS messageContent,
        |       person.firstName AS personFirstName,
        |       person.lastName AS personLastName""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("posterLocation", "messageContent", "personFirstName", "personLastName")
      .projection(
        "cacheR[r.location] AS posterLocation",
        "coalesce(cacheN[message.content], cacheN[message.imageFile]) AS messageContent",
        "cacheN[person.firstName] AS personFirstName",
        "cacheN[person.lastName] AS personLastName"
      )
      .remoteBatchProperties("cacheNFromStore[message.imageFile]", "cacheNFromStore[message.content]")
      .remoteBatchPropertiesWithFilter(
        "cacheNFromStore[person.lastName]",
        "cacheNFromStore[person.firstName]"
      )("person.firstName IS NOT NULL")
      .filter("person:Person")
      .relationshipIndexOperator(
        "(message)-[r:COMMENT_HAS_CREATOR(location = 'London')]->(person)",
        getValue = Map("location" -> GetValue)
      )
      .build()
  }

  test("should batch properties in relationship indexes by id seek") {
    val query =
      """MATCH (person:Person)<-[r:COMMENT_HAS_CREATOR {id:$CommentCreatorId}]-(message)
        |WHERE person.firstName IS NOT NULL
        |RETURN r.id AS commentorId,
        |       coalesce(message.content,message.imageFile) AS messageContent,
        |       person.firstName AS personFirstName,
        |       person.lastName AS personLastName""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("commentorId", "messageContent", "personFirstName", "personLastName")
      .projection(
        "cacheR[r.id] AS commentorId",
        "coalesce(cacheN[message.content], cacheN[message.imageFile]) AS messageContent",
        "cacheN[person.firstName] AS personFirstName",
        "cacheN[person.lastName] AS personLastName"
      )
      .remoteBatchProperties("cacheNFromStore[message.imageFile]", "cacheNFromStore[message.content]")
      .remoteBatchPropertiesWithFilter(
        "cacheNFromStore[person.lastName]",
        "cacheNFromStore[person.firstName]"
      )("person.firstName IS NOT NULL")
      .filter("person:Person")
      .relationshipIndexOperator(
        "(message)-[r:COMMENT_HAS_CREATOR(id = ???)]->(person)",
        paramExpr = Some(ExplicitParameter("CommentCreatorId", CTAny)(InputPosition.NONE)),
        getValue = Map("id" -> GetValue),
        unique = true
      )
      .build()
  }

  test("should batch properties for aggregating functions like max") {
    val query =
      """MATCH (person)
        |WITH MAX(person.age) as maxAge, person
        |WHERE person.age = maxAge
        |RETURN person.name""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("`person.name`")
      .projection("cacheN[person.name] AS `person.name`")
      .remoteBatchPropertiesWithFilter(
        "cacheNFromStore[person.age]",
        "cacheNFromStore[person.name]"
      )("person.age = maxAge")
      .aggregation(Seq("person AS person"), Seq("MAX(cacheN[person.age]) AS maxAge"))
      .remoteBatchProperties("cacheNFromStore[person.age]")
      .allNodeScan("person")
      .build()
  }

  test("should batch properties for aggregating functions with grouping keys") {
    val query =
      """MATCH (person)
        |WITH person, max(person.age) - person.age AS ageDifference
        |RETURN person.name, ageDifference""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("`person.name`", "ageDifference")
      .projection("cacheN[person.name] AS `person.name`")
      .remoteBatchProperties("cacheNFromStore[person.name]")
      .projection("anon_0 - anon_1 AS ageDifference")
      .aggregation(Seq("cacheN[person.age] AS anon_1", "person AS person"), Seq("max(cacheN[person.age]) AS anon_0"))
      .remoteBatchProperties("cacheNFromStore[person.age]")
      .allNodeScan("person")
      .build()
  }

  test("should batch properties for collect function") {
    val query =
      """MATCH (person)
        |RETURN collect(person.age) as ages""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("ages")
      .aggregation(Seq(), Seq("collect(cacheN[person.age]) AS ages"))
      .remoteBatchProperties("cacheNFromStore[person.age]")
      .allNodeScan("person")
      .build()
  }

  test("should batch properties for rollup apply") {
    val query =
      """
        |MATCH (p:Person)
        |RETURN p.name, [(p)<-[r:COMMENT_HAS_CREATOR]-(message) | message.name] AS title
        |""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("`p.name`", "title")
      .projection("cacheN[p.name] AS `p.name`")
      .remoteBatchProperties("cacheNFromStore[p.name]")
      .rollUpApply("title", "anon_0")
      .|.projection("cacheN[message.name] AS anon_0")
      .|.remoteBatchProperties("cacheNFromStore[message.name]")
      .|.expandAll("(p)<-[:COMMENT_HAS_CREATOR]-(message)")
      .|.argument("p")
      .nodeByLabelScan("p", "Person")
      .build()
  }

  test("should propagate cached properties for an unwind projection") {
    val query =
      """
        |MATCH (n:Person)
        |UNWIND [n.firstName] AS foo
        |MATCH (n)-[:KNOWS]-(friend:Person {lastName: foo})
        |RETURN n.firstName, friend.firstName""".stripMargin
    val plan = planner.plan(query).stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("cacheN[n.firstName] AS `n.firstName`", "cacheN[friend.firstName] AS `friend.firstName`")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[friend.firstName]")("friend.lastName = foo")
      .filter("friend:Person")
      .expandAll("(n)-[:KNOWS]-(friend)")
      .unwind("[cacheN[n.firstName]] AS foo")
      .remoteBatchProperties("cacheNFromStore[n.firstName]")
      .nodeByLabelScan("n", "Person")
      .build())
  }

  test("should propagate cached properties for a distinct projection") {
    val query =
      """
        |MATCH (n:Person)
        |WHERE n.firstName = 'foo'
        |RETURN DISTINCT n.lastName""".stripMargin
    val plan = planner.plan(query).stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .distinct("cacheN[n.lastName] AS `n.lastName`")
      .remoteBatchProperties("cacheNFromStore[n.lastName]")
      .nodeIndexOperator("n:Person(firstName = 'foo')", getValue = Map("firstName" -> DoNotGetValue))
      .build())
  }

  test("should batch properties for letSelectOrAntiSemiApply and selectOrSemiApply") {
    val query =
      """
        |MATCH (a:Person)
        |WHERE  NOT EXISTS { (:Post)-[:POST_HAS_CREATOR]->(n) } OR (a)-[:KNOWS]-(:Person{lastName:"Smith"}) OR a.age > 30
        |RETURN a.firstName, a.age""".stripMargin
    val plan = planner.plan(query).stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("cacheN[a.firstName] AS `a.firstName`", "cacheN[a.age] AS `a.age`")
      .selectOrSemiApply("anon_2")
      .|.remoteBatchPropertiesWithFilter("cacheNFromStore[anon_1.lastName]")("anon_1.lastName = 'Smith'")
      .|.filter("anon_1:Person")
      .|.expandAll("(a)-[:KNOWS]-(anon_1)")
      .|.argument("a")
      .letSelectOrAntiSemiApply("anon_2", "cacheN[a.age] > 30")
      .|.expandAll("(anon_0)-[:POST_HAS_CREATOR]->()")
      .|.nodeByLabelScan("anon_0", "Post")
      .remoteBatchProperties("cacheNFromStore[a.firstName]", "cacheNFromStore[a.age]")
      .nodeByLabelScan("a", "Person")
      .build())
  }

  test(
    "should propagate batch properties with LetSemiApply and SelectOrAntiSemiApply"
  ) {
    val query =
      """
        |MATCH (a: Person)
        |WHERE (a {lastName:"Smith"})-[:KNOWS]-(:Person{lastName:"Smyth"}) OR NOT (a {lastName:"Smyth"})-[:KNOWS]-(:Person{lastName:"Smith"})
        |RETURN a.firstName, a.lastName""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .projection("cacheN[a.firstName] AS `a.firstName`", "cacheN[a.lastName] AS `a.lastName`")
        .remoteBatchProperties("cacheNFromStore[a.firstName]")
        .selectOrAntiSemiApply("anon_2")
        .|.remoteBatchPropertiesWithFilter("cacheNFromStore[anon_1.lastName]")("anon_1.lastName = 'Smith'")
        .|.filter("anon_1:Person")
        .|.expandAll("(a)-[:KNOWS]-(anon_1)")
        .|.filter("cacheN[a.lastName] = 'Smyth'")
        .|.argument("a")
        .letSemiApply("anon_2")
        .|.remoteBatchPropertiesWithFilter("cacheNFromStore[anon_0.lastName]")("anon_0.lastName = 'Smyth'")
        .|.filter("anon_0:Person")
        .|.expandAll("(a)-[:KNOWS]-(anon_0)")
        .|.remoteBatchPropertiesWithFilter("cacheNFromStore[a.lastName]")("a.lastName = 'Smith'")
        .|.argument("a")
        .nodeByLabelScan("a", "Person")
        .build()
    )
  }

  test("should propagate cached properties for cartesian products, apply and anti-semi-apply") {
    val query =
      """MATCH (a:Person )
        |MATCH (b:Person {firstName: "John"})
        |WHERE NOT (b)-[:KNOWS]->(:Person {firstName: "Jon"})
        |RETURN a.name, b.firstName
        |""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .projection("cacheN[a.name] AS `a.name`", "cacheN[b.firstName] AS `b.firstName`")
        .cartesianProduct()
        .|.remoteBatchProperties("cacheNFromStore[a.name]")
        .|.nodeByLabelScan("a", "Person")
        .antiSemiApply()
        .|.expandInto("(b)-[:KNOWS]->(anon_0)")
        .|.remoteNodeIndexOperator(
          "anon_0:Person(firstName = 'Jon')",
          argumentIds = Set("b"),
          getValue = Map("firstName" -> DoNotGetValue)
        )
        .nodeIndexOperator("b:Person(firstName = 'John')", getValue = Map("firstName" -> GetValue))
        .build()
    )
  }

  test("optional match with properties from outer matches should not fetch properties again") {
    val query =
      """
        |MATCH (p:Person {firstName: 'foo'})
        |OPTIONAL MATCH (p)-[:KNOWS]-(s:Person) WHERE s.firstName <> p.firstName
        |RETURN p.firstName,s.firstName
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("cacheN[p.firstName] AS `p.firstName`", "cacheN[s.firstName] AS `s.firstName`")
      .apply()
      .|.optional("p")
      .|.remoteBatchPropertiesWithFilter("cacheNFromStore[s.firstName]")(
        "NOT s.firstName = cacheN[p.firstName]"
      )
      .|.filter("s:Person")
      .|.expandAll("(p)-[:KNOWS]-(s)")
      .|.argument("p")
      .nodeIndexOperator("p:Person(firstName = 'foo')", getValue = Map("firstName" -> GetValue))
      .build())
  }

  test("should propagate cached properties from a nodeindex scan ") {
    val query =
      """
        |MATCH (p)-[:KNOWS]-(s:Person) WHERE s.firstName <> p.firstName
        |RETURN p.firstName,s.firstName
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("cacheN[p.firstName] AS `p.firstName`", "cacheN[s.firstName] AS `s.firstName`")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[p.firstName]")("NOT cacheN[s.firstName] = p.firstName")
      .expandAll("(s)-[:KNOWS]-(p)")
      .nodeIndexOperator("s:Person(firstName)", getValue = Map("firstName" -> GetValue))
      .build())
  }

  test("should batch properties for anti-conditional apply and shortestpath") {
    val query =
      """
        |MATCH p=shortestPath((a:Person{lastName:"Smith"})-[:KNOWS*]-(b:Person{lastName:"Smith"}))
        |WHERE all(r IN relationships(p) WHERE r.creationDate < $max_creation_date)
        |AND all(n IN nodes(p) WHERE n.firstName = "John")
        |AND length(p) > 5
        |RETURN a.lastName, [n in nodes(p) | n.lastName] as lastNames, b.lastName
        |""".stripMargin

    planner.plan(query).stripProduceResults should equal(
      planner.subPlanBuilder()
        .projection(
          "cacheN[a.lastName] AS `a.lastName`",
          "[n IN nodes(p) | n.lastName] AS lastNames",
          "cacheN[b.lastName] AS `b.lastName`"
        )
        .antiConditionalApply("p")
        .|.top(1, "anon_1 ASC")
        .|.projection("length(p) AS anon_1")
        .|.filter("length(p) > 5")
        .|.projection(Map("p" -> varLengthPathExpression(v"a", v"anon_0", v"b")))
        .|.expand(
          "(a)-[anon_0:KNOWS*1..]-(b)",
          expandMode = ExpandInto,
          projectedDir = OUTGOING,
          nodePredicates = Seq(Predicate("n", "n.firstName = 'John'")),
          relationshipPredicates = Seq(Predicate("r", "r.creationDate < $max_creation_date"))
        )
        .|.argument("a", "b")
        .apply()
        .|.optional("a", "b")
        .|.shortestPath(
          "(a)-[anon_0:KNOWS*1..]-(b)",
          pathName = Some("p"),
          nodePredicates = Seq(Predicate("n", "n.firstName = 'John'")),
          relationshipPredicates = Seq(Predicate("r", "r.creationDate < $max_creation_date")),
          pathPredicates = Seq("length(p) > 5"),
          withFallback = true
        )
        .|.argument("a", "b")
        .cartesianProduct()
        .|.remoteBatchPropertiesWithFilter("cacheNFromStore[b.lastName]")("b.lastName = 'Smith'")
        .|.nodeByLabelScan("b", "Person")
        .remoteBatchPropertiesWithFilter("cacheNFromStore[a.lastName]")("a.lastName = 'Smith'")
        .nodeByLabelScan("a", "Person")
        .build()
    )
  }

  test("should propagate batched properties after a statefulshortestpath") {
    val query =
      """
        |MATCH p=ANY SHORTEST ((a)((:Person{lastName:"Smith"})-[:KNOWS]-(:Person{firstName:"John"}))*(b{lastName:"Smith"}))
        |WHERE length(p) > 5
        |RETURN a.lastName, [n in nodes(p) | n.lastName] as lastNames, b.firstName
        |""".stripMargin
    planner.plan(query).stripProduceResults should equal(planner.subPlanBuilder()
      .projection(Map(
        "a.lastName" -> cachedNodeProp("a", "lastName"),
        "lastNames" -> listComprehension(
          v"n",
          nodes(varLengthPathExpression(v"a", v"anon_3", v"b")),
          None,
          Some(prop("n", "lastName"))
        ),
        "b.firstName" -> cachedNodeProp("b", "firstName")
      ))
      .remoteBatchProperties("cacheNFromStore[a.lastName]")
      .filter(greaterThan(
        length(varLengthPathExpression(v"a", v"anon_3", v"b")),
        literalInt(5)
      ))
      .statefulShortestPath(
        "b",
        "a",
        "SHORTEST 1 (a) ((`anon_0`)-[`anon_1`]-(`anon_2`)){0, } (b)",
        None,
        Set(),
        Set(("anon_1", "anon_3")),
        Set(("a", "a")),
        Set(),
        StatefulShortestPath.Selector.Shortest(CountInteger(1)),
        new TestNFABuilder(0, "b")
          .addTransition(0, 1, "(b) (anon_2 WHERE anon_2.firstName = 'John' AND anon_2:Person)")
          .addTransition(0, 3, "(b) (a)")
          .addTransition(1, 2, "(anon_2)-[anon_1:KNOWS]-(anon_0 WHERE anon_0.lastName = 'Smith' AND anon_0:Person)")
          .addTransition(2, 1, "(anon_0) (anon_2 WHERE anon_2.firstName = 'John' AND anon_2:Person)")
          .addTransition(2, 3, "(anon_0) (a)")
          .setFinalState(3)
          .build(),
        ExpandAll,
        reverseGroupVariableProjections = true
      )
      .remoteBatchPropertiesWithFilter("cacheNFromStore[b.firstName]")(
        "b.lastName = 'Smith'"
      )
      .allNodeScan("b")
      .build())
  }

  test("should batch properties for a value hash join") {
    val query =
      """
        | MATCH (al:Message), (a:Person)
        |    WHERE al.title = a.name
        |    RETURN al.title, a.name
        |""".stripMargin

    planner.plan(query).stripProduceResults should equal(planner.subPlanBuilder()
      .projection("cacheN[al.title] AS `al.title`", "cacheN[a.name] AS `a.name`")
      .valueHashJoin("cacheN[a.name] = cacheN[al.title]")
      .|.remoteBatchProperties("cacheNFromStore[al.title]")
      .|.nodeByLabelScan("al", "Message")
      .remoteBatchProperties("cacheNFromStore[a.name]")
      .nodeByLabelScan("a", "Person")
      .build())
  }

  test("should not throw an error when fetching remote batch properties between two ordered operators") {
    val query =
      """
        |MATCH (a:Person)
        |CALL {
        |  WITH a
        |  MATCH (a)-[:KNOWS]->(b{name: a.firstName})
        |  RETURN b
        |  ORDER BY b.name
        |  LIMIT 1
        |}
        |RETURN a.lastName, b.name
        |ORDER BY b.name
        |LIMIT 100
        |""".stripMargin

    val expectedPlan = if (orderPreserving)
      planner
        .planBuilder()
        .produceResults("`a.lastName`", "`b.name`")
        .projection("cacheN[a.lastName] AS `a.lastName`")
        .remoteBatchProperties("cacheNFromStore[a.lastName]")
        .top(100, "`b.name` ASC")
        .projection("cacheN[b.name] AS `b.name`")
        .apply()
        .|.top(1, "`b.name` ASC")
        .|.projection("cacheN[b.name] AS `b.name`")
        .|.remoteBatchPropertiesWithFilter("cacheNFromStore[b.name]")("b.name = cacheN[a.firstName]")
        .|.expandAll("(a)-[:KNOWS]->(b)")
        .|.argument("a")
        .remoteBatchProperties("cacheNFromStore[a.firstName]")
        .nodeByLabelScan("a", "Person", IndexOrderNone)
        .build()
    else
      planner
        .planBuilder()
        .produceResults("`a.lastName`", "`b.name`")
        .projection("cacheN[a.lastName] AS `a.lastName`")
        .top(100, "`b.name` ASC")
        .projection("cacheN[b.name] AS `b.name`")
        .apply()
        .|.top(1, "`b.name` ASC")
        .|.projection("cacheN[b.name] AS `b.name`")
        .|.remoteBatchPropertiesWithFilter("cacheNFromStore[b.name]")("b.name = cacheN[a.firstName]")
        .|.expandAll("(a)-[:KNOWS]->(b)")
        .|.argument("a")
        .remoteBatchProperties("cacheNFromStore[a.firstName]", "cacheNFromStore[a.lastName]")
        .nodeByLabelScan("a", "Person", IndexOrderNone)
        .build()
    planner.plan(query) should equal(expectedPlan)

  }

  test("should plan pruning var expand if it has remoteBatchProperties") {
    val query =
      """
        |MATCH path=(:Person {id:$Person})-[:KNOWS*1..3]-(friend)
        |WHERE friend.firstName=$Name
        |RETURN friend.firstName, min(length(path)) AS distance
        |""".stripMargin

    planner.plan(query).stripProduceResults shouldEqual
      planner.subPlanBuilder()
        .projection("anon_1 AS distance", "cacheN[friend.firstName] AS `friend.firstName`")
        .remoteBatchPropertiesWithFilter("cacheNFromStore[friend.firstName]")("friend.firstName = $Name")
        .bfsPruningVarExpand("(anon_0)-[:KNOWS*1..3]-(friend)", depthName = Some("anon_1"), mode = ExpandAll)
        .nodeIndexOperator(
          "anon_0:Person(id = ???)",
          paramExpr = Some(parameter("Person", CTAny)),
          getValue = Map("id" -> DoNotGetValue),
          unique = true
        )
        .build()
  }

  test("should plan sort after remoteBatchProperties") {
    val query =
      """MATCH (subject:Person)
        |RETURN subject.firstName as name,
        |  subject.name as lastName
        |  ORDER BY name
        |""".stripMargin

    planner.plan(query) should equal(
      planner.planBuilder()
        .produceResults("name", "lastName")
        .projection("cacheN[subject.name] AS lastName")
        .sort("name ASC")
        .projection("cacheN[subject.firstName] AS name")
        .remoteBatchProperties("cacheNFromStore[subject.name]", "cacheNFromStore[subject.firstName]")
        .nodeByLabelScan("subject", "Person")
        .build()
    )
  }

  test(
    "should plan index-backed ORDER BY or not depending on runtime and order being invalidated by remoteBatchProperties"
  ) {
    val query =
      """MATCH (subject:Person)
        |  WHERE subject.firstName IS NOT NULL
        |RETURN
        |    subject.firstName as firstName,
        |    subject.name as lastName
        |  ORDER BY firstName
        |""".stripMargin

    val expectedPlan = if (orderPreserving) {
      planner.planBuilder()
        .produceResults("firstName", "lastName")
        .projection("cacheN[subject.firstName] AS firstName", "cacheN[subject.name] AS lastName")
        .remoteBatchProperties("cacheNFromStore[subject.name]")
        .nodeIndexOperator(
          "subject:Person(firstName)",
          indexOrder = IndexOrderAscending,
          getValue = Map("firstName" -> GetValue)
        )
        .build()
    } else {
      planner.planBuilder()
        .produceResults("firstName", "lastName")
        .projection("cacheN[subject.name] AS lastName")
        // remoteBatchProperties invalidated our order. Therefore, we need to sort again such that elements are produced in the right order
        .sort("firstName ASC")
        .projection("cacheN[subject.firstName] AS firstName")
        .remoteBatchPropertiesWithFilter(
          "cacheNFromStore[subject.firstName]",
          "cacheNFromStore[subject.name]"
        )("subject.firstName IS NOT NULL")
        .nodeByLabelScan("subject", "Person", IndexOrderNone)
        .build()
    }

    planner.plan(query) should equal(expectedPlan)
  }

  test("should insert remoteBatchProperties between an aggregation and projection on the same property") {
    val query =
      """
        |MATCH (subject:Person { firstName: $firstName })
        |MATCH p=(subject)<-[:POST_HAS_CREATOR]-()-[:KNOWS*0..2]-()<-[:POST_HAS_CREATOR]-(person)-[:KNOWS]->(friend)
        |WHERE person<>subject AND friend.firstName IN $interests
        |WITH person, friend, min(length(p)) AS pathLength
        |ORDER BY friend.firstName
        |RETURN person.name AS name, count(friend) AS score, collect(friend.firstName) AS interests,((pathLength - 1)/2) AS distance
        |ORDER BY score DESC LIMIT 20
        |""".stripMargin

    planner.plan(query).stripProduceResults shouldEqual planner
      .subPlanBuilder()
      .top(20, "score DESC")
      .aggregation(
        Seq("cacheN[person.name] AS name", "(pathLength - 1) / 2 AS distance"),
        Seq("count(friend) AS score", "collect(cacheN[friend.firstName]) AS interests")
      )
      .___CONDITION_BEGIN___(!orderPreserving)
      // remoteBatchProperties invalidated our order. Therefore, we need to sort again such that elements arrive in collect in the right order
      .sort("`friend.firstName` ASC")
      // these two `friend.firstName` were separated by the Namespacer. We therefore do not know that we already have the value in scope
      .projection("cacheN[friend.firstName] AS `friend.firstName`")
      .___CONDITION_END___()
      .remoteBatchProperties("cacheNFromStore[person.name]")
      .sort("`friend.firstName` ASC")
      .projection("cacheN[friend.firstName] AS `friend.firstName`")
      .remoteBatchProperties(
        "cacheNFromStore[friend.firstName]"
      ) // we need to plan this friend.firstName again, otherwise projection is very slow.
      .aggregation(
        Map("person" -> v"person", "friend" -> v"friend"),
        Map("pathLength" -> min(length(PathExpressionBuilder.node("subject").inTo("anon_0", "anon_1").bothToVarLength(
          "anon_2",
          "anon_3"
        ).inTo("anon_4", "person").outTo("anon_5", "friend").build())))
      )
      .remoteBatchPropertiesWithFilter("cacheNFromStore[friend.firstName]")("friend.firstName IN $interests")
      .filter("NOT anon_5 IN anon_2")
      .expandAll("(person)-[anon_5:KNOWS]->(friend)")
      .filter("NOT person = subject", "NOT anon_4 = anon_0")
      .expandAll("(anon_3)<-[anon_4:POST_HAS_CREATOR]-(person)")
      .expand("(anon_1)-[anon_2:KNOWS*0..2]-(anon_3)", expandMode = ExpandAll, projectedDir = OUTGOING)
      .expandAll("(subject)<-[anon_0:POST_HAS_CREATOR]-(anon_1)")
      .nodeIndexOperator(
        "subject:Person(firstName = ???)",
        paramExpr = Some(parameter("firstName", CTAny)),
        getValue = Map("firstName" -> DoNotGetValue)
      )
      .build()
  }

  test("should cache index values if they are used in other index seeks: index_backed_order_by-q15 modified") {
    val query =
      """
        |MATCH (a:PROFILES), (b:PROFILES)
        |WHERE
        |    b.children STARTS WITH "x" AND
        |    a.pets = b.children
        |RETURN id(a), id(b)
        |""".stripMargin

    val planner = spdPlanner
      .setAllNodesCardinality(1632803)
      .setLabelCardinality("PROFILES", 1632803)
      .addNodeIndex(
        "PROFILES",
        Seq("gender", "pets", "children"),
        existsSelectivity = 371135.0 / 1632803,
        uniqueSelectivity = 1.0 / 253204.0
      )
      .addNodeIndex(
        "PROFILES",
        Seq("gender", "hair_color", "eye_color", "pets", "children"),
        existsSelectivity = 337392.0 / 1632803,
        uniqueSelectivity = 1.0 / 273680.0
      )
      .addNodeIndex(
        "PROFILES",
        Seq("pets", "children"),
        existsSelectivity = 371135.0 / 1632803,
        uniqueSelectivity = 1.0 / 247714.0
      )
      .addNodeIndex(
        "PROFILES",
        Seq("children"),
        existsSelectivity = 475697.0 / 1632803,
        uniqueSelectivity = 1.0 / 176353
      )
      .addNodeIndex("PROFILES", Seq("pets"), existsSelectivity = 700681.0 / 1632803, uniqueSelectivity = 1.0 / 268273.0)
      .build()
    // Without additional filter, the value hash join would pull in many more nodes than necessary,
    // which is why the apply is probably more efficient, although there are many more roundtrips to the shard.
    planner.plan(query) shouldEqual planner.planBuilder()
      .produceResults("`id(a)`", "`id(b)`")
      .projection("id(a) AS `id(a)`", "id(b) AS `id(b)`")
      .apply()
      .|.remoteNodeIndexOperator(
        "a:PROFILES(pets = cacheN[b.children])",
        argumentIds = Set("b"),
        getValue = Map("pets" -> DoNotGetValue)
      )
      .nodeIndexOperator("b:PROFILES(children STARTS WITH 'x')", getValue = Map("children" -> GetValue))
      .build()
  }

  test("should prefer value hash join over nested index loops when lhs is small enough: index_backed_order_by-q15") {
    // Fewer roundtrips to the store the better the performance and not too many nodes on the RHS
    val query =
      """
        |MATCH (a:PROFILES), (b:PROFILES)
        |WHERE
        |    a.pets STARTS WITH "x" AND
        |    b.children STARTS WITH "x" AND
        |    a.pets = b.children
        |RETURN id(a), id(b)
        |""".stripMargin

    val planner = spdPlanner
      .setAllNodesCardinality(1632803)
      .setLabelCardinality("PROFILES", 1632803)
      .addNodeIndex(
        "PROFILES",
        Seq("gender", "pets", "children"),
        existsSelectivity = 371135.0 / 1632803,
        uniqueSelectivity = 1.0 / 253204.0
      )
      .addNodeIndex(
        "PROFILES",
        Seq("gender", "hair_color", "eye_color", "pets", "children"),
        existsSelectivity = 337392.0 / 1632803,
        uniqueSelectivity = 1.0 / 273680.0
      )
      .addNodeIndex(
        "PROFILES",
        Seq("pets", "children"),
        existsSelectivity = 371135.0 / 1632803,
        uniqueSelectivity = 1.0 / 247714.0
      )
      .addNodeIndex(
        "PROFILES",
        Seq("children"),
        existsSelectivity = 475697.0 / 1632803,
        uniqueSelectivity = 1.0 / 176353
      )
      .addNodeIndex("PROFILES", Seq("pets"), existsSelectivity = 700681.0 / 1632803, uniqueSelectivity = 1.0 / 268273.0)
      .build()

    planner.plan(query) shouldEqual planner.planBuilder()
      .produceResults("`id(a)`", "`id(b)`")
      .projection("id(a) AS `id(a)`", "id(b) AS `id(b)`")
      .valueHashJoin("cacheN[b.children] = cacheN[a.pets]")
      .|.nodeIndexOperator(
        "a:PROFILES(pets STARTS WITH 'x')", // reduces the number of nodes on the RHS
        getValue = Map("pets" -> GetValue)
      )
      .nodeIndexOperator("b:PROFILES(children STARTS WITH 'x')", getValue = Map("children" -> GetValue))
      .build()
  }

  test("should fetch batched properties before an expandAll") {
    val query =
      """
        |  MATCH (p)-[:KNOWS]-(s:Person) WHERE s.lastName <> p.lastName
        |  RETURN p.firstName,s.firstName
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan should equal(planner.subPlanBuilder()
      .projection("cacheN[p.firstName] AS `p.firstName`", "cacheN[s.firstName] AS `s.firstName`")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[p.firstName]")("NOT cacheN[s.lastName] = p.lastName")
      .expandAll("(s)-[:KNOWS]-(p)")
      .remoteBatchProperties("cacheNFromStore[s.lastName]", "cacheNFromStore[s.firstName]")
      .nodeByLabelScan("s", "Person")
      .build())
  }

  test(
    "should plan node hashjoin and pruning var expands: ldbc_bi_sf001: Read_10a"
  ) { // Test to ensure that cost estimate for remote batch properties helps
    val query =
      """
        |MATCH path=(startPerson:Person {id: $personId})-[:KNOWS*1..4]-(expertCandidatePerson:Person)
        |MATCH (expertCandidatePerson)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(:Country {name: $country})
        |WITH expertCandidatePerson, min(length(path)) AS shortestPathDistance
        |WHERE shortestPathDistance >=3
        |MATCH (expertCandidatePerson)<-[:POST_HAS_CREATOR]-(message:Message)
        |WHERE (message)-[:MESSAGE_HAS_TAG]->(:Tag)-[:HAS_TYPE]->(:TagClass {name: $tagClass})
        |MATCH (message)-[:MESSAGE_HAS_TAG]-(tag:Tag)
        |RETURN expertCandidatePerson.id, tag.name, count(DISTINCT message) AS messageCount
        |ORDER BY messageCount DESC, tag.name ASC, expertCandidatePerson.id ASC
        |LIMIT 100
        |""".stripMargin

    planner.plan(query).stripProduceResults shouldEqual planner.subPlanBuilder()
      .top(100, "messageCount DESC", "`tag.name` ASC", "`expertCandidatePerson.id` ASC")
      .aggregation(
        Seq("cacheN[expertCandidatePerson.id] AS `expertCandidatePerson.id`", "cacheN[tag.name] AS `tag.name`"),
        Seq("count(DISTINCT message) AS messageCount")
      )
      .remoteBatchProperties("cacheNFromStore[expertCandidatePerson.id]", "cacheNFromStore[tag.name]")
      .filter("tag:Tag")
      .expandAll("(message)-[:MESSAGE_HAS_TAG]-(tag)")
      .semiApply()
      .|.remoteBatchPropertiesWithFilter("cacheNFromStore[anon_3.name]")("anon_3.name = $tagClass")
      .|.filter("anon_3:TagClass")
      .|.expandAll("(anon_2)-[:HAS_TYPE]->(anon_3)")
      .|.filter("anon_2:Tag")
      .|.expandAll("(message)-[:MESSAGE_HAS_TAG]->(anon_2)")
      .|.argument("message")
      .filter("message:Message")
      .expandAll("(expertCandidatePerson)<-[:POST_HAS_CREATOR]-(message)")
      .filter("shortestPathDistance >= 3")
      .aggregation(Seq("expertCandidatePerson AS expertCandidatePerson"), Seq("min(anon_4) AS shortestPathDistance"))
      .nodeHashJoin("anon_0")
      .|.expandAll("(expertCandidatePerson)-[:IS_LOCATED_IN]->(anon_0)")
      .|.filter("expertCandidatePerson:Person")
      // we are testing that we run this operator
      .|.bfsPruningVarExpand(
        "(startPerson)-[:KNOWS*1..4]-(expertCandidatePerson)",
        depthName = Some("anon_4"),
        mode = ExpandAll
      ) // we are testing that we run this operator
      .|.nodeIndexOperator(
        "startPerson:Person(id = ???)",
        paramExpr = Some(parameter("personId", CTAny)),
        getValue = Map("id" -> DoNotGetValue),
        unique = true
      )
      .filter("anon_0:City")
      .expandAll("(anon_1)<-[:IS_PART_OF]-(anon_0)")
      .nodeIndexOperator(
        "anon_1:Country(name = ???)",
        paramExpr = Some(parameter("country", CTAny)),
        getValue = Map("name" -> DoNotGetValue)
      )
      .build()
  }

  test("should plan join when using join hints") { // simplified ldbc_bi_sf001: Read_2a_join_hint to only test joins
    val query =
      """
        |MATCH (tag:Tag)-[:HAS_TYPE]->(:TagClass {name: $tagClass})
        |OPTIONAL MATCH (message:Message)-[:MESSAGE_HAS_TAG]->(tag)
        |
        |// makes 100-1000x faster
        |USING JOIN ON tag
        |
        |WHERE $date <= message.creationDate
        |WITH tag, message.creationDate < $date /*secondWindowStart*/ AS isInFirstWindow
        |RETURN tag.name
        |""".stripMargin

    planner.plan(query).stripProduceResults shouldEqual planner.subPlanBuilder()
      .projection("cacheN[tag.name] AS `tag.name`")
      .remoteBatchProperties("cacheNFromStore[tag.name]")
      .projection("cacheN[message.creationDate] < $date AS isInFirstWindow")
      .remoteBatchProperties("cacheNFromStore[message.creationDate]")
      .leftOuterHashJoin("tag")
      .|.expandAll("(message)-[:MESSAGE_HAS_TAG]->(tag)")
      .|.nodeIndexOperator(
        "message:Message(creationDate >= ???)",
        paramExpr = Some(parameter("date", CTAny)),
        getValue = Map("creationDate" -> GetValue)
      )
      .filter("tag:Tag")
      .expandAll("(anon_0)<-[:HAS_TYPE]-(tag)")
      .nodeIndexOperator(
        "anon_0:TagClass(name = ???)",
        paramExpr = Some(parameter("tagClass", CTAny)),
        getValue = Map("name" -> DoNotGetValue)
      )
      .build()
  }

  test("should plan use plans with pre-fetched properties even when planning multiple cartesian products") {
    val query =
      """
        |MATCH (tag:Tag)-[:HAS_TYPE]->(:TagClass {name: $tagClass})
        |MATCH (message:Message)-[:MESSAGE_HAS_TAG]->(:Tag {name: $tagClass})
        |MATCH (person: Person {name: $tagClass})
        |WHERE $date <= message.creationDate
        |WITH tag, message.creationDate < $date /*secondWindowStart*/ AS isInFirstWindow, message.title AS messageTitle, person.name AS matchingPersonTag
        |RETURN tag.name, messageTitle, matchingPersonTag
        |""".stripMargin
    planner.plan(query).stripProduceResults should equal(planner.subPlanBuilder()
      .projection("cacheN[tag.name] AS `tag.name`")
      .projection(
        "cacheN[message.creationDate] < $date AS isInFirstWindow",
        "cacheN[message.title] AS messageTitle",
        "cacheN[person.name] AS matchingPersonTag"
      )
      .cartesianProduct()
      .|.cartesianProduct()
      .|.|.remoteBatchProperties("cacheNFromStore[tag.name]")
      .|.|.filter("tag:Tag")
      .|.|.expandAll("(anon_0)<-[:HAS_TYPE]-(tag)")
      .|.|.nodeIndexOperator(
        "anon_0:TagClass(name = ???)",
        paramExpr = Some(parameter("tagClass", CTAny)),
        getValue = Map("name" -> DoNotGetValue)
      )
      .|.remoteBatchPropertiesWithFilter("cacheNFromStore[message.creationDate]", "cacheNFromStore[message.title]")(
        "message.creationDate >= $date"
      )
      .|.filter("message:Message")
      .|.expandAll("(anon_1)<-[:MESSAGE_HAS_TAG]-(message)")
      .|.nodeIndexOperator(
        "anon_1:Tag(name = ???)",
        paramExpr = Some(parameter("tagClass", CTAny)),
        getValue = Map("name" -> DoNotGetValue)
      )
      .remoteBatchPropertiesWithFilter("cacheNFromStore[person.name]")("person.name = $tagClass")
      .nodeByLabelScan("person", "Person")
      .build())(SymmetricalLogicalPlanEquality)
  }

  test("should cache properties on the LHS when used in a nested index join") {
    val query =
      """
        |MATCH (person:Person), (city:City)
        |WHERE  person.firstName = city.name
        |RETURN *
        |""".stripMargin

    planner.plan(query) should equal(planner
      .planBuilder()
      .produceResults("city", "person")
      .apply()
      .|.remoteNodeIndexOperator(
        "person:Person(firstName = cacheN[city.name])",
        argumentIds = Set("city"),
        getValue = Map("firstName" -> DoNotGetValue)
      )
      .nodeIndexOperator("city:City(name)", getValue = Map("name" -> GetValue))
      .build())
  }

  test("should cache properties on the LHS when used in a nested index join: distinct match clauses") {
    val query =
      """
        |MATCH (person:Person)
        |WHERE person.id < 10000 // A filter to reduce the number of nodes on the LHS
        |MATCH (friend:Person { id: person.id })
        |RETURN person.id,friend.id
        |""".stripMargin
    // The person.id < 1000 filter reduces the number of nodes on the LHS, and since the RHS is a very selective index seek per LHS node,
    // the nested index join is preferred here over a value hash join.
    planner.plan(query) shouldEqual planner
      .planBuilder()
      .produceResults("`person.id`", "`friend.id`")
      .projection("cacheN[person.id] AS `person.id`", "cacheN[friend.id] AS `friend.id`")
      .apply()
      .|.remoteNodeIndexOperator(
        "friend:Person(id = cacheN[person.id])",
        argumentIds = Set("person"),
        getValue = Map("id" -> GetValue),
        unique = true
      )
      .nodeIndexOperator("person:Person(id < 10000)", getValue = Map("id" -> GetValue), unique = true)
      .build()
  }

  test("should push limit to run before remoteBatchProperties.") {
    val query =
      """
        |MATCH (p: Person {id:$id})
        |WITH p
        |MATCH (p)-[:KNOWS*1..3]-(poster:Person)<-[:POST_HAS_CREATOR]-(post:Message)
        |WITH p,
        |     {
        |        status: post.status,
        |        createdAt: post.createdAt,
        |        createdBy: poster.name
        |      } AS details
        |LIMIT 10
        |RETURN collect(details) AS details
        |""".stripMargin
    planner.plan(query).stripProduceResults shouldEqual planner.subPlanBuilder()
      .aggregation(Seq(), Seq("collect(details) AS details"))
      .projection(
        "{status: cacheN[post.status], createdAt: cacheN[post.createdAt], createdBy: cacheN[poster.name]} AS details"
      )
      .remoteBatchProperties("cacheNFromStore[post.status]", "cacheNFromStore[post.createdAt]")
      .limit(10)
      .filter("post:Message")
      .expandAll("(poster)<-[:POST_HAS_CREATOR]-(post)")
      .remoteBatchProperties("cacheNFromStore[poster.name]")
      .filter("poster:Person")
      .expand("(p)-[:KNOWS*1..3]-(poster)")
      .nodeIndexOperator(
        "p:Person(id = ???)",
        paramExpr = Some(parameter("id", CTAny)),
        getValue = Map("id" -> DoNotGetValue),
        unique = true
      )
      .build()
  }

  test("should report pushed down predicates to horizon when required") {
    val query =
      """
        |MATCH (p: Person {id:$id})<-[:POST_HAS_CREATOR]-()<-[:REPLY_OF]-(reply:Message)-[:POST_HAS_CREATOR]->(friend: Person)
        |WITH p,  friend, max(reply.creationDate) AS latestReply
        |WHERE friend.year=2000
        |RETURN p.name,latestReply,friend.name
        |""".stripMargin
    planner.plan(query).stripProduceResults shouldEqual planner.subPlanBuilder()
      .projection("cacheN[p.name] AS `p.name`", "cacheN[friend.name] AS `friend.name`")
      .remoteBatchProperties("cacheNFromStore[p.name]")
      .remoteBatchPropertiesWithFilter(
        "cacheNFromStore[friend.year]",
        "cacheNFromStore[friend.name]"
      )("friend.year = 2000")
      .aggregation(Seq("p AS p", "friend AS friend"), Seq("max(cacheN[reply.creationDate]) AS latestReply"))
      .remoteBatchProperties("cacheNFromStore[reply.creationDate]")
      .filter("NOT anon_2 = anon_0", "friend:Person")
      .expandAll("(reply)-[anon_2:POST_HAS_CREATOR]->(friend)")
      .filter("reply:Message")
      .expandAll("(anon_1)<-[:REPLY_OF]-(reply)")
      .expandAll("(p)<-[anon_0:POST_HAS_CREATOR]-(anon_1)")
      .nodeIndexOperator(
        "p:Person(id = ???)",
        paramExpr = Some(parameter("id", CTAny)),
        getValue = Map("id" -> DoNotGetValue),
        unique = true
      )
      .build()
  }

  test(
    "should only fetch property from pushed down predicates if no other properties are used from the same variable later in the query"
  ) {
    // remoteBatchPropertiesWithFilter will require at least one property to be fetched from shards
    val query =
      """
        |MATCH (p: Person {name: "Smith"})
        |RETURN p
        |""".stripMargin
    planner.plan(query).stripProduceResults shouldEqual planner.subPlanBuilder()
      .remoteBatchPropertiesWithFilter("cacheNFromStore[p.name]")("p.name = 'Smith'")
      .nodeByLabelScan("p", "Person")
      .build()
  }

  test("should not fetch property from pushed down predicates if other properties to fetch") {
    val query =
      """
        |MATCH (p: Person {name: "Smith"})
        |RETURN p.lastName
        |""".stripMargin
    planner.plan(query).stripProduceResults shouldEqual planner.subPlanBuilder()
      .projection("cacheN[p.lastName] AS `p.lastName`")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[p.lastName]")("p.name = 'Smith'")
      .nodeByLabelScan("p", "Person")
      .build()
  }

  test("Should push down expressions with operators that are constant at runtime") {
    val query =
      """
        |  MATCH (p:Person)
        |  WHERE p.age IS NOT NULL
        |    AND 50 > (p.age - 10) AND p.age < 20
        |  RETURN p
        |""".stripMargin
    val plan = planner.plan(query).stripProduceResults

    plan shouldEqual planner.subPlanBuilder()
      .remoteBatchPropertiesWithFilter("cacheNFromStore[p.age]")("p.age IS NOT NULL", "50 > p.age - 10", "p.age < 20")
      .nodeByLabelScan("p", "Person", IndexOrderNone)
      .build()
  }

  test("should inline predicates referencing a variable") {
    val query =
      """
        |WITH 'Patrick' AS friends_name
        |MATCH (person:Person {id:"ID"})-[knows:KNOWS]->(friend)
        |  WHERE friend.firstName = friends_name
        |RETURN
        |  friend.lastName""".stripMargin
    planner.plan(query).stripProduceResults shouldEqual planner.subPlanBuilder()
      .projection("cacheN[friend.lastName] AS `friend.lastName`")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[friend.lastName]")("friend.firstName = 'Patrick'")
      .expandAll("(person)-[:KNOWS]->(friend)")
      .projection("'Patrick' AS friends_name")
      .nodeIndexOperator("person:Person(id = 'ID')", unique = true)
      .build()
  }

  test("should inline predicates referencing an already cached variable") {
    val query =
      """
        |MATCH (person:Person {id:"ID"})-[knows:KNOWS]->(friend:Person)-[friend_knows:KNOWS]->(fof)
        |  WHERE friend_knows.creationDate < knows.creationDate
        |RETURN
        |  friend_knows.creationDate""".stripMargin
    planner.plan(query).stripProduceResults shouldEqual planner.subPlanBuilder()
      .projection("cacheR[friend_knows.creationDate] AS `friend_knows.creationDate`")
      .remoteBatchPropertiesWithFilter("cacheRFromStore[friend_knows.creationDate]")(
        "friend_knows.creationDate < cacheR[knows.creationDate]"
      )
      .filter("NOT friend_knows = knows")
      .expandAll("(friend)-[friend_knows:KNOWS]->()")
      .remoteBatchProperties("cacheRFromStore[knows.creationDate]")
      .filter("friend:Person")
      .expandAll("(person)-[knows:KNOWS]->(friend)")
      .nodeIndexOperator("person:Person(id = 'ID')", unique = true)
      .build()
  }

  test("Should inline predicate referencing argument from a nested exists query") {
    val query =
      """
        |MATCH (dog)<--({canAffordDog:
        |  EXISTS {
        |    WITH toBoolean(sum(0)) AS n1, dog AS dog RETURN 0
        |  }
        | }) RETURN dog.name AS name""".stripMargin
    planner.plan(query).stripProduceResults shouldEqual planner.subPlanBuilder()
      .projection("cacheN[dog.name] AS name")
      .remoteBatchProperties("cacheNFromStore[dog.name]")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[anon_0.canAffordDog]")("anon_0.canAffordDog = anon_2")
      .letSemiApply("anon_2")
      .|.projection("0 AS 0")
      .|.projection("toBoolean(anon_1) AS n1")
      .|.aggregation(Seq("dog AS dog"), Seq("sum(0) AS anon_1"))
      .|.argument("dog")
      .allRelationshipsScan("(anon_0)-[]->(dog)")
      .build()
  }

  test("date predicate should be pushed down to the shards") {
    val spdPlannerTemporal = spdPlanner.setAllNodesCardinality(10000)
      .build()

    val query =
      """
        |MATCH (n)
        |  WHERE n.birthDate > date('1995-09-14')
        |RETURN n.birthDate AS bDate
        |""".stripMargin
    val plan = spdPlannerTemporal.plan(query).stripProduceResults

    plan shouldEqual spdPlannerTemporal.subPlanBuilder()
      .projection("cacheN[n.birthDate] AS `bDate`")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[n.birthDate]")(
        "n.birthDate > RuntimeConstant(anon_0, date('1995-09-14'))"
      )
      .allNodeScan("n")
      .build()
  }

  test("date predicate without argument should not be pushed down to the shards") {
    val spdPlannerTemporal = spdPlanner.setAllNodesCardinality(10000)
      .build()

    val query =
      """
        |MATCH (n)
        |  WHERE n.prop > date()
        |RETURN n.prop AS nProp
        |""".stripMargin
    val plan = spdPlannerTemporal.plan(query).stripProduceResults

    plan shouldEqual spdPlannerTemporal.subPlanBuilder()
      .projection("cacheN[n.prop] AS nProp")
      .filter("cacheN[n.prop] > date()")
      .remoteBatchProperties("cacheNFromStore[n.prop]")
      .allNodeScan("n")
      .build()
  }

  test("datetime predicate should be pushed down to the shards") {
    val spdPlannerTemporal = spdPlanner.setAllNodesCardinality(10000)
      .build()

    val query =
      """
        |MATCH (n)
        |  WHERE n.birthDate > datetime('2010-01-01')
        |RETURN n.birthDate AS bDate
        |""".stripMargin
    val plan = spdPlannerTemporal.plan(query).stripProduceResults

    plan shouldEqual spdPlannerTemporal.subPlanBuilder()
      .projection("cacheN[n.birthDate] AS `bDate`")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[n.birthDate]")(
        "n.birthDate > RuntimeConstant(anon_0, datetime('2010-01-01'))"
      )
      .allNodeScan("n")
      .build()
  }

  test("datetime predicate without argument should not be pushed down to the shards") {
    val spdPlannerTemporal = spdPlanner.setAllNodesCardinality(10000)
      .build()

    val query =
      """
        |MATCH (n)
        |  WHERE n.prop > datetime()
        |RETURN n.prop AS nProp
        |""".stripMargin
    val plan = spdPlannerTemporal.plan(query).stripProduceResults

    plan shouldEqual spdPlannerTemporal.subPlanBuilder()
      .projection("cacheN[n.prop] AS nProp")
      .filter("cacheN[n.prop] > datetime()")
      .remoteBatchProperties("cacheNFromStore[n.prop]")
      .allNodeScan("n")
      .build()
  }

  test("localdatetime predicate should be pushed down to the shards") {
    val spdPlannerTemporal = spdPlanner.setAllNodesCardinality(10000)
      .build()

    val query =
      """
        |MATCH (n)
        |  WHERE n.birthDate > localdatetime('2010-01-01')
        |RETURN n.birthDate AS bDate
        |""".stripMargin
    val plan = spdPlannerTemporal.plan(query).stripProduceResults

    plan shouldEqual spdPlannerTemporal.subPlanBuilder()
      .projection("cacheN[n.birthDate] AS `bDate`")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[n.birthDate]")(
        "n.birthDate > RuntimeConstant(anon_0, localdatetime('2010-01-01'))"
      )
      .allNodeScan("n")
      .build()
  }

  test("localdatetime predicate without argument should not be pushed down to the shards") {
    val spdPlannerTemporal = spdPlanner.setAllNodesCardinality(10000)
      .build()

    val query =
      """
        |MATCH (n)
        |  WHERE n.prop > localdatetime()
        |RETURN n.prop AS nProp
        |""".stripMargin
    val plan = spdPlannerTemporal.plan(query).stripProduceResults

    plan shouldEqual spdPlannerTemporal.subPlanBuilder()
      .projection("cacheN[n.prop] AS nProp")
      .filter("cacheN[n.prop] > localdatetime()")
      .remoteBatchProperties("cacheNFromStore[n.prop]")
      .allNodeScan("n")
      .build()
  }

  test("localtime predicate should be pushed down to the shards") {
    val spdPlannerTemporal = spdPlanner.setAllNodesCardinality(10000)
      .build()

    val query =
      """
        |MATCH (n)
        |  WHERE n.birthTime > localtime('04:10')
        |RETURN n.birthTime AS bTime
        |""".stripMargin
    val plan = spdPlannerTemporal.plan(query).stripProduceResults

    plan shouldEqual spdPlannerTemporal.subPlanBuilder()
      .projection("cacheN[n.birthTime] AS `bTime`")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[n.birthTime]")(
        "n.birthTime > RuntimeConstant(anon_0, localtime('04:10'))"
      )
      .allNodeScan("n")
      .build()
  }

  test("localtime predicate without argument should not be pushed down to the shards") {
    val spdPlannerTemporal = spdPlanner.setAllNodesCardinality(10000)
      .build()

    val query =
      """
        |MATCH (n)
        |  WHERE n.prop > localtime()
        |RETURN n.prop AS nProp
        |""".stripMargin
    val plan = spdPlannerTemporal.plan(query).stripProduceResults

    plan shouldEqual spdPlannerTemporal.subPlanBuilder()
      .projection("cacheN[n.prop] AS nProp")
      .filter("cacheN[n.prop] > localtime()")
      .remoteBatchProperties("cacheNFromStore[n.prop]")
      .allNodeScan("n")
      .build()
  }

  test("time predicate should be pushed down to the shards") {
    val spdPlannerTemporal = spdPlanner.setAllNodesCardinality(10000)
      .build()

    val query =
      """
        |MATCH (n)
        |  WHERE n.birthTime > time('21:40:32+01:00')
        |RETURN n.birthTime AS bTime
        |""".stripMargin
    val plan = spdPlannerTemporal.plan(query).stripProduceResults

    plan shouldEqual spdPlannerTemporal.subPlanBuilder()
      .projection("cacheN[n.birthTime] AS `bTime`")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[n.birthTime]")(
        "n.birthTime > RuntimeConstant(anon_0, time('21:40:32+01:00'))"
      )
      .allNodeScan("n")
      .build()
  }

  test("time predicate without argument should not be pushed down to the shards") {
    val spdPlannerTemporal = spdPlanner.setAllNodesCardinality(10000)
      .build()

    val query =
      """
        |MATCH (n)
        |  WHERE n.prop > time()
        |RETURN n.prop AS nProp
        |""".stripMargin
    val plan = spdPlannerTemporal.plan(query).stripProduceResults

    plan shouldEqual spdPlannerTemporal.subPlanBuilder()
      .projection("cacheN[n.prop] AS nProp")
      .filter("cacheN[n.prop] > time()")
      .remoteBatchProperties("cacheNFromStore[n.prop]")
      .allNodeScan("n")
      .build()
  }

  test("duration predicate should be pushed down to the shards") {
    val spdPlannerTemporal = spdPlanner.setAllNodesCardinality(10000)
      .build()

    val query =
      """
        |MATCH (n)
        |  WHERE n.executionDuration > duration('PT1M')
        |RETURN n.executionDuration AS execDur
        |""".stripMargin
    val plan = spdPlannerTemporal.plan(query).stripProduceResults

    plan shouldEqual spdPlannerTemporal.subPlanBuilder()
      .projection("cacheN[n.executionDuration] AS `execDur`")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[n.executionDuration]")(
        "n.executionDuration > RuntimeConstant(anon_0, duration('PT1M'))"
      )
      .allNodeScan("n")
      .build()
  }

  test("should not pushdown predicates if transaction state is not empty") {
    // remotebatchpropertieswithfilter will require at least one property to be fetched from shards
    val query =
      """
        |MATCH (p: Person {name: "Smith"})
        |RETURN p.name
        |""".stripMargin

    val plannerWithNonEmptyTransactionState =
      spdPlanner.setTxStateHasChanges().setAllNodesCardinality(10000).setLabelCardinality("Person", 10000)
        .build()

    plannerWithNonEmptyTransactionState.plan(
      query
    ).stripProduceResults shouldEqual plannerWithNonEmptyTransactionState.subPlanBuilder()
      .projection("cacheN[p.name] AS `p.name`")
      .filter("cacheN[p.name] = 'Smith'")
      .remoteBatchProperties("cacheNFromStore[p.name]")
      .nodeByLabelScan("p", "Person")
      .build()
  }

  test("should not push down predicates on the horizon when tx state is non-empty") {
    val query =
      """
        |MATCH (p: Person {id:$id})<-[:POST_HAS_CREATOR]-()<-[:REPLY_OF]-(reply:Message)-[:POST_HAS_CREATOR]->(friend: Person)
        |WITH p,  friend, max(reply.creationDate) AS latestReply
        |WHERE friend.year=2000
        |RETURN p.name,latestReply,friend.name
        |""".stripMargin

    val plannerWithNonEmptyTransactionState =
      spdPlanner
        .setTxStateHasChanges()
        .setAllNodesCardinality(3181725)
        .setLabelCardinality("Person", 9892)
        .setLabelCardinality("Message", 3055774)
        .setRelationshipCardinality("()-[:POST_HAS_CREATOR]->()", 1003605)
        .setRelationshipCardinality("(:Message)-[:POST_HAS_CREATOR]->()", 1003605)
        .setRelationshipCardinality("()-[:POST_HAS_CREATOR]->(:Person)", 1003605)
        .setRelationshipCardinality("(:Message)-[:POST_HAS_CREATOR]->(:Person)", 1003605)
        .setRelationshipCardinality("()-[:REPLY_OF]->()", 2052169)
        .setRelationshipCardinality("()-[:REPLY_OF]->(:Message)", 2052169)
        .setRelationshipCardinality("(:Message)-[:REPLY_OF]->()", 0)
        .setRelationshipCardinality("(:Message)-[:REPLY_OF]->(:Message)", 0)
        .setRelationshipCardinality("()-[]->()", 2052169 + 1003605 + 180623)
        .addNodeIndex("Person", List("id"), existsSelectivity = 1.0, uniqueSelectivity = 1.0 / 9892.0, isUnique = true)
        .build()

    plannerWithNonEmptyTransactionState.plan(
      query
    ).stripProduceResults shouldEqual plannerWithNonEmptyTransactionState.subPlanBuilder()
      .projection("cacheN[p.name] AS `p.name`", "cacheN[friend.name] AS `friend.name`")
      .remoteBatchProperties("cacheNFromStore[p.name]")
      .filter("cacheN[friend.year] = 2000")
      .remoteBatchProperties("cacheNFromStore[friend.year]", "cacheNFromStore[friend.name]")
      .aggregation(Seq("p AS p", "friend AS friend"), Seq("max(cacheN[reply.creationDate]) AS latestReply"))
      .remoteBatchProperties("cacheNFromStore[reply.creationDate]")
      .filter("NOT anon_2 = anon_0", "friend:Person")
      .expandAll("(reply)-[anon_2:POST_HAS_CREATOR]->(friend)")
      .filter("reply:Message")
      .expandAll("(anon_1)<-[:REPLY_OF]-(reply)")
      .expandAll("(p)<-[anon_0:POST_HAS_CREATOR]-(anon_1)")
      .nodeIndexOperator(
        "p:Person(id = ???)",
        paramExpr = Some(parameter("id", CTAny)),
        getValue = Map("id" -> DoNotGetValue),
        unique = true
      )
      .build()
  }

  test("should cache property if entity was stored in map at some point") {
    val query =
      """MATCH (n)-[:KNOWS]->(known)
        |WITH n, {other: known} as nodes
        |WITH n, head(collect(nodes)) AS latest
        |WITH n, latest.other AS oneFriend
        |WHERE n.name = oneFriend.name
        |RETURN n, oneFriend
        |""".stripMargin

    planner.plan(query) should equal(
      planner.planBuilder()
        .produceResults("n", "oneFriend")
        .filter("cacheN[n.name] = cacheN[oneFriend.name]")
        .remoteBatchProperties("cacheNFromStore[n.name]", "cacheNFromStore[oneFriend.name]")
        .projection("latest.other AS oneFriend")
        .projection("head(anon_0) AS latest")
        .aggregation(Seq("n AS n"), Seq("collect(nodes) AS anon_0"))
        .projection("{other: known} AS nodes")
        .relationshipTypeScan("(n)-[:KNOWS]->(known)", IndexOrderNone)
        .build()
    )
  }

  test("should correctly rewrite ListIRExpression in InPlannerRemoteBatchingWithoutPushdown") {
    // Use a write query to get in the code path for InPlannerRemoteBatchingWithoutPushdown
    val query =
      """
        |MATCH ()<-[r:KNOWS]-()
        |WHERE single(abt IN [()<-[:REPLY_OF]-() | 1] WHERE true)
        |DELETE r
        |""".stripMargin

    planner.plan(query) should equal(
      planner.planBuilder()
        .produceResults()
        .emptyResult()
        .deleteRelationship("r")
        .filter("single(abt IN anon_1 WHERE true)")
        .rollUpApply("anon_1", "anon_0")
        .|.projection("1 AS anon_0")
        .|.relationshipTypeScan("()-[:REPLY_OF]->()", IndexOrderNone)
        .relationshipTypeScan("()-[r:KNOWS]->()", IndexOrderNone)
        .build()
    )
  }

  test("should properly handle property access in QPP") {
    val query =
      """
        |MATCH (a)
        |      ((left)-[rel]->(right) WHERE rel.prop = a.prop)+
        |      (b)
        |WITH rel, a.prop AS prop
        |RETURN rel, prop
        |""".stripMargin

    val planner = spdPlanner
      .setAllNodesCardinality(10000)
      .setAllRelationshipsCardinality(100000)
      .build()

    val `(a) ((left) ... (right))+ (b)` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "a",
      end = "b",
      innerStart = "left",
      innerEnd = "right",
      groupNodes = Set.empty,
      groupRelationships = Set(("rel", "rel")),
      innerRelationships = Set("rel"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    planner.plan(query).stripProduceResults shouldEqual
      planner.subPlanBuilder()
        .projection("cacheN[a.prop] AS `prop`")
        .repeatTrail(`(a) ((left) ... (right))+ (b)`)
        .|.remoteBatchPropertiesWithFilter("cacheRFromStore[rel.prop]")("rel.prop = cacheN[a.prop]")
        .|.filter("isRepeatTrailUnique(rel)")
        .|.expandAll("(left)-[rel]->(right)")
        .|.argument("left", "a")
        .remoteBatchProperties("cacheNFromStore[a.prop]")
        .allNodeScan("a")
        .build()
  }

  test("should properly handle multiple external properties accessed in QPP") {
    val query =
      """
        |MATCH (start)-[x]->(a)
        |      ((left)-[rel]->(right) WHERE start.min < rel.prop < x.max)+
        |      (b)
        |WITH rel, start.min AS startMin, x.max AS xMax
        |RETURN rel, startMin, xMax
        |""".stripMargin

    val planner = spdPlanner
      .setAllNodesCardinality(10000)
      .setAllRelationshipsCardinality(100000)
      .build()

    val `(a) ((left) ... (right))+ (b)` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "a",
      end = "b",
      innerStart = "left",
      innerEnd = "right",
      groupNodes = Set.empty,
      groupRelationships = Set(("rel", "rel")),
      innerRelationships = Set("rel"),
      previouslyBoundRelationships = Set("x"),
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    planner.plan(query).stripProduceResults shouldEqual
      planner.subPlanBuilder()
        .projection("cacheN[start.min] AS `startMin`", "cacheR[x.max] AS `xMax`")
        .repeatTrail(`(a) ((left) ... (right))+ (b)`)
        .|.remoteBatchPropertiesWithFilter("cacheRFromStore[rel.prop]")(
          "cacheN[start.min] < rel.prop",
          "rel.prop < cacheR[x.max]"
        )
        .|.filter(isRepeatTrailUnique("rel"))
        .|.expandAll("(left)-[rel]->(right)")
        .|.argument("left", "start", "x")
        .remoteBatchProperties("cacheRFromStore[x.max]")
        .expandAll("(start)-[x]->(a)")
        .remoteBatchProperties("cacheNFromStore[start.min]")
        .allNodeScan("start")
        .build()
  }

  test("should properly handle property access in allReduce") {
    val query =
      """
        |MATCH (a)
        |      ((left)-[rel]->(right))+
        |      (b)
        |  WHERE allReduce(
        |    sum = 0,
        |    step IN rel | sum + step.prop,
        |    sum < a.prop
        |  )
        |RETURN rel
        |""".stripMargin

    val planner = spdPlanner
      .setAllNodesCardinality(10000)
      .setAllRelationshipsCardinality(100000)
      .build()

    val `(a) ((left) ... (right))+ (b) WITH sum = 0` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "a",
      end = "b",
      innerStart = "left",
      innerEnd = "right",
      groupNodes = Set.empty,
      groupRelationships = Set(("rel", "rel")),
      innerRelationships = Set("rel"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      expansionMode = ExpandAll,
      accumulators = Set(
        ("0", "sum", "sum")
      )
    )

    planner.plan(CypherVersion.Cypher25, query).stripProduceResults shouldEqual
      planner.subPlanBuilder()
        .repeatTrail(`(a) ((left) ... (right))+ (b) WITH sum = 0`)
        .|.filter("cacheN[a.prop] > sum")
        .|.projection("sum + cacheR[rel.prop] AS sum")
        .|.remoteBatchProperties("cacheRFromStore[rel.prop]")
        .|.filter(isRepeatTrailUnique("rel"))
        .|.expandAll("(left)-[rel]->(right)")
        .|.argument("left", "a", "sum")
        .remoteBatchProperties("cacheNFromStore[a.prop]")
        .allNodeScan("a")
        .build()
  }

  test("need not fetch properties from composite index when the filter is applied on the index itself") {
    val query =
      """
        |MATCH (p:Person)
        |WHERE p.firstName='Spongebob' AND p.lastName='Squarepants'
        |RETURN p
        |""".stripMargin
    planner.plan(query) shouldEqual planner.planBuilder()
      .produceResults("p")
      .nodeIndexOperator(
        "p:Person(firstName = 'Spongebob', lastName = 'Squarepants')",
        getValue = Map("firstName" -> DoNotGetValue, "lastName" -> DoNotGetValue),
        supportPartitionedScan = false
      )
      .build()
  }

  test("should fetch properties from composite index for filtering on multiple properties") {
    val query =
      """
        |MATCH (p:Person)
        |WHERE p.firstName STARTS WITH "J" AND p.lastName STARTS WITH "Smith"
        |RETURN p
        |""".stripMargin

    planner.plan(query) shouldEqual
      planner.planBuilder()
        .produceResults("p")
        .filter("cacheN[p.lastName] STARTS WITH 'Smith'")
        .nodeIndexOperator(
          "p:Person(firstName STARTS WITH 'J', lastName)",
          indexOrder = IndexOrderNone,
          getValue = Map("firstName" -> DoNotGetValue, "lastName" -> GetValue),
          supportPartitionedScan = false
        )
        .build()
  }

  test("should fetch property from a single index when filtering using composed queries") {
    val query =
      """
        |MATCH (p:Person)
        |WHERE p.firstName STARTS WITH "Sponge" AND p.firstName ENDS WITH "bob"
        |RETURN p
        |""".stripMargin
    planner.plan(query) shouldEqual planner.planBuilder()
      .produceResults("p")
      .filter("cacheN[p.firstName] ENDS WITH 'bob'")
      .nodeIndexOperator(
        "p:Person(firstName STARTS WITH 'Sponge')",
        indexOrder = IndexOrderNone,
        getValue = Map("firstName" -> GetValue)
      )
      .build()
  }

  test("should prefer plans with index seeks rather than scan with filters") {
    val lowSelectivityPlanner = spdPlanner
      .setAllNodesCardinality(10)
      .setLabelCardinality("Person", 10)
      .addNodeIndex("Person", List("firstName"), existsSelectivity = 1.0, uniqueSelectivity = 0.1, isUnique = false)
      .build()

    val query =
      """
        |MATCH (p:Person)
        |WHERE p.firstName STARTS WITH ''
        |RETURN p.firstName
        |""".stripMargin

    lowSelectivityPlanner.plan(query) shouldEqual lowSelectivityPlanner.planBuilder()
      .produceResults("`p.firstName`")
      .projection("cacheN[p.firstName] AS `p.firstName`")
      .nodeIndexOperator(
        "p:Person(firstName STARTS WITH '')",
        indexOrder = IndexOrderNone,
        getValue = Map("firstName" -> GetValue)
      )
      .build()
  }

  test("should not push predicate below an aliasing projection which predicate depends on") {
    val query =
      """
        |MATCH (a)-[r]->(b)
        |WITH a AS x
        |WHERE x.prop > 123
        |RETURN x
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .remoteBatchPropertiesWithFilter("cacheNFromStore[x.prop]")("x.prop > 123")
      .projection("a AS x")
      .allRelationshipsScan("(a)-[]->()")
      .build()
  }

  test("should not push predicate below a function call projection which predicate depends on") {
    val query =
      """
        |MATCH (a)-[r]->(b)
        |WITH startNode(r) AS x
        |WHERE x.prop > 123
        |RETURN x
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .remoteBatchPropertiesWithFilter("cacheNFromStore[x.prop]")("x.prop > 123")
      .projection("startNode(r) AS x")
      .allRelationshipsScan("()-[r]->()")
      .build()
  }

  test("should plan limit before remoteBatchPropertiesWithFilter when within the same projection") {
    val query =
      """
        |MATCH (n), (m) WITH n, m LIMIT 20000 WHERE n.prop > 5 RETURN n, m LIMIT 25
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    if (orderPreserving) {
      plan shouldEqual planner.subPlanBuilder()
        .limit(25)
        .remoteBatchPropertiesWithFilter("cacheNFromStore[n.prop]")("n.prop > 5")
        .limit(20000)
        .cartesianProduct()
        .|.allNodeScan("m")
        .allNodeScan("n")
        .build()
    } else {
      plan shouldEqual planner.subPlanBuilder()
        .limit(25)
        .filter("cacheN[n.prop] > 5")
        .limit(20000)
        .cartesianProduct()
        .|.allNodeScan("m")
        .remoteBatchProperties("cacheNFromStore[n.prop]")
        .allNodeScan("n")
        .build()
    }
  }

  test("should get value from index when property is used later") {
    val query =
      """
        |MATCH (n:Person {firstName: "Me"}) RETURN n.firstName
        |""".stripMargin

    val plan = planner.plan(query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("`n.firstName`")
      .projection("cacheN[n.firstName] AS `n.firstName`")
      .nodeIndexOperator("n:Person(firstName = 'Me')", getValue = Map("firstName" -> GetValue))
      .build()
  }

  test("should get value from index when property is used later - when index entity has been renamed") {
    val query =
      """
        |MATCH (n:Person {firstName: "Me"}) WITH n AS m RETURN m.firstName
        |""".stripMargin

    val plan = planner.plan(query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("`m.firstName`")
      .projection(Map("m.firstName" -> cachedNodeProp("n", "firstName", "m")))
      .projection("n AS m")
      .nodeIndexOperator("n:Person(firstName = 'Me')", getValue = Map("firstName" -> GetValue))
      .build()
  }

  test(
    "should get value from index when property is used later - when index entity has been renamed and other property is returned"
  ) {
    val query =
      """
        |MATCH (n:Person {firstName: "Me"}) WITH n AS m RETURN m.firstName, m.lastName
        |""".stripMargin

    val plan = planner.plan(query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("`m.firstName`", "`m.lastName`")
      .projection(Map(
        "m.firstName" -> cachedNodeProp("n", "firstName", "m"),
        "m.lastName" -> cachedNodeProp("n", "lastName", "m")
      ))
      .remoteBatchProperties(cachedNodeProp("n", "lastName", "m", knownToAccessStore = true))
      .projection("n AS m")
      .nodeIndexOperator("n:Person(firstName = 'Me')", getValue = Map("firstName" -> GetValue))
      .build()
  }

  test(
    "should not get value from index when property not used - when index entity has been renamed"
  ) {
    val query =
      """
        |MATCH (n:Person {firstName: "Me"}) WITH n as m RETURN m.lastName
        |""".stripMargin

    val plan = planner.plan(query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("`m.lastName`")
      .projection("cacheN[m.lastName] AS `m.lastName`")
      .remoteBatchProperties("cacheNFromStore[m.lastName]")
      .projection("n AS m")
      .nodeIndexOperator("n:Person(firstName = 'Me')", getValue = Map("firstName" -> DoNotGetValue))
      .build()
  }

  // Eager Aggregation clears the cache, so we cannot get renamed variable from cache after aggregation
  test(
    "should run remoteBatchProperties after aggregation on indexed property"
  ) {
    val query =
      """
        |MATCH (n:Person {firstName: "Me"}) WITH n, COUNT(*) AS c RETURN n.firstName, c
        |""".stripMargin

    val plan = planner.plan(query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("`n.firstName`", "c")
      .projection("cacheN[n.firstName] AS `n.firstName`")
      .remoteBatchProperties("cacheNFromStore[n.firstName]")
      .aggregation(Seq("n AS n"), Seq("count(*) AS c"))
      .nodeIndexOperator("n:Person(firstName = 'Me')", getValue = Map("firstName" -> GetValue)) // DoNotGetValue?
      .build()
  }

  test(
    "should not get value from index when indexed node is returned after renaming in aggregation"
  ) {
    val query =
      """
        |MATCH (n:Person {firstName: "Me"}) WITH n AS m, COUNT(*) AS c RETURN m, c
        |""".stripMargin

    val plan = planner.plan(query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("m", "c")
      .aggregation(Seq("n AS m"), Seq("count(*) AS c"))
      .nodeIndexOperator("n:Person(firstName = 'Me')", getValue = Map("firstName" -> DoNotGetValue))
      .build()
  }

  test(
    "should not get value from index when property is not used afterwards"
  ) {
    val query =
      """
        |MATCH (n:Person {firstName: "Me"}) WITH n AS m, 1 AS n RETURN n
        |""".stripMargin

    val plan = planner.plan(query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("n")
      .projection("n AS m", "1 AS n")
      .nodeIndexOperator("n:Person(firstName = 'Me')", getValue = Map("firstName" -> DoNotGetValue))
      .build()
  }

  test(
    "should get value from index when used in another predicate later after renaming"
  ) {
    val query =
      """
        |MATCH (n:Person {firstName: $fn}) WITH n AS m WHERE m.firstName <> "me" RETURN m.lastName
        |""".stripMargin

    val plan = planner.plan(query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("`m.lastName`")
      .projection(Map("m.lastName" -> cachedNodeProp("n", "lastName", "m")))
      .remoteBatchProperties(cachedNodeProp("n", "lastName", "m", knownToAccessStore = true))
      .filter(not(equals(cachedNodeProp("n", "firstName", "m"), literalString("me"))))
      .projection("n AS m")
      .nodeIndexOperator(
        "n:Person(firstName = ???)",
        paramExpr = Some(parameter("fn", CTAny)),
        getValue = Map("firstName" -> GetValue)
      )
      .build()
  }

  test("should SEEK from a selective index and filter by the rest of the predicates on the shard.") {
    val query =
      """
        |MATCH (n: Person {firstName: "John"})
        |WHERE n.lastName CONTAINS "Smith"
        |RETURN n.firstName, n.lastName
        |""".stripMargin

    val customIndexPlanner = spdPlanner
      .setAllNodesCardinality(10000)
      .setLabelCardinality("Person", 10000)
      .addNodeIndex(
        "Person",
        List("firstName"),
        existsSelectivity = 1.0,
        uniqueSelectivity = 0.1,
        isUnique = false
      )
      .addNodeIndex("Person", List("firstName"), existsSelectivity = 1.0, uniqueSelectivity = 0.1, isUnique = false)
      .build()

    val plan = customIndexPlanner.plan(query).stripProduceResults
    plan shouldEqual customIndexPlanner.subPlanBuilder()
      .projection("cacheN[n.firstName] AS `n.firstName`", "cacheN[n.lastName] AS `n.lastName`")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[n.lastName]")("n.lastName CONTAINS 'Smith'")
      .nodeIndexOperator("n:Person(firstName = 'John')", getValue = Map("firstName" -> GetValue))
      .build()
  }

  test("should prefilter with has labels before nested subqueries") {
    val query =
      """
        |MATCH (this:Person)
        |WHERE (this.name STARTS WITH 'A' AND EXISTS {
        |  MATCH (this)<-[:POST_HAS_CREATOR]-(message1:Message)
        |  WHERE message1.language = 'en' AND EXISTS {
        |    MATCH (message1)<-[:REPLY_OF]-(reply1:Message)
        |    WHERE reply1.language = 'esp' AND EXISTS {
        |      MATCH (reply1)-[:COMMENT_HAS_CREATOR]->(person1:Person)
        |      WHERE person1.name STARTS WITH 'B' AND NOT (EXISTS {
        |        MATCH (person1)<-[:POST_HAS_CREATOR]-(message2:Message)
        |        WHERE message2.language = 'en'
        |     })
        |    }
        |  }
        |})
        |RETURN this.name
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("cacheN[this.name] AS `this.name`")
      .semiApply()
      .|.remoteBatchPropertiesWithFilter("cacheNFromStore[message1.language]")("message1.language = 'en'")
      .|.semiApply()
      .|.|.remoteBatchPropertiesWithFilter("cacheNFromStore[reply1.language]")("reply1.language = 'esp'")
      .|.|.semiApply()
      .|.|.|.antiSemiApply()
      .|.|.|.|.remoteBatchPropertiesWithFilter("cacheNFromStore[message2.language]")("message2.language = 'en'")
      .|.|.|.|.filter("message2:Message")
      .|.|.|.|.expandAll("(person1)<-[:POST_HAS_CREATOR]-(message2)")
      .|.|.|.|.argument("person1")
      .|.|.|.remoteBatchPropertiesWithFilter("cacheNFromStore[person1.name]")("person1.name STARTS WITH 'B'")
      .|.|.|.filter("person1:Person")
      .|.|.|.expandAll("(reply1)-[:COMMENT_HAS_CREATOR]->(person1)")
      .|.|.|.argument("reply1")
      .|.|.filter("reply1:Message")
      .|.|.expandAll("(message1)<-[:REPLY_OF]-(reply1)")
      .|.|.argument("message1")
      .|.filter("message1:Message")
      .|.expandAll("(this)<-[:POST_HAS_CREATOR]-(message1)")
      .|.argument("this")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[this.name]")("this.name STARTS WITH 'A'")
      .nodeByLabelScan("this", "Person")
      .build()
  }

  test("index seek should identify renames within a call subquery") {
    val query =
      """
        |MATCH (n:Person)
        |WHERE n.firstName STARTS WITH "A"
        |CALL (*) {
        |  WITH n AS m
        |  WHERE m.firstName <> "Adam"
        |  RETURN m AS k
        |}
        |RETURN k.firstName, k.lastName
        |""".stripMargin

    val plan = planner.plan(query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("`k.firstName`", "`k.lastName`")
      .projection(Map(
        "k.firstName" -> cachedNodeProp("n", "firstName", "k"),
        "k.lastName" -> cachedNodeProp("n", "lastName", "k")
      ))
      .remoteBatchProperties(cachedNodeProp("n", "lastName", "k", knownToAccessStore = true))
      .projection("m AS k")
      .filter(not(equals(cachedNodeProp("n", "firstName", "m"), literalString("Adam"))))
      .projection("n AS m")
      .nodeIndexOperator("n:Person(firstName STARTS WITH 'A')", getValue = Map("firstName" -> GetValue))
      .build()
  }

  test("index seek should identify renames within a call subquery with union") {
    val query =
      """
        |MATCH (n:Person)
        |WHERE n.firstName STARTS WITH "A"
        |CALL (*) {
        |  WITH n AS m
        |  WHERE m.lastName <> "Smith"
        |  RETURN m AS k
        |  UNION
        |  WITH n AS m
        |  WHERE m.age > 25
        |  RETURN m AS k
        |}
        |RETURN k.firstName
        |""".stripMargin

    val plan = planner.plan(query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("`k.firstName`")
      .projection(Map("k.firstName" -> cachedNodeProp("n", "firstName", "k")))
      .apply()
      .|.distinct("n AS n", "k AS k")
      .|.union()
      .|.|.projection("k AS k")
      .|.|.projection("m AS k")
      .|.|.remoteBatchPropertiesWithFilter(cachedNodeProp("n", "age", "m", knownToAccessStore = true))(greaterThan(
        prop("m", "age"),
        literalInt(25)
      ))
      .|.|.projection("n AS m")
      .|.|.argument("n")
      .|.projection("k AS k")
      .|.projection("m AS k")
      .|.filter(not(equals(
        cachedNodeProp("n", "lastName", "m"),
        literal("Smith")
      )))
      .|.projection("n AS m")
      .|.argument("n")
      .remoteBatchProperties("cacheNFromStore[n.lastName]")
      .nodeIndexOperator("n:Person(firstName STARTS WITH 'A')", getValue = Map("firstName" -> GetValue))
      .build()
  }

  test(
    "index seek should not fetch values if rename on one side of union matches a newly introduced variable on the other side"
  ) {
    val query =
      """
        |MATCH (n:Person)
        |WHERE n.firstName STARTS WITH "A"
        |CALL (*) {
        |  MATCH (m:Person)
        |  WHERE m.lastName <> "Smith"
        |  RETURN m AS k
        |  UNION
        |  WITH n AS m
        |  WHERE m.age > 25
        |  RETURN m AS k
        |}
        |RETURN k.firstName
        |""".stripMargin

    val plan = planner.plan(query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("`k.firstName`")
      .projection(Map("k.firstName" -> cachedNodeProp("k", "firstName", "k")))
      .remoteBatchProperties("cacheNFromStore[k.firstName]")
      .apply()
      .|.distinct("n AS n", "k AS k")
      .|.union()
      .|.|.projection("k AS k")
      .|.|.projection("m AS k")
      .|.|.remoteBatchPropertiesWithFilter(cachedNodeProp("m", "age", "m", knownToAccessStore = true))(greaterThan(
        prop("m", "age"),
        literalInt(25)
      ))
      .|.|.projection("n AS m")
      .|.|.argument("n")
      .|.projection("k AS k")
      .|.projection("m AS k")
      .|.remoteBatchPropertiesWithFilter(cachedNodeProp("m", "lastName", "m", knownToAccessStore = true))(not(equals(
        prop("m", "lastName"),
        literal("Smith")
      )))
      .|.nodeByLabelScan("m", "Person", IndexOrderNone, "n")
      .nodeIndexOperator("n:Person(firstName STARTS WITH 'A')", getValue = Map("firstName" -> DoNotGetValue))
      .build()
  }

  test(
    "index seek should not get values for renames within a call subquery with unions each renaming a different variable"
  ) {
    val query =
      """
        |MATCH (n:Person)-[:KNOWS]->(p:Person)
        |WHERE n.firstName STARTS WITH "A"
        |CALL (*) {
        |  WITH n AS m
        |  WHERE m.lastName <> "Smith"
        |  RETURN m AS k
        |  UNION
        |  WITH p AS m
        |  WHERE m.firstName <> "Alice"
        |  RETURN m AS k
        |}
        |RETURN k.firstName
        |""".stripMargin

    val plan = planner.plan(query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("`k.firstName`")
      .projection("cacheN[k.firstName] AS `k.firstName`")
      .remoteBatchProperties("cacheNFromStore[k.firstName]")
      .apply()
      .|.distinct("p AS p", "k AS k", "n AS n")
      .|.union()
      .|.|.projection("k AS k")
      .|.|.projection("m AS k")
      .|.|.remoteBatchPropertiesWithFilter("cacheNFromStore[m.firstName]")("NOT m.firstName = 'Alice'")
      .|.|.projection("p AS m")
      .|.|.argument("n", "p")
      .|.projection("k AS k")
      .|.projection("m AS k")
      .|.filter(not(equals(cachedNodeProp("n", "lastName", "m"), literal("Smith"))))
      .|.projection("n AS m")
      .|.argument("n", "p")
      .filter("p:Person")
      .expandAll("(n)-[:KNOWS]->(p)")
      .remoteBatchProperties("cacheNFromStore[n.lastName]")
      .nodeIndexOperator("n:Person(firstName STARTS WITH 'A')", getValue = Map("firstName" -> DoNotGetValue))
      .build()
  }

  test(
    "index seek should get values for renames within a call subquery with unions each renaming a different variable if the property is used within the union"
  ) {
    val query =
      """
        |MATCH (n:Person)-[:KNOWS]->(p:Person)
        |WHERE n.firstName STARTS WITH "A"
        |CALL (*) {
        |  WITH n AS m
        |  WHERE m.firstName <> "Adam"
        |  RETURN m AS k
        |  UNION
        |  WITH p AS m
        |  WHERE m.firstName <> "Alice"
        |  RETURN m AS k
        |}
        |RETURN k.firstName
        |""".stripMargin

    val plan = planner.plan(query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("`k.firstName`")
      .projection("cacheN[k.firstName] AS `k.firstName`")
      .remoteBatchProperties("cacheNFromStore[k.firstName]")
      .apply()
      .|.distinct("p AS p", "k AS k", "n AS n")
      .|.union()
      .|.|.projection("k AS k")
      .|.|.projection("m AS k")
      .|.|.remoteBatchPropertiesWithFilter("cacheNFromStore[m.firstName]")("NOT m.firstName = 'Alice'")
      .|.|.projection("p AS m")
      .|.|.argument("n", "p")
      .|.projection("k AS k")
      .|.projection("m AS k")
      .|.filter(not(equals(cachedNodeProp("n", "firstName", "m"), literalString("Adam"))))
      .|.projection("n AS m")
      .|.argument("n", "p")
      .filter("p:Person")
      .expandAll("(n)-[:KNOWS]->(p)")
      .nodeIndexOperator("n:Person(firstName STARTS WITH 'A')", getValue = Map("firstName" -> GetValue))
      .build()
  }

  test("Plan RBPs before the Cartesian Product and use index on both sides") {
    val query =
      """MATCH (p:Person), (c:City)
        |WHERE p.id <= 5 AND c.name STARTS WITH 'M'
        |WITH p SKIP 0
        |RETURN p.lastName
        |""".stripMargin

    // LeafPlanOptions should consider placing RemoteBatchProperties on top of the index p:Person(id <= 5) for bestExtraPropertiesResult
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("cacheN[p.lastName] AS `p.lastName`")
      .skip(0)
      .cartesianProduct()
      .|.nodeIndexOperator("c:City(name STARTS WITH 'M')", getValue = Map("name" -> DoNotGetValue))
      .remoteBatchProperties("cacheNFromStore[p.lastName]")
      .nodeIndexOperator("p:Person(id <= 5)", getValue = Map("id" -> DoNotGetValue), unique = true)
      .build()
  }

  test("should not replace expressions in remoteBatchPropertiesWithFilter that are aggregations") {
    val planner =
      spdPlanner
        .setAllNodesCardinality(100)
        .build()

    val plan =
      planner.plan(
        """WITH count(*) - 5 AS count
          |MATCH (a)
          |WHERE a.prop < count
          |return a""".stripMargin
      )

    plan should equal(
      planner.planBuilder()
        .produceResults("a")
        .remoteBatchPropertiesWithFilter("cacheNFromStore[a.prop]")("a.prop < count")
        .apply()
        .|.allNodeScan("a", "count")
        .projection("anon_0 - 5 AS count")
        .aggregation(Seq(), Seq("count(*) AS anon_0"))
        .argument()
        .build()
    )
  }

  test("should push down parameters or constants to shard") {
    val planner =
      spdPlanner
        .setAllNodesCardinality(1000)
        .build()

    val query =
      """MATCH (txn1)
        |WHERE txn1.transactionDate <= $date AND txn1.duration <= duration({days:35})
        |RETURN txn1.duration""".stripMargin

    val plan = planner.plan(query)

    plan should equal(
      planner.planBuilder().produceResults("`txn1.duration`")
        .projection("cacheN[txn1.duration] AS `txn1.duration`")
        // as we only use txn1.duration, we only need to fetch that property
        .remoteBatchPropertiesWithFilter("cacheNFromStore[txn1.duration]")(
          "txn1.duration <= RuntimeConstant(anon_0, duration({days:35}))",
          "txn1.transactionDate <= $date"
        )
        .allNodeScan("txn1")
        .build()
    )
  }

  test("should push down variables of parameters or constants to shard") {
    val planner =
      spdPlanner
        .setAllNodesCardinality(1000)
        .build()

    val query =
      """WITH $date AS date, duration({days:35}) AS duration
        |MATCH (txn1)
        |WHERE txn1.transactionDate <= date AND txn1.duration <= duration
        |RETURN txn1""".stripMargin

    val plan = planner.plan(query)

    plan should equal(
      planner.planBuilder()
        .produceResults("txn1")
        .remoteBatchPropertiesWithFilter("cacheNFromStore[txn1.duration]")(
          "txn1.duration <= RuntimeConstant(anon_0, duration({days:35}))",
          "txn1.transactionDate <= $date"
        )
        .apply()
        .|.allNodeScan("txn1")
        .projection("$date AS date", "RuntimeConstant(anon_0, duration({days:35})) AS duration")
        .argument()
        .build()
    )
  }

  test("should push down nested variables of parameters or constants to shard") {
    val planner =
      spdPlanner
        .setAllNodesCardinality(1000)
        .build()

    val query =
      """WITH $date AS curDate
        |WITH curDate, curDate - duration({days:35}) AS curDate35
        |MATCH (txn1)
        |WHERE txn1.transactionDate <= curDate35
        |RETURN txn1""".stripMargin

    val plan = planner.plan(query)

    plan should equal(
      planner.planBuilder()
        .produceResults("txn1")
        .remoteBatchPropertiesWithFilter("cacheNFromStore[txn1.transactionDate]")(
          "txn1.transactionDate <= RuntimeConstant(anon_0, $date - duration({days:35}))"
        )
        .apply()
        .|.allNodeScan("txn1")
        .projection("curDate - RuntimeConstant(anon_1, duration({days:35})) AS curDate35")
        .projection("$date AS curDate")
        .argument()
        .build()
    )
  }

  test(
    "should replace projected variables with corresponding parameters or constants in remoteBatchPropertiesWithFilter"
  ) {
    val planner =
      spdPlanner
        .setAllNodesCardinality(1000)
        .build()

    val query =
      """WITH $param1 AS param1, $param2 AS param2
        |MATCH (n)
        |WHERE n.prop < (param1 + 5) * 2 - param2
        |RETURN n""".stripMargin

    val plan = planner.plan(query)

    val expressionToPushdown =
      subtract(
        multiply(
          add(
            parameter("param1", CTAny),
            literalInt(5)
          ),
          literalInt(2)
        ),
        parameter("param2", CTAny)
      )

    plan should equal(
      planner.planBuilder()
        .produceResults("n")
        .remoteBatchPropertiesWithFilter("cacheNFromStore[n.prop]")(
          lessThan(
            prop("n", "prop"),
            expressionToPushdown
          )
        )
        .projection("$param1 AS param1", "$param2 AS param2") // this is projection is not needed
        .allNodeScan("n")
        .build()
    )
  }

  test("should fetch batch properties before expand even if the variable is renamed") {
    val query =
      """
        |MATCH (message:Message)-[:POST_HAS_CREATOR]->(n: Person)
        |WITH n AS contentCreator
        |RETURN contentCreator.name, contentCreator.id
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection(Map(
        "contentCreator.name" -> cachedNodeProp("n", "name", "contentCreator"),
        "contentCreator.id" -> cachedNodeProp("n", "id", "contentCreator")
      ))
      .projection("n AS contentCreator")
      .filter("message:Message")
      .expandAll("(n)<-[:POST_HAS_CREATOR]-(message)")
      .remoteBatchProperties("cacheNFromStore[n.name]", "cacheNFromStore[n.id]")
      .nodeByLabelScan("n", "Person", IndexOrderNone)
      .build()
  }

  test("should fetch properties before Apply-Optional, no properties used on RHS of Apply") {
    val query =
      """
        |MATCH (p:Person)
        |OPTIONAL MATCH (p)-[:KNOWS]->(someone)
        |RETURN p.prop
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("cacheN[p.prop] AS `p.prop`")
      .optionalExpandAll("(p)-[:KNOWS]->()")
      .remoteBatchProperties("cacheNFromStore[p.prop]")
      .nodeByLabelScan("p", "Person")
      .build()
  }

  test("should fetch properties before Apply-Optional, property used on RHS of Apply") {
    val query =
      """
        |MATCH (p:Person)
        |OPTIONAL MATCH (p)-[:KNOWS]->(someone)
        |  WHERE someone.name = p.name
        |RETURN p.prop
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("cacheN[p.prop] AS `p.prop`")
      .apply()
      .|.optional("p")
      .|.remoteBatchPropertiesWithFilter("cacheNFromStore[someone.name]")("someone.name = cacheN[p.name]")
      .|.expandAll("(p)-[:KNOWS]->(someone)")
      .|.argument("p")
      .remoteBatchProperties("cacheNFromStore[p.prop]", "cacheNFromStore[p.name]")
      .nodeByLabelScan("p", "Person", IndexOrderNone)
      .build()
  }

  test("should fetch used properties before Apply-Optional, other properties fetched at the lowest cardinality point") {
    val query =
      """
        |MATCH (p:Person)
        |OPTIONAL MATCH (p)-[:KNOWS]->(someone)
        |  WHERE someone.name = p.name
        |WITH p WHERE id(p) = 1 // reduce cardinality
        |RETURN p.prop
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("cacheN[p.prop] AS `p.prop`")
      .remoteBatchProperties("cacheNFromStore[p.prop]")
      .filter("id(p) = 1")
      .apply()
      .|.optional("p")
      .|.remoteBatchPropertiesWithFilter("cacheNFromStore[someone.name]")("someone.name = cacheN[p.name]")
      .|.expandAll("(p)-[:KNOWS]->(someone)")
      .|.argument("p")
      .remoteBatchProperties("cacheNFromStore[p.name]")
      .nodeByLabelScan("p", "Person")
      .build()
  }

}
