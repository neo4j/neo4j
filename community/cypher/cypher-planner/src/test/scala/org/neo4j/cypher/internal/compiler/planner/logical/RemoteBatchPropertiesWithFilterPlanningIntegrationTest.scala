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
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.Predicate
import org.neo4j.cypher.internal.logical.builder.TestNFABuilder
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandInto
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath
import org.neo4j.cypher.internal.planner.spi.DatabaseMode
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import java.lang.Boolean.TRUE

class RemoteBatchPropertiesWithFilterPlanningIntegrationTest extends CypherFunSuite
    with LogicalPlanningIntegrationTestSupport with AstConstructionTestSupport {

  private val spdPlanner = plannerBuilder()
    .setDatabaseMode(DatabaseMode.SHARDED)
    .withSetting(
      GraphDatabaseInternalSettings.cypher_remote_batch_properties_implementation,
      RemoteBatchPropertiesImplementation.PLANNER
    )
    .withSetting(GraphDatabaseInternalSettings.push_predicates_into_remote_batch_properties, TRUE)

  // Graph counts based on a subset of LDBC SF 1
  final private val planner = spdPlanner
    .setAllNodesCardinality(3181725)
    .setLabelCardinality("Person", 9892)
    .setLabelCardinality("Message", 3055774)
    .setLabelCardinality("Post", 1003605)
    .setLabelCardinality("Comment", 2052169)
    .setRelationshipCardinality("()-[:KNOWS]->()", 180623)
    .setRelationshipCardinality("(:Person)-[:KNOWS]->()", 180623)
    .setRelationshipCardinality("()-[:KNOWS]->(:Person)", 180623)
    .setRelationshipCardinality("(:Person)-[:KNOWS]->(:Person)", 180623)
    .setRelationshipCardinality("()-[:POST_HAS_CREATOR]->()", 1003605)
    .setRelationshipCardinality("(:Message)-[:POST_HAS_CREATOR]->()", 1003605)
    .setRelationshipCardinality("(:Post)-[:POST_HAS_CREATOR]->()", 1003605)
    .setRelationshipCardinality("()-[:POST_HAS_CREATOR]->(:Person)", 1003605)
    .setRelationshipCardinality("(:Message)-[:POST_HAS_CREATOR]->(:Person)", 1003605)
    .setRelationshipCardinality("()-[:COMMENT_HAS_CREATOR]->()", 2052169)
    .setRelationshipCardinality("(:Message)-[:COMMENT_HAS_CREATOR]->()", 2052169)
    .setRelationshipCardinality("(:Comment)-[:COMMENT_HAS_CREATOR]->()", 2052169)
    .setRelationshipCardinality("()-[:COMMENT_HAS_CREATOR]->(:Person)", 2052169)
    .setRelationshipCardinality("(:Message)-[:COMMENT_HAS_CREATOR]->(:Person)", 2052169)
    .addNodeIndex("Person", List("id"), existsSelectivity = 1.0, uniqueSelectivity = 1.0 / 9892.0, isUnique = true)
    .addNodeIndex("Person", List("firstName"), existsSelectivity = 1.0, uniqueSelectivity = 1 / 1323.0)
    .addNodeIndex("Message", List("creationDate"), existsSelectivity = 1.0, uniqueSelectivity = 3033542.0 / 3055774.0)
    .addRelationshipIndex("COMMENT_HAS_CREATOR", List("location"), existsSelectivity = 1.0, uniqueSelectivity = 0.1)
    .addRelationshipIndex(
      "COMMENT_HAS_CREATOR",
      List("id"),
      existsSelectivity = 1.0,
      uniqueSelectivity = 1.0 / 2052169,
      isUnique = true
    )
    .build()

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
      .remoteBatchPropertiesWithFilter("cacheNFromStore[person.firstName]")("cacheN[person.firstName] IS NOT NULL")
      .nodeIndexOperator(
        "person:Person(id = ???)",
        paramExpr = Some(parameter("Person", CTAny)),
        getValue = Map("id" -> DoNotGetValue),
        unique = true
      )
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

    val query = """
                  |OPTIONAL MATCH (n0)-[r1]->(n1) WHERE n0.prop = 42
                  |MATCH (a:L0 {prop:42})-[r2]->(n1), (n0)
                  |RETURN a.prop AS expandIntoProp, n0.prop AS standaloneProp
                  |""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("expandIntoProp", "standaloneProp")
      .projection("cacheN[a.prop] AS expandIntoProp", "cacheN[n0.prop] AS standaloneProp")
      .expandInto("(a)-[r2]->(n1)")
      .filterExpression(assertIsNode("n0"))
      .apply()
      .|.nodeIndexOperator(
        "a:L0(prop = 42)",
        argumentIds = Set("n0", "n1", "r1"),
        getValue = Map("prop" -> GetValue)
      )
      .optional()
      .expandAll("(n0)-[r1]->(n1)")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[n0.prop]")("cacheN[n0.prop] = 42")
      .allNodeScan("n0")
      .build()
  }

  test("should also batch properties used in filters, even if just once") {
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
      .filter("cacheN[person.lastName] = cacheN[friend.lastName]", "person:Person")
      .remoteBatchPropertiesWithFilter(
        "cacheNFromStore[friend.lastName]",
        "cacheNFromStore[person.lastName]",
        "cacheRFromStore[knows.creationDate]",
        "cacheNFromStore[person.firstName]",
        "cacheNFromStore[friend.firstName]"
      )("cacheR[knows.creationDate] < $max_creation_date")
      .expandAll("(friend)<-[knows:KNOWS]-(person)")
      .nodeByLabelScan("friend", "Person")
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
        .|.optional("person", "knows", "friend")
        .|.remoteBatchPropertiesWithFilter("cacheNFromStore[message.creationDate]", "cacheNFromStore[message.id]")(
          "cacheN[message.creationDate] IS NOT NULL"
        )
        .|.expandAll("(friend)<-[has_creator:POST_HAS_CREATOR|COMMENT_HAS_CREATOR]-(message)")
        .|.argument("friend", "person", "knows")
        .filter("cacheN[person.firstName] = cacheN[friend.firstName]")
        .remoteBatchProperties(
          "cacheNFromStore[person.firstName]",
          "cacheNFromStore[friend.firstName]",
          "cacheNFromStore[friend.lastName]"
        )
        .expand("(person)-[knows:KNOWS*1..2]-(friend)")
        .nodeIndexOperator(
          "person:Person(id = ???)",
          paramExpr = Some(parameter("Person", CTAny)),
          getValue =
            Map("id" -> DoNotGetValue), // context.plannerState.accessedProperties contains `person.id`, we need to list projected values in a different way
          unique = true
        )
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
      .|.filter("p:Message")
      .|.remoteBatchPropertiesWithFilter("cacheNFromStore[p.creationDate]")(
        "cacheN[p.creationDate] < $max_creation_date"
      )
      .|.expandAll("(anon_1)<-[anon_0:POST_HAS_CREATOR]-(p)")
      .|.nodeIndexOperator("anon_1:Person(firstName = 'Smith')", getValue = Map("firstName" -> DoNotGetValue))
      .projection("p AS p")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[p.creationDate]")("cacheN[p.creationDate] > $max_creation_date")
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
        |       friend.lastName AS friendLastName""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("personLastName", "friendLastName")
      .projection(Map(
        "personLastName" -> cachedNodeProp(
          // notice how `originalEntity` and `entityVariable` differ here
          variable = "person",
          propKey = "lastName",
          currentVarName = "earlyAdopter"
        ),
        "friendLastName" -> cachedNodeProp(variable = "friend", propKey = "lastName", currentVarName = "friend")
      ))
      .remoteBatchProperties("cacheNFromStore[friend.lastName]")
      .filter("friend:Person")
      .expandAll("(earlyAdopter)-[knows:KNOWS]->(friend)")
      .projection("person AS earlyAdopter")
      .top(10, "earlyAdopterSince ASC")
      .projection("cacheN[person.creationDate] AS earlyAdopterSince")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[person.lastName]", "cacheNFromStore[person.creationDate]")(
        "cacheN[person.lastName] IS NOT NULL",
        "cacheN[person.creationDate] < $max_creation_date"
      )
      .nodeByLabelScan("person", "Person")
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
      )("cacheN[message.creationDate] < $Date0")
      .expandAll("(friend)<-[anon_0:POST_HAS_CREATOR|COMMENT_HAS_CREATOR]-(message)")
      .projection("friend AS friend")
      .filter("NOT person = friend")
      .bfsPruningVarExpand("(person)-[:KNOWS*1..2]-(friend)")
      .nodeIndexOperator(
        "person:Person(id = ???)",
        paramExpr = Some(parameter("Person", CTAny)),
        getValue = Map("id" -> DoNotGetValue),
        unique = true
      )
      .build()
  }

  test("should batch properties in relationship indexes") {
    val query =
      """MATCH (person:Person)<-[r:COMMENT_HAS_CREATOR]-(message)
        |USING INDEX r:COMMENT_HAS_CREATOR(location)
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
      .remoteBatchProperties("cacheNFromStore[message.content]", "cacheNFromStore[message.imageFile]")
      .filterExpression(hasLabels("person", "Person"))
      .remoteBatchPropertiesWithFilter("cacheNFromStore[person.lastName]", "cacheNFromStore[person.firstName]")(
        "cacheN[person.firstName] IS NOT NULL"
      )
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
      .remoteBatchProperties("cacheNFromStore[message.content]", "cacheNFromStore[message.imageFile]")
      .filterExpressionOrString(
        hasLabels("person", "Person")
      )
      .remoteBatchPropertiesWithFilter("cacheNFromStore[person.lastName]", "cacheNFromStore[person.firstName]")(
        "cacheN[person.firstName] IS NOT NULL"
      )
      .relationshipIndexOperator(
        "(message)-[r:COMMENT_HAS_CREATOR(id = ???)]->(person)",
        paramExpr = Some(ExplicitParameter("CommentCreatorId", CTAny)(InputPosition.NONE)),
        getValue = Map("id" -> GetValue),
        unique = true
      )
      .build()
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
      .selectOrSemiApply("anon_4")
      .|.filterExpression(hasLabels("anon_3", "Person"))
      .|.remoteBatchPropertiesWithFilter("cacheNFromStore[anon_3.lastName]")("cacheN[anon_3.lastName] = 'Smith'")
      .|.expandAll("(a)-[anon_2:KNOWS]-(anon_3)")
      .|.argument("a")
      .letSelectOrAntiSemiApply("anon_4", "cacheN[a.age] > 30")
      .|.expandAll("(anon_0)-[anon_1:POST_HAS_CREATOR]->(n)")
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
        .selectOrAntiSemiApply("anon_4")
        .|.filterExpression(hasLabels("anon_3", "Person"))
        .|.remoteBatchPropertiesWithFilter("cacheNFromStore[anon_3.lastName]")("cacheN[anon_3.lastName] = 'Smith'")
        .|.expandAll("(a)-[anon_2:KNOWS]-(anon_3)")
        .|.filter("cacheN[a.lastName] = 'Smyth'")
        .|.argument("a")
        .letSemiApply("anon_4")
        .|.filterExpression(hasLabels("anon_1", "Person"))
        .|.remoteBatchPropertiesWithFilter("cacheNFromStore[anon_1.lastName]")("cacheN[anon_1.lastName] = 'Smyth'")
        .|.expandAll("(a)-[anon_0:KNOWS]-(anon_1)")
        .|.remoteBatchPropertiesWithFilter("cacheNFromStore[a.lastName]")("cacheN[a.lastName] = 'Smith'")
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
        .remoteBatchProperties("cacheNFromStore[a.name]")
        .cartesianProduct()
        .|.nodeByLabelScan("a", "Person")
        .antiSemiApply()
        .|.expandInto("(b)-[anon_0:KNOWS]->(anon_1)")
        .|.nodeIndexOperator(
          "anon_1:Person(firstName = 'Jon')",
          argumentIds = Set("b"),
          getValue = Map("firstName" -> DoNotGetValue)
        )
        .nodeIndexOperator("b:Person(firstName = 'John')", getValue = Map("firstName" -> GetValue))
        .build()
    )
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
        ) // TODO: this shortestPath should eventually be rewritten into a stateful shortest path to allow batching of properties.
        .|.argument("a", "b")
        .cartesianProduct()
        .|.remoteBatchPropertiesWithFilter("cacheNFromStore[b.lastName]")("cacheN[b.lastName] = 'Smith'")
        .|.nodeByLabelScan("b", "Person")
        .remoteBatchPropertiesWithFilter("cacheNFromStore[a.lastName]")("cacheN[a.lastName] = 'Smith'")
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
      .filterExpression(greaterThan(
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
        StatefulShortestPath.Selector.Shortest(1),
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
      .remoteBatchPropertiesWithFilter("cacheNFromStore[b.lastName]", "cacheNFromStore[b.firstName]")(
        "cacheN[b.lastName] = 'Smith'"
      )
      .allNodeScan("b")
      .build())
  }

  test("should plan pruning var expand if it has remoteBatchPropertiesWithFilter") {
    val query =
      """
        |MATCH path=(:Person {id:$Person})-[:KNOWS*1..3]-(friend)
        |WHERE friend.firstName=$Name
        |RETURN friend.firstName, min(length(path)) AS distance
        |""".stripMargin

    planner.plan(query).stripProduceResults shouldEqual
      planner.subPlanBuilder()
        .aggregation(Seq("cacheN[friend.firstName] AS `friend.firstName`"), Seq("min(anon_1) AS distance"))
        .remoteBatchPropertiesWithFilter("cacheNFromStore[friend.firstName]")("cacheN[friend.firstName] = $Name")
        .bfsPruningVarExpand("(anon_0)-[:KNOWS*1..3]-(friend)", depthName = Some("anon_1"), mode = ExpandAll)
        .nodeIndexOperator(
          "anon_0:Person(id = ???)",
          paramExpr = Some(parameter("Person", CTAny)),
          getValue = Map("id" -> DoNotGetValue),
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
      .remoteBatchProperties("cacheNFromStore[person.name]")
      .filter("cacheN[person.age] = maxAge")
      .aggregation(Seq("person AS person"), Seq("MAX(cacheN[person.age]) AS maxAge"))
      .remoteBatchProperties("cacheNFromStore[person.age]")
      .allNodeScan("person")
      .build()
  }
}
