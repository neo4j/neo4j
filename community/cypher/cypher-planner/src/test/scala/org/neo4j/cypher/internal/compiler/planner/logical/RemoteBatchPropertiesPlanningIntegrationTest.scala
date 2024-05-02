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
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class RemoteBatchPropertiesPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningIntegrationTestSupport {

  // Graph counts based on a subset of LDBC SF 1
  final private val planner =
    plannerBuilder()
      .withSetting(
        GraphDatabaseInternalSettings.cypher_property_caching_mode,
        GraphDatabaseInternalSettings.PropertyCachingMode.REMOTE_BATCH_PROPERTIES
      )
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
      .addNodeIndex("Person", List("id"), existsSelectivity = 1.0, uniqueSelectivity = 1.0, isUnique = true)
      .addNodeIndex("Message", List("creationDate"), existsSelectivity = 1.0, uniqueSelectivity = 3033542.0 / 3055774.0)
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

  test("should also batch properties used in filters, even if just once") {
    val query =
      """MATCH (person:Person)-[knows:KNOWS]->(friend:Person)
        |  WHERE person.firstName = friend.firstName AND knows.creationDate < $max_creation_date
        |RETURN person.lastName AS personLastName,
        |       friend.lastName AS friendLastName,
        |       knows.creationDate AS knowsSince""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("personLastName", "friendLastName", "knowsSince")
      .projection(
        "cacheN[person.lastName] AS personLastName",
        "cacheN[friend.lastName] AS friendLastName",
        "cacheR[knows.creationDate] AS knowsSince"
      )
      .remoteBatchProperties("cacheNFromStore[person.lastName]", "cacheNFromStore[friend.lastName]")
      .filter(
        "cacheN[person.firstName] = cacheN[friend.firstName]",
        "cacheR[knows.creationDate] < $max_creation_date",
        "person:Person"
      )
      .remoteBatchProperties("cacheRFromStore[knows.creationDate]", "cacheNFromStore[person.firstName]")
      .expandAll("(friend)<-[knows:KNOWS]-(person)")
      .remoteBatchProperties("cacheNFromStore[friend.firstName]")
      .nodeByLabelScan("friend", "Person")
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
        paramExpr = Some(ExplicitParameter("Person", CTAny)(InputPosition.NONE)),
        getValue = Map("id" -> GetValue),
        unique = true
      )
      .build()
  }

  test("should batch properties of renamed entities") {
    val query =
      """MATCH (person:Person)
        |  WHERE person.creationDate < $max_creation_date
        |WITH person AS earlyAdopter, person.creationDate AS earlyAdopterSince ORDER BY earlyAdopterSince LIMIT 10
        |MATCH (earlyAdopter)-[knows:KNOWS]->(friend:Person)
        |RETURN earlyAdopter.lastName AS personLastName,
        |       friend.lastName AS friendLastName""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("personLastName", "friendLastName")
      .projection(Map(
        "personLastName" -> CachedProperty(
          // notice how `originalEntity` and `entityVariable` differ here
          originalEntity = Variable("person")(InputPosition.NONE),
          entityVariable = Variable("earlyAdopter")(InputPosition.NONE),
          PropertyKeyName("lastName")(InputPosition.NONE),
          NODE_TYPE
        )(InputPosition.NONE),
        "friendLastName" -> CachedProperty(
          originalEntity = Variable("friend")(InputPosition.NONE),
          entityVariable = Variable("friend")(InputPosition.NONE),
          PropertyKeyName("lastName")(InputPosition.NONE),
          NODE_TYPE
        )(InputPosition.NONE)
      ))
      .remoteBatchProperties("cacheNFromStore[friend.lastName]")
      .filter("friend:Person")
      .expandAll("(earlyAdopter)-[knows:KNOWS]->(friend)")
      .projection("person AS earlyAdopter")
      .remoteBatchProperties("cacheNFromStore[person.lastName]")
      .top(10, "earlyAdopterSince ASC")
      .projection("cacheN[person.creationDate] AS earlyAdopterSince")
      .filter("cacheN[person.creationDate] < $max_creation_date")
      .remoteBatchProperties("cacheNFromStore[person.creationDate]")
      .nodeByLabelScan("person", "Person")
      .build()
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
      .expandAll("(friend)<-[knows:KNOWS]-(person)")
      .nodeByLabelScan("friend", "Person")
      .build()
  }

  // Arguably we will want to relax this rule, and limit the number of round-trips to the shards when filters aren't very selective
  test("should batch properties wherever cardinality is the smallest") {
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
      .remoteBatchProperties("cacheNFromStore[friend.firstName]", "cacheNFromStore[friend.lastName]")
      .filter("NOT cacheN[person.id] = cacheN[friend.id]") // This filter has a very small impact on cardinality (40.6238 -> 40.6197), we could probably batch all friend properties together
      .remoteBatchProperties("cacheNFromStore[friend.id]")
      .expand("(person)-[anon_0:KNOWS*1..2]-(friend)")
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
        "cacheNFromStore[message.imageFile]",
        "cacheNFromStore[friend.lastName]",
        "cacheNFromStore[friend.id]",
        "cacheNFromStore[friend.firstName]",
        "cacheNFromStore[message.content]"
      )
      .top(20, "`message.creationDate` DESC", "`message.id` ASC")
      .projection("cacheN[message.creationDate] AS `message.creationDate`", "cacheN[message.id] AS `message.id`")
      .remoteBatchProperties("cacheNFromStore[message.id]")
      .filter("cacheN[message.creationDate] < $Date0")
      .remoteBatchProperties("cacheNFromStore[message.creationDate]")
      .expandAll("(friend)<-[anon_0:POST_HAS_CREATOR|COMMENT_HAS_CREATOR]-(message)")
      .projection("friend AS friend")
      .filter("NOT person = friend")
      .bfsPruningVarExpand("(person)-[:KNOWS*1..2]-(friend)")
      .nodeIndexOperator(
        "person:Person(id = ???)",
        paramExpr = Some(ExplicitParameter("Person", CTAny)(InputPosition.NONE)),
        unique = true
      )
      .build()
  }
}
