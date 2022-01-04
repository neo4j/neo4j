/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.cypher.internal.javacompat.NotificationTestSupport.TestProcedures
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_BINDING_VAR_LENGTH_RELATIONSHIP
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_BTREE_INDEX_SYNTAX
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_COERCION_OF_LIST_TO_BOOLEAN
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_CREATE_CONSTRAINT_ON_ASSERT_SYNTAX
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_CREATE_INDEX_SYNTAX
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_CREATE_PROPERTY_EXISTENCE_CONSTRAINT_SYNTAX
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_DROP_CONSTRAINT_SYNTAX
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_DROP_INDEX_SYNTAX
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_HEX_LITERAL_SYNTAX
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_OCTAL_LITERAL_SYNTAX
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_PERIODIC_COMMIT
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_POINTS_COMPARE
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_PROCEDURE
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_PROCEDURE_RETURN_FIELD
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_PROPERTY_EXISTENCE_SYNTAX
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_SELF_REFERENCE_TO_VARIABLE_IN_CREATE_PATTERN
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_USE_OF_PATTERN_EXPRESSION
import org.neo4j.graphdb.impl.notification.NotificationDetail
import org.neo4j.graphdb.schema.IndexSettingImpl.FULLTEXT_EVENTUALLY_CONSISTENT
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_CARTESIAN_MAX
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_WGS84_MIN
import org.neo4j.kernel.impl.index.schema.GenericNativeIndexProvider
import org.neo4j.kernel.impl.index.schema.fusion.NativeLuceneFusionIndexProviderFactory30
import org.scalatest.BeforeAndAfterAll

abstract class DeprecationAcceptanceTestBase extends CypherFunSuite with BeforeAndAfterAll with DeprecationTestSupport {

  override def beforeAll(): Unit = {
    // Used for testing deprecated procedures
    dbms.registerProcedure(classOf[TestProcedures])
  }

  override def afterAll(): Unit = {
    dbms.shutdown()
  }

  // DEPRECATED PROCEDURE THINGS

  test("deprecated procedure calls") {
    val queries = Seq("CALL oldProc()", "CALL oldProc() RETURN 1")
    val detail = NotificationDetail.Factory.deprecatedName("oldProc", "newProc")
    assertNotificationInSupportedVersions(queries, DEPRECATED_PROCEDURE, detail)
  }

  test("deprecated procedure result field") {
    val query = "CALL changedProc() YIELD oldField RETURN oldField"
    val detail = NotificationDetail.Factory.deprecatedField("changedProc", "oldField")
    assertNotificationInSupportedVersions(query, DEPRECATED_PROCEDURE_RETURN_FIELD, detail)
  }

  // DEPRECATED INDEX / CONSTRAINT SYNTAX in 4.X

  test("deprecated create index syntax") {
    assertNotificationInSupportedVersions("CREATE INDEX ON :Label(prop)", DEPRECATED_CREATE_INDEX_SYNTAX)
  }

  test("deprecated create btree index syntax") {
    // Note: This index syntax was introduced in 4.X

    // CREATE BTREE INDEX ...
    assertNotificationInSupportedVersions_4_X("CREATE BTREE INDEX FOR (n:Label) ON (n.prop)", DEPRECATED_BTREE_INDEX_SYNTAX)
    assertNotificationInSupportedVersions_4_X("CREATE BTREE INDEX name FOR ()-[r:TYPE]-() ON (r.prop)", DEPRECATED_BTREE_INDEX_SYNTAX)

    // CREATE INDEX ... OPTIONS { <btree options> }
    assertNotificationInSupportedVersions_4_X(
      s"""CREATE INDEX FOR (n:Label) ON (n.prop)
         |OPTIONS {IndexProvider: '${GenericNativeIndexProvider.DESCRIPTOR.name()}'}""".stripMargin,
      DEPRECATED_BTREE_INDEX_SYNTAX
    )
    assertNotificationInSupportedVersions_4_X(
      s"""CREATE INDEX FOR ()-[r:TYPE]-() ON (r.prop)
         |OPTIONS {indexprovider: '${NativeLuceneFusionIndexProviderFactory30.DESCRIPTOR.name()}'}""".stripMargin,
      DEPRECATED_BTREE_INDEX_SYNTAX
    )
    assertNotificationInSupportedVersions_4_X(
      s"""CREATE INDEX FOR (n:Label) ON (n.prop)
         |OPTIONS {IndexConfig: {`${SPATIAL_CARTESIAN_MAX.getSettingName}`: [40, 60]}}""".stripMargin,
      DEPRECATED_BTREE_INDEX_SYNTAX
    )
    assertNotificationInSupportedVersions_4_X(
      s"""CREATE INDEX FOR ()-[r:TYPE]-() ON (r.prop)
         |OPTIONS {indexconfig: {`${SPATIAL_WGS84_MIN.getSettingName}`: [-40, -60]}}""".stripMargin,
      DEPRECATED_BTREE_INDEX_SYNTAX
    )
    assertNoNotificationInSupportedVersions_4_X(
      s"""CREATE INDEX FOR (n:Label) ON (n.prop)
         |OPTIONS {indexconfig: {`${FULLTEXT_EVENTUALLY_CONSISTENT.getSettingName}`: false}}""".stripMargin,
      DEPRECATED_BTREE_INDEX_SYNTAX
    )
  }

  test("deprecated drop index syntax") {
    assertNotificationInSupportedVersions("DROP INDEX ON :Label(prop)", DEPRECATED_DROP_INDEX_SYNTAX)
  }

  test("deprecated drop node key constraint syntax") {
    assertNotificationInSupportedVersions("DROP CONSTRAINT ON (n:Label) ASSERT (n.prop) IS NODE KEY", DEPRECATED_DROP_CONSTRAINT_SYNTAX)
  }

  test("deprecated drop uniqueness constraint syntax") {
    assertNotificationInSupportedVersions("DROP CONSTRAINT ON (n:Label) ASSERT (n.prop) IS UNIQUE", DEPRECATED_DROP_CONSTRAINT_SYNTAX)
  }

  test("deprecated drop node property existence constraint syntax") {
    assertNotificationInSupportedVersions("DROP CONSTRAINT ON (n:Label) ASSERT EXISTS (n.prop)", DEPRECATED_DROP_CONSTRAINT_SYNTAX)
  }

  test("deprecated drop relationship existence constraint syntax") {
    assertNotificationInSupportedVersions("DROP CONSTRAINT ON ()-[r:TYPE]-() ASSERT EXISTS (r.prop)", DEPRECATED_DROP_CONSTRAINT_SYNTAX)
  }

  test("deprecated create node property existence constraint syntax - deprecate version 0") {
    assertNotificationInSupportedVersions("CREATE CONSTRAINT ON (n:Label) ASSERT EXISTS (n.prop)",
      DEPRECATED_CREATE_PROPERTY_EXISTENCE_CONSTRAINT_SYNTAX)
  }

  test("deprecated create relationship property existence constraint syntax - deprecate version 0") {
    assertNotificationInSupportedVersions("CREATE CONSTRAINT ON ()-[r:TYPE]-() ASSERT EXISTS (r.prop)",
      DEPRECATED_CREATE_PROPERTY_EXISTENCE_CONSTRAINT_SYNTAX)
  }

  test("deprecated create node property existence constraint syntax - deprecate version 1") {
    assertNotificationInSupportedVersions_4_X("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS NOT NULL",
      DEPRECATED_CREATE_CONSTRAINT_ON_ASSERT_SYNTAX)
  }

  test("deprecated create relationship property existence constraint syntax - deprecate version 1") {
    assertNotificationInSupportedVersions_4_X("CREATE CONSTRAINT ON ()-[r:TYPE]-() ASSERT (r.prop) IS NOT NULL",
      DEPRECATED_CREATE_CONSTRAINT_ON_ASSERT_SYNTAX)
  }

  test("deprecated create node key constraint syntax") {
    assertNotificationInSupportedVersions("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS NODE KEY",
      DEPRECATED_CREATE_CONSTRAINT_ON_ASSERT_SYNTAX)
  }

  test("deprecated create uniqueness constraint syntax") {
    assertNotificationInSupportedVersions("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS UNIQUE",
      DEPRECATED_CREATE_CONSTRAINT_ON_ASSERT_SYNTAX)
  }

  test("deprecated create constraint backed by btree index syntax") {
    // OPTIONS was introduced in 4.2
    // FOR ... REQUIRE was introduced in 4.4 and can therefore not be used in this test (since it tests 4.3)
    assertNotificationInSupportedVersions_4_X(
      s"""CREATE CONSTRAINT ON (n:Label)
         |ASSERT (n.prop) IS NODE KEY
         |OPTIONS {IndexProvider: '${GenericNativeIndexProvider.DESCRIPTOR.name()}'}""".stripMargin,
      DEPRECATED_BTREE_INDEX_SYNTAX
    )
    assertNotificationInSupportedVersions_4_X(
      s"""CREATE CONSTRAINT ON (n:Label)
         |ASSERT (n.prop) IS UNIQUE
         |OPTIONS {indexprovider: '${NativeLuceneFusionIndexProviderFactory30.DESCRIPTOR.name()}'}""".stripMargin,
      DEPRECATED_BTREE_INDEX_SYNTAX
    )
    assertNotificationInSupportedVersions_4_X(
      s"""CREATE CONSTRAINT ON (n:Label)
         |ASSERT (n.prop) IS NODE KEY
         |OPTIONS {IndexConfig: {`${SPATIAL_CARTESIAN_MAX.getSettingName}`: [40, 60]}}""".stripMargin,
      DEPRECATED_BTREE_INDEX_SYNTAX
    )
    assertNotificationInSupportedVersions_4_X(
      s"""CREATE CONSTRAINT ON (n:Label)
         |ASSERT (n.prop) IS UNIQUE
         |OPTIONS {indexconfig: {`${SPATIAL_WGS84_MIN.getSettingName}`: [-40, -60]}}""".stripMargin,
      DEPRECATED_BTREE_INDEX_SYNTAX
    )
    assertNoNotificationInSupportedVersions_4_X(
      s"""CREATE CONSTRAINT ON (n:Label)
         |ASSERT (n.prop) IS UNIQUE
         |OPTIONS {indexconfig: {`${FULLTEXT_EVENTUALLY_CONSISTENT.getSettingName}`: false}}""".stripMargin,
      DEPRECATED_BTREE_INDEX_SYNTAX
    )
  }

  test("deprecated show btree index syntax") {
    // Note: Show indexes was introduced in Neo4j 4.2
    assertNotificationInSupportedVersions_4_X("SHOW BTREE INDEXES", DEPRECATED_BTREE_INDEX_SYNTAX)
  }

  // OTHER DEPRECATIONS IN 4.X

  test("deprecated octal literal syntax") {
    assertNotificationInSupportedVersions("RETURN 0123 AS octal", DEPRECATED_OCTAL_LITERAL_SYNTAX)
  }

  test("deprecated hex literal syntax") {
    assertNotificationInSupportedVersions("RETURN 0X12B AS hex", DEPRECATED_HEX_LITERAL_SYNTAX)
  }

  test("deprecated binding variable length relationship") {
    val query = "MATCH ()-[rs*]-() RETURN rs"
    val detail = NotificationDetail.Factory.bindingVarLengthRelationship("rs")
    assertNotificationInSupportedVersions(query, DEPRECATED_BINDING_VAR_LENGTH_RELATIONSHIP, detail)
  }

  test("not deprecated binding variable length relationship") {
    assertNoNotificationInSupportedVersions("MATCH p = ()-[*]-() RETURN relationships(p) AS rs", DEPRECATED_BINDING_VAR_LENGTH_RELATIONSHIP)
  }

  test("deprecated pattern expression syntax") {
    val queries = Seq(
      "MATCH (a) RETURN (a)--()",
      "MATCH (a) WHERE ANY (x IN (a)--() WHERE 1=1) RETURN a",
    )

    assertNotificationInSupportedVersions(queries, DEPRECATED_USE_OF_PATTERN_EXPRESSION)
  }

  test("not deprecated pattern expression syntax") {
    // Existential subqueries was introduced in Neo4j 4.0
    assertNoNotificationInSupportedVersions_4_X("MATCH (a) WHERE EXISTS {(x) WHERE (x)--()} RETURN a", DEPRECATED_USE_OF_PATTERN_EXPRESSION)

    val queries = Seq(
      "MATCH (a)--() RETURN a",
      "MATCH (a) WHERE exists((a)--()) RETURN a",
      "MATCH (a) WHERE (a)--() RETURN a",
      "MATCH (a) RETURN [p=(a)--(b) | p]",
      "RETURN NOT exists(()--())",
      "RETURN NOT ()--()",
      "RETURN ()--() OR ()--()--()",
      """
        |EXPLAIN
        |MATCH (actor:Actor)
        |RETURN actor,
        |  CASE
        |    WHEN (actor)-[:WON]->(:Oscar) THEN 'Oscar winner'
        |    WHEN (actor)-[:WON]->(:GoldenGlobe) THEN 'Golden Globe winner'
        |    ELSE 'None'
        |  END AS accolade
        |""".stripMargin,
      """
        |EXPLAIN
        |MATCH (movie:Movie)<-[:ACTED_IN]-(actor:Actor)
        |WITH movie, collect(actor) AS cast
        |WHERE ANY(actor IN cast WHERE (actor)-[:WON]->(:Award))
        |RETURN movie
        |""".stripMargin
    )
    assertNoNotificationInSupportedVersions(queries, DEPRECATED_USE_OF_PATTERN_EXPRESSION)
  }

  test("deprecated property existence syntax") {
    val queries = Seq(
      // On node
      "MATCH (n) WHERE EXISTS(n.prop) RETURN n",

      // On node with not
      "MATCH (n) WHERE NOT EXISTS(n.prop) RETURN n",

      // On relationship
      "MATCH ()-[r]-() WITH r WHERE EXISTS(r.prop) RETURN r.prop",

      // On map key
      "WITH {key:'blah'} as map RETURN EXISTS(map.key)",

      // On map notation in WHERE
      "MATCH (n) WHERE EXISTS(n['prop']) RETURN n",

      // On map notation in WHERE with NOT
      "MATCH (n) WHERE NOT EXISTS(n['prop']) RETURN n",

      // On map notation in RETURN
      "MATCH (n) RETURN EXISTS(n['prop'])"
    )

    assertNotificationInSupportedVersions(queries, DEPRECATED_PROPERTY_EXISTENCE_SYNTAX)
  }

  test("exists on paths should not be deprecated") {
    assertNoNotificationInSupportedVersions("MATCH (n) WHERE EXISTS( (n)-[:REL]->() ) RETURN count(n)", DEPRECATED_PROPERTY_EXISTENCE_SYNTAX)
  }

  test("exists subclause should not be deprecated") {
    // Note: Exists subclause was introduced in Neo4j 4.0
    assertNoNotificationInSupportedVersions_4_X("MATCH (n) WHERE EXISTS { MATCH (n)-[]->() } RETURN n.prop",
      DEPRECATED_PROPERTY_EXISTENCE_SYNTAX)
  }

  test("deprecated coercion list to boolean") {
    val queries = Seq(
      "RETURN NOT []",
      "RETURN NOT [1]",
      "RETURN NOT ['a']",
      "RETURN ['a'] OR []",
      "RETURN TRUE OR []",
      "RETURN NOT (TRUE OR [])",
      "RETURN ['a'] AND []",
      "RETURN TRUE AND []",
      "RETURN NOT (TRUE AND [])",
      "MATCH (n) WHERE [] RETURN TRUE",
      "MATCH (n) WHERE range(0, 10) RETURN TRUE",
      "MATCH (n) WHERE range(0, 10) RETURN range(0, 10)",
      "RETURN NOT ()--()",
      "RETURN ()--() OR ()--()--()",
      """
        |EXPLAIN
        |MATCH (actor:Actor)
        |RETURN actor,
        |  CASE
        |    WHEN (actor)-[:WON]->(:Oscar) THEN 'Oscar winner'
        |    WHEN (actor)-[:WON]->(:GoldenGlobe) THEN 'Golden Globe winner'
        |    ELSE 'None'
        |  END AS accolade
        |""".stripMargin,
      """
        |EXPLAIN
        |MATCH (movie:Movie)<-[:ACTED_IN]-(actor:Actor)
        |WITH movie, collect(actor) AS cast
        |WHERE ANY(actor IN cast WHERE (actor)-[:WON]->(:Award))
        |RETURN movie
        |""".stripMargin
    )

    assertNotificationInSupportedVersions(queries, DEPRECATED_COERCION_OF_LIST_TO_BOOLEAN)

    assertNoNotificationInSupportedVersions("RETURN NOT TRUE", DEPRECATED_COERCION_OF_LIST_TO_BOOLEAN)
  }

  test("should not allow referencing elements being created by the pattern within that same pattern") {
    val badQueries = Seq(
      "CREATE (a {prop:7})-[r:R {prop: a.prop}]->(b)",
      "CREATE (a {prop:7})-[r:R]->(b {prop: a.prop})",
      "CREATE (a {prop: r.prop})-[r:R {prop:7}]->(b)",
      "CREATE (a)-[r:R {prop:7}]->(b {prop: r.prop})",
      "CREATE (a {prop:b.prop})-[r:R]->(b {prop: 7})",
      "CREATE (a)-[r:R {prop:b.prop}]->(b {prop: 7})",
      "CREATE (a:A:B)-[r:R]->(b {prop: labels(a)})",
      "CREATE p=(a {prop:7})-[r:R {prop: a.prop}]->(b)",
      "CREATE p=(a {prop:7})-[:R]->(b {prop: nodes(p)[0].prop})",
      "CREATE (a {prop:7})-[:R {prop: a.prop}]->(b)",
      "CREATE (a {prop:7})-[r:R]->({prop: a.prop})",
      "CREATE p=({prop:7})-[:R]->(b {prop: nodes(p)[0].prop})",
    )

    assertNotificationInSupportedVersions(badQueries, DEPRECATED_SELF_REFERENCE_TO_VARIABLE_IN_CREATE_PATTERN)

    val okQueries = Seq(
      "CREATE (a)-[:R]->(a)",
      "CREATE (a {prop: 7})-[:R]->(a)",
      "MATCH (b) CREATE ()-[:R]->(a {prop: b.prop})",
      "CREATE (a {prop: 7}) CREATE (b {prop: a.prop})",
      "MATCH (a {prop:7})-[r:R {prop: a.prop}]->(b) RETURN *",
      "CREATE (a {prop:7}) CREATE (a)-[:R]->(b {prop: a.prop})",
      "MATCH (a) CREATE (a)-[:R]->(b {prop: a.prop})",
    )

    assertNoNotificationInSupportedVersions(okQueries, DEPRECATED_SELF_REFERENCE_TO_VARIABLE_IN_CREATE_PATTERN)
  }

  test("deprecated periodic commit hint") {
    val query = "USING PERIODIC COMMIT LOAD CSV FROM 'file:///artists.csv' AS line CREATE (:Artist {name: line[1], year: toInteger(line[2])})"
    assertNotificationInSupportedVersions(query, DEPRECATED_PERIODIC_COMMIT)
  }

  test("comparing points is deprecated") {
    val deprecated = Seq(
      "MATCH (n) WHERE n.prop < point({x:0, y:0}) RETURN n",
      "MATCH (n) WHERE n.prop <= point({x:0, y:0}) RETURN n",
      "MATCH (n) WHERE n.prop > point({x:0, y:0}) RETURN n",
      "MATCH (n) WHERE n.prop >= point({x:0, y:0}) RETURN n",
      "RETURN point({x:0, y:0}) < point({x:0, y:0}) <= point({x:0, y:0})"
    )
    assertNotificationInSupportedVersions(deprecated, DEPRECATED_POINTS_COMPARE)
  }

  test("btree index hint") {
    val deprecated = Seq(
      "MATCH (n:Label) USING BTREE INDEX n:Label(prop) WHERE n.prop = 'test' RETURN n",
      "MATCH (n:Label) USING BTREE INDEX SEEK n:Label(prop) WHERE n.prop = 'test' RETURN n",
      "MATCH ()-[r:REL]->() USING BTREE INDEX r:REL(prop) WHERE r.prop = 'test' RETURN r",
      "MATCH ()-[r:REL]->() USING BTREE INDEX SEEK r:REL(prop) WHERE r.prop = 'test' RETURN r",
    )

    val notDeprecated = Seq(
      "MATCH (n:Label) USING INDEX n:Label(prop) WHERE n.prop = 'test' RETURN n",
      "MATCH ()-[r:REL]->() USING TEXT INDEX r:REL(prop) WHERE r.prop = 'test' RETURN r",
      "MATCH (n:Label) USING TEXT INDEX SEEK n:Label(prop) WHERE n.prop = 'test' RETURN n",
      "MATCH ()-[r:REL]->() USING INDEX SEEK r:REL(prop) WHERE r.prop = 'test' RETURN r",
    )

    assertNotificationInSupportedVersions(deprecated, DEPRECATED_BTREE_INDEX_SYNTAX)

    assertNoNotificationInSupportedVersions(notDeprecated, DEPRECATED_BTREE_INDEX_SYNTAX)
  }
}
