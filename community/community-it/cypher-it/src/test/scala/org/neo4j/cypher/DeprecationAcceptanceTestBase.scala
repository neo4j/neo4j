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
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_COERCION_OF_LIST_TO_BOOLEAN
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_CREATE_CONSTRAINT_ON_ASSERT_SYNTAX
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_CREATE_INDEX_SYNTAX
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_CREATE_PROPERTY_EXISTENCE_CONSTRAINT_SYNTAX
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_DROP_CONSTRAINT_SYNTAX
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_DROP_INDEX_SYNTAX
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_FUNCTION
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_HEX_LITERAL_SYNTAX
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_OCTAL_LITERAL_SYNTAX
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_PARAMETER_SYNTAX
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_PROCEDURE
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_PROCEDURE_RETURN_FIELD
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_PROPERTY_EXISTENCE_SYNTAX
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_RELATIONSHIP_TYPE_SEPARATOR
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_SHOW_EXISTENCE_CONSTRAINT_SYNTAX
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_SHOW_SCHEMA_SYNTAX
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_USE_OF_PATTERN_EXPRESSION
import org.neo4j.graphdb.impl.notification.NotificationCode.LENGTH_ON_NON_PATH
import org.neo4j.graphdb.impl.notification.NotificationDetail
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
    val queries = Seq("EXPLAIN CALL oldProc()", "EXPLAIN CALL oldProc() RETURN 1")
    val detail = NotificationDetail.Factory.deprecatedName("oldProc", "newProc")
    assertNotificationInSupportedVersions(queries, DEPRECATED_PROCEDURE, detail)
  }

  test("deprecated procedure result field") {
    val query = "EXPLAIN CALL changedProc() YIELD oldField RETURN oldField"
    val detail = NotificationDetail.Factory.deprecatedField("changedProc", "oldField")
    assertNotificationInSupportedVersions(query, DEPRECATED_PROCEDURE_RETURN_FIELD, detail)
  }

  // DEPRECATED INDEX / CONSTRAINT SYNTAX in 4.X

  test("deprecated create index syntax") {
    assertNotificationInSupportedVersions("EXPLAIN CREATE INDEX ON :Label(prop)", DEPRECATED_CREATE_INDEX_SYNTAX)
  }

  test("deprecated drop index syntax") {
    assertNotificationInSupportedVersions("EXPLAIN DROP INDEX ON :Label(prop)", DEPRECATED_DROP_INDEX_SYNTAX)
  }

  test("deprecated drop node key constraint syntax") {
    assertNotificationInSupportedVersions("EXPLAIN DROP CONSTRAINT ON (n:Label) ASSERT (n.prop) IS NODE KEY", DEPRECATED_DROP_CONSTRAINT_SYNTAX)
  }

  test("deprecated drop uniqueness constraint syntax") {
    assertNotificationInSupportedVersions("EXPLAIN DROP CONSTRAINT ON (n:Label) ASSERT (n.prop) IS UNIQUE", DEPRECATED_DROP_CONSTRAINT_SYNTAX)
  }

  test("deprecated drop node property existence constraint syntax") {
    assertNotificationInSupportedVersions("EXPLAIN DROP CONSTRAINT ON (n:Label) ASSERT EXISTS (n.prop)", DEPRECATED_DROP_CONSTRAINT_SYNTAX)
  }

  test("deprecated drop relationship existence constraint syntax") {
    assertNotificationInSupportedVersions("EXPLAIN DROP CONSTRAINT ON ()-[r:TYPE]-() ASSERT EXISTS (r.prop)", DEPRECATED_DROP_CONSTRAINT_SYNTAX)
  }

  test("deprecated create node property existence constraint syntax") {
    assertNotificationInSupportedVersions("EXPLAIN CREATE CONSTRAINT ON (n:Label) ASSERT EXISTS (n.prop)",
      DEPRECATED_CREATE_PROPERTY_EXISTENCE_CONSTRAINT_SYNTAX)
  }

  test("deprecated create relationship property existence constraint syntax") {
    assertNotificationInSupportedVersions("EXPLAIN CREATE CONSTRAINT ON ()-[r:TYPE]-() ASSERT EXISTS (r.prop)",
      DEPRECATED_CREATE_PROPERTY_EXISTENCE_CONSTRAINT_SYNTAX)
  }

  test("deprecated create node property existence constraint syntax - deprecate version 1") {
    assertNotificationInSupportedVersions_4_X("EXPLAIN CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS NOT NULL",
      DEPRECATED_CREATE_CONSTRAINT_ON_ASSERT_SYNTAX)
  }

  test("deprecated create relationship property existence constraint syntax - deprecate version 1") {
    assertNotificationInSupportedVersions_4_X("EXPLAIN CREATE CONSTRAINT ON ()-[r:TYPE]-() ASSERT (r.prop) IS NOT NULL",
      DEPRECATED_CREATE_CONSTRAINT_ON_ASSERT_SYNTAX)
  }

  test("deprecated create node key constraint syntax") {
    assertNotificationInSupportedVersions("EXPLAIN CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS NODE KEY",
      DEPRECATED_CREATE_CONSTRAINT_ON_ASSERT_SYNTAX)
  }

  test("deprecated create uniqueness constraint syntax") {
    assertNotificationInSupportedVersions("EXPLAIN CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS UNIQUE",
      DEPRECATED_CREATE_CONSTRAINT_ON_ASSERT_SYNTAX)
  }

  test("deprecated show index syntax") {
    val queries = Seq(
      "EXPLAIN SHOW INDEXES BRIEF",
      "EXPLAIN SHOW INDEXES BRIEF OUTPUT",
      "EXPLAIN SHOW INDEXES VERBOSE",
      "EXPLAIN SHOW INDEXES VERBOSE OUTPUT",
    )

    // Note: Show indexes was introduced in Neo4j 4.2
    assertNotificationInSupportedVersions_4_X(queries, DEPRECATED_SHOW_SCHEMA_SYNTAX)
  }

  test("deprecated show constraint syntax") {
    val queries = Seq(
      "EXPLAIN SHOW CONSTRAINTS BRIEF",
      "EXPLAIN SHOW CONSTRAINTS BRIEF OUTPUT",
      "EXPLAIN SHOW CONSTRAINTS VERBOSE",
      "EXPLAIN SHOW CONSTRAINTS VERBOSE OUTPUT",
    )

    // Note: Show constraints was introduced in Neo4j 4.2
    assertNotificationInSupportedVersions_4_X(queries, DEPRECATED_SHOW_SCHEMA_SYNTAX)
  }

  test("deprecated show existence constraint syntax") {
    val queries = Seq(
      "EXPLAIN SHOW EXISTS CONSTRAINT",
      "EXPLAIN SHOW NODE EXISTS CONSTRAINT",
      "EXPLAIN SHOW RELATIONSHIP EXISTS CONSTRAINT",
    )

    // Note: Show constraints was introduced in Neo4j 4.2
    assertNotificationInSupportedVersions_4_X(queries, DEPRECATED_SHOW_EXISTENCE_CONSTRAINT_SYNTAX)
  }

  // OTHER DEPRECATIONS IN 4.X

  test("deprecated octal literal syntax") {
    assertNotificationInSupportedVersions("EXPLAIN RETURN 0123 AS octal", DEPRECATED_OCTAL_LITERAL_SYNTAX)
  }

  test("deprecated hex literal syntax") {
    assertNotificationInSupportedVersions("EXPLAIN RETURN 0X12B AS hex", DEPRECATED_HEX_LITERAL_SYNTAX)
  }

  test("deprecated binding variable length relationship") {
    val query = "EXPLAIN MATCH ()-[rs*]-() RETURN rs"
    val detail = NotificationDetail.Factory.bindingVarLengthRelationship("rs")
    assertNotificationInSupportedVersions(query, DEPRECATED_BINDING_VAR_LENGTH_RELATIONSHIP, detail)
  }

  test("not deprecated binding variable length relationship") {
    assertNoNotificationInSupportedVersions("EXPLAIN MATCH p = ()-[*]-() RETURN relationships(p) AS rs", DEPRECATED_BINDING_VAR_LENGTH_RELATIONSHIP)
  }

  test("deprecated pattern expression syntax") {
    val queries = Seq(
      "EXPLAIN MATCH (a) RETURN (a)--()",
      "EXPLAIN MATCH (a) WHERE ANY (x IN (a)--() WHERE 1=1) RETURN a",
    )

    assertNotificationInSupportedVersions(queries, DEPRECATED_USE_OF_PATTERN_EXPRESSION)
  }

  test("not deprecated pattern expression syntax") {
    // Existential subqueries was introduced in Neo4j 4.0
    assertNoNotificationInSupportedVersions_4_X("EXPLAIN MATCH (a) WHERE EXISTS {(x) WHERE (x)--()} RETURN a", DEPRECATED_USE_OF_PATTERN_EXPRESSION)

    val queries = Seq(
      "EXPLAIN MATCH (a)--() RETURN a",
      "EXPLAIN MATCH (a) WHERE exists((a)--()) RETURN a",
      "EXPLAIN MATCH (a) WHERE (a)--() RETURN a",
      "EXPLAIN MATCH (a) RETURN [p=(a)--(b) | p]",
      "EXPLAIN RETURN NOT exists(()--())",
      "EXPLAIN RETURN NOT ()--()",
      "EXPLAIN RETURN ()--() OR ()--()--()",
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
      "EXPLAIN MATCH (n) WHERE EXISTS(n.prop) RETURN n",

      // On node with not
      "EXPLAIN MATCH (n) WHERE NOT EXISTS(n.prop) RETURN n",

      // On relationship
      "EXPLAIN MATCH ()-[r]-() WITH r WHERE EXISTS(r.prop) RETURN r.prop",

      // On map key
      "EXPLAIN WITH {key:'blah'} as map RETURN EXISTS(map.key)",

      // On map notation in WHERE
      "EXPLAIN MATCH (n) WHERE EXISTS(n['prop']) RETURN n",

      // On map notation in WHERE with NOT
      "EXPLAIN MATCH (n) WHERE NOT EXISTS(n['prop']) RETURN n",

      // On map notation in RETURN
      "EXPLAIN MATCH (n) RETURN EXISTS(n['prop'])"
    )

    assertNotificationInSupportedVersions(queries, DEPRECATED_PROPERTY_EXISTENCE_SYNTAX)
  }

  test("exists on paths should not be deprecated") {
    assertNoNotificationInSupportedVersions("EXPLAIN MATCH (n) WHERE EXISTS( (n)-[:REL]->() ) RETURN count(n)", DEPRECATED_PROPERTY_EXISTENCE_SYNTAX)
  }

  test("exists subclause should not be deprecated") {
    // Note: Exists subclause was introduced in Neo4j 4.0
    assertNoNotificationInSupportedVersions_4_X("EXPLAIN MATCH (n) WHERE EXISTS { MATCH (n)-[]->() } RETURN n.prop",
      DEPRECATED_PROPERTY_EXISTENCE_SYNTAX)
  }

  test("deprecated coercion list to boolean") {
    val queries = Seq(
      "EXPLAIN RETURN NOT []",
      "EXPLAIN RETURN NOT [1]",
      "EXPLAIN RETURN NOT ['a']",
      "EXPLAIN RETURN ['a'] OR []",
      "EXPLAIN RETURN TRUE OR []",
      "EXPLAIN RETURN NOT (TRUE OR [])",
      "EXPLAIN RETURN ['a'] AND []",
      "EXPLAIN RETURN TRUE AND []",
      "EXPLAIN RETURN NOT (TRUE AND [])",
      "EXPLAIN MATCH (n) WHERE [] RETURN TRUE",
      "EXPLAIN MATCH (n) WHERE range(0, 10) RETURN TRUE",
      "EXPLAIN MATCH (n) WHERE range(0, 10) RETURN range(0, 10)",
      "EXPLAIN RETURN NOT ()--()",
      "EXPLAIN RETURN ()--() OR ()--()--()",
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

    assertNoNotificationInSupportedVersions("EXPLAIN RETURN NOT TRUE", DEPRECATED_COERCION_OF_LIST_TO_BOOLEAN)
  }

  // FUNCTIONALITY DEPRECATED IN 3.5, REMOVED IN 4.0

  test("deprecated toInt") {
    val query = "EXPLAIN RETURN toInt('1') AS one"
    val detail = NotificationDetail.Factory.deprecatedName("toInt", "toInteger")
    assertNotificationInLastMajorVersion(query, DEPRECATED_FUNCTION, detail)
  }

  test("deprecated upper") {
    val query = "EXPLAIN RETURN upper('foo') AS upper"
    val detail = NotificationDetail.Factory.deprecatedName("upper", "toUpper")
    assertNotificationInLastMajorVersion(query, DEPRECATED_FUNCTION, detail)
  }

  test("deprecated lower") {
    val query = "EXPLAIN RETURN lower('BAR') AS lower"
    val detail = NotificationDetail.Factory.deprecatedName("lower", "toLower")
    assertNotificationInLastMajorVersion(query, DEPRECATED_FUNCTION, detail)
  }

  test("deprecated rels") {
    val query = "EXPLAIN MATCH p = ()-->() RETURN rels(p) AS r"
    val detail = NotificationDetail.Factory.deprecatedName("rels", "relationships")
    assertNotificationInLastMajorVersion(query, DEPRECATED_FUNCTION, detail)
  }

  test("deprecated filter") {
    val query = "EXPLAIN WITH [1,2,3] AS list RETURN filter(x IN list WHERE x % 2 = 1) AS odds"
    val detail = NotificationDetail.Factory.deprecatedName("filter(...)", "[...]")
    assertNotificationInLastMajorVersion(query, DEPRECATED_FUNCTION, detail)
  }

  test("deprecated extract") {
    val query = "EXPLAIN WITH [1,2,3] AS list RETURN extract(x IN list | x * 10) AS tens"
    val detail = NotificationDetail.Factory.deprecatedName("extract(...)", "[...]")
    assertNotificationInLastMajorVersion(query, DEPRECATED_FUNCTION, detail)
  }

  test("deprecated parameter syntax") {
    assertNotificationInLastMajorVersion("EXPLAIN RETURN {param} AS parameter", DEPRECATED_PARAMETER_SYNTAX)
  }

  test("deprecated parameter syntax for property map") {
    assertNotificationInLastMajorVersion("EXPLAIN CREATE (:Label {props})", DEPRECATED_PARAMETER_SYNTAX)
  }

  test("deprecated length of string") {
    assertNotificationInLastMajorVersion("EXPLAIN RETURN length('a string')", LENGTH_ON_NON_PATH)
  }

  test("deprecated length of list") {
    assertNotificationInLastMajorVersion("EXPLAIN RETURN length([1, 2, 3])", LENGTH_ON_NON_PATH)
  }

  test("deprecated length of pattern expression") {
    assertNotificationInLastMajorVersion("EXPLAIN MATCH (a) WHERE a.name='Alice' RETURN length((a)-->()-->())", LENGTH_ON_NON_PATH)
  }

  test("deprecated future ambiguous reltype separator") {
    val queries = Seq(
      "EXPLAIN MATCH (a)-[:A|:B|:C {foo:'bar'}]-(b) RETURN a,b",
      "EXPLAIN MATCH (a)-[x:A|:B|:C]-() RETURN a",
      "EXPLAIN MATCH (a)-[:A|:B|:C*]-() RETURN a"
    )

    assertNotificationInLastMajorVersion(queries, DEPRECATED_RELATIONSHIP_TYPE_SEPARATOR)

    // clear caches of the rewritten queries to not keep notifications around
    dbms.clearQueryCaches()
  }

  test("not deprecated reltype separator cases") {
    val queries = Seq(
      "EXPLAIN MATCH (a)-[:A|B|C {foo:'bar'}]-(b) RETURN a,b",
      "EXPLAIN MATCH (a)-[:A|:B|:C]-(b) RETURN a,b",
      "EXPLAIN MATCH (a)-[:A|B|C]-(b) RETURN a,b"
    )

    assertNoNotificationInLastMajorVersion(queries, DEPRECATED_RELATIONSHIP_TYPE_SEPARATOR)
  }
}
