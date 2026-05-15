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
package org.neo4j.cypher

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.util.test_helpers.GqlExceptionMatchers.InvalidSyntaxStatus
import org.neo4j.cypher.internal.util.test_helpers.GqlExceptionMatchers.Reparsesable_42I67
import org.neo4j.cypher.internal.util.test_helpers.GqlExceptionMatchers.gqlException
import org.neo4j.cypher.internal.util.test_helpers.GqlExceptionMatchers.gqlStatus
import org.neo4j.cypher.util.DontRunOnSpdBuild
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.exceptions.InvalidSemanticsException
import org.neo4j.exceptions.NotSystemDatabaseException
import org.neo4j.exceptions.RuntimeUnsupportedException
import org.neo4j.exceptions.SyntaxException
import org.neo4j.gqlstatus.GqlStatusInfoCodes
import org.neo4j.graphdb.schema.AnalyzerProvider
import org.neo4j.service.Services

import scala.jdk.CollectionConverters.ListHasAsScala

// No need to run on SPD, we already have an enterprise version of this test: CombineCommandsAndRegularCypherAcceptanceTest
class CommunityCombineCommandsAndRegularCypherAcceptanceTest extends CommunityCombineCommandsAcceptanceTestBase
    with DontRunOnSpdBuild {
  // Tests for combining listing and terminating commands with regular Cypher

  test("Should fail to combine Cypher with commands in Cypher 5 ") {
    // WHEN
    val exceptionAfter = the[SyntaxException] thrownBy {
      execute("CYPHER 5 SHOW PROCEDURES YIELD name MATCH (n) RETURN *")
    }

    // THEN
    exceptionAfter should be(gqlException(
      "Invalid input 'MATCH':",
      InvalidSyntaxStatus.withCause(gqlStatus(
        GqlStatusInfoCodes.STATUS_42I06,
        "error: syntax error or access rule violation - invalid input. Invalid input 'MATCH', expected:",
        fuzzyStatusDescr = true
      ).withCause(Reparsesable_42I67)),
      fuzzyMsg = true
    ))

    // WHEN
    val exceptionBefore = the[SyntaxException] thrownBy {
      execute("CYPHER 5 MATCH (n) SHOW PROCEDURES YIELD name RETURN *")
    }

    // THEN
    exceptionBefore should be(gqlException(
      "Invalid input 'SHOW':",
      InvalidSyntaxStatus.withCause(gqlStatus(
        GqlStatusInfoCodes.STATUS_42I06,
        "error: syntax error or access rule violation - invalid input. Invalid input 'SHOW', expected:",
        fuzzyStatusDescr = true
      ).withCause(Reparsesable_42I67)),
      fuzzyMsg = true
    ))
  }

  test("Should fail to combine Cypher with show current graph type in community") {
    // WHEN
    val exceptionAfter = the[RuntimeUnsupportedException] thrownBy {
      execute("SHOW CURRENT GRAPH TYPE YIELD specification MATCH (n) RETURN *")
    }

    // THEN
    exceptionAfter should be(gqlException(
      "51N27: 'SHOW CURRENT GRAPH TYPE' is not supported in community edition.",
      gqlStatus(
        GqlStatusInfoCodes.STATUS_51N27,
        "error: system configuration or operation exception - not supported in this edition. " +
          "'SHOW CURRENT GRAPH TYPE' is not supported in community edition."
      )
    ))
    val causeAfter = exceptionAfter.getCause
    causeAfter should not be null
    causeAfter shouldBe a[CantCompileQueryException]
    causeAfter.asInstanceOf[CantCompileQueryException] should be(gqlException(
      "51N27: 'SHOW CURRENT GRAPH TYPE' is not supported in community edition.",
      gqlStatus(
        GqlStatusInfoCodes.STATUS_51N27,
        "error: system configuration or operation exception - not supported in this edition. " +
          "'SHOW CURRENT GRAPH TYPE' is not supported in community edition."
      )
    ))

    // WHEN
    val exceptionBefore = the[RuntimeUnsupportedException] thrownBy {
      execute("MATCH (n) SHOW CURRENT GRAPH TYPE YIELD specification RETURN *")
    }

    // THEN
    exceptionBefore should be(gqlException(
      "51N27: 'SHOW CURRENT GRAPH TYPE' is not supported in community edition.",
      gqlStatus(
        GqlStatusInfoCodes.STATUS_51N27,
        "error: system configuration or operation exception - not supported in this edition. " +
          "'SHOW CURRENT GRAPH TYPE' is not supported in community edition."
      )
    ))
    val causeBefore = exceptionBefore.getCause
    causeBefore should not be null
    causeBefore shouldBe a[CantCompileQueryException]
    causeBefore.asInstanceOf[CantCompileQueryException] should be(gqlException(
      "51N27: 'SHOW CURRENT GRAPH TYPE' is not supported in community edition.",
      gqlStatus(
        GqlStatusInfoCodes.STATUS_51N27,
        "error: system configuration or operation exception - not supported in this edition. " +
          "'SHOW CURRENT GRAPH TYPE' is not supported in community edition."
      )
    ))
  }

  test("Should fail to combine Cypher with commands on system") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    val systemDbClauseRules = gqlStatus(
      GqlStatusInfoCodes.STATUS_42NA9,
      s"error: syntax error or access rule violation - system database rules. " +
        "The system database supports a restricted set of Cypher clauses. " +
        "The supported clauses include procedure calls (if the procedure is allowed), a subset of show and terminate commands, and combinations of the two. " +
        "'YIELD' and 'RETURN' are also permitted when combined with procedure calls, show, or terminate commands."
    )

    // WHEN
    val exceptionCommandAllowed = the[InvalidSemanticsException] thrownBy {
      execute("SHOW SETTINGS YIELD name MATCH (n) RETURN *")
    }

    // THEN
    exceptionCommandAllowed should be(gqlException(
      "The following clauses are not allowed on a system database: MATCH.",
      InvalidSyntaxStatus.withCause(gqlStatus(
        GqlStatusInfoCodes.STATUS_42N17,
        "error: syntax error or access rule violation - unsupported request. " +
          "'MATCH' is not allowed on the system database."
      ).withCause(systemDbClauseRules))
    ))

    // WHEN
    val exceptionCommandDisallowed = the[InvalidSemanticsException] thrownBy {
      execute("SHOW CONSTRAINTS YIELD name MATCH (n) RETURN *")
    }

    // THEN
    exceptionCommandDisallowed should be(gqlException(
      "The following clauses are not allowed on a system database: MATCH, SHOW CONSTRAINTS.",
      InvalidSyntaxStatus.withCause(gqlStatus(
        GqlStatusInfoCodes.STATUS_42N17,
        "error: syntax error or access rule violation - unsupported request. " +
          "'MATCH, SHOW CONSTRAINTS' is not allowed on the system database."
      ).withCause(systemDbClauseRules))
    ))

    // WHEN
    val exceptionCommandRequired = the[InvalidSemanticsException] thrownBy {
      execute("SHOW DATABASES YIELD name MATCH (n) RETURN *")
    }

    // THEN
    exceptionCommandRequired should be(gqlException(
      "The following clauses are not allowed on a system database: MATCH.",
      InvalidSyntaxStatus.withCause(gqlStatus(
        GqlStatusInfoCodes.STATUS_42N17,
        "error: syntax error or access rule violation - unsupported request. " +
          "'MATCH' is not allowed on the system database."
      ).withCause(systemDbClauseRules))
    ))
  }

  test("Should terminate transactions with regular cypher MATCH") {
    // GIVEN
    val (unwindQuery, latch) = setupUserWithOneTransaction()
    execute(
      s"CREATE ({user: '$username', email: '$username@email.com'}), ({user: '$username2', email: '$username2@email.com'})"
    )

    try {
      val unwindTransactionId = getTransactionIdExecutingQuery(unwindQuery)

      // WHEN
      val result = execute(
        s"""TERMINATE TRANSACTION '$unwindTransactionId'
           |YIELD message, username
           |MATCH (n {user: username})
           |RETURN message, n.email AS email""".stripMargin
      ).toList

      // THEN
      result should be(List(Map(
        "message" -> "Transaction terminated.",
        "email" -> s"$username@email.com"
      )))
    } finally {
      execute("MATCH (u) DELETE u")
      latch.finishAndWaitForAllToFinish()
    }
  }

  test("Should show functions and regular cypher with UNION") {
    // WHEN
    val result = execute(
      """SHOW FUNCTIONS
        |  YIELD name
        |RETURN 'function' AS type, name
        |  ORDER BY name
        |UNION
        |{
        |  OPTIONAL MATCH (n)
        |  RETURN 'match' AS type, n.name AS name
        |}""".stripMargin
    )

    // THEN
    val expected =
      allFunctionsNames.distinct.map(name => Map("type" -> "function", "name" -> name)) :+
        Map("type" -> "match", "name" -> null)

    result.toList should be(expected)
  }

  test("Should show indexes and unwind result") {
    // GIVEN
    graph.createNodeIndexWithName("indexNode", "Label", "prop1", "prop2")
    graph.createRelationshipIndexWithName("indexRel", "REL_TYPE", "prop")

    // WHEN
    val result = execute(
      """SHOW RANGE INDEXES
        |  YIELD name, labelsOrTypes, properties
        |WITH name, labelsOrTypes[0] AS labelOrType, properties
        |UNWIND properties AS property
        |RETURN name, labelOrType, property
        |ORDER BY name, property
        |""".stripMargin
    )

    // THEN
    result.toList should be(List(
      Map("name" -> "indexNode", "labelOrType" -> "Label", "property" -> "prop1"),
      Map("name" -> "indexNode", "labelOrType" -> "Label", "property" -> "prop2"),
      Map("name" -> "indexRel", "labelOrType" -> "REL_TYPE", "property" -> "prop")
    ))
  }

  test("Should show databases and procedure call on system database") {
    // GIVEN
    val query =
      """SHOW DATABASES
        |  YIELD name, aliases
        |CALL db.index.fulltext.listAvailableAnalyzers()
        |  YIELD analyzer
        |RETURN name, analyzer IN aliases AS analyzerAlias
        |ORDER BY name, analyzerAlias
        |""".stripMargin

    // This is how the `db.index.fulltext.listAvailableAnalyzers` procedure fetches the analyzers
    val allFulltextAnalyzers = Services.loadAll(classOf[AnalyzerProvider]).stream.toList.asScala.toList

    // WHEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    val result = execute(query)

    // THEN
    result.toList should be(
      allFulltextAnalyzers.flatMap(_ =>
        List(
          Map[String, Any]("name" -> "neo4j", "analyzerAlias" -> false),
          Map[String, Any]("name" -> "system", "analyzerAlias" -> false)
        )
      ).sortBy(m => (m("name").asInstanceOf[String], m("analyzerAlias").asInstanceOf[Boolean]))
    )

    // WHEN
    selectDatabase(GraphDatabaseSettings.DEFAULT_DATABASE_NAME)
    val exception = the[NotSystemDatabaseException] thrownBy {
      execute(query)
    }

    // THEN
    exception should be(gqlException(
      "This is an administration command and it should be executed against the system database: SHOW DATABASES",
      gqlStatus(
        GqlStatusInfoCodes.STATUS_51N28,
        "error: system configuration or operation exception - not supported by this database. This Cypher command must be executed against the database `system`."
      )
    ))
  }
}
