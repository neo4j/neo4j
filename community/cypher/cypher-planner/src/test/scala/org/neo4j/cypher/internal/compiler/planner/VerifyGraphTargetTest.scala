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
package org.neo4j.cypher.internal.compiler.planner

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.factory.neo4j.JavaCCParser
import org.neo4j.cypher.internal.compiler.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.dbms.api.DatabaseNotFoundException
import org.neo4j.exceptions.InvalidSemanticsException
import org.neo4j.kernel.database.DatabaseReferenceImpl
import org.neo4j.kernel.database.DatabaseReferenceRepository
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.database.NormalizedDatabaseName

import java.util.Optional

class VerifyGraphTargetTest extends CypherFunSuite {

  val databaseReferenceRepository = mock[DatabaseReferenceRepository]
  val sessionDb = mock[NamedDatabaseId]

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    Mockito.reset(databaseReferenceRepository)
  }

  test("should accept statement without USE clause") {
    mockReferenceRepository(graphReference(sessionDb))
    verifyGraphTarget("RETURN 1")
  }

  test("should accept USE targeting the session graph") {
    val query =
      """
        |USE neo4j
        |RETURN 1
        |""".stripMargin

    mockReferenceRepository(graphReference(sessionDb))
    verifyGraphTarget(query)
  }

  test("should accept USE with namespace targeting the session graph") {
    val query =
      """
        |USE somewhere.neo4j
        |RETURN 1
        |""".stripMargin

    mockReferenceRepository(graphReference(sessionDb))
    verifyGraphTarget(query)
  }

  test("should not accept USE targeting a graph which is not the session one") {
    val query =
      """
        |USE foo
        |RETURN 1
        |""".stripMargin

    mockReferenceRepository(graphReference(mock[NamedDatabaseId]))
    the[InvalidSemanticsException] thrownBy verifyGraphTarget(query) should have message "Query routing is not available in embedded sessions. Try running the query using a Neo4j driver or the HTTP API."
  }

  test("should not accept USE targeting a non-existent graph") {
    val query =
      """
        |USE foo
        |RETURN 1
        |""".stripMargin

    the[DatabaseNotFoundException] thrownBy verifyGraphTarget(query) should have message "Database foo not found"
  }

  test("should accept a combination of ambient and explicit graph selection targeting the session graph") {
    val query =
      """
        |CALL {
        |  USE neo4j
        |  RETURN 1
        |}
        |RETURN 1
        |""".stripMargin
    mockReferenceRepository(graphReference(sessionDb))
    verifyGraphTarget(query)
  }

  test("should not accept a combination of ambient and explicit graph selection targeting different graphs") {
    val query =
      """
        |CALL {
        |  USE foo
        |  RETURN 1
        |}
        |RETURN 1
        |""".stripMargin

    mockReferenceRepository(graphReference(mock[NamedDatabaseId]))
    the[InvalidSemanticsException] thrownBy verifyGraphTarget(query) should have message "Using multiple graphs in the same query is not supported on standard databases. This capability is supported on composite databases only."
  }

  test("should accept a combination of ambient and explicit graph selection in UNION targeting the session graph") {
    val query =
      """
        |RETURN 1 AS x
        |UNION
        |USE neo4j
        |RETURN 1 AS x
        |""".stripMargin
    mockReferenceRepository(graphReference(sessionDb))
    verifyGraphTarget(query)
  }

  test("should not accept a combination of ambient and explicit graph selection in UNION targeting different graphs") {
    val query =
      """
        |RETURN 1 AS x
        |UNION
        |USE foo
        |RETURN 1 AS x
        |""".stripMargin
    mockReferenceRepository(graphReference(mock[NamedDatabaseId]))
    the[InvalidSemanticsException] thrownBy verifyGraphTarget(query) should have message "Using multiple graphs in the same query is not supported on standard databases. This capability is supported on composite databases only."
  }

  private def mockReferenceRepository(reference: DatabaseReferenceImpl.Internal) = {
    when(databaseReferenceRepository.getInternalByAlias(any[NormalizedDatabaseName])).thenReturn(Optional.of(reference))
  }

  private def graphReference(databaseId: NamedDatabaseId): DatabaseReferenceImpl.Internal = {
    val graphReference = mock[DatabaseReferenceImpl.Internal]
    when(graphReference.databaseId()).thenReturn(databaseId)
    graphReference
  }

  private def verifyGraphTarget(query: String): Unit = {
    val parsedQuery = parse(query)
    val state = mock[BaseState]
    when(state.statement()).thenReturn(parsedQuery)

    val plannerContext = mock[PlannerContext]
    when(plannerContext.databaseReferenceRepository).thenReturn(databaseReferenceRepository)
    when(plannerContext.databaseId).thenReturn(sessionDb)
    when(plannerContext.cancellationChecker).thenReturn(mock[CancellationChecker])
    val phaseTracer = mock[CompilationPhaseTracer]
    when(phaseTracer.beginPhase(any())).thenReturn(mock[CompilationPhaseTracer.CompilationPhaseEvent])
    when(plannerContext.tracer).thenReturn(phaseTracer)
    VerifyGraphTarget.transform(state, plannerContext)
  }

  private def parse(query: String): Query =
    JavaCCParser.parse(query, Neo4jCypherExceptionFactory(query, None)) match {
      case q: Query => q
      case _        => fail("Must be a Query")
    }
}
