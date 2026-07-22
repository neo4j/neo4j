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
import org.mockito.Mockito.when
import org.neo4j.cypher.CommunityCypherTestSuite
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.UseAsMultipleGraphsSelector
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.UseAsSingleGraphSelector
import org.neo4j.cypher.internal.compiler.CypherPlannerConfiguration
import org.neo4j.cypher.internal.compiler.phases.CompilationPhases
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.frontend.helpers.TestContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.frontend.phases.StrictResolveCallables
import org.neo4j.cypher.internal.frontend.phases.factories.ParsingConfig
import org.neo4j.cypher.internal.notification.InternalNotificationLogger
import org.neo4j.cypher.internal.planner.spi.IDPPlannerName
import org.neo4j.cypher.internal.planning.CypherGraphTargetVerifier
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.ErrorMessageProvider
import org.neo4j.cypher.internal.util.test_helpers.GqlExceptionMatchers.gqlException
import org.neo4j.cypher.internal.util.test_helpers.GqlExceptionMatchers.gqlStatus
import org.neo4j.cypher.messages.MessageUtilProvider
import org.neo4j.dbms.api.DatabaseNotFoundException
import org.neo4j.exceptions.InvalidSemanticsException
import org.neo4j.gqlstatus.GqlStatusInfoCodes
import org.neo4j.kernel.database.DatabaseIdFactory
import org.neo4j.kernel.database.DatabaseReferenceImpl
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.database.TestDatabaseReferenceRepository

import java.util.UUID

class VerifyGraphTargetTest extends CommunityCypherTestSuite {

  val neo4j: DatabaseReferenceImpl.Internal = TestDatabaseReferenceRepository.internalDatabaseReference("neo4j")
  val foo: DatabaseReferenceImpl.Internal = TestDatabaseReferenceRepository.internalDatabaseReference("foo")

  val constituent: DatabaseReferenceImpl.Internal =
    TestDatabaseReferenceRepository.internalDatabaseReferenceIn("shard0", "composite")

  val compositeRef: DatabaseReferenceImpl.Composite =
    TestDatabaseReferenceRepository.compositeDatabaseReference("composite", java.util.Set.of(constituent))
  val databaseReferenceRepository = new TestDatabaseReferenceRepository.Fixed(neo4j, foo, constituent, compositeRef)
  val sessionDbName = "neo4j"
  val sessionDb: NamedDatabaseId = DatabaseIdFactory.from(sessionDbName, UUID.randomUUID())

  def beforeAll(): Unit = {}

  test(s"should accept constituent if allowCompositeQueries set to true") {
    val query =
      """
        |USE composite.shard0
        |RETURN 1
        |""".stripMargin

    verifyGraphTarget(query, CypherVersion.Cypher5, compositeRef.databaseId(), allowCompositeQueries = true)
  }

  CypherVersion.values().foreach(version => {
    test(s"Cypher $version: should accept statement without USE clause") {
      verifyGraphTarget("RETURN 1", version)
    }

    test(s"Cypher Cypher $version: should accept USE targeting the session graph") {
      val query =
        """
          |USE neo4j
          |RETURN 1
          |""".stripMargin

      verifyGraphTarget(query, version)
    }

    test(s"Cypher $version: should not accept USE targeting a graph which is not the session one") {
      val query =
        """
          |USE foo
          |RETURN 1
          |""".stripMargin

      the[InvalidSemanticsException] thrownBy verifyGraphTarget(
        query,
        version
      ) should be(
        gqlException(
          "Query routing is not available in embedded sessions. Try running the query using a Neo4j driver or the HTTP API.",
          gqlStatus(
            GqlStatusInfoCodes.STATUS_08N05,
            "error: connection exception - unable to route administration command. Routing administration commands is not supported in embedded sessions. Connect to the system database directly or try running the query using a Neo4j driver or the HTTP API."
          )
        )
      )
    }

    test(s"Cypher $version: should not accept USE targeting a non-existent graph") {
      val query =
        """
          |USE other
          |RETURN 1
          |""".stripMargin

      checkDatabaseNotFoundError(query, "other", version)
    }

    test(
      s"Cypher $version: should accept a combination of ambient and explicit graph selection targeting the session graph"
    ) {
      val query =
        """
          |CALL {
          |  USE neo4j
          |  RETURN 1 AS one
          |}
          |RETURN 1
          |""".stripMargin
      verifyGraphTarget(query, version)
    }

    test(
      s"Cypher $version: should not accept a combination of ambient and explicit graph selection targeting different graphs"
    ) {
      val query =
        """
          |CALL {
          |  USE foo
          |  RETURN 1 AS one
          |}
          |RETURN 1
          |""".stripMargin

      the[InvalidSemanticsException] thrownBy verifyGraphTarget(
        query,
        version
      ) should have message MessageUtilProvider.createMultipleGraphReferencesError("foo")
    }

    test(
      s"Cypher $version: should accept a combination of ambient and explicit graph selection in UNION targeting the session graph"
    ) {
      val query =
        """
          |RETURN 1 AS x
          |UNION
          |USE neo4j
          |RETURN 1 AS x
          |""".stripMargin
      verifyGraphTarget(query, version)
    }

    test(
      s"Cypher $version: should not accept a combination of ambient and explicit graph selection in UNION targeting different graphs"
    ) {
      val query =
        """
          |RETURN 1 AS x
          |UNION
          |USE foo
          |RETURN 1 AS x
          |""".stripMargin
      the[InvalidSemanticsException] thrownBy verifyGraphTarget(
        query,
        version
      ) should have message MessageUtilProvider.createMultipleGraphReferencesError("foo")
    }

    test(s"Cypher $version: should not accept constituent if allowCompositeQueries not set to true") {
      val query =
        """
          |USE composite.shard0
          |RETURN 1
          |""".stripMargin

      // missing reference in repository!
      if (version == CypherVersion.Cypher5) {
        checkDatabaseNotFoundError(query, "composite.shard0", version)
      } else {
        the[InvalidSemanticsException] thrownBy verifyGraphTarget(
          query,
          version,
          compositeRef.databaseId()
        ) should have message MessageUtilProvider.createMultipleGraphReferencesError("composite.shard0")
      }
    }

    test(s"Cypher $version: should accept constituent if allowCompositeQueries set to true") {
      val query =
        """
          |USE composite.shard0
          |RETURN 1
          |""".stripMargin

      verifyGraphTarget(query, version, compositeRef.databaseId(), allowCompositeQueries = true)
    }

    test(s"Cypher $version: should only accept existent constituent if allowCompositeQueries set to true") {
      val query =
        """
          |USE composite.other
          |RETURN 1
          |""".stripMargin

      checkDatabaseNotFoundError(query, "composite.other", version)
    }

    test(
      s"Cypher $version: should accept query if the target is a composite db and allowCompositeQueries set to true"
    ) {
      val query =
        """
          |RETURN 1
          |""".stripMargin

      verifyGraphTarget(query, version, allowCompositeQueries = true, targetsComposite = true)
    }
  })

  private def verifyGraphTarget(
    query: String,
    version: CypherVersion,
    sessionDb: NamedDatabaseId = neo4j.databaseId(),
    allowCompositeQueries: Boolean = false,
    targetsComposite: Boolean = false
  ): Unit = {
    val parsedQuery = parse(version, query)
    val state = mock[BaseState]
    when(state.statement()).thenReturn(parsedQuery)

    val plannerContext = mock[PlannerContext]
    val semanticFeatures =
      if (allowCompositeQueries && targetsComposite) Seq(UseAsMultipleGraphsSelector)
      else Seq(UseAsSingleGraphSelector)
    when(plannerContext.semanticFeatures).thenReturn(semanticFeatures)
    when(plannerContext.graphTargetVerifier).thenReturn(CypherGraphTargetVerifier(databaseReferenceRepository))
    when(plannerContext.databaseId).thenReturn(sessionDb)
    when(plannerContext.cancellationChecker).thenReturn(mock[CancellationChecker])
    when(plannerContext.notificationLogger).thenReturn(mock[InternalNotificationLogger])
    val phaseTracer = mock[CompilationPhaseTracer]
    when(phaseTracer.beginPhase(any())).thenReturn(mock[CompilationPhaseTracer.CompilationPhaseEvent])
    when(plannerContext.tracer).thenReturn(phaseTracer)

    val conf = mock[CypherPlannerConfiguration]
    when(plannerContext.config).thenReturn(conf)
    when(conf.queryRouterForCompositeQueriesEnabled).thenReturn(allowCompositeQueries)

    VerifyGraphTarget.transform(state, plannerContext)
  }

  private def checkDatabaseNotFoundError(query: String, dbName: String, version: CypherVersion): Unit = {
    val e = the[DatabaseNotFoundException] thrownBy verifyGraphTarget(query, version)
    e should have message s"Database $dbName not found"
    e.gqlStatus() shouldBe "42001"
    e.statusDescription() shouldBe "error: syntax error or access rule violation - invalid syntax"

    e.cause() should not be empty
    val cause = e.cause().get()
    cause.gqlStatus() shouldBe "42N00"
    cause.statusDescription() shouldBe
      s"error: syntax error or access rule violation - graph reference not found. A graph reference with the name `$dbName` was not found. Verify that the spelling is correct."
    cause.cause() shouldBe empty
  }

  private def parse(version: CypherVersion, query: String): Query = {
    val parsing = CompilationPhases.parsing(ParsingConfig(resolveCallables = StrictResolveCallables.NoResolver))
    val state = InitialState(query, IDPPlannerName, new AnonymousVariableNameGenerator)
    val ctx = new TestContext(cypherVersion = version) {
      override def errorMessageProvider: ErrorMessageProvider = MessageUtilProvider
    }
    parsing.transform(state, ctx)
      .statement() match {
      case q: Query => q
      case _        => fail(s"Must be a Query, it's not in $version")
    }
  }
}
