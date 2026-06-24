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
package org.neo4j.cypher.internal.preparser

import org.neo4j.configuration.Config
import org.neo4j.cypher.CommunityCypherTestSuite
import org.neo4j.cypher.internal.CachingPreParser
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.TestExecutorCaffeineCacheFactory
import org.neo4j.cypher.internal.ast.semantics.SemanticErrorDef
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.cache.LFUCache
import org.neo4j.cypher.internal.compiler.phases.CompilationPhases
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.frontend.phases.InternalUsageStats
import org.neo4j.cypher.internal.frontend.phases.InternalUsageStatsNoOp
import org.neo4j.cypher.internal.frontend.phases.Monitors
import org.neo4j.cypher.internal.frontend.phases.StrictResolveCallables
import org.neo4j.cypher.internal.frontend.phases.factories.ParsingConfig
import org.neo4j.cypher.internal.notification.InternalNotificationLogger
import org.neo4j.cypher.internal.notification.devNullLogger
import org.neo4j.cypher.internal.planner.spi.IDPPlannerName
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.ErrorMessageProvider
import org.neo4j.cypher.internal.util.Neo4jCypherExceptionFactory
import org.neo4j.cypher.messages.MessageUtilProvider
import org.neo4j.kernel.database.DatabaseReference

class InputQueryTest extends CommunityCypherTestSuite {

  private val preParser =
    new CachingPreParser(
      CypherConfiguration.fromConfig(Config.defaults()),
      new LFUCache[PreParsedQuery.CacheKey, PreParsedQuery](TestExecutorCaffeineCacheFactory, 0)
    )

  private def parser =
    CompilationPhases.parsing(
      ParsingConfig(resolveCallables = StrictResolveCallables.NoResolver)
    )

  private def toPreParsedQuery(queryString: String, version: CypherVersion) =
    preParser.preParseQuery(queryString, devNullLogger, version)

  // Note, only parse in default version
  private def toFullyParsedQuery(queryString: String, version: CypherVersion) = FullyParsedQuery(
    state = parser.transform(
      from = InitialState(queryString, IDPPlannerName, new AnonymousVariableNameGenerator),
      context = createContext(version)
    ),
    options = QueryOptions.default(version)
  )

  test("same input string should have same cache keys (FullyParsedQuery)") {

    val queryString =
      """UNWIND [0, 1] AS x
        |MATCH (a)--(b)
        |RETURN x, a, b
        |""".stripMargin

    val a = toFullyParsedQuery(queryString, CypherVersion.Legacy.legacyVersion())
    val b = toFullyParsedQuery(queryString, CypherVersion.Legacy.legacyVersion())

    a.cacheKey shouldEqual b.cacheKey
    a.cacheKey.hashCode() shouldEqual b.cacheKey.hashCode()
  }

  test("same input string should have same cache keys (PreParsedQuery)") {

    val queryString =
      """UNWIND [0, 1] AS x
        |MATCH (a)--(b)
        |RETURN x, a, b
        |""".stripMargin

    val a = toPreParsedQuery(queryString, CypherVersion.Legacy.legacyVersion())
    val b = toPreParsedQuery(queryString, CypherVersion.Legacy.legacyVersion())

    a.cacheKey shouldEqual b.cacheKey
    a.cacheKey.hashCode() shouldEqual b.cacheKey.hashCode()
  }

  private def createContext(version: CypherVersion) = {
    new BaseContext {
      override def cypherVersion: CypherVersion = version

      override def notificationLogger: InternalNotificationLogger = devNullLogger
      override def tracer: CompilationPhaseTracer = CompilationPhaseTracer.NO_TRACING
      override def cypherExceptionFactory: CypherExceptionFactory = Neo4jCypherExceptionFactory("", None)
      override def monitors: Monitors = mock[Monitors]
      override val errorHandler: Seq[SemanticErrorDef] => Unit = _ => ()
      override val errorMessageProvider: ErrorMessageProvider = MessageUtilProvider
      override def cancellationChecker: CancellationChecker = CancellationChecker.NeverCancelled
      override def internalUsageStats: InternalUsageStats = InternalUsageStatsNoOp
      override def sessionDatabase: DatabaseReference = null
      override def semanticFeatures: Seq[SemanticFeature] = Seq.empty
      override def isScopeQuery: Boolean = false
      override def shadowedFunctions: Set[String] = Set.empty
      override def isDebugSession: Boolean = false
    }
  }
}
