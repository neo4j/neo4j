/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.scoping.inspection_tool

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.semantics.SemanticErrorDef
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.frontend.PlannerName
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.InternalUsageStats
import org.neo4j.cypher.internal.frontend.phases.InternalUsageStatsNoOp
import org.neo4j.cypher.internal.frontend.phases.Monitors
import org.neo4j.cypher.internal.notification.devNullLogger
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.ErrorMessageProvider
import org.neo4j.cypher.internal.util.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.util.NotImplementedErrorMessageProvider
import org.neo4j.kernel.database.DatabaseReference
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.database.NormalizedCatalogEntry
import org.neo4j.kernel.database.NormalizedDatabaseName

import java.util.Optional
import java.util.UUID

class InspectionContext(
  override val cypherVersion: CypherVersion,
  val isComposite: Boolean = false,
  databaseName: String = "mock",
  query: String = "mock",
  override val semanticFeatures: Seq[SemanticFeature] = Seq(),
  override val shadowedFunctions: Set[String] = Set.empty
) extends BaseContext {

  var errors: Seq[SemanticErrorDef] = Seq.empty

  override def tracer: CompilationPhaseTracer = CompilationPhaseTracer.NO_TRACING
  override def notificationLogger: devNullLogger.type = devNullLogger
  override def cypherExceptionFactory: CypherExceptionFactory = Neo4jCypherExceptionFactory(query, None)
  override def monitors: Monitors = ???

  override def errorHandler: Seq[SemanticErrorDef] => Unit = (errs: Seq[SemanticErrorDef]) => {
    val newErrs = errs.filterNot(errors.contains)
    errors ++= newErrs
  }

  override def errorMessageProvider: ErrorMessageProvider = NotImplementedErrorMessageProvider
  override def cancellationChecker: CancellationChecker = CancellationChecker.NeverCancelled
  override def internalUsageStats: InternalUsageStats = InternalUsageStatsNoOp

  override def sessionDatabase: DatabaseReference = {
    val outerComposite = isComposite
    new DatabaseReference {
      override def alias(): NormalizedDatabaseName = ???
      override def namespace(): Optional[NormalizedDatabaseName] = Optional.empty()
      override def isPrimary: Boolean = true
      override def id(): UUID = ???
      override def namedDatabaseId(): NamedDatabaseId = ???
      override def toPrettyString: String = s"${fullName().name()} (isComposite: $isComposite)"
      override def fullName(): NormalizedDatabaseName = new NormalizedDatabaseName(databaseName)
      override def compareTo(o: DatabaseReference): Int = 0
      override def isComposite: Boolean = outerComposite
      override def owningDatabaseName: String = ???
      override def catalogEntry(): NormalizedCatalogEntry = ???
      override def isShard: Boolean = false
    }
  }

  override def isScopeQuery: Boolean = false
  override val isDebugSession: Boolean = true
}

object InspectionPlannerName extends PlannerName {
  override def name: String = "no planner"
  override def toTextOutput: String = "no planner"
  override def version: String = "no version"
}
