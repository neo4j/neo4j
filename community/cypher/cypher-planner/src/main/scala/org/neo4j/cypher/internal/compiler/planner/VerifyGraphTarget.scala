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

import org.neo4j.cypher.internal.ast.CatalogName
import org.neo4j.cypher.internal.ast.GraphDirectReference
import org.neo4j.cypher.internal.ast.GraphSelection
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.Union
import org.neo4j.cypher.internal.ast.UseGraph
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.frontend.phases.BaseContains
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase
import org.neo4j.cypher.internal.frontend.phases.VisitorPhase
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerFactory
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.DefaultPostCondition
import org.neo4j.cypher.messages.MessageUtilProvider
import org.neo4j.dbms.api.DatabaseNotFoundException
import org.neo4j.exceptions.InvalidSemanticsException
import org.neo4j.kernel.database.DatabaseReferenceRepository
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.database.NormalizedDatabaseName

import scala.annotation.tailrec
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.javaapi.OptionConverters.toScala

/**
 * Verifies correct graph selection done with USE clause.
 * Query router performs graph selection evaluation and sends a query
 * to a correct target, so this check is here mainly for queries submitted through
 * Core API which do not go through Query router.
 * USE clause is allowed for Core API queries, but since no routing is performed for such queries,
 * the USE clause is permitted to evaluate only to the session graph.
 * This verifier performs check for combination of explicit and ambient graph selection which is
 * useful even for queries that have gone through Query router as this check is not (and cannot be)
 * performed by semantic analysis.
 */
case object VerifyGraphTarget extends VisitorPhase[PlannerContext, BaseState] with StepSequencer.Step
    with DefaultPostCondition
    with PlanPipelineTransformerFactory {

  override def phase: CompilationPhaseTracer.CompilationPhase = CompilationPhase.LOGICAL_PLANNING

  override def visit(value: BaseState, context: PlannerContext): Unit = {
    verifyGraphTarget(
      context.databaseReferenceRepository,
      value.statement(),
      context.databaseId,
      context.config.queryRouterForCompositeQueriesEnabled
    )
  }

  override def preConditions: Set[StepSequencer.Condition] =
    Set(BaseContains[Statement](), BaseContains[SemanticState]())

  // necessary because VisitorPhase defines empty postConditions
  override def postConditions: Set[StepSequencer.Condition] = Set(completed)

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set.empty

  override def getTransformer(
    pushdownPropertyReads: Boolean,
    semanticFeatures: Seq[SemanticFeature]
  ): VisitorPhase[PlannerContext, BaseState] = this

  private def verifyGraphTarget(
    databaseReferenceRepository: DatabaseReferenceRepository,
    statement: Statement,
    databaseId: NamedDatabaseId,
    allowCompositeQueries: Boolean
  ): Unit = {
    evaluateGraphSelection(statement) match {
      case Some(graphNameWithContext) =>
        val normalizedDatabaseName = new NormalizedDatabaseName(graphNameWithContext.graphName.qualifiedNameString)
        toScala(
          databaseReferenceRepository.getInternalByAlias(
            normalizedDatabaseName
          )
        ) match {
          case None if !allowCompositeQueries || !isConstituent(databaseReferenceRepository, normalizedDatabaseName) =>
            throw new DatabaseNotFoundException(
              s"Database ${graphNameWithContext.graphName.qualifiedNameString} not found"
            )
          case Some(databaseReference)
            if !allowCompositeQueries && !databaseReference.databaseId().equals(databaseId) =>
            graphNameWithContext match {
              // If an explicit graph selection is combined with ambient one and both target different graphs,
              // it makes the query effectively a composite one.
              case GraphNameWithContext(graphName, true) =>
                throw new InvalidSemanticsException(
                  MessageUtilProvider.createMultipleGraphReferencesError(graphName.qualifiedNameString)
                )
              // If we are here it means that the query came from the Core API, because Query router would send
              // the query to the correct database if it came from Bolt or HTTP API
              case GraphNameWithContext(_, false) => throw new InvalidSemanticsException(
                  "Query routing is not available in embedded sessions. Try running the query using a Neo4j driver or the HTTP API."
                )
            }
          case _ =>
        }
      case _ =>
    }
  }

  private def isConstituent(
    databaseReferenceRepository: DatabaseReferenceRepository,
    normalizedDatabaseName: NormalizedDatabaseName
  ): Boolean =
    databaseReferenceRepository.getCompositeDatabaseReferences.asScala
      .flatMap(_.constituents().asScala)
      .map(_.fullName())
      .exists(_ == normalizedDatabaseName)

  private def evaluateGraphSelection(statement: Statement): Option[GraphNameWithContext] =
    findGraphSelection(statement).map(evaluateGraphSelection)

  private def findGraphSelection(statement: Statement): Option[PositionalGraphSelection] = {
    // Semantic analysis ensures correct position and use of graph selection.
    // so here it is enough just to find one if there is any.
    // In other words, we don't have to duplicate the checks done by semantic analysis here.
    leadingGraphSelection(statement) match {
      case Some(graphSelection) => Some(PositionalGraphSelection(graphSelection, leading = true))
      case None                 =>
        // Unfortunately, combination of ambient and explicit graph selection is allowed,
        // so there can be a graph selection somewhere deeper in the query.
        statement.folder.treeFindByClass[UseGraph] match {
          case Some(graphSelection) => Some(PositionalGraphSelection(graphSelection, leading = false))
          case None                 => None
        }
    }
  }

  private def evaluateGraphSelection(graphSelection: PositionalGraphSelection): GraphNameWithContext =
    graphSelection.graphSelection.graphReference match {
      case direct: GraphDirectReference => GraphNameWithContext(direct.catalogName, !graphSelection.leading)
      // Semantic analysis should make sure we don't end up here, so the error does not have to be super descriptive
      case _ => throw new InvalidSemanticsException("Expected static graph selection")
    }

  @tailrec
  private def leftmostSingleQuery(statement: Statement): Option[SingleQuery] =
    statement match {
      case sq: SingleQuery => Some(sq)
      case union: Union    => leftmostSingleQuery(union.lhs)
      case _               => None
    }

  private def leadingGraphSelection(statement: Statement): Option[GraphSelection] = {
    val singleQuery = leftmostSingleQuery(statement)
    val clause = singleQuery.flatMap(_.clauses.headOption)
    clause.collect {
      case gs: GraphSelection => gs
    }
  }

  private case class PositionalGraphSelection(graphSelection: GraphSelection, leading: Boolean)

  private case class GraphNameWithContext(graphName: CatalogName, combinedWithAmbientGraph: Boolean)
}
