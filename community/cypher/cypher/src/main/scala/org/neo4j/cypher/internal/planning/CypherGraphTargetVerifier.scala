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
package org.neo4j.cypher.internal.planning

import org.neo4j.cypher.internal.ast.CatalogName
import org.neo4j.cypher.internal.ast.GraphDirectReference
import org.neo4j.cypher.internal.ast.GraphFunctionReference
import org.neo4j.cypher.internal.ast.GraphSelection
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.Union
import org.neo4j.cypher.internal.ast.UseGraph
import org.neo4j.cypher.internal.compiler.planner.GraphTargetVerifier
import org.neo4j.cypher.internal.evaluator.SimpleInternalExpressionEvaluator
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.functions.GraphByElementId
import org.neo4j.cypher.messages.MessageUtilProvider
import org.neo4j.dbms.api.DatabaseNotFoundHelper
import org.neo4j.exceptions.InvalidSemanticsException
import org.neo4j.kernel.database.DatabaseReferenceImpl
import org.neo4j.kernel.database.DatabaseReferenceRepository
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.neo4j.values.ElementIdDecoder
import org.neo4j.values.storable.StringValue
import org.neo4j.values.virtual.MapValue

import scala.annotation.tailrec
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.javaapi.OptionConverters.toScala

class CypherGraphTargetVerifier(databaseReferenceRepository: DatabaseReferenceRepository) extends GraphTargetVerifier {

  override def verifyGraphTarget(
    statement: Statement,
    databaseId: NamedDatabaseId,
    allowCompositeQueries: Boolean,
    params: MapValue
  ): Unit = {
    evaluateGraphSelection(statement, databaseReferenceRepository, params) match {
      case Some(graphNameWithContext) =>
        // add deprecation for aliases that need to be quoted if it's not a composite. This needs to be updated when we pass here for composite databases
        if (graphNameWithContext.graphName.resolveByDisplayName) {
          resolveByDisplayName(databaseReferenceRepository, graphNameWithContext, databaseId, allowCompositeQueries)
        } else {
          resolveByCatalogName(
            databaseReferenceRepository,
            graphNameWithContext,
            databaseId,
            allowCompositeQueries
          )
        }
      case _ =>
    }
  }

  private def resolveByDisplayName(
    databaseReferenceRepository: DatabaseReferenceRepository,
    graphNameWithContext: GraphNameWithContext,
    databaseId: NamedDatabaseId,
    allowCompositeQueries: Boolean
  ): Unit = {
    val catalogName = graphNameWithContext.graphName
    val normalizedDatabaseName = new NormalizedDatabaseName(catalogName.simplifiedQualifiedNameString)
    toScala(databaseReferenceRepository.getByDisplayName(normalizedDatabaseName)) match {
      case None =>
        throw DatabaseNotFoundHelper.databaseNameNotFoundWithoutDot(catalogName.qualifiedNameString)
      case Some(databaseReference)
        if !allowCompositeQueries && databaseReference.namespace().isPresent =>
        throw InvalidSemanticsException.accessingMultipleGraphsOnlySupportedOnCompositeDatabases(
          MessageUtilProvider.createMultipleGraphReferencesError(normalizedDatabaseName.name)
        )
      case Some(databaseReference) if allowCompositeQueries && databaseReference.namespace().isPresent =>
      // accessing constituent is allowed
      case Some(databaseReference: DatabaseReferenceImpl.Internal)
        if !databaseReference.databaseId().equals(databaseId) =>
        unsupportedQueryRouting(graphNameWithContext)
      case _ =>
    }
  }

  private def resolveByCatalogName(
    databaseReferenceRepository: DatabaseReferenceRepository,
    graphNameWithContext: GraphNameWithContext,
    databaseId: NamedDatabaseId,
    allowCompositeQueries: Boolean
  ): Unit = {
    val catalogName = graphNameWithContext.graphName
    val normalizedDatabaseName = new NormalizedDatabaseName(catalogName.qualifiedNameString)
    toScala(databaseReferenceRepository.getByAlias(normalizeCatalogName(normalizedDatabaseName.name()))) match {
      case None
        if !allowCompositeQueries || !isConstituent(
          databaseReferenceRepository,
          normalizedDatabaseName
        ) =>
        throw DatabaseNotFoundHelper.databaseNameNotFoundWithoutDot(
          graphNameWithContext.graphName.qualifiedNameString
        )
      case Some(databaseReference: DatabaseReferenceImpl.Internal)
        if !databaseReference.databaseId().equals(databaseId) =>
        unsupportedQueryRouting(graphNameWithContext)
      case _ =>
    }
  }

  private def normalizeCatalogName(databaseAlias: String): String =
    if (databaseAlias.matches("`.*\\..*`")) {
      val unquoted: String = databaseAlias.substring(1, databaseAlias.length - 1)
      unquoted.replace("``", "`")
    } else {
      databaseAlias
    }

  private def unsupportedQueryRouting(graphNameWithContext: GraphNameWithContext): Unit = {
    if (graphNameWithContext.combinedWithAmbientGraph) {
      throw InvalidSemanticsException.accessingMultipleGraphsOnlySupportedOnCompositeDatabases(
        MessageUtilProvider.createMultipleGraphReferencesError(graphNameWithContext.graphName.qualifiedNameString)
      )
    } else {
      throw InvalidSemanticsException.routingNotSupportedInEmbedded()
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

  private def evaluateGraphSelection(
    statement: Statement,
    databaseReferenceRepository: DatabaseReferenceRepository,
    params: MapValue
  ): Option[GraphNameWithContext] =
    findGraphSelection(statement)
      .map(evaluateGraphSelection(_, databaseReferenceRepository, params))

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

  private def evaluateGraphSelection(
    graphSelection: PositionalGraphSelection,
    databaseReferenceRepository: DatabaseReferenceRepository,
    params: MapValue
  ): GraphNameWithContext =
    graphSelection.graphSelection.graphReference match {
      case direct: GraphDirectReference => GraphNameWithContext(direct.catalogName, !graphSelection.leading)
      case byElementId: GraphFunctionReference if byElementId.functionInvocation.function.equals(GraphByElementId) =>
        val elementIdExpr = byElementId.arguments.head.asInstanceOf[FunctionInvocation].args.head
        val elementIdValue =
          new SimpleInternalExpressionEvaluator().evaluate(elementIdExpr, params).asInstanceOf[StringValue]
        val databaseId = ElementIdDecoder.database(elementIdValue.stringValue())

        GraphNameWithContext(
          CatalogName.of(
            databaseReferenceRepository.getByUuid(databaseId).orElseThrow(() =>
              DatabaseNotFoundHelper.byElementIdFunction(elementIdValue.stringValue())
            )
              .name(),
            resolveStrictly = true
          ),
          !graphSelection.leading
        )
      // Semantic analysis should make sure we don't end up here, so the error does not have to be super descriptive
      case _ => throw InvalidSemanticsException.expectedStaticGraphSelection()
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
