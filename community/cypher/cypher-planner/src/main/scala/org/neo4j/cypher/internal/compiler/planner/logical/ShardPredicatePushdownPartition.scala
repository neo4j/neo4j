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

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.ExecutionModel.Volcano
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.ConstantTemporalFunction
import org.neo4j.cypher.internal.compiler.planner.logical.schema.GraphSchemaOptimizations
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.IndexCompatiblePredicatesProviderContext
import org.neo4j.cypher.internal.expressions.AndedPropertyInequalities
import org.neo4j.cypher.internal.expressions.BinaryOperatorExpression
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LeftUnaryOperatorExpression
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.Literal
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.MultiOperatorExpression
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RightUnaryOperatorExpression
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.ir.UnionQuery
import org.neo4j.cypher.internal.logical.plans.CachedProperties
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.Selection.LabelAndRelTypeInfo
import org.neo4j.cypher.internal.macros.AssertMacros3
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.PredicateHelper.coercePredicatesWithAnds
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship

import scala.annotation.tailrec

/**
 * A utility class that holds the partition of how predicates must be applied
 *
 * @param preFilterBeforePushdown - the collection of predicates to apply before fetching properties from shards,e.g., hasLabels
 * @param filterOnShards - the variable and collection predicates dependent on that variable alone that can be applied on the shard
 * @param filterOnMainWithRemoteProperties - the collection of predicates that can only be applied after fetching properties from shards (includes predicates using multiple variables)
 */
case class ShardPredicatePushdownPartition(
  preFilterBeforePushdown: Set[Expression],
  filterOnShards: Option[PushedPredicatesDetails],
  filterOnMainWithRemoteProperties: Set[Expression]
)

case class PushedPredicatesDetails(
  logicalVariable: LogicalVariable,
  expressions: Set[Expression],
  importedPerRowValues: Set[Expression],
  importedConstantValues: Set[Expression]
)

object ShardPredicatePushdownPartition {

  def withPreFilterBeforePushdown(exprs: Set[Expression]): ShardPredicatePushdownPartition =
    new ShardPredicatePushdownPartition(exprs, None, Set.empty)

  def withFilterOnMainWithRemoteProperties(exprs: Set[Expression]): ShardPredicatePushdownPartition =
    new ShardPredicatePushdownPartition(Set.empty, None, exprs)

  def withPredicatesOnShards(
    variable: LogicalVariable,
    exprs: Set[Expression],
    importedPerRowValues: Set[Expression],
    importedConstantValues: Set[Expression]
  ): ShardPredicatePushdownPartition =
    new ShardPredicatePushdownPartition(
      Set.empty,
      Some(PushedPredicatesDetails(variable, exprs, importedPerRowValues, importedConstantValues)),
      Set.empty
    )

  /**
   * Identifies how the given set of predicates must be applied. All the predicates that can be pushed down to the shard must meet the following criteria:
   * 1. Use a single variable
   * 2. Contain at least one non-cached property access for this variable
   * 3. Only contain the supported operators (listed in predicatePushdownSupport)
   *
   * If multiple variables have predicates that can be pushed down to the shard, only the variable with the most selective predicates is pushed down. The rest of the predicates will be executed on the main after fetching properties from the shard.
   * If predicates do not match the above criteria but use a non-cached property access, then these predicates will be executed on the main after fetching properties from the shard.
   * All other predicates that do not match the above criteria, will be executed on the main shard before fetching any properties from the shard, to reduce the number of rows to fetch properties for.
   *
   * @param input - the incoming logical plan on top of which the selection must be applied.
   * @param context - the logical planning context
   * @param predicates - the predicates to partition
   * @return
   */
  def apply(
    input: LogicalPlan,
    context: LogicalPlanningContext,
    predicates: Set[Expression]
  ): ShardPredicatePushdownPartition = {

    val alreadyCachedProperties = context.staticComponents.planningAttributes.cachedPropertiesPerPlan.get(input.id)

    val (noPropAccesses, supportedPropAccesses, unsupportedPropAccesses) =
      predicates.foldLeft(
        (Set.empty[Expression], Map.empty[LogicalVariable, PushedPredicatesDetails], Set.empty[Expression])
      ) {
        (acc, expr) =>
          acc match {
            case (noUncachedPropAccesses, supportedUncachedPropAccesses, unsupportedUncachedPropAccesses) =>
              supportsPredicatesPushdown(
                context.semanticTable,
                expr,
                alreadyCachedProperties,
                input.availableSymbols
              ) match {
                case PredicatePushdownSupported(logicalVariable, importedPerRowValues, importedConstantValues) =>
                  (
                    noUncachedPropAccesses,
                    supportedUncachedPropAccesses.updatedWith(logicalVariable) {
                      case Some(existingPushedPredicates) => Some(existingPushedPredicates.copy(
                          expressions = existingPushedPredicates.expressions + expr,
                          importedPerRowValues = existingPushedPredicates.importedPerRowValues ++ importedPerRowValues,
                          importedConstantValues =
                            existingPushedPredicates.importedConstantValues ++ importedConstantValues
                        ))
                      case None => Some(PushedPredicatesDetails(
                          logicalVariable,
                          Set(expr),
                          importedPerRowValues,
                          importedConstantValues
                        ))
                    },
                    unsupportedUncachedPropAccesses
                  )
                case PredicatePushdownUnsupported if containsUncachedPropertyAccess(expr, alreadyCachedProperties) =>
                  (noUncachedPropAccesses, supportedUncachedPropAccesses, unsupportedUncachedPropAccesses + expr)
                case PredicatePushdownUnsupported =>
                  (noUncachedPropAccesses + expr, supportedUncachedPropAccesses, unsupportedUncachedPropAccesses)
              }
          }
      }
    if (context.settings.executionModel == Volcano) {
      // Predicate pushdown is not supported in the slotted runtime. Lets filter everything in main.
      ShardPredicatePushdownPartition(
        noPropAccesses,
        None,
        unsupportedPropAccesses ++ supportedPropAccesses.values.flatMap(_.expressions)
      )
    } else {
      val predicatesToPushdownToShard = mostSelectivePredicates(input, context, supportedPropAccesses)

      ShardPredicatePushdownPartition(
        noPropAccesses,
        predicatesToPushdownToShard,
        unsupportedPropAccesses ++ supportedPropAccesses.flatMap {
          case (variable, propertyPredicateDetails)
            if predicatesToPushdownToShard.exists(_.logicalVariable != variable) => propertyPredicateDetails.expressions
          case _ => Set.empty
        }
      )
    }
  }

  /*
   * Identify the most selective predicate by
   *  1. appending the predicates to the incominq query graph
   *  2. calculating the cardinality of the new query graph.
   *  3. selecting the set of predicates with the lowest estimated cardinality
   */
  private def mostSelectivePredicates(
    input: LogicalPlan,
    context: LogicalPlanningContext,
    candidates: Map[LogicalVariable, PushedPredicatesDetails]
  ): Option[PushedPredicatesDetails] = {
    if (candidates.size < 2) {
      // No need to identify predicates in this case.
      candidates.values.headOption
    } else {
      val labelAndRelTypeInfo =
        LabelAndRelTypeInfo(context.plannerState.input.labelInfo, context.plannerState.input.relTypeInfo)
      val incomingCardinality = context.staticComponents.planningAttributes.cardinalities.get(input.id)
      val solvedBeforePredicate = context.staticComponents.planningAttributes.solveds.get(input.id) match {
        case query: SinglePlannerQuery => query
        case _: UnionQuery             =>
          // The union query doesn't have a single query graph to ammend. We can create a new singlePlannerQuery that all selectivity will be calculated against.
          RegularSinglePlannerQuery(QueryGraph(argumentIds = input.availableSymbols))
      }

      // to identify the most selective predicates we build a dummy single planner query with the set of predicates and the previously available symbol
      def calculateCardinality(variable: LogicalVariable, preds: Set[Expression]): Cardinality = {
        coercePredicatesWithAnds(preds.toSeq) match {
          case Some(andedPredicates) =>
            // create a dummy "solved" query graph and add the predicates to it, so that the cardinality model can calculate the new cardinality.
            val solved = solvedBeforePredicate.updateTailOrSelf(_.amendQueryGraph(_.addPredicates(andedPredicates)))
            context.staticComponents.metrics.cardinality(
              solved,
              labelAndRelTypeInfo.labelInfo,
              labelAndRelTypeInfo.relTypeInfo,
              context.semanticTable,
              IndexCompatiblePredicatesProviderContext.default,
              GraphSchemaOptimizations.Disabled
            )
          case None =>
            // We should never hit this scenario since any variable with empty sets of predicates should never be added to the map.
            AssertMacros3.checkOnlyWhenAssertionsAreEnabled(
              false,
              s"Unexpected empty set of predicates found for pushdown candidate variable $variable"
            )
            incomingCardinality
        }
      }

      val mostSelectiveOption = candidates.minBy {
        case (variable, preds) =>
          // TODO: could we also consider the cost of importing values per row here?
          calculateCardinality(variable, preds.expressions)
      }

      Some(mostSelectiveOption._2)
    }
  }

  private def containsUncachedPropertyAccess(
    expression: Expression,
    alreadyCachedProperties: CachedProperties
  ): Boolean = expression.folder.treeExists {
    case Property(logicalVariable: LogicalVariable, propertyKey) =>
      !alreadyCachedProperties.contains(logicalVariable, propertyKey)
  }

  private case class AccumulatedPropertyAccesses(
    variable: Option[LogicalVariable],
    nonCachedProperties: Set[PropertyKeyName],
    importedPerRowValues: Set[Expression] = Set.empty,
    importedConstantValues: Set[Expression] = Set.empty
  )

  private def supportsPredicatesPushdown(
    semanticTable: SemanticTable,
    expression: Expression,
    alreadyCachedProperties: CachedProperties,
    availableSymbols: Set[LogicalVariable]
  ): PredicatesPushdownSupport = {
    @tailrec
    def findAllSupportedPropertyAccesses(
      expressionQueue: List[Expression],
      knownUncachedPropertyAccesses: AccumulatedPropertyAccesses
    ): Either[PredicatesPushdownSupport, AccumulatedPropertyAccesses] = expressionQueue match {
      case Nil => Right(knownUncachedPropertyAccesses)
      case firstExpression :: nextExpressions => firstExpression match {
          case variable: LogicalVariable =>
            val importedPerRowValues =
              if (availableSymbols.contains(variable))
                knownUncachedPropertyAccesses.importedPerRowValues + variable // TODO: further check if this is a constant.
              else knownUncachedPropertyAccesses.importedPerRowValues
            findAllSupportedPropertyAccesses(
              nextExpressions,
              knownUncachedPropertyAccesses.copy(importedPerRowValues = importedPerRowValues)
            )

          case prop @ Property(variable: LogicalVariable, propertyKey)
            if alreadyCachedProperties.contains(variable, propertyKey) =>
            findAllSupportedPropertyAccesses(
              nextExpressions,
              knownUncachedPropertyAccesses.copy(importedPerRowValues =
                knownUncachedPropertyAccesses.importedPerRowValues + prop
              )
            )

          case Property(variable: LogicalVariable, propertyKeyName)
            if availableSymbols.contains(variable) && semanticTable.typeFor(variable).isAnyOf(CTNode, CTRelationship) =>
            knownUncachedPropertyAccesses.variable match {
              case Some(existingVariable) if existingVariable != variable =>
                // multiple variables found in the expression, cannot push to shard.
                Left(PredicatePushdownUnsupported)
              case _ => findAllSupportedPropertyAccesses(
                  nextExpressions,
                  knownUncachedPropertyAccesses.copy(
                    variable = Some(variable),
                    nonCachedProperties = knownUncachedPropertyAccesses.nonCachedProperties + propertyKeyName
                  )
                )
            }
          case Property(_: LogicalVariable, _) =>
            Left(PredicatePushdownUnsupported)
          case Property(nestedExpression, _) =>
            findAllSupportedPropertyAccesses(nextExpressions :+ nestedExpression, knownUncachedPropertyAccesses)
          case e: BinaryOperatorExpression =>
            findAllSupportedPropertyAccesses(
              nextExpressions ++ List(e.lhs, e.rhs),
              knownUncachedPropertyAccesses
            )
          case e: LeftUnaryOperatorExpression =>
            findAllSupportedPropertyAccesses(
              nextExpressions :+ e.rhs,
              knownUncachedPropertyAccesses
            )
          case e: RightUnaryOperatorExpression =>
            findAllSupportedPropertyAccesses(
              nextExpressions :+ e.lhs,
              knownUncachedPropertyAccesses
            )
          case e: MultiOperatorExpression =>
            findAllSupportedPropertyAccesses(
              nextExpressions ++ e.exprs,
              knownUncachedPropertyAccesses
            )
          case ListLiteral(expressions) =>
            findAllSupportedPropertyAccesses(nextExpressions ++ expressions, knownUncachedPropertyAccesses)
          case AndedPropertyInequalities(_, _, inequalities) =>
            findAllSupportedPropertyAccesses(nextExpressions ++ inequalities, knownUncachedPropertyAccesses)
          case _: Parameter => findAllSupportedPropertyAccesses(nextExpressions, knownUncachedPropertyAccesses)
          case _: Literal   => findAllSupportedPropertyAccesses(nextExpressions, knownUncachedPropertyAccesses)
          case ConstantTemporalFunction(_) =>
            findAllSupportedPropertyAccesses(nextExpressions, knownUncachedPropertyAccesses)
          case _ => Left(PredicatePushdownUnsupported)
        }
    }

    findAllSupportedPropertyAccesses(List(expression), AccumulatedPropertyAccesses(None, Set.empty)) match {
      case Right(AccumulatedPropertyAccesses(
          Some(variable),
          nonCachedProperties,
          importedPerRowValues,
          importedConstantValues
        )) if nonCachedProperties.nonEmpty =>
        PredicatePushdownSupported(variable, importedPerRowValues, importedConstantValues)
      case _ => PredicatePushdownUnsupported
    }
  }

  sealed private trait PredicatesPushdownSupport

  private case class PredicatePushdownSupported(
    logicalVariable: LogicalVariable,
    importedPerRowValues: Set[Expression],
    importedConstantValues: Set[Expression]
  ) extends PredicatesPushdownSupport
  private case object PredicatePushdownUnsupported extends PredicatesPushdownSupport
}
