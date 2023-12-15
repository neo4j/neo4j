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
package org.neo4j.cypher.internal.runtime

import org.neo4j.cypher.internal.expressions.CachedHasProperty
import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.ScopeExpression
import org.neo4j.cypher.internal.logical.plans.BFSPruningVarExpand
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NFA
import org.neo4j.cypher.internal.logical.plans.NestedPlanExpression
import org.neo4j.cypher.internal.logical.plans.PruningVarExpand
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath
import org.neo4j.cypher.internal.logical.plans.VarExpand
import org.neo4j.cypher.internal.runtime.ast.ConstantExpressionVariable
import org.neo4j.cypher.internal.runtime.ast.ExpressionVariable
import org.neo4j.cypher.internal.runtime.ast.RuntimeConstant
import org.neo4j.cypher.internal.runtime.ast.TemporaryExpressionVariable
import org.neo4j.cypher.internal.util.Foldable
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildrenNewAccForSiblings
import org.neo4j.cypher.internal.util.Rewritable
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.Attribute
import org.neo4j.cypher.internal.util.topDown

import scala.collection.mutable

/**
 * Piece of physical planning which
 *
 *   1) identifies variables that have expression scope (expression variables)
 *   2) allocates slots for these in the expression slot space (separate from ExecutionContext longs and refs)
 *   3) rewrites instances of these variables to [[ExpressionVariable]]s with the correct slots offset
 */
object expressionVariableAllocation {

  /**
   * Attribute listing the expression variables in scope for nested logical plans. Only the root
   * of the nested plan tree will have in expression variables listed here.
   */
  class AvailableExpressionVariables extends Attribute[LogicalPlan, Seq[ExpressionVariable]]

  case class Result[T](rewritten: T, nExpressionSlots: Int, availableExpressionVars: AvailableExpressionVariables)

  def allocate[T <: Foldable with Rewritable](input: T): Result[T] = {

    val globalMapping = mutable.Map[String, ExpressionVariable]()
    val availableExpressionVars = new AvailableExpressionVariables
    // We reserve the first number of slots for runtime constants
    val numberOfConstantVariables = input.folder.treeCount {
      case _: RuntimeConstant => ()
    }
    var constantCounter = 0

    def allocateVariables(
      outerVars: List[ExpressionVariable],
      variables: Set[LogicalVariable]
    ): List[ExpressionVariable] = {
      var innerVars = outerVars
      for (variable <- variables) {
        val nextVariable = TemporaryExpressionVariable(numberOfConstantVariables + innerVars.length, variable.name)
        globalMapping += variable.name -> nextVariable
        innerVars = nextVariable :: innerVars
      }
      innerVars
    }

    def allocateConstant(variable: LogicalVariable): Unit = {
      val nextVariable = ConstantExpressionVariable(constantCounter, variable.name)
      constantCounter += 1
      globalMapping += variable.name -> nextVariable
    }

    // Note: we use the treeFold to keep track of the expression variables in scope
    // We don't need the result, the side-effect mutated `globalMapping` and
    // `availableExpressionVars` contain all the data we need.
    input.folder.treeFold(List.empty[ExpressionVariable]) {
      case x: ScopeExpression =>
        outerVars =>
          val innerVars = allocateVariables(outerVars, x.introducedVariables)
          TraverseChildrenNewAccForSiblings(innerVars, _ => outerVars)

      case x: VarExpand =>
        outerVars =>
          val innerVars =
            allocateVariables(outerVars, (x.nodePredicates ++ x.relationshipPredicates).map(_.variable).toSet)
          TraverseChildrenNewAccForSiblings(innerVars, _ => outerVars)

      case x: PruningVarExpand =>
        outerVars =>
          val innerVars =
            allocateVariables(outerVars, (x.nodePredicates ++ x.relationshipPredicates).map(_.variable).toSet)
          TraverseChildrenNewAccForSiblings(innerVars, _ => outerVars)

      case x: BFSPruningVarExpand =>
        outerVars =>
          val innerVars =
            allocateVariables(outerVars, (x.nodePredicates ++ x.relationshipPredicates).map(_.variable).toSet)
          TraverseChildrenNewAccForSiblings(innerVars, _ => outerVars)

      case x: FindShortestPaths =>
        outerVars =>
          val innerVars =
            allocateVariables(outerVars, (x.perStepNodePredicates ++ x.perStepRelPredicates).map(_.variable).toSet)
          TraverseChildrenNewAccForSiblings(innerVars, _ => outerVars)

      case x: StatefulShortestPath =>
        outerVars =>
          val innerVars =
            allocateVariables(
              outerVars,
              (
                x.singletonNodeVariables.map(_.nfaExprVar) ++
                  x.singletonRelationshipVariables.map(_.nfaExprVar) ++
                  x.nodeVariableGroupings.map(_.singleton) ++
                  x.relationshipVariableGroupings.map(_.singleton)
              )
            )
          TraverseChildrenNewAccForSiblings(innerVars, _ => outerVars)

      case x: NFA =>
        outerVars =>
          // all of the predicates in the NFA use expression variables because they are not yet written to the row
          // when evaluated during NFA traversal
          val predicateVarNames = x.predicateVariables

          val innerVars =
            allocateVariables(outerVars, predicateVarNames)
          TraverseChildrenNewAccForSiblings(innerVars, _ => outerVars)

      case x: NestedPlanExpression =>
        outerVars => {
          availableExpressionVars.set(x.plan.id, outerVars)
          TraverseChildrenNewAccForSiblings(outerVars, _ => outerVars)
        }

      case x: RuntimeConstant =>
        outerVars => {
          allocateConstant(x.variable)
          TraverseChildren(outerVars)
        }
    }

    val rewriter =
      topDown(Rewriter.lift {
        // Cached properties would have to be cached together with the Expression Variables.
        // Not caching the property until we have support for that.
        case cp @ CachedProperty(_, v, p, _, _) if globalMapping.contains(v.name) =>
          Property(globalMapping(v.name), p)(cp.position)
        case cp @ CachedHasProperty(_, v, p, _, _) if globalMapping.contains(v.name) =>
          Property(globalMapping(v.name), p)(cp.position)
        case x: LogicalVariable if globalMapping.contains(x.name) =>
          globalMapping(x.name)
      })

    val nExpressionSlots = globalMapping.values.map(_.offset).reduceOption(math.max).map(_ + 1).getOrElse(0)
    Result(input.endoRewrite(rewriter), nExpressionSlots, availableExpressionVars)
  }
}
