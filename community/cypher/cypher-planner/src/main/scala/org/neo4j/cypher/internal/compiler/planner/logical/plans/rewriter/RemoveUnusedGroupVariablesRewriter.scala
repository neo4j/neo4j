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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.logical.plans.PlanWithVariableGroupings
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.topDown

/**
 * Before
 * .produceResults("1 AS s")
 * .projection("1 AS s")
 * .trail((a) ((n)-[r]->(m))+ (b), nodeGroupVariables = Set("n", "m"), relationshipGroupVariables = Set("r"))
 * .|.filter(isRepeatTrailUnique(r))
 * .|.expandAll((n)-[r]->(m))
 * .|.argument(n)
 * .allNodeScan(a)
 *
 * After
 * .produceResults("1 AS s")
 * .projection("1 AS s")
 * .trail((a) ((n)-[r]->(m))+ (b), nodeGroupVariables = Set.empty, relationshipGroupVariables = Set.empty)
 * .|.filter(isRepeatTrailUnique(r))
 * .|.expandAll((n)-[r]->(m))
 * .|.argument(n)
 * .allNodeScan(a)
 *
 * The rewriter finds all usages of group variables. If group variables are not used, then the Trail/StatefulShortestPath
 * will be rewritten so that it does not generate any unused group variables. 
 * Given that group variables are lists, this optimisation can save time and space.
 *
 * Should run before [[TrailToVarExpandRewriter]] as these rewrites cannot happen if group variables are not removed.
 */
case object RemoveUnusedGroupVariablesRewriter extends Rewriter {

  override def apply(plan: AnyRef): AnyRef = {
    val allVariableReferences = findAllVariableReferences(plan)
    val allGroupVariableDeclarations = findGroupVariableDeclarations(plan)
    val unusedGroupVariableDeclarations = allGroupVariableDeclarations.diff(allVariableReferences)
    instance(unusedGroupVariableDeclarations)(plan)
  }

  def instance(unusedGroupVariables: Set[LogicalVariable]): Rewriter = topDown(Rewriter.lift {
    case p: PlanWithVariableGroupings =>
      val usedNodeVariables = p.nodeVariableGroupings.filterNot(g => unusedGroupVariables.contains(g.group))
      val usedRelVariables = p.relationshipVariableGroupings.filterNot(g => unusedGroupVariables.contains(g.group))
      p.withVariableGroupings(
        nodeVariableGroupings = usedNodeVariables,
        relationshipVariableGroupings = usedRelVariables
      )(SameId(p.id))
  })

  def findGroupVariableDeclarations(plan: AnyRef): Set[LogicalVariable] = {
    plan.folder.treeFold(Set.empty[LogicalVariable]) {
      case p: PlanWithVariableGroupings =>
        val groupVars = p.nodeVariableGroupings.map(_.group) ++ p.relationshipVariableGroupings.map(_.group)
        acc => TraverseChildren(acc ++ groupVars)
    }
  }

  def findAllVariableReferences(plan: AnyRef): Set[LogicalVariable] = {
    def inner(plan: AnyRef): Map[LogicalVariable, Int] =
      plan.folder.treeFold(Map.empty[LogicalVariable, Int]) {
        case variable: LogicalVariable => acc =>
            SkipChildren(
              acc.updatedWith(variable) {
                case Some(existingCount) => Some(existingCount + 1)
                case None                => Some(1)
              }
            )
      }

    // If a variable only appears once, that is the declaration of the variable and the variable is otherwise unused.
    // If a variable appears at least twice, there is a reference to the variable.
    inner(plan)
      .filter { case (_, count) => count > 1 }
      .keySet
  }
}
