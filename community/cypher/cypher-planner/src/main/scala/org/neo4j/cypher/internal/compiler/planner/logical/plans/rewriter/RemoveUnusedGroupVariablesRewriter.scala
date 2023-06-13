/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.logical.plans.ProjectingPlan
import org.neo4j.cypher.internal.logical.plans.Trail
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.topDown

/**
 * Before
 * .produceResults("1 AS s")
 * .projection(project = Seq("1 AS s"), discard = Set("a", "b", "r", "n", "m"))
 * .trail((a) ((n)-[r]->(m))+ (b), nodeGroupVariables = Set("n", "m"), relationshipGroupVariables = Set("r"))
 * .|.filter(isRepeatTrailUnique(r))
 * .|.expandAll((n)-[r]->(m))
 * .|.argument(n)
 * .allNodeScan(a)
 *
 * After
 * .produceResults("1 AS s")
 * .projection(project = Seq("1 AS s"), discard = Set("a", "b"))
 * .trail((a) ((n)-[r]->(m))+ (b), nodeGroupVariables = Set.empty, relationshipGroupVariables = Set.empty)
 * .|.filter(isRepeatTrailUnique(r))
 * .|.expandAll((n)-[r]->(m))
 * .|.argument(n)
 * .allNodeScan(a)
 *
 * The rewriter finds all usages of group variables. If group variables are not used, then the Trail will be rewritten
 * so that it does not generate any unused group variables. Given that group variables are lists, this optimisation can
 * save time and space.
 *
 * Variables referenced in Projection(discard = Set[String]) should not count towards a legitimate variable usage, given
 * that they are discarded.
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

  def instance(unusedGroupVariables: Set[String]): Rewriter = topDown(Rewriter.lift {
    case t: Trail =>
      val usedNodeVariables = t.nodeVariableGroupings.filterNot(g => unusedGroupVariables.contains(g.groupName.name))
      t.copy(nodeVariableGroupings = usedNodeVariables)(SameId(t.id))
  })

  def findGroupVariableDeclarations(plan: AnyRef): Set[String] = {
    plan.folder.treeFold(Set.empty[String]) {
      case Trail(_, _, _, _, _, _, _, nodeGroupVariables, relationshipGroupVariables, _, _, _, _) =>
        val groupVars = nodeGroupVariables.map(_.groupName.name) ++ relationshipGroupVariables.map(_.groupName.name)
        acc => TraverseChildren(acc ++ groupVars)
    }
  }

  // variables references = total variable count - variable declarations - variable discard (if exists)
  def findAllVariableReferences(plan: AnyRef): Set[String] = {
    plan.folder
      .treeFold(Map.empty[String, Int]) {
        case plan: ProjectingPlan => acc =>
            TraverseChildren(
              plan.discardSymbols.foldLeft(acc) {
                case (acc, variable) => acc + (variable.name -> -1)
              }
            )
        case LogicalVariable(name) => acc =>
            SkipChildren(
              acc.updatedWith(name) {
                case Some(existingCount) => Some(existingCount + 1)
                case None                => Some(1)
              }
            )
      }
      .filterNot { case (_, count) => count == 1 }
      .keySet
  }
}
