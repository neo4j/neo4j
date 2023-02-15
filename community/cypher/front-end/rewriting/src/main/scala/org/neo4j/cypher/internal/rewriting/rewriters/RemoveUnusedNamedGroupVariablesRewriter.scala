/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.rewriting.rewriters

import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.QuantifiedPath
import org.neo4j.cypher.internal.expressions.VariableGrouping
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.topDown

case object RemoveUnusedNamedGroupVariablesRewriter extends Rewriter {

  def getUsage(x: AnyRef, groupVariables: Map[LogicalVariable, Int]): Map[LogicalVariable, Int] =
    x.folder.treeFold(groupVariables) {
      case lv: LogicalVariable =>
        acc =>
          acc.get(lv) match {
            case Some(value) => SkipChildren(acc + (lv -> (value + 1)))
            case None        => TraverseChildren(acc)
          }
    }

  override def apply(x: AnyRef): AnyRef = {
    val groupVariables = findGroupVariables(x).map(_.group).map(_ -> 0).toMap
    val unusedGroupVariables = getUsage(x, groupVariables).filterNot { case (_, usage) => usage > 1 }.keySet
    instance(unusedGroupVariables)(x)
  }

  def findGroupVariables(input: AnyRef): Set[VariableGrouping] = {
    input.folder.treeFold(Set.empty[VariableGrouping]) {
      case QuantifiedPath(_, _, _, variableGroupings) =>
        acc => SkipChildren(acc ++ variableGroupings)
      case _ =>
        acc => TraverseChildren(acc)
    }
  }

  def instance(unusedGroupVariables: Set[LogicalVariable]): Rewriter = topDown(Rewriter.lift {
    case qpp @ QuantifiedPath(_, _, _, variableGroupings) =>
      qpp.copy(variableGroupings = removeAnonymousVariables(variableGroupings, unusedGroupVariables))(qpp.position)
  })

  def removeAnonymousVariables(
    variableGroupings: Set[VariableGrouping],
    unusedGroupVariables: Set[LogicalVariable]
  ): Set[VariableGrouping] = {
    variableGroupings.filterNot(varGroup => unusedGroupVariables.contains(varGroup.group))
  }
}
