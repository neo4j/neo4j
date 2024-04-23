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
import org.neo4j.cypher.internal.expressions.MultiRelationshipPathStep
import org.neo4j.cypher.internal.expressions.NodeRelPair
import org.neo4j.cypher.internal.expressions.PathStep
import org.neo4j.cypher.internal.expressions.RepeatPathStep
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.Rewriter.TopDownMergeableRewriter
import org.neo4j.cypher.internal.util.topDown

/**
 * This rewriter transforms RepeatPathStep with only one relationship reference into MultiRelationshipPathStep
 * to remove the reference to the from node.
 *
 * This makes it possible to remove more NodeGroupVariables later in [[RemoveUnusedGroupVariablesRewriter]]
 * so it needs to be run before that rewriter
 */
case object RepeatPathStepToMultiRelationshipRewriter extends Rewriter with TopDownMergeableRewriter {

  override def apply(input: AnyRef): AnyRef = instance.apply(input)

  override val innerRewriter: Rewriter =
    Rewriter.lift {
      case repeatPathStep @ RepeatPathStep(variables: Seq[NodeRelPair], toNode: LogicalVariable, next: PathStep)
        if variables.size == 1 =>
        MultiRelationshipPathStep(variables.head.relationship, OUTGOING, toNode = Some(toNode), next = next)(
          repeatPathStep.position
        )
    }

  val instance: Rewriter = topDown(innerRewriter)
}
