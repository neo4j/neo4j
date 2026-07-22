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
package org.neo4j.cypher.internal.frontend.phases.rewriting.cnf

import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.In
import org.neo4j.cypher.internal.expressions.IsNotNull
import org.neo4j.cypher.internal.expressions.Or
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.rewriting.conditions.AndRewrittenToAnds
import org.neo4j.cypher.internal.rewriting.conditions.AndsAboveOrs
import org.neo4j.cypher.internal.rewriting.rewriters.astRewriters.NormalizePredicates
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.RewriterWithParent
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.DefaultPostCondition
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.topDownWithParent

/**
 * Removes redundant IS NOT NULL predicates before applying [[distributeLawsRewriter]].
 *
 * {{{
 * MATCH (n) WHERE (n.prop IS NOT NULL AND n.prop = $param) OR EXISTS {...}
 * =>
 * MATCH (n) WHERE n.prop = $param OR EXISTS {...}
 * }}}
 */
case object RemoveRedundantIsNotNullPredicates extends CnfPhaseRewriter with DefaultPostCondition {

  override def preConditions: Set[StepSequencer.Condition] = Set(
    NormalizePredicates.completed,
    !AndsAboveOrs,
    !AndRewrittenToAnds
  )

  override def instance(from: BaseState, context: BaseContext): Rewriter = {
    new RemoveIsNotNullRewriter(context.cancellationChecker)
  }

  private class RemoveIsNotNullRewriter(cancellationChecker: CancellationChecker) extends Rewriter {
    override def apply(x: AnyRef): AnyRef = whereRewriterInstance.apply(x)

    private val whereRewriterInstance: Rewriter = {
      bottomUp.onRewriter(
        Rewriter.lift {
          case w @ Where(expr) =>
            w.copy(expression = expr.endoRewrite(andRewriterInstance))(w.position)
        },
        cancellation = cancellationChecker
      )
    }

    private val andRewriterInstance: Rewriter = {
      topDownWithParent(
        cancellation = cancellationChecker,
        rewriter = RewriterWithParent.lift {
          // We're only interested in AND that is not nested directly under another AND,
          // so that we can collect expressions from the whole tree.
          //       and  <-- only match this node
          //      /   \
          //    and   and
          //    / \   / \
          // and and and and
          case (rootAnd: And, parent) if parentOfRootAnd(parent) =>
            // collect expressions that appear on either side of A = B predicates
            val equalsArgs = rootAnd.folder.treeFold(Set.empty[Expression]) {
              case _: And           => acc => TraverseChildren(acc)
              case Equals(lhs, rhs) => acc => SkipChildren(acc + lhs + rhs)
              case In(lhs, _)       => acc => SkipChildren(acc + lhs)
              case _                => acc => SkipChildren(acc)

            }
            val pruner = pruneIsNotNullRewriter(equalsArgs)
            rootAnd.rewrite(pruner)
        },
        stopper = {
          case (_: And | _: Or, _) => false
          case _                   => true
        }
      )
    }

    private def parentOfRootAnd(parent: Option[AnyRef]): Boolean = {
      parent match {
        case Some(_: And) => false
        case _            => true
      }
    }

    private def pruneIsNotNullRewriter(equalsArgs: Set[Expression]): Rewriter = {
      bottomUp.onRewriter(
        Rewriter.lift {
          case And(IsNotNull(left), right) if equalsArgs.contains(left)  => right
          case And(left, IsNotNull(right)) if equalsArgs.contains(right) => left
        },
        stopper = !_.isInstanceOf[And],
        cancellation = cancellationChecker
      )
    }
  }
}
