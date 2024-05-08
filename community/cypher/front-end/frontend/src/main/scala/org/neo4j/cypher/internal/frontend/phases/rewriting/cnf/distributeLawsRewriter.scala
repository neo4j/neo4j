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

import org.neo4j.cypher.internal.ast.ExistsExpression
import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.Or
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.rewriting.cnf.distributeLawsRewriter.conversionLimit
import org.neo4j.cypher.internal.frontend.phases.rewriting.cnf.distributeLawsRewriter.dnfCounts
import org.neo4j.cypher.internal.frontend.phases.rewriting.cnf.distributeLawsRewriter.step
import org.neo4j.cypher.internal.rewriting.AstRewritingMonitor
import org.neo4j.cypher.internal.rewriting.conditions.AndRewrittenToAnds
import org.neo4j.cypher.internal.rewriting.conditions.AndsAboveOrs
import org.neo4j.cypher.internal.rewriting.conditions.OrRewrittenToOrs
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.rewriting.rewriters.copyVariables
import org.neo4j.cypher.internal.rewriting.rewriters.repeatWithSizeLimit
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.RewriterWithParent
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.topDownWithParent

case class distributeLawsRewriter(cancellationChecker: CancellationChecker)(implicit monitor: AstRewritingMonitor)
    extends Rewriter {

  def apply(that: AnyRef): AnyRef = {
    instance(that)
  }

  private val instance = topDownWithParent(
    RewriterWithParent.lift {
      case (or: Or, _) => rewriteOrIfSmallEnough(or)
    },
    // Rewrite the Or, but immediately stop at its children.
    stopper = {
      case (_, Some(_: Or)) => true
      case _                => false
    },
    cancellationChecker
  )

  private def rewriteOrIfSmallEnough(or: Or): AnyRef = {
    if (dnfCounts(or) <= conversionLimit(or)) {
      rewriteOrRepeatedly(or)
    } else {
      monitor.abortedRewritingDueToLargeDNF(or)
      or
    }
  }

  private val rewriteOrRepeatedly: Rewriter = repeatWithSizeLimit(bottomUp(step))(monitor)
}

case object distributeLawsRewriter extends CnfPhase {
  // converting from DNF to CNF is exponentially expensive, so we only do it for a small amount of clauses
  // see https://en.wikipedia.org/wiki/Conjunctive_normal_form#Conversion_into_CNF
  val DNF_CONVERSION_LIMIT = 8

  override def instance(from: BaseState, context: BaseContext): Rewriter = {
    implicit val monitor: AstRewritingMonitor = context.monitors.newMonitor[AstRewritingMonitor]()
    distributeLawsRewriter(context.cancellationChecker)
  }

  private[cnf] def dnfCounts(or: Or): Int =
    or.folder.treeFold(0) {
      case Or(lhs: And, rhs: And) => acc => TraverseChildren(acc + andCount(lhs) + andCount(rhs))
      case Or(_, rhs: And)        => acc => TraverseChildren(acc + andCount(rhs))
      case Or(lhs: And, _)        => acc => TraverseChildren(acc + andCount(lhs))
    }

  private def andCount(and: And): Int =
    and.folder.treeFold(0) {
      case _: And => acc => TraverseChildren(acc + 1)
      case _      =>
        // Only count immediately nested Ands. Skip other children, so that we do not count
        // Ands nested under some other AST nodes, e.g. a Not.
        acc => SkipChildren(acc)
    }

  private def conversionLimit(ast: AnyRef): Int = {
    val containsExpensiveExpressions = ast.folder.treeExists {
      // duplicating too many pattern expressions can result in a very long planning time
      case _: PatternExpression |
        _: ExistsExpression => true
    }
    if (containsExpensiveExpressions)
      distributeLawsRewriter.DNF_CONVERSION_LIMIT / 2
    else
      distributeLawsRewriter.DNF_CONVERSION_LIMIT
  }

  private val step = Rewriter.lift {
    case p @ Or(exp1, And(exp2, exp3)) =>
      And(Or(exp1, exp2)(p.position), Or(exp1.endoRewrite(copyVariables), exp3)(p.position))(p.position)
    case p @ Or(And(exp1, exp2), exp3) =>
      And(Or(exp1, exp3)(p.position), Or(exp2, exp3.endoRewrite(copyVariables))(p.position))(p.position)
  }

  override def preConditions: Set[StepSequencer.Condition] = Set(
    !AndRewrittenToAnds,
    !OrRewrittenToOrs
  )

  override def postConditions: Set[StepSequencer.Condition] = Set(AndsAboveOrs)

  override def invalidatedConditions: Set[StepSequencer.Condition] = SemanticInfoAvailable
}
