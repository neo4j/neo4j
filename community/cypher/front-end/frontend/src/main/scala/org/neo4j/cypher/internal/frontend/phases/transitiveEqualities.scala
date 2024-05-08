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
package org.neo4j.cypher.internal.frontend.phases

import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.Or
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerFactory
import org.neo4j.cypher.internal.frontend.phases.rewriting.cnf.rewriteEqualityToInPredicate
import org.neo4j.cypher.internal.frontend.phases.transitiveEqualities.PropertyEquivalence
import org.neo4j.cypher.internal.frontend.phases.transitiveEqualities.PropertyMapping
import org.neo4j.cypher.internal.frontend.phases.transitiveEqualities.Transitions
import org.neo4j.cypher.internal.rewriting.conditions.AndRewrittenToAnds
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.RewriterStopper
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.DefaultPostCondition
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.helpers.fixedPoint

/**
 * Applies Transitive Property of Equality to WHERE clauses.
 *
 * Given a where clause, `WHERE a.prop = b.prop AND b.prop = 42` we rewrite the query
 * into `WHERE a.prop = 42 AND b.prop = 42`
 */
case object transitiveEqualities extends StatementRewriter with StepSequencer.Step with DefaultPostCondition
    with PlanPipelineTransformerFactory {

  override def instance(from: BaseState, context: BaseContext): Rewriter =
    transitiveEqualities(context.cancellationChecker)

  case class Transitions(
    mapping: Map[Property, Expression] = Map.empty,
    equivalence: Map[Property, Property] = Map.empty
  ) {

    def withMapping(e: (Property, Expression)): Transitions = copy(mapping = mapping + e)
    def withEquivalence(e: (Property, Property)): Transitions = copy(equivalence = equivalence + e)

    def emergentEqualities: Map[Property, Expression] = {
      val sharedKeys = equivalence.keySet.intersect(mapping.keySet)

      sharedKeys.map(k => equivalence(k) -> mapping(k)).toMap -- mapping.keySet
    }

    def ++(other: Transitions): Transitions = copy(mapping ++ other.mapping, equivalence ++ other.equivalence)
  }

  object Transitions {
    def empty: Transitions = Transitions()
  }

  override def preConditions: Set[StepSequencer.Condition] = Set(
    // This rewriter matches on Equals, so it must run before that is rewritten to In
    !rewriteEqualityToInPredicate.completed,
    // This rewriter matches on And, so it must run before that is rewritten to Ands
    !AndRewrittenToAnds
  )

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set(
    // This Rewriter creates Equals AST nodes.
    rewriteEqualityToInPredicate.completed
  ) ++ SemanticInfoAvailable // Introduces new AST nodes

  override def getTransformer(
    pushdownPropertyReads: Boolean,
    semanticFeatures: Seq[SemanticFeature]
  ): Transformer[BaseContext, BaseState, BaseState] = this

  object PropertyEquivalence {

    def unapply(v: Any): Option[(Property, Property, Equals)] = v match {
      case equals @ Equals(p1: Property, p2: Property) => Some((p1, p2, equals))
      case _                                           => None
    }
  }

  object PropertyMapping {

    def unapply(v: Any): Option[(Property, Expression)] = v match {
      case Equals(p1: Property, expr: Expression) => Some((p1, expr))
      case _                                      => None
    }
  }
}

case class transitiveEqualities(cancellationChecker: CancellationChecker) extends Rewriter {

  def apply(that: AnyRef): AnyRef = instance.apply(that)

  private val instance: Rewriter = bottomUp(Rewriter.lift {
    case where: Where => fixedPoint(cancellationChecker)((w: Where) => w.endoRewrite(whereRewriter))(where)
  })

  private def subTreeReference(prop: Property, other: Expression): Boolean =
    other.folder.treeExists {
      case PropertyEquivalence(p1, p2, _) => p1 == prop || p2 == prop
    }

  // Collects property equalities, e.g `a.prop = 42`
  private def collect(e: Expression): Transitions = e.folder.treeFold(Transitions.empty) {
    case _: Or                          => acc => SkipChildren(acc)
    case _: And                         => acc => TraverseChildren(acc)
    case PropertyEquivalence(p1, p2, _) => acc => SkipChildren(acc.withEquivalence(p1 -> p2))
    case PropertyMapping(p: Property, other) if !subTreeReference(p, other) =>
      acc => SkipChildren(acc.withMapping(p -> other))
    case Not(Equals(_, _)) => acc => SkipChildren(acc)
  }

  // NOTE that this might introduce duplicate predicates, however at a later rewrite
  // when AND is turned into ANDS we remove all duplicates
  private val whereRewriter: Rewriter = bottomUp(Rewriter.lift {
    // Do not rewrite if there are multiple scopes in the WHERE clause to avoid to leak variables between different scopes
    case and: And if and.containsScopeExpression =>
      and
    case and @ And(lhs, rhs) =>
      val transitions = collect(lhs) ++ collect(rhs)
      val inner = andRewriter(transitions)
      val newAnd = and.copy(lhs = lhs.endoRewrite(inner), rhs = rhs.endoRewrite(inner))(and.position)

      // ALSO take care of case WHERE b.prop = a.prop AND b.prop = 42
      // turns into WHERE b.prop = a.prop AND b.prop = 42 AND a.prop = 42
      transitions.emergentEqualities.foldLeft(newAnd) {
        case (acc, (prop, expr)) =>
          And(acc, Equals(prop, expr)(acc.position))(acc.position)
      }
  })

  private def andRewriter(transitions: Transitions): Rewriter = {
    val stopOnNotEquals: RewriterStopper = {
      case Not(Equals(_, _)) => true
      case _                 => false
    }

    bottomUp(
      Rewriter.lift {
        case PropertyEquivalence(_, p2, equals) if transitions.mapping.contains(p2) =>
          equals.copy(rhs = transitions.mapping(p2))(equals.position)
      },
      stopOnNotEquals
    )
  }
}
