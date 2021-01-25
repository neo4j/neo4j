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
package org.neo4j.cypher.internal.ir.helpers

import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.HasTypes
import org.neo4j.cypher.internal.expressions.NodePatternExpression
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.Range
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.PatternLength
import org.neo4j.cypher.internal.ir.Predicate
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.ir.helpers.PatternConverters.PatternElementDestructor
import org.neo4j.cypher.internal.macros.AssertMacros
import org.neo4j.cypher.internal.rewriting.rewriters.AddUniquenessPredicates
import org.neo4j.cypher.internal.rewriting.rewriters.InnerVariableNamer
import org.neo4j.cypher.internal.rewriting.rewriters.LabelPredicateNormalizer
import org.neo4j.cypher.internal.rewriting.rewriters.MatchPredicateNormalizerChain
import org.neo4j.cypher.internal.rewriting.rewriters.PropertyPredicateNormalizer
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.UnNamedNameGenerator.isNamed
import org.neo4j.cypher.internal.util.topDown

object ExpressionConverters {
  val normalizer = MatchPredicateNormalizerChain(PropertyPredicateNormalizer, LabelPredicateNormalizer)

  private def getQueryGraphArguments(expr: Expression, availableSymbols: Set[String]) = {
    val dependencies = expr.dependencies.map(_.name)
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(dependencies.subsetOf(availableSymbols),
      s"Trying to plan a PatternExpression where a dependency is not available. Dependencies: $dependencies. Available: ${availableSymbols}")
    dependencies
  }

  /**
   * Turn a PatternExpression into a query graph.
   *
   * @param exp the pattern expression
   * @param availableSymbols all symbols available in the outer scope. Only used to assert that all dependencies can be satisfied.
   */
  def asQueryGraph(exp: PatternExpression,
                   availableSymbols: Set[String],
                   innerVariableNamer: InnerVariableNamer): QueryGraph = {
    val addUniquenessPredicates = AddUniquenessPredicates(innerVariableNamer)
    val uniqueRels = addUniquenessPredicates.collectUniqueRels(exp.pattern)
    val uniquePredicates = addUniquenessPredicates.createPredicatesFor(uniqueRels, exp.pattern.position)
    val relChain: RelationshipChain = exp.pattern.element
    val predicates: IndexedSeq[Expression] = relChain.fold(uniquePredicates.toIndexedSeq) {
      case pattern: AnyRef if normalizer.extract.isDefinedAt(pattern) => acc => acc ++ normalizer.extract(pattern)
      case _                                                          => identity
    }

    val rewrittenChain = relChain.endoRewrite(topDown(Rewriter.lift(normalizer.replace)))

    val patternContent = rewrittenChain.destructed
    QueryGraph(
      patternRelationships = patternContent.rels.toSet,
      patternNodes = patternContent.nodeIds.toSet,
      argumentIds = getQueryGraphArguments(exp, availableSymbols)
    ).addPredicates(predicates: _*)
  }

  /**
   * Turn a NodePatternExpression into a query graph
   * @param exp the NodePatternExpression
   * @param availableSymbols all symbols available in the outer scope. Unfortunately used to to intersect with wrongly computed dependencies.
   */
  def asQueryGraph(exp: NodePatternExpression,
                   availableSymbols: Set[String]): QueryGraph = {
    val predicates: Seq[Expression] = exp.patterns.collect {
      case pattern if normalizer.extract.isDefinedAt(pattern) => normalizer.extract(pattern)
    }.flatten

    val rewrittenPattern = exp.patterns.map(_.endoRewrite(topDown(Rewriter.lift(normalizer.replace))))

    // TODO it would be nicer to be able to use getQueryGraphArguments, but dependencies of NodePatternExpression are not correct
    val dependencies = exp.dependencies.map(_.name)
    val qgArguments = availableSymbols intersect dependencies

    QueryGraph(
      patternNodes = rewrittenPattern.map(_.variable.get.name).toSet,
      argumentIds = qgArguments
    ).addPredicates(predicates: _*)
  }

  /**
   * Turn a PatternComprehension into a query graph.
   *
   * @param exp the pattern comprehension
   * @param availableSymbols all symbols available in the outer scope. Only used to assert that all dependencies can be satisfied.
   */
  def asQueryGraph(exp: PatternComprehension,
                   availableSymbols: Set[String],
                   innerVariableNamer: InnerVariableNamer): QueryGraph = {
    val addUniquenessPredicates = AddUniquenessPredicates(innerVariableNamer)
    val uniqueRels = addUniquenessPredicates.collectUniqueRels(exp.pattern)
    val uniquePredicates = addUniquenessPredicates.createPredicatesFor(uniqueRels, exp.pattern.position)
    val relChain: RelationshipChain = exp.pattern.element
    val predicates: IndexedSeq[Expression] = relChain.fold(uniquePredicates.toIndexedSeq) {
      case pattern: AnyRef if normalizer.extract.isDefinedAt(pattern) => acc => acc ++ normalizer.extract(pattern)
      case _                                                          => identity
    } ++ exp.predicate

    val rewrittenChain = relChain.endoRewrite(topDown(Rewriter.lift(normalizer.replace)))

    val patternContent = rewrittenChain.destructed
    QueryGraph(
      patternRelationships = patternContent.rels.toSet,
      patternNodes = patternContent.nodeIds.toSet,
      argumentIds = getQueryGraphArguments(exp, availableSymbols)
    ).addPredicates(predicates: _*)
  }

  implicit class PredicateConverter(val predicate: Expression) extends AnyVal {
    def asPredicates: Set[Predicate] = {
      asPredicates(Set.empty)
    }

    def asPredicates(outerScope: Set[String]): Set[Predicate] = {
      predicate.treeFold(Set.empty[Predicate]) {
        // n:Label
        case p@HasLabels(Variable(name), labels) =>
          acc => val newAcc = acc ++ labels.map { label =>
                Predicate(Set(name), p.copy(labels = Seq(label))(p.position))
            }
            SkipChildren(newAcc)
        // r:T
        case p@HasTypes(Variable(name), types) =>
          acc => val newAcc = acc ++ types.map { typ =>
            Predicate(Set(name), p.copy(types = Seq(typ))(p.position))
          }
            SkipChildren(newAcc)
        // and
        case _: Ands =>
          acc => TraverseChildren(acc)
        case p: Expression =>
          acc => SkipChildren(acc + Predicate(p.idNames -- outerScope, p))
      }
    }
  }

  implicit class IdExtractor(val exp: Expression) extends AnyVal {
    def idNames: Set[String] = exp.dependencies.map(id => id.name)
  }

  implicit class RangeConvertor(val length: Option[Option[Range]]) extends AnyVal {
    def asPatternLength: PatternLength = length match {
      case Some(Some(Range(Some(left), Some(right)))) => VarPatternLength(left.value.toInt, Some(right.value.toInt))
      case Some(Some(Range(Some(left), None))) => VarPatternLength(left.value.toInt, None)
      case Some(Some(Range(None, Some(right)))) => VarPatternLength(1, Some(right.value.toInt))
      case Some(Some(Range(None, None))) => VarPatternLength.unlimited
      case Some(None) => VarPatternLength.unlimited
      case None => SimplePatternLength
    }
  }

}
