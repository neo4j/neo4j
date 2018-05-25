/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.ir.v3_3.helpers

import org.neo4j.cypher.internal.frontend.v3_3.ast.rewriters.{LabelPredicateNormalizer, MatchPredicateNormalizerChain, PropertyPredicateNormalizer, addUniquenessPredicates}
import org.neo4j.cypher.internal.frontend.v3_3.ast.{Ands, Expression, HasLabels, Not, Ors, PatternComprehension, PatternExpression, Range, RelationshipChain, Variable}
import org.neo4j.cypher.internal.frontend.v3_3.helpers.UnNamedNameGenerator._
import org.neo4j.cypher.internal.frontend.v3_3.{Rewriter, topDown}
import org.neo4j.cypher.internal.ir.v3_3.helpers.PatternConverters._
import org.neo4j.cypher.internal.ir.v3_3.{QueryGraph, _}

object ExpressionConverters {
  val normalizer = MatchPredicateNormalizerChain(PropertyPredicateNormalizer, LabelPredicateNormalizer)

  implicit class PatternExpressionConverter(val exp: PatternExpression) extends AnyVal {
    def asQueryGraph: QueryGraph = {
      val uniqueRels = addUniquenessPredicates.collectUniqueRels(exp.pattern)
      val uniquePredicates = addUniquenessPredicates.createPredicatesFor(uniqueRels, exp.pattern.position)
      val relChain: RelationshipChain = exp.pattern.element
      val predicates: IndexedSeq[Expression] = relChain.fold(uniquePredicates.toIndexedSeq) {
        case pattern: AnyRef if normalizer.extract.isDefinedAt(pattern) => acc => acc ++ normalizer.extract(pattern)
        case _                                                          => identity
      }

      val rewrittenChain = relChain.endoRewrite(topDown(Rewriter.lift(normalizer.replace)))

      val patternContent = rewrittenChain.destructed
      val qg = QueryGraph(
        patternRelationships = patternContent.rels.toSet,
        patternNodes = patternContent.nodeIds.toSet
      ).addPredicates(predicates: _*)
      qg.addArgumentIds(qg.idsWithoutOptionalMatchesOrUpdates.filter(_.isNamed).toIndexedSeq)
    }
  }

  implicit class PatternComprehensionConverter(val exp: PatternComprehension) extends AnyVal {
    def asQueryGraph: QueryGraph = {
      val uniqueRels = addUniquenessPredicates.collectUniqueRels(exp.pattern)
      val uniquePredicates = addUniquenessPredicates.createPredicatesFor(uniqueRels, exp.pattern.position)
      val relChain: RelationshipChain = exp.pattern.element
      val predicates: IndexedSeq[Expression] = relChain.fold(uniquePredicates.toIndexedSeq) {
        case pattern: AnyRef if normalizer.extract.isDefinedAt(pattern) => acc => acc ++ normalizer.extract(pattern)
        case _                                                          => identity
      } ++ exp.predicate

      val rewrittenChain = relChain.endoRewrite(topDown(Rewriter.lift(normalizer.replace)))

      val patternContent = rewrittenChain.destructed
      val qg = QueryGraph(
        patternRelationships = patternContent.rels.toSet,
        patternNodes = patternContent.nodeIds.toSet
      ).addPredicates(predicates: _*)
      qg.addArgumentIds(qg.idsWithoutOptionalMatchesOrUpdates.filter(_.isNamed).toIndexedSeq)
    }
  }

  implicit class PredicateConverter(val predicate: Expression) extends AnyVal {
    def asPredicates: Set[Predicate] = {
      predicate.treeFold(Set.empty[Predicate]) {
        // n:Label
        case p@HasLabels(Variable(name), labels) =>
          acc => val newAcc = acc ++ labels.map { label =>
                Predicate(Set(name), p.copy(labels = Seq(label))(p.position))
            }
            (newAcc, None)
        // and
        case _: Ands =>
          acc => (acc, Some(identity))
        case p: Expression =>
          acc => (acc + Predicate(p.idNames, p), None)
      }.map(filterUnnamed)
    }

    private def filterUnnamed(predicate: Predicate): Predicate = predicate match {
      case Predicate(deps, e: PatternExpression) =>
        Predicate(deps.filter(x => isNamed(x)), e)
      case Predicate(deps, e@Not(_: PatternExpression)) =>
        Predicate(deps.filter(x => isNamed(x)), e)
      case Predicate(deps, ors@Ors(exprs)) =>
        val newDeps = exprs.foldLeft(Set.empty[String]) { (acc, exp) =>
          exp match {
            case e: PatternExpression =>
              acc ++ e.idNames.filter(x => isNamed(x))
            case e@Not(_: PatternExpression) =>
              acc ++ e.idNames.filter(x => isNamed(x))
            case e if e.treeExists { case _: PatternExpression => true} =>
              acc ++ (e.idNames -- unnamedIdNamesInNestedPatternExpressions(e))
            case e =>
              acc ++ e.idNames
          }
        }
        Predicate(newDeps, ors)
      case Predicate(deps, expr) if expr.treeExists { case _: PatternExpression => true} =>
        Predicate(deps -- unnamedIdNamesInNestedPatternExpressions(expr), expr)
      case p => p
    }

    private def unnamedIdNamesInNestedPatternExpressions(expression: Expression) = {
      val patternExpressions = expression.treeFold(Seq.empty[PatternExpression]) {
        case p: PatternExpression => acc => (acc :+ p, None)
      }

      val unnamedIdsInPatternExprs = patternExpressions.flatMap(_.idNames)
        .filterNot(x => isNamed(x))
        .toSet

      unnamedIdsInPatternExprs
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
