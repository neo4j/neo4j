/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_1.ast.convert.plannerQuery

import org.neo4j.cypher.internal.compiler.v3_1.ast.convert.plannerQuery.PatternConverters._
import org.neo4j.cypher.internal.compiler.v3_1.ast.rewriters.{LabelPredicateNormalizer, MatchPredicateNormalizerChain, PropertyPredicateNormalizer, addUniquenessPredicates}
import org.neo4j.cypher.internal.frontend.v3_1.ast._
import org.neo4j.cypher.internal.frontend.v3_1.{Rewriter, topDown}
import org.neo4j.cypher.internal.ir.v3_1.helpers.UnNamedNameGenerator._
import org.neo4j.cypher.internal.ir.v3_1.{PatternLength, QueryGraph, SimplePatternLength, VarPatternLength}

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
      qg.addArgumentIds(qg.coveredIds.filter(_.name.isNamed).toIndexedSeq)
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
      qg.addArgumentIds(qg.coveredIds.filter(_.name.isNamed).toIndexedSeq)
    }
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
