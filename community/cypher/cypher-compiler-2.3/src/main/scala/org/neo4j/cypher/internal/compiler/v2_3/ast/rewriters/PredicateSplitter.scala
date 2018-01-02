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
package org.neo4j.cypher.internal.compiler.v2_3.ast.rewriters

import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.frontend.v2_3._

object PredicateSplitter {
  val empty = PredicateSplitter(Map.empty)

  def apply(semanticTable: SemanticTable, statement: Statement): PredicateSplitter = {
    val recordedScopes = semanticTable.recordedScopes
    val lookup = (clause: Clause) => recordedScopes(clause).identifierDefinitions.values.map(_.asIdentifier).toSet
    apply(lookup, statement)
  }

  def apply(scopeLookup: Clause => Set[Identifier], statement: Statement): PredicateSplitter = {
    statement.treeFold(PredicateSplitter.empty) {
      case clause@Match(false, pattern, _, Some(where)) =>
        (acc, children) =>
          val namedPaths = namedPatternPartIdentifiers(pattern)
          val predicates = conjunctionPredicates(where.expression)
          val (matchPredicates, withPredicates) = predicates.partition(x => (x.dependencies `intersect` namedPaths).isEmpty)

          val newAcc =
            if (withPredicates.isEmpty) {
              acc
            } else {
              val newMatchClause = clause.copy(where = optWhere(where, matchPredicates))(clause.position)

              val identifiersInScope = scopeLookup(clause)
              val returnItems = (identifiersInScope ++ namedPaths).map(_.asAlias).toSeq
              val newWithWhere = optWhere(where, withPredicates).endoRewrite(bottomUp(Rewriter.lift { case ident: Identifier if namedPaths(ident) => ident.copyId }))
              val newWithClause = With(distinct = false, ReturnItems(includeExisting = false, returnItems)(where.position), None, None, None, newWithWhere)(where.position)

              acc + (Ref(clause) -> (newMatchClause -> newWithClause))
            }

          children(newAcc)
    }
  }

  private def namedPatternPartIdentifiers(pattern: Pattern): Set[Identifier] = pattern.patternParts.flatMap {
      case NamedPatternPart(_, _: ShortestPaths) => None
      case part: NamedPatternPart => Some(part.identifier)
      case _ => None
    }.toSet

  private def optWhere(where: Where, expressions: Set[Expression]) =
    if (expressions.isEmpty)
      None
    else {
      val expr = if (expressions.size == 1) expressions.head else Ands(expressions)(where.expression.position)
      Some(where.copy(expr)(where.position))
    }
}

object conjunctionPredicates extends (Expression => Set[Expression]) {
  def apply(v: Expression): Set[Expression] = v match {
    case Ands(exprs) => exprs
    case expr => Set(expr)
  }
}

case class PredicateSplitter(rewrites: Map[Ref[Clause], Seq[Clause]]) {
  def +(entry: (Ref[Match], (Match, With))): PredicateSplitter = {
    val (key, (newMatch, newWith)) = entry
    copy(rewrites = rewrites + (key -> Seq(newMatch, newWith)))
  }

  object statementRewriter extends Rewriter {
    val instance = topDown(Rewriter.lift {
      case query: SingleQuery =>
        val oldClauses = query.clauses
        val newClauses = oldClauses.flatMap {
          case clause: Match => rewrites.getOrElse(Ref(clause), Seq(clause))
          case clause        => Seq(clause)
        }
        query.copy(clauses = newClauses)(query.position)
    })

    override def apply(in: AnyRef): AnyRef = {
      val result = instance(in)
      result
    }
  }
}
