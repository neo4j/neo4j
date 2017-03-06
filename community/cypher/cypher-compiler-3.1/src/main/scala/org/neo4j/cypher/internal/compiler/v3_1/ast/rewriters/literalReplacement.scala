/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_1.ast.rewriters

import org.neo4j.cypher.internal.frontend.v3_1.ast._
import org.neo4j.cypher.internal.frontend.v3_1.symbols._
import org.neo4j.cypher.internal.frontend.v3_1.{IdentityMap, Rewriter, ast, bottomUp}

object literalReplacement {

  case class LiteralReplacement(parameter: Parameter, value: AnyRef)
  type LiteralReplacements = IdentityMap[Expression, LiteralReplacement]

  case class ExtractParameterRewriter(replaceableLiterals: LiteralReplacements) extends Rewriter {
    def apply(that: AnyRef): AnyRef = rewriter.apply(that)

    private val rewriter: Rewriter = bottomUp(Rewriter.lift {
      case l: Expression if replaceableLiterals.contains(l) => replaceableLiterals(l).parameter
    })
  }

  private val literalMatcher: PartialFunction[Any, LiteralReplacements => (LiteralReplacements, Option[LiteralReplacements => LiteralReplacements])] = {
    case _: ast.Match |
         _: ast.Create |
         _: ast.CreateUnique |
         _: ast.Merge |
         _: ast.SetClause |
         _: ast.Return |
         _: ast.With |
         _: ast.CallClause =>
      acc => (acc, Some(identity))
    case _: ast.Clause |
         _: ast.PeriodicCommitHint |
         _: ast.Limit =>
      acc => (acc, None)
    case n: ast.NodePattern =>
      acc => (n.properties.treeFold(acc)(literalMatcher), None)
    case r: ast.RelationshipPattern =>
      acc => (r.properties.treeFold(acc)(literalMatcher), None)
    case ast.ContainerIndex(_, _: ast.StringLiteral) =>
      acc => (acc, None)
    case l: ast.StringLiteral =>
      acc =>
        val parameter = ast.Parameter(s"  AUTOSTRING${acc.size}", CTString)(l.position)
        (acc + (l -> LiteralReplacement(parameter, l.value)), None)
    case l: ast.IntegerLiteral =>
      acc =>
        val parameter = ast.Parameter(s"  AUTOINT${acc.size}", CTInteger)(l.position)
        (acc + (l -> LiteralReplacement(parameter, l.value)), None)
    case l: ast.DoubleLiteral =>
      acc =>
        val parameter = ast.Parameter(s"  AUTODOUBLE${acc.size}", CTFloat)(l.position)
        (acc + (l -> LiteralReplacement(parameter, l.value)), None)
    case l: ast.BooleanLiteral =>
      acc =>
        val parameter = ast.Parameter(s"  AUTOBOOL${acc.size}", CTBoolean)(l.position)
        (acc + (l -> LiteralReplacement(parameter, l.value)), None)
    case l: ast.ListLiteral if l.expressions.forall(_.isInstanceOf[Literal])=>
      acc =>
        val parameter = ast.Parameter(s"  AUTOLIST${acc.size}", CTList(CTAny))(l.position)
        val values: Seq[AnyRef] = l.expressions.map(_.asInstanceOf[Literal].value).toIndexedSeq
        (acc + (l -> LiteralReplacement(parameter, values)), None)
  }

  def apply(term: ASTNode): (Rewriter, Map[String, Any]) = {
    val containsParameter: Boolean = term.treeExists {
      case _:Parameter => true
    }

    if (containsParameter) {
      (Rewriter.noop, Map.empty)
    } else {
      val replaceableLiterals = term.treeFold(IdentityMap.empty: LiteralReplacements)(literalMatcher)

      val extractedParams: Map[String, AnyRef] = replaceableLiterals.map {
        case (_, LiteralReplacement(parameter, value)) => (parameter.name, value)
      }

      (ExtractParameterRewriter(replaceableLiterals), extractedParams)
    }
  }
}
