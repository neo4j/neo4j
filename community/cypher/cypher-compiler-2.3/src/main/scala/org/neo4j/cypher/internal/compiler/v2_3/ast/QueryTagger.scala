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
package org.neo4j.cypher.internal.compiler.v2_3.ast

import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.frontend.v2_3.parser.CypherParser

import scala.annotation.tailrec

sealed class QueryTag(aName: String) {
  val name = aName.trim.toLowerCase
  val token = s":$name"

  override def toString = token
}

object QueryTags {
  val all = Set[QueryTag](
    MatchTag,
    OptionalMatchTag,
    RegularMatchTag,
    ShortestPathTag,
    NamedPathTag,
    SingleLengthRelTag,
    VarLengthRelTag,
    DirectedRelTag,
    UnDirectedRelTag,
    SingleNodePatternTag,
    RelPatternTag,

    WhereTag,
    WithTag,
    ReturnTag,
    StartTag,
    UnionTag,
    UnwindTag,
    LoadCSVTag,
    UpdatesTag,

    ComplexExpressionTag,
    FilteringExpressionTag,
    LiteralExpressionTag,
    ParameterExpressionTag,
    IdentifierExpressionTag
  )

  private val tagsByName: Map[String, QueryTag] = all.map { tag => tag.name -> tag }.toMap

  def parse(text: String) = {
    val tokens = tokenize(text.trim)
    val tags = tokens.map { piece =>
      tagsByName.getOrElse(piece, throw new IllegalArgumentException(s":$piece is an unknown query tag"))
    }
    tags
  }

  @tailrec
  private def tokenize(input: String, tags: Set[String] = Set.empty): Set[String] = {
    if (input.isEmpty)
      tags
    else {
      if (input.charAt(0) == ':') {
        val boundary = input.indexOf(':', 1)
        if (boundary == -1) {
          val tag = input.substring(1).trim
          tags + tag
        } else {
          val tag = input.substring(1, boundary).trim
          val tail = input.substring(boundary)
          tokenize(tail, tags + tag)
        }
      } else
        throw new IllegalArgumentException(s"'$input' does not start with a query tag token")
    }
  }
}

// Matching

case object MatchTag extends QueryTag("match")
case object OptionalMatchTag extends QueryTag("opt-match")
case object RegularMatchTag extends QueryTag("reg-match")

case object ShortestPathTag extends QueryTag("shortest-path")
case object NamedPathTag extends QueryTag("named-path")

case object SingleLengthRelTag extends QueryTag("single-length-rel")
case object VarLengthRelTag extends QueryTag("var-length-rel")

case object DirectedRelTag extends QueryTag("directed-rel")
case object UnDirectedRelTag extends QueryTag("undirected-rel")

case object SingleNodePatternTag extends QueryTag("single-node-pattern")
case object RelPatternTag extends QueryTag("rel-pattern")

case object WhereTag extends QueryTag("where")

// Projection

case object WithTag extends QueryTag("with")
case object ReturnTag extends QueryTag("return")

// Others

case object StartTag extends QueryTag("start")
case object UnionTag extends QueryTag("union")
case object UnwindTag extends QueryTag("unwind")
case object LoadCSVTag extends QueryTag("load-csv")

// Updates

case object UpdatesTag extends QueryTag("updates")

// Expressions

case object ComplexExpressionTag extends QueryTag("complex-expr")
case object FilteringExpressionTag extends QueryTag("filtering-expr")
case object LiteralExpressionTag extends QueryTag("literal-expr")
case object ParameterExpressionTag extends QueryTag("parameter-expr")
case object IdentifierExpressionTag extends QueryTag("identifier-expr")

object QueryTagger extends QueryTagger[String] {

  def apply(input: String) = default(input)

  val default: QueryTagger[String] = fromString(forEachChild(
    // Clauses
    lift[ASTNode] {
      case x: Match =>
        val tags = Set[QueryTag](
          MatchTag,
          if (x.optional) OptionalMatchTag else RegularMatchTag
        )
        val containsSingleNode = x.pattern.patternParts.exists(_.element.isSingleNode)
        if (containsSingleNode) tags + SingleNodePatternTag else tags

      case x: Where =>
        Set(WhereTag)

      case x: With =>
        Set(WithTag)

      case x: Return =>
        Set(ReturnTag)

      case x: Start =>
        Set(StartTag)

      case x: Union =>
        Set(UnionTag)

      case x: Unwind =>
        Set(UnwindTag)

      case x: LoadCSV =>
        Set(LoadCSVTag)

      case x: UpdateClause =>
        Set(UpdatesTag)
    } ++

    // Pattern features
    lift[ASTNode] {
      case x: ShortestPaths => Set(ShortestPathTag)
      case x: NamedPatternPart => Set(NamedPathTag)
      case x: RelationshipPattern =>
        Set(
          RelPatternTag,
          if (x.isSingleLength) SingleLengthRelTag else VarLengthRelTag,
          if (x.isDirected) DirectedRelTag else UnDirectedRelTag
        )
    } ++

    // <expr> unless identifier or literal
    lift[ASTNode] {
      case x: Identifier => Set.empty
      case x: Literal => Set.empty
      case x: Expression => Set(ComplexExpressionTag)
    } ++

    // subtype of <expr>
    lift[ASTNode] {
      case x: Identifier => Set(IdentifierExpressionTag)
      case x: Literal => Set(LiteralExpressionTag)
      case x: Parameter => Set(ParameterExpressionTag)
      case x: FilteringExpression => Set(FilteringExpressionTag)
    }
  ))

  // run parser and pass statement to next query tagger
  case class fromString(next: QueryTagger[Statement])
    extends QueryTagger[String] {

    val parser = new CypherParser

    def apply(queryText: String): Set[QueryTag] = next(parser.parse(queryText))
  }

  // run inner query tagger on each child ast node and return union over all results
  case class forEachChild(inner: QueryTagger[ASTNode]) extends QueryTagger[Statement] {
    def apply(input: Statement) = input.treeFold(Set.empty[QueryTag]) {
      case node: ASTNode => (acc, children) => children(acc ++ inner(node))
    }
  }

  def lift[T](f: PartialFunction[T, Set[QueryTag]]): QueryTagger[T] = f.lift.andThen(_.getOrElse(Set.empty))

  implicit class RichQueryTagger[T](lhs: QueryTagger[T]) {
    def ++(rhs: QueryTagger[T]): QueryTagger[T] = (input: T) => lhs(input) `union` rhs(input)
  }
}

