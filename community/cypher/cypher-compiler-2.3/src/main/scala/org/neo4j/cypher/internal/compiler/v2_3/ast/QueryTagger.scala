/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.cypher.internal.compiler.v2_3.parser.{CypherParser, ParserMonitor}

sealed class QueryTag(aName: String) {

  val name = aName.toLowerCase

  override def toString = s":$name"
}

case object MatchTag extends QueryTag("match")
case object OptionalMatchTag extends QueryTag("opt-match")
case object RegularMatchTag extends QueryTag("reg-match")
case object ComplexExpressionTag extends QueryTag("complex-expr")
case object FilteringExpressionTag extends QueryTag("filtering-expr")
case object LiteralExpressionTag extends QueryTag("literal-expr")
case object ParameterExpressionTag extends QueryTag("parameter-expr")
case object IdentifierExpressionTag extends QueryTag("identifier-expr")

object QueryTagger extends QueryTagger[String] {

  def apply(input: String) = default(input)

  val default: QueryTagger[String] = fromString(forEachChild(
    lift[ASTNode] { case x: Match if !x.optional => Set(MatchTag, RegularMatchTag) } ++
    lift[ASTNode] { case x: Match if x.optional => Set(MatchTag, OptionalMatchTag) } ++
    lift[ASTNode] {
      case x: Identifier => Set.empty
      case x: Literal => Set.empty
      case x: Expression => Set(ComplexExpressionTag)
    } ++
    lift[ASTNode] {
      case x: Identifier => Set(IdentifierExpressionTag)
      case x: Literal => Set(LiteralExpressionTag)
      case x: Parameter => Set(ParameterExpressionTag)
      case x: FilteringExpression => Set(FilteringExpressionTag)
    }
  ))

  case class fromString(next: QueryTagger[Statement], monitor: ParserMonitor[Statement] = ParserMonitor.empty[Statement])
    extends QueryTagger[String] {

    val parser = new CypherParser(monitor)

    def apply(queryText: String): Set[QueryTag] = next(parser.parse(queryText))
  }

  case class forEachChild(inner: QueryTagger[ASTNode]) extends QueryTagger[Statement] {
    def apply(input: Statement) = input.treeFold(Set.empty[QueryTag]) {
      case node: ASTNode => (acc, children) => children(acc ++ inner(node))
    }
  }

  def lift[T](f: PartialFunction[T, Set[QueryTag]]): QueryTagger[T] = f.lift.andThen(_.getOrElse(Set.empty))

  implicit class RichQueryTagger[T](self: QueryTagger[T]) {
    def ++(other: QueryTagger[T]): QueryTagger[T] = (input: T) => self(input) ++ other(input)
    //    def --(other: QueryTagger[T]): QueryTagger[T] = (input: T) => self(input) -- other(input)
    //    def &&(other: QueryTagger[T]): QueryTagger[T] = (input: T) => self(input) intersect other(input)
  }
}

