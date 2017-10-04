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
package org.neo4j.cypher.internal.compiler.v3_4.ast

import org.neo4j.cypher.internal.util.v3_4.ASTNode
import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.v3_4.functions._
import org.neo4j.cypher.internal.frontend.v3_4.parser.CypherParser
import org.neo4j.cypher.internal.v3_4.expressions._

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

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
    CallProcedureTag,

    UpdatesTag,
    LoadCSVTag,
    CreateTag,
    DeleteTag,
    DetachDeleteTag,
    SetTag,
    RemoveTag,
    MergeTag,
    OnMatchTag,
    OnCreateTag,
    CreateUniqueTag,
    ForeachTag,

    LimitTag,
    SkipTag,
    OrderByTag,

    CreateIndexTag,
    CreateConstraintTag,
    DropIndexTag,
    DropConstraintTag,

    AggregationTag,
    MathFunctionTag,
    StringFunctionTag,

    CaseTag,
    ComplexExpressionTag,
    FilteringExpressionTag,
    LiteralExpressionTag,
    ParameterExpressionTag,
    VariableExpressionTag
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
case object SkipTag extends QueryTag("skip")
case object LimitTag extends QueryTag("limit")
case object OrderByTag extends QueryTag("order-by")
case object CallProcedureTag extends QueryTag("call-procedure")

// Updates

case object UpdatesTag extends QueryTag("updates")
case object LoadCSVTag extends QueryTag("load-csv")
case object CreateTag extends QueryTag("create")
case object DeleteTag extends QueryTag("delete")
case object DetachDeleteTag extends QueryTag("detach-delete")
case object SetTag extends QueryTag("set")
case object RemoveTag extends QueryTag("remove")
case object MergeTag extends QueryTag("merge")
case object OnMatchTag extends QueryTag("on-match")
case object OnCreateTag extends QueryTag("on-create")
case object CreateUniqueTag extends QueryTag("create-unique")
case object ForeachTag extends QueryTag("foreach")

// Expressions

case object ComplexExpressionTag extends QueryTag("complex-expr")
case object FilteringExpressionTag extends QueryTag("filtering-expr")
case object LiteralExpressionTag extends QueryTag("literal-expr")
case object ParameterExpressionTag extends QueryTag("parameter-expr")
case object VariableExpressionTag extends QueryTag("variable-expr")
case object CaseTag extends QueryTag("case-expr")

// Commands

case object CreateIndexTag extends QueryTag("create-index")
case object CreateConstraintTag extends QueryTag("create-constraint")
case object DropIndexTag extends QueryTag("drop-index")
case object DropConstraintTag extends QueryTag("drop-constraint")

// Functions

case object MathFunctionTag extends QueryTag("math-function")
case object StringFunctionTag extends QueryTag("string-function")
case object AggregationTag extends QueryTag("aggregation")

object QueryTagger extends QueryTagger[String] {

  def apply(input: String) = Try(default(input)) match {
    case Success(set) => set
    case Failure(exception) => Set.empty // in case there was a syntax error
  }

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
        Set(LoadCSVTag, UpdatesTag)

      case x: CallClause =>
        Set(CallProcedureTag)

      case x: UpdateClause =>
        val specificTag = x match {
          case u: Create => Set(CreateTag)
          case Delete(_, forced) => if (forced) Set(DetachDeleteTag) else Set(DeleteTag)
          case u: SetClause => Set(SetTag)
          case u: Remove => Set(RemoveTag)
          case u: Merge => Set(MergeTag) ++ u.actions.map {
            case _: OnCreate => OnCreateTag
            case _: OnMatch => OnMatchTag
          }
          case u: CreateUnique => Set(CreateUniqueTag)
          case u: Foreach => Set(ForeachTag)
          case _ => Set.empty[QueryTag]
        }
        specificTag ++ Set(UpdatesTag)
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

    // <expr> unless variable or literal
    lift[ASTNode] {
      case x: Variable => Set.empty
      case x: Literal => Set.empty
      case x: Expression => Set(ComplexExpressionTag)
    } ++

    // subtype of <expr>
    lift[ASTNode] {
      case x: Variable => Set(VariableExpressionTag)
      case x: Literal => Set(LiteralExpressionTag)
      case x: Parameter => Set(ParameterExpressionTag)
      case x: FilteringExpression => Set(FilteringExpressionTag)
      case x: CaseExpression => Set(CaseTag)
    } ++

    // return clause extras
    lift[ASTNode] {
      case x: Limit => Set(LimitTag)
      case x: Skip => Set(SkipTag)
      case x: OrderBy => Set(OrderByTag)
    } ++

    // commands
    lift[ASTNode] {
      case x: CreateIndex => Set(CreateIndexTag)
      case x: DropIndex => Set(DropIndexTag)
      case x: CreateNodeKeyConstraint => Set(CreateConstraintTag)
      case x: CreateUniquePropertyConstraint => Set(CreateConstraintTag)
      case x: CreateNodePropertyExistenceConstraint => Set(CreateConstraintTag)
      case x: CreateRelationshipPropertyExistenceConstraint => Set(CreateConstraintTag)
      case x: DropUniquePropertyConstraint => Set(DropConstraintTag)
      case x: DropNodePropertyExistenceConstraint => Set(DropConstraintTag)
      case x: DropRelationshipPropertyExistenceConstraint => Set(DropConstraintTag)
    } ++

    // functions
    lift[ASTNode] {
      case f: FunctionInvocation if mathFunctions contains f.function => Set(MathFunctionTag)
      case f: FunctionInvocation if stringFunctions contains f.function => Set(StringFunctionTag)
      case f: FunctionInvocation if isAggregation(f.function) => Set(AggregationTag)
    }
  ))

  private def isAggregation(function: Function) = function match {
    case x: AggregatingFunction => true
    case _ => false
  }

  private def stringFunctions: Set[Function] = Set(Replace, Substring, Left, Right, LTrim, RTrim,
                                                   ToLower, ToUpper, Split, Reverse, ToString)

  private def mathFunctions: Set[Function] = Set(Abs, Ceil, Floor, Round, Sign, Rand,
                                                 Log, Log10, Exp, E, Sqrt,
                                                 Sin, Cos, Tan, Cot, Asin, Acos, Atan, Atan2,
                                                 Pi, Degrees, Radians, Haversin)

  // run parser and pass statement to next query tagger
  case class fromString(next: QueryTagger[Statement])
    extends QueryTagger[String] {

    val parser = new CypherParser

    def apply(queryText: String): Set[QueryTag] = next(parser.parse(queryText))
  }

  // run inner query tagger on each child ast node and return union over all results
  case class forEachChild(inner: QueryTagger[ASTNode]) extends QueryTagger[Statement] {
    def apply(input: Statement) = input.treeFold(Set.empty[QueryTag]) {
      case node: ASTNode => acc => (acc ++ inner(node), Some(identity))
    }
  }

  def lift[T](f: PartialFunction[T, Set[QueryTag]]): QueryTagger[T] = f.lift.andThen(_.getOrElse(Set.empty))

  implicit class RichQueryTagger[T](lhs: QueryTagger[T]) {
    def ++(rhs: QueryTagger[T]): QueryTagger[T] = (input: T) => lhs(input) `union` rhs(input)
  }
}

