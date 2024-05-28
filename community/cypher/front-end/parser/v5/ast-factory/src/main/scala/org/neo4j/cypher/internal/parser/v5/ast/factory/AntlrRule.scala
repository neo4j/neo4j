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
package org.neo4j.cypher.internal.parser.v5.ast.factory

import org.neo4j.cypher.internal.ast.CallClause
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.SubqueryCall
import org.neo4j.cypher.internal.ast.UseGraph
import org.neo4j.cypher.internal.expressions.CaseExpression
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.GraphPatternQuantifier
import org.neo4j.cypher.internal.expressions.ListComprehension
import org.neo4j.cypher.internal.expressions.Literal
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.MapProjection
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.NumberLiteral
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.ParenthesizedPath
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternElement
import org.neo4j.cypher.internal.expressions.PatternPart
import org.neo4j.cypher.internal.expressions.QuantifiedPath
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.parser.AstRuleCtx
import org.neo4j.cypher.internal.parser.v5.ast.factory.ast.CypherAstParser
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.Neo4jCypherExceptionFactory

import scala.reflect.ClassTag

trait AntlrRule[+T <: AstRuleCtx] {
  def apply(queryText: String): Cst[T]
}

object AntlrRule {

  type Parser = CypherAstParser

  def fromParser[T <: AstRuleCtx](runParser: Parser => T): AntlrRule[T] = fromQueryAndParser(runParser)

  def fromQueryAndParser[T <: AstRuleCtx](runParser: Parser => T): AntlrRule[T] = new AntlrRule[T] {

    override def apply(cypher: String): Cst[T] = {
      val ctx = CypherAstParser.parse[T](cypher, Neo4jCypherExceptionFactory(cypher, None), None, runParser)
      val theAst = Option(ctx.ast[ASTNode]())
      new Cst(ctx) {
        val parsingErrors: List[Exception] = List.empty
        override def ast: Option[ASTNode] = theAst
      }
    }
  }

  def CallClause: AntlrRule[Cst.CallClause] = fromParser(_.callClause())
  def MatchClause: AntlrRule[Cst.MatchClause] = fromParser(_.matchClause())
  def CaseExpression: AntlrRule[Cst.CaseExpression] = fromParser(_.caseExpression())
  def Clause: AntlrRule[Cst.Clause] = fromParser(_.clause())
  def Expression: AntlrRule[Cst.Expression] = fromParser(_.expression())
  def FunctionInvocation: AntlrRule[Cst.FunctionInvocation] = fromParser(_.functionInvocation())
  def ListComprehension: AntlrRule[Cst.ListComprehension] = fromParser(_.listComprehension())
  def Map: AntlrRule[Cst.Map] = fromParser(_.map())
  def MapProjection: AntlrRule[Cst.MapProjection] = fromParser(_.mapProjection())
  def NodePattern: AntlrRule[Cst.NodePattern] = fromParser(_.nodePattern())
  def NumberLiteral: AntlrRule[Cst.NumberLiteral] = fromParser(_.numberLiteral())
  def Parameter: AntlrRule[Cst.Parameter] = fromParser(_.parameter("ANY"))
  def ParenthesizedPath: AntlrRule[Cst.ParenthesizedPath] = fromParser(_.parenthesizedPath())
  def PatternComprehension: AntlrRule[Cst.PatternComprehension] = fromParser(_.patternComprehension())
  def Quantifier: AntlrRule[Cst.Quantifier] = fromParser(_.quantifier())
  def RelationshipPattern: AntlrRule[Cst.RelationshipPattern] = fromParser(_.relationshipPattern())
  def PatternPart: AntlrRule[Cst.Pattern] = fromParser(_.pattern())
  def Statement: AntlrRule[Cst.Statement] = fromParser(_.statement())
  def UseClause: AntlrRule[Cst.UseClause] = fromParser(_.useClause())
  def StringLiteral: AntlrRule[Cst.StringLiteral] = fromParser(_.stringLiteral())
  def SubqueryClause: AntlrRule[Cst.SubqueryClause] = fromParser(_.subqueryClause())
  def Variable: AntlrRule[Cst.Variable] = fromParser(_.variable())
  def Literal: AntlrRule[Cst.Literal] = fromParser(_.literal())

  // The reason for using Statements rather than Statement, is that Statements exhausts the input
  // reading until the EOF, even if it does not know how to parse all of it, whereas Statement
  // (and the rest of parser rules) will stop the moment they find a token they cannot recognize
  def Statements: AntlrRule[Cst.Statements] = fromParser(_.statements())

  def from[T <: ASTNode](implicit ct: ClassTag[T]): AntlrRule[_ <: AstRuleCtx] = ct.runtimeClass match {
    case AstTypes.StatementsCls           => Statements
    case AstTypes.StatementCls            => Statement
    case AstTypes.ExpressionCls           => Expression
    case AstTypes.VariableCls             => Variable
    case AstTypes.ParameterCls            => Parameter
    case AstTypes.NumberLiteralCls        => NumberLiteral
    case AstTypes.CaseExpressionCls       => CaseExpression
    case AstTypes.RelationshipPatternCls  => RelationshipPattern
    case AstTypes.NodePatternCls          => NodePattern
    case AstTypes.CallClauseCls           => CallClause
    case AstTypes.FunctionInvocationCls   => FunctionInvocation
    case AstTypes.ClauseCls               => Clause
    case AstTypes.ListComprehensionCls    => ListComprehension
    case AstTypes.MapProjectionCls        => MapProjection
    case AstTypes.ParenthesizedPathCls    => ParenthesizedPath
    case AstTypes.PatternComprehensionCls => PatternComprehension
    case AstTypes.QuantifierCls           => Quantifier
    case AstTypes.PatternPartCls          => PatternPart
    case AstTypes.UseClauseCls            => UseClause
    case AstTypes.StringLiteralCls        => StringLiteral
    case AstTypes.SubqueryClauseCls       => SubqueryClause
    case AstTypes.QuantifiedPathCls       => ParenthesizedPath
    case AstTypes.LiteralCls              => Literal
    case AstTypes.MapExpressionCls        => Map
    case other                            => throw new IllegalArgumentException(s"Unsupported type $other")
  }
}

object AstTypes {
  val StatementCls: Class[Statement] = classOf[Statement]
  val StatementsCls: Class[Statements] = classOf[Statements]
  val ExpressionCls: Class[Expression] = classOf[Expression]
  val ParameterCls: Class[Parameter] = classOf[Parameter]
  val CaseExpressionCls: Class[CaseExpression] = classOf[CaseExpression]
  val VariableCls: Class[Variable] = classOf[Variable]
  val NumberLiteralCls: Class[NumberLiteral] = classOf[NumberLiteral]
  val NodePatternCls: Class[NodePattern] = classOf[NodePattern]
  val RelationshipPatternCls: Class[RelationshipPattern] = classOf[RelationshipPattern]
  val CallClauseCls: Class[CallClause] = classOf[CallClause]
  val ClauseCls: Class[Clause] = classOf[Clause]
  val FunctionInvocationCls: Class[FunctionInvocation] = classOf[FunctionInvocation]
  val ListComprehensionCls: Class[ListComprehension] = classOf[ListComprehension]
  val MapProjectionCls: Class[MapProjection] = classOf[MapProjection]
  val ParenthesizedPathCls: Class[ParenthesizedPath] = classOf[ParenthesizedPath]
  val PatternComprehensionCls: Class[PatternComprehension] = classOf[PatternComprehension]
  val QuantifierCls: Class[GraphPatternQuantifier] = classOf[GraphPatternQuantifier]
  val PatternElementCls: Class[PatternElement] = classOf[PatternElement]
  val PatternPartCls: Class[PatternPart] = classOf[PatternPart]
  val UseClauseCls: Class[UseGraph] = classOf[UseGraph]
  val StringLiteralCls: Class[StringLiteral] = classOf[StringLiteral]
  val SubqueryClauseCls: Class[SubqueryCall] = classOf[SubqueryCall]
  val QuantifiedPathCls: Class[QuantifiedPath] = classOf[QuantifiedPath]
  val LiteralCls: Class[Literal] = classOf[Literal]
  val MapExpressionCls: Class[MapExpression] = classOf[MapExpression]
}
