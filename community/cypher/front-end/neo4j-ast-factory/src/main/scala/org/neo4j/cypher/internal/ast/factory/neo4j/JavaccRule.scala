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
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.ast.CallClause
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.SubqueryCall
import org.neo4j.cypher.internal.ast.UseGraph
import org.neo4j.cypher.internal.ast.factory.ParameterType
import org.neo4j.cypher.internal.expressions.CaseExpression
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.GraphPatternQuantifier
import org.neo4j.cypher.internal.expressions.ListComprehension
import org.neo4j.cypher.internal.expressions.Literal
import org.neo4j.cypher.internal.expressions.MapProjection
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.NumberLiteral
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.ParenthesizedPath
import org.neo4j.cypher.internal.expressions.PatternAtom
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternElement
import org.neo4j.cypher.internal.expressions.PatternPart
import org.neo4j.cypher.internal.expressions.QuantifiedPath
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.parser.javacc.Cypher
import org.neo4j.cypher.internal.parser.javacc.CypherCharStream
import org.neo4j.cypher.internal.parser.javacc.Token
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory

import scala.reflect.ClassTag

trait JavaccRule[+T] {
  def apply(queryText: String): T
}

object JavaccRule {

  type Parser = cypherJavaccParserFactory.Type

  def fromParser[T](runParser: Parser => T): JavaccRule[T] = fromQueryAndParser(identity, runParser)

  def fromQueryAndParser[T](transformQuery: String => String, runParser: Parser => T): JavaccRule[T] = {
    (queryText: String) =>
      val p = cypherJavaccParserFactory(transformQuery(queryText))
      val res = runParser(p)
      p.EndOfFile()
      res
  }

  def MatchClause: JavaccRule[Clause] = fromParser(_.MatchClause())
  def MergeClause: JavaccRule[Clause] = fromParser(_.MergeClause())
  def CallClause: JavaccRule[Clause] = fromParser(_.CallClause())
  def CaseExpression: JavaccRule[Expression] = fromParser(_.CaseExpression())
  def Clause: JavaccRule[Clause] = fromParser(_.Clause())
  def Expression: JavaccRule[Expression] = fromParser(_.Expression())
  def FunctionInvocation: JavaccRule[Expression] = fromParser(_.FunctionInvocation(false))
  def ListComprehension: JavaccRule[Expression] = fromParser(_.ListComprehension())
  def MapLiteral: JavaccRule[Expression] = fromParser(_.MapLiteral())
  def MapProjection: JavaccRule[Expression] = fromParser(_.MapProjection())
  def NodePattern: JavaccRule[NodePattern] = fromParser(_.NodePattern())
  def NumberLiteral: JavaccRule[Expression] = fromParser(_.NumberLiteral())
  def Parameter: JavaccRule[Parameter] = fromParser(_.Parameter(ParameterType.ANY))
  def ParenthesizedPath: JavaccRule[PatternAtom] = fromParser(_.ParenthesizedPath())
  def PatternComprehension: JavaccRule[Expression] = fromParser(_.PatternComprehension())
  def Quantifier: JavaccRule[GraphPatternQuantifier] = fromParser(_.Quantifier())
  def RelationshipPattern: JavaccRule[RelationshipPattern] = fromParser(_.RelationshipPattern())
  def PatternElement: JavaccRule[PatternElement] = fromParser(_.PatternElement())
  def PatternPart: JavaccRule[PatternPart] = fromParser(_.Pattern())
  def Statement: JavaccRule[Statement] = fromParser(_.Statement())
  def UseClause: JavaccRule[UseGraph] = fromParser(_.UseClause())

  // The reason for using Statements rather than Statement, is that it will wrap any ParseException in exceptionFactory.syntaxException(...),
  // just like the production code path, and thus produce correct assertable error messages.
  def Statements: JavaccRule[Statements] = fromParser(_.Statements())

  def StringLiteral: JavaccRule[Expression] = fromParser(_.StringLiteral())
  def SubqueryClause: JavaccRule[Clause] = fromParser(_.SubqueryClause())
  def Variable: JavaccRule[Variable] = fromParser(_.Variable())
  def UnescapedSymbolicNameString: JavaccRule[Token] = fromParser(_.UnescapedSymbolicNameString())

  def from[T <: ASTNode](implicit ct: ClassTag[T]): JavaccRule[T] = ct.runtimeClass match {
    case AstTypes.StatementsCls           => Statements.asInstanceOf[JavaccRule[T]]
    case AstTypes.StatementCls            => Statement.asInstanceOf[JavaccRule[T]]
    case AstTypes.ExpressionCls           => Expression.asInstanceOf[JavaccRule[T]]
    case AstTypes.VariableCls             => Variable.asInstanceOf[JavaccRule[T]]
    case AstTypes.ParameterCls            => Parameter.asInstanceOf[JavaccRule[T]]
    case AstTypes.NumberLiteralCls        => NumberLiteral.asInstanceOf[JavaccRule[T]]
    case AstTypes.CaseExpressionCls       => CaseExpression.asInstanceOf[JavaccRule[T]]
    case AstTypes.RelationshipPatternCls  => RelationshipPattern.asInstanceOf[JavaccRule[T]]
    case AstTypes.NodePatternCls          => NodePattern.asInstanceOf[JavaccRule[T]]
    case AstTypes.CallClauseCls           => CallClause.asInstanceOf[JavaccRule[T]]
    case AstTypes.FunctionInvocationCls   => FunctionInvocation.asInstanceOf[JavaccRule[T]]
    case AstTypes.ClauseCls               => Clause.asInstanceOf[JavaccRule[T]]
    case AstTypes.ListComprehensionCls    => ListComprehension.asInstanceOf[JavaccRule[T]]
    case AstTypes.MapProjectionCls        => MapProjection.asInstanceOf[JavaccRule[T]]
    case AstTypes.ParenthesizedPathCls    => ParenthesizedPath.asInstanceOf[JavaccRule[T]]
    case AstTypes.PatternComprehensionCls => PatternComprehension.asInstanceOf[JavaccRule[T]]
    case AstTypes.QuantifierCls           => Quantifier.asInstanceOf[JavaccRule[T]]
    case AstTypes.PatternElementCls       => PatternElement.asInstanceOf[JavaccRule[T]]
    case AstTypes.PatternPartCls          => PatternPart.asInstanceOf[JavaccRule[T]]
    case AstTypes.UseClauseCls            => UseClause.asInstanceOf[JavaccRule[T]]
    case AstTypes.StringLiteralCls        => StringLiteral.asInstanceOf[JavaccRule[T]]
    case AstTypes.SubqueryClauseCls       => SubqueryClause.asInstanceOf[JavaccRule[T]]
    case AstTypes.QuantifiedPathCls       => ParenthesizedPath.asInstanceOf[JavaccRule[T]]
    case AstTypes.LiteralCls              => Expression.asInstanceOf[JavaccRule[T]] // JavaCC have no literal parser
    case other                            => throw new IllegalArgumentException(s"Unsupported type $other")
  }

  // ParserFactory is only really needed to create the Parser type alias above without writing down all 30+ type parameters that Cypher[A,B,C,..] has.
  trait ParserFactory[P] {
    type Type = P
    def apply(q: String): P
  }

  object ParserFactory {
    def apply[P](f: String => P): ParserFactory[P] = q => f(q)
  }

  private val exceptionFactory = OpenCypherExceptionFactory(None)

  // noinspection TypeAnnotation
  val cypherJavaccParserFactory = ParserFactory { (queryText: String) =>
    val charStream = new CypherCharStream(queryText)
    val astExceptionFactory = new Neo4jASTExceptionFactory(exceptionFactory)
    val astFactory = new Neo4jASTFactory(queryText, astExceptionFactory, null)
    new Cypher(astFactory, astExceptionFactory, charStream)
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
}
