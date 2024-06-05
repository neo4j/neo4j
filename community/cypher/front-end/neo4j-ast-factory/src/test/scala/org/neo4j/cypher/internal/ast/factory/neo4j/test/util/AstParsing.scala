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
package org.neo4j.cypher.internal.ast.factory.neo4j.test.util

import org.neo4j.cypher.internal.ast.CallClause
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.SubqueryCall
import org.neo4j.cypher.internal.ast.UseGraph
import org.neo4j.cypher.internal.ast.factory.ParameterType
import org.neo4j.cypher.internal.ast.factory.neo4j.Neo4jASTExceptionFactory
import org.neo4j.cypher.internal.ast.factory.neo4j.Neo4jASTFactory
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.Cypher5
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.Cypher5JavaCc
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.ParseFailure
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.ParseResult
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.ParseResults
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.ParseSuccess
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.ParserInTest
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
import org.neo4j.cypher.internal.expressions.PatternPart
import org.neo4j.cypher.internal.expressions.QuantifiedPath
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.parser.AstRuleCtx
import org.neo4j.cypher.internal.parser.javacc.Cypher
import org.neo4j.cypher.internal.parser.javacc.CypherCharStream
import org.neo4j.cypher.internal.parser.v5.CypherParser
import org.neo4j.cypher.internal.parser.v5.ast.factory.ast.CypherAstParser
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.internal.helpers.Exceptions

import scala.reflect.ClassTag
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/** Methods for parsing [[ASTNode]]s */
trait AstParsing extends Parsers.Implicit {

  def parseAst[T <: ASTNode : ClassTag](cypher: String)(implicit parsers: Parsers[T]): ParseResults[T] = {
    ParseResults[T](cypher, ParserInTest.AllParsers.map(p => p -> parseAst[T](p, cypher)).toMap)
  }

  private def parseAst[T <: ASTNode : ClassTag](
    parser: ParserInTest,
    cypher: String
  )(implicit parsers: Parsers[T]): ParseResult = {
    Try(parsers.parse(parser, cypher)) match {
      case Success(ast)       => ParseSuccess(ast)
      case Failure(throwable) => ParseFailure(throwable)
    }
  }
}

object AstParsing extends AstParsing {
  sealed trait ParserInTest

  object ParserInTest {
    val AllParsers: Seq[ParserInTest] = Seq(Cypher5JavaCc, Cypher5) // TODO Add Cypher6
  }
  case object Cypher5JavaCc extends ParserInTest
  case object Cypher5 extends ParserInTest
  // Note, there is no cypher 6 parser yet, only added here as a preparation
  case object Cypher6 extends ParserInTest

  case class ParseResults[T](cypher: String, result: Map[ParserInTest, ParseResult]) {
    def apply(parser: ParserInTest): ParseResult = result(parser)
  }

  sealed trait ParseResult {

    def toTry: Try[Any] = this match {
      case ParseSuccess(ast) => Success(ast)
      case f: ParseFailure   => Failure(f.throwable)
    }
  }
  case class ParseSuccess[T](ast: T) extends ParseResult

  case class ParseFailure(throwable: Throwable) extends ParseResult {
    override def toString: String = s"Failed parsing:\n${Exceptions.stringify(throwable)}"
  }
}

trait Parser[T <: ASTNode] {
  def parse(cypher: String): T
}

trait ParserFactory {
  def statements(): Parser[Statements]
  def statement(): Parser[Statement]
  def expression(): Parser[Expression]
  def callClause(): Parser[CallClause]
  def matchClause(): Parser[Match]
  def caseExpression(): Parser[CaseExpression]
  def clause(): Parser[Clause]
  def functionInvocation(): Parser[FunctionInvocation]
  def listComprehension(): Parser[ListComprehension]
  def map(): Parser[MapExpression]
  def mapProjection(): Parser[MapProjection]
  def nodePattern(): Parser[NodePattern]
  def numberLiteral(): Parser[NumberLiteral]
  def parameter(): Parser[Parameter]
  def parenthesizedPath(): Parser[ParenthesizedPath]
  def patternComprehension(): Parser[PatternComprehension]
  def quantifier(): Parser[GraphPatternQuantifier]
  def relationshipPattern(): Parser[RelationshipPattern]
  def pattern(): Parser[PatternPart]
  def useClause(): Parser[UseGraph]
  def stringLiteral(): Parser[StringLiteral]
  def subqueryClause(): Parser[SubqueryCall]
  def variable(): Parser[Variable]
  def literal(): Parser[Literal]
  def quantifiedPath(): Parser[QuantifiedPath]
}

case class Parsers[T <: ASTNode] private (parsers: Map[ParserInTest, Parser[T]]) {
  def parse(parser: ParserInTest, cypher: String): T = parsers(parser).parse(cypher)
}

object Parsers {

  trait Implicit {
    implicit val StatementsParsers: Parsers[Statements] = from(_.statements())
    implicit val StatementParsers: Parsers[Statement] = from(_.statement())
    implicit val ExpressionParsers: Parsers[Expression] = from(_.expression())
    implicit val CallClauseParsers: Parsers[CallClause] = from(_.callClause())
    implicit val MatchClauseParsers: Parsers[Match] = from(_.matchClause())
    implicit val CaseExpressionParsers: Parsers[CaseExpression] = from(_.caseExpression())
    implicit val ClauseParsers: Parsers[Clause] = from(_.clause())
    implicit val FunctionInvocationParsers: Parsers[FunctionInvocation] = from(_.functionInvocation())
    implicit val ListComprehensionParsers: Parsers[ListComprehension] = from(_.listComprehension())
    implicit val MapParsers: Parsers[MapExpression] = from(_.map())
    implicit val MapProjectionParsers: Parsers[MapProjection] = from(_.mapProjection())
    implicit val NodePatternParsers: Parsers[NodePattern] = from(_.nodePattern())
    implicit val NumberLiteralParsers: Parsers[NumberLiteral] = from(_.numberLiteral())
    implicit val ParameterParsers: Parsers[Parameter] = from(_.parameter())
    implicit val ParenthesizedPathParsers: Parsers[ParenthesizedPath] = from(_.parenthesizedPath())
    implicit val PatternComprehensionParsers: Parsers[PatternComprehension] = from(_.patternComprehension())
    implicit val QuantifierParsers: Parsers[GraphPatternQuantifier] = from(_.quantifier())
    implicit val QuantifiedPathParsers: Parsers[QuantifiedPath] = from(_.quantifiedPath())
    implicit val RelationshipPatternParsers: Parsers[RelationshipPattern] = from(_.relationshipPattern())
    implicit val PatternPartParsers: Parsers[PatternPart] = from(_.pattern())
    implicit val UseClauseParsers: Parsers[UseGraph] = from(_.useClause())
    implicit val StringLiteralParsers: Parsers[StringLiteral] = from(_.stringLiteral())
    implicit val SubqueryClauseParsers: Parsers[SubqueryCall] = from(_.subqueryClause())
    implicit val VariableParsers: Parsers[Variable] = from(_.variable())
    implicit val LiteralParsers: Parsers[Literal] = from(_.literal())
  }

  private val factories = Map[ParserInTest, ParserFactory](
    Cypher5 -> Cypher5Factory,
    Cypher5JavaCc -> Cypher5JavaCcFactory
  )

  private def from[T <: ASTNode](f: ParserFactory => Parser[T]): Parsers[T] =
    Parsers(ParserInTest.AllParsers.map(p => p -> f.apply(factories(p))).toMap)

  private object Cypher5Factory extends ParserFactory {

    private def parse[T <: ASTNode](f: CypherParser => AstRuleCtx): Parser[T] = (cypher: String) => {
      new CypherAstParser(cypher, Neo4jCypherExceptionFactory(cypher, None), None).parse(f)
    }
    override def statements(): Parser[Statements] = parse(_.statements())
    override def statement(): Parser[Statement] = parse(_.statement())
    override def expression(): Parser[Expression] = parse(_.expression())
    override def callClause(): Parser[CallClause] = parse(_.callClause())
    override def matchClause(): Parser[Match] = parse(_.matchClause())
    override def caseExpression(): Parser[CaseExpression] = parse(_.caseExpression())
    override def clause(): Parser[Clause] = parse(_.clause())
    override def functionInvocation(): Parser[FunctionInvocation] = parse(_.functionInvocation())
    override def listComprehension(): Parser[ListComprehension] = parse(_.listComprehension())
    override def map(): Parser[MapExpression] = parse(_.map())
    override def mapProjection(): Parser[MapProjection] = parse(_.mapProjection())
    override def nodePattern(): Parser[NodePattern] = parse(_.nodePattern())
    override def numberLiteral(): Parser[NumberLiteral] = parse(_.numberLiteral())
    override def parameter(): Parser[Parameter] = parse(_.parameter("ANY"))
    override def parenthesizedPath(): Parser[ParenthesizedPath] = parse(_.parenthesizedPath())
    override def patternComprehension(): Parser[PatternComprehension] = parse(_.patternComprehension())
    override def quantifier(): Parser[GraphPatternQuantifier] = parse(_.quantifier())
    override def relationshipPattern(): Parser[RelationshipPattern] = parse(_.relationshipPattern())
    override def pattern(): Parser[PatternPart] = parse(_.pattern())
    override def useClause(): Parser[UseGraph] = parse(_.useClause())
    override def stringLiteral(): Parser[StringLiteral] = parse(_.stringLiteral())
    override def subqueryClause(): Parser[SubqueryCall] = parse(_.subqueryClause())
    override def variable(): Parser[Variable] = parse(_.variable())
    override def literal(): Parser[Literal] = parse(_.literal())
    override def quantifiedPath(): Parser[QuantifiedPath] = parse(_.parenthesizedPath())
  }

  private object Cypher5JavaCcFactory extends ParserFactory {

    // ParserFactory is only really needed to create the Parser type alias above without writing down all 30+ type parameters
    trait JavaCcParserFactory[P] {
      type Type = P
      def apply(q: String): P
    }

    object JavaCcParserFactory {
      def apply[P](f: String => P): JavaCcParserFactory[P] = q => f(q)
    }

    // noinspection TypeAnnotation
    val factory = JavaCcParserFactory { (cypher: String) =>
      val charStream = new CypherCharStream(cypher)
      val astExceptionFactory = new Neo4jASTExceptionFactory(OpenCypherExceptionFactory(None))
      val astFactory = new Neo4jASTFactory(cypher, astExceptionFactory, null)
      new Cypher(astFactory, astExceptionFactory, charStream)
    }
    type JavaCcParser = factory.Type

    private def parse[T <: ASTNode](f: JavaCcParser => T): Parser[T] =
      (cypher: String) => f.apply(factory.apply(cypher))

    override def statements(): Parser[Statements] = parse(_.Statements())
    override def statement(): Parser[Statement] = parse(_.Statement())
    override def expression(): Parser[Expression] = parse(_.Expression())
    override def callClause(): Parser[CallClause] = parse(_.CallClause().asInstanceOf[CallClause])
    override def matchClause(): Parser[Match] = parse(_.MatchClause().asInstanceOf[Match])
    override def caseExpression(): Parser[CaseExpression] = parse(_.CaseExpression().asInstanceOf[CaseExpression])
    override def clause(): Parser[Clause] = parse(_.Clause())
    override def functionInvocation(): Parser[FunctionInvocation] = parse(_.FunctionInvocation(false))

    override def listComprehension(): Parser[ListComprehension] =
      parse(_.ListComprehension().asInstanceOf[ListComprehension])
    override def map(): Parser[MapExpression] = parse(_.MapLiteral().asInstanceOf[MapExpression])
    override def mapProjection(): Parser[MapProjection] = parse(_.MapProjection().asInstanceOf[MapProjection])
    override def nodePattern(): Parser[NodePattern] = parse(_.NodePattern())
    override def numberLiteral(): Parser[NumberLiteral] = parse(_.NumberLiteral().asInstanceOf[NumberLiteral])
    override def parameter(): Parser[Parameter] = parse(_.Parameter(ParameterType.ANY))

    override def parenthesizedPath(): Parser[ParenthesizedPath] =
      parse(_.ParenthesizedPath().asInstanceOf[ParenthesizedPath])

    override def patternComprehension(): Parser[PatternComprehension] =
      parse(_.PatternComprehension().asInstanceOf[PatternComprehension])
    override def quantifier(): Parser[GraphPatternQuantifier] = parse(_.Quantifier())
    override def relationshipPattern(): Parser[RelationshipPattern] = parse(_.RelationshipPattern())
    override def pattern(): Parser[PatternPart] = parse(_.Pattern())
    override def useClause(): Parser[UseGraph] = parse(_.UseClause())
    override def stringLiteral(): Parser[StringLiteral] = parse(_.StringLiteral().asInstanceOf[StringLiteral])
    override def subqueryClause(): Parser[SubqueryCall] = parse(_.SubqueryClause().asInstanceOf[SubqueryCall])
    override def variable(): Parser[Variable] = parse(_.Variable())
    override def literal(): Parser[Literal] = parse(_.Expression().asInstanceOf[Literal])
    override def quantifiedPath(): Parser[QuantifiedPath] = parse(_.ParenthesizedPath().asInstanceOf[QuantifiedPath])
  }
}
