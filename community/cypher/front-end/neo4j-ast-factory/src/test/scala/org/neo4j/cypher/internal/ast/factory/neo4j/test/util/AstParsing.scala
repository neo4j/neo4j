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

import org.neo4j.cypher.internal.ast.factory.neo4j.JavaccRule
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.Antlr
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.AstConstructionError
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.JavaCc
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.ParseError
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.ParseResult
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.ParseResults
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.ParseSuccess
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.ParserInTest
import org.neo4j.cypher.internal.cst.factory.neo4j.AntlrRule
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.internal.helpers.Exceptions

import scala.reflect.ClassTag
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/** Methods for parsing [[ASTNode]]s */
trait AstParsing {

  def parseAst[T <: ASTNode : ClassTag](cypher: String): ParseResults[T] = {
    ParseResults[T](cypher, ParserInTest.AllParsers.map(p => p -> parseAst[T](p, cypher)).toMap)
  }

  private def parseAst[T <: ASTNode : ClassTag](parser: ParserInTest, cypher: String): ParseResult = {
    parser match {
      case JavaCc =>
        val javaCcRule = JavaccRule.from[T]
        Try(javaCcRule(cypher)) match {
          case Success(ast)       => ParseSuccess(ast)
          case Failure(throwable) => ParseError(throwable)
        }
      case Antlr =>
        val antlrRule = AntlrRule.from[T]
        Try(antlrRule(cypher).ast) match {
          case Success(ast) => ParseSuccess(ast.orNull)
          case Failure(throwable) => Try(antlrRule.parseWithoutAst(cypher)) match {
              case Success(_)              => AstConstructionError(throwable)
              case Failure(parseThrowable) => ParseError(parseThrowable)
            }
        }
    }
  }
}

object AstParsing extends AstParsing {
  sealed trait ParserInTest

  object ParserInTest {
    val AllParsers: Seq[ParserInTest] = Seq(JavaCc, Antlr)
  }
  case object JavaCc extends ParserInTest
  case object Antlr extends ParserInTest

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

  sealed trait ParseFailure extends ParseResult {
    def throwable: Throwable

    override def toString: String = s"Failed parsing:\n${Exceptions.stringify(throwable)}"
  }

  object ParseFailure {
    def unapply(f: ParseFailure): Option[Throwable] = Some(f.throwable)
  }
  case class ParseError(throwable: Throwable) extends ParseFailure
  case class AstConstructionError(throwable: Throwable) extends ParseFailure
}
