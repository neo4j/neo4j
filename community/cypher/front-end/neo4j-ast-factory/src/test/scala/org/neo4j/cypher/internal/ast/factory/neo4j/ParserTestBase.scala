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

import org.antlr.v4.runtime.ParserRuleContext
import org.neo4j.cypher.internal.cst.factory.neo4j.AntlrRule
import org.neo4j.cypher.internal.parser.javacc.ParseException
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.util.Failure
import scala.util.Success
import scala.util.Try

trait ParserTestBase[S <: ParserRuleContext, T, J] extends CypherFunSuite {

  type Extra

  def convert(astNode: T): J

  def convert(astNode: T, extra: Extra): J = convert(astNode)

  case class ResultCheck(actuals: Seq[J], text: String) {

    def or(other: ResultCheck): ResultCheck = copy(actuals = actuals ++ other.actuals)

    def shouldGive(expected: J): Unit = {
      actuals foreach {
        actual =>
          actual should equal(expected)
      }
    }

    def shouldGive(expected: InputPosition => J): Unit = {
      shouldGive(expected(InputPosition(0, 0, 0)))
    }

    def shouldMatch(expected: PartialFunction[J, Unit]): Unit = {
      actuals foreach {
        actual => expected.isDefinedAt(actual) should equal(true)
      }
    }

    def shouldVerify(expected: J => Unit): Unit = {
      actuals foreach expected
    }

    override def toString: String = s"ResultCheck( $text -> $actuals )"
  }

  def parsing(s: String)(implicit javaccRule: JavaccRule[T], antlrRule: AntlrRule[S]): ResultCheck = {
    parseWithAntlrRule(s, expectSucceeds = true)
    convertResult(parseWithJavaccRule(s), None, s)
  }

  def parsingWith(s: String, extra: Extra)(implicit javaccRule: JavaccRule[T], antlrRule: AntlrRule[S]): ResultCheck = {
    parseWithAntlrRule(s, expectSucceeds = true)
    convertResult(parseWithJavaccRule(s), Some(extra), s)
  }

  def partiallyParsing(s: String)(implicit javaccRule: JavaccRule[T], antlrRule: AntlrRule[S]): ResultCheck = {
    parseWithAntlrRule(s, expectSucceeds = true)
    convertResult(parseWithJavaccRule(s), None, s)
  }

  def assertFails(s: String)(implicit javaccRule: JavaccRule[T], antlrRule: AntlrRule[S]): Unit = {
    parseWithAntlrRule(s, expectSucceeds = false)
    parseWithJavaccRule(s).toOption match {
      case None        =>
      case Some(thing) => fail(s"'$s' should not have been parsed correctly with JavaCC, parsed as $thing")
    }
  }

  def assertFailsOnlyJavaCC(s: String)(implicit javaccRule: JavaccRule[T], antlrRule: AntlrRule[S]): Unit = {
    parseWithAntlrRule(s, expectSucceeds = true)
    parseWithJavaccRule(s).toOption match {
      case None        =>
      case Some(thing) => fail(s"'$s' should not have been parsed correctly with JavaCC, parsed as $thing")
    }
  }

  def assertFailsWithException(
    s: String,
    expected: Exception
  )(implicit javaccRule: JavaccRule[T], antlrRule: AntlrRule[S]): Unit = {
    // We currently do not check the exact error message for ANTLR, just that it fails
    parseWithAntlrRule(s, expectSucceeds = false)

    parseWithJavaccRule(s) match {
      case Failure(exception) =>
        exception.getClass should be(expected.getClass)
        exception.getMessage shouldBe fixLineSeparator(expected.getMessage)
      case Success(thing) => fail(s"'$s' should not have been parsed correctly, parsed as $thing")
    }
  }

  def assertFailsWithMessage(
    s: String,
    expectedMessage: String,
    failsOnlyJavaCC: Boolean = false
  )(implicit javaccRule: JavaccRule[T], antlrRule: AntlrRule[S]): Unit = {
    // We currently do not check the exact error message for ANTLR, just that it fails
    parseWithAntlrRule(s, expectSucceeds = failsOnlyJavaCC)

    parseWithJavaccRule(s) match {
      case Failure(exception) =>
        exception.getMessage shouldBe fixLineSeparator(expectedMessage)
      case Success(thing) => fail(s"'$s' should not have been parsed correctly, parsed as $thing")
    }
  }

  def assertFailsWithMessageStart(
    s: String,
    expectedMessage: String,
    failsOnlyJavaCC: Boolean = false
  )(implicit javaccRule: JavaccRule[T], antlrRule: AntlrRule[S]): Unit = {
    // We currently do not check the exact error message for ANTLR, just that it fails
    parseWithAntlrRule(s, expectSucceeds = failsOnlyJavaCC)

    parseWithJavaccRule(s) match {
      case Failure(exception) =>
        exception.getMessage should startWith(fixLineSeparator(expectedMessage))
      case Success(thing) => fail(s"'$s' should not have been parsed correctly, parsed as $thing")
    }
  }

  def assertFailsWithMessageContains(
    s: String,
    expectedMessage: String
  )(implicit javaccRule: JavaccRule[T], antlrRule: AntlrRule[S]): Unit = {
    // We currently do not check the exact error message for ANTLR, just that it fails
    parseWithAntlrRule(s, expectSucceeds = false)

    parseWithJavaccRule(s) match {
      case Failure(exception) =>
        exception.getMessage.contains(fixLineSeparator(expectedMessage)) shouldBe true
      case Success(thing) => fail(s"'$s' should not have been parsed correctly, parsed as $thing")
    }
  }

  private def fixLineSeparator(message: String): String = {
    // This is needed because current version of scala seems to produce \n from multi line strings
    // This produces a problem with windows line endings \r\n
    if (message.contains(System.lineSeparator()))
      message
    else
      message.replaceAll("\n", System.lineSeparator())
  }

  protected def parseWithJavaccRule(queryText: String)(implicit rule: JavaccRule[T]): Try[T] = Try(rule(queryText))

  protected def parseWithAntlrRule(queryText: String, expectSucceeds: Boolean)(implicit rule: AntlrRule[S]): Unit = {
    val antlrResult = rule(queryText)

    antlrResult.parsingErrors match {
      case Seq() if !expectSucceeds =>
        fail(s"'$queryText' should not have been parsed correctly with ANTLR")
      case errors if errors.nonEmpty && expectSucceeds =>
        fail(
          s"'$queryText' should have been parsed correctly with ANTLR, instead failed with following errors: ${errors.mkString}"
        )
      case _ =>
    }
  }

  private def convertResult(r: Try[T], extra: Option[Extra], input: String) = r match {
    case Success(t) =>
      val converted = extra match {
        case None    => convert(t)
        case Some(e) => convert(t, e)
      }
      ResultCheck(Seq(converted), input)

    case Failure(exception) => fail(generateErrorMessage(input, exception), exception)
  }

  private def generateErrorMessage(input: String, exception: Throwable): String = {
    val defaultMessage = s"'$input' failed with: $exception"
    exception match {
      case e: ParseException =>
        if (input.contains("\n")) {
          defaultMessage
        } else {
          val pos = e.currentToken.beginOffset
          val indentation = " ".repeat(pos)
          s"""
             |'$input'
             | $indentation^
             |failed with $exception""".stripMargin
        }
      case _ => defaultMessage
    }
  }
}
