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
package org.neo4j.cypher.internal.frontend.prettifier

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.AdministrationCommand
import org.neo4j.cypher.internal.ast.AlterUser
import org.neo4j.cypher.internal.ast.Auth
import org.neo4j.cypher.internal.ast.CatalogName
import org.neo4j.cypher.internal.ast.CreateLocalDatabaseAlias
import org.neo4j.cypher.internal.ast.CreateUser
import org.neo4j.cypher.internal.ast.DenyPrivilege
import org.neo4j.cypher.internal.ast.ExternalAuth
import org.neo4j.cypher.internal.ast.GrantPrivilege
import org.neo4j.cypher.internal.ast.GraphDirectReference
import org.neo4j.cypher.internal.ast.NamedDatabasesScope
import org.neo4j.cypher.internal.ast.NamespacedName
import org.neo4j.cypher.internal.ast.NativeAuth
import org.neo4j.cypher.internal.ast.Node
import org.neo4j.cypher.internal.ast.PasswordChange
import org.neo4j.cypher.internal.ast.PatternQualifier
import org.neo4j.cypher.internal.ast.PrivilegeCommand
import org.neo4j.cypher.internal.ast.Relationship
import org.neo4j.cypher.internal.ast.RevokePrivilege
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.UnaliasedReturnItem
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.DecimalDoubleLiteral
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.SensitiveStringLiteral
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.UnarySubtract
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.frontend.prettifier.PrettifierTestSupport.ChangedBetween5And25
import org.neo4j.cypher.internal.frontend.prettifier.PrettifierTestSupport.FailsInCypher25AndLater
import org.neo4j.cypher.internal.frontend.prettifier.PrettifierTestSupport.FailsInCypher5
import org.neo4j.cypher.internal.frontend.prettifier.PrettifierTestSupport.IgnoreInCypher5
import org.neo4j.cypher.internal.frontend.prettifier.PrettifierTestSupport.SameAcrossVersions
import org.neo4j.cypher.internal.frontend.prettifier.PrettifierTestSupport.Test
import org.neo4j.cypher.internal.parser.AstParserFactory
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalactic.source.Position
import org.scalatest.exceptions.TestFailedException

trait AbstractPrettifierTest extends CypherFunSuite {
  def prettifier: Prettifier

  def testPrettifier(prettifierTest: Test)(implicit pos: Position): Unit = prettifierTest match {
    case SameAcrossVersions(inputString, expected) =>
      test(inputString) {
        CypherVersion.values().foreach { version =>
          testPrettifier(version, inputString, expected)
        }
      }
    case ChangedBetween5And25(inputString, expectedCypher5, expectedCypher25AndLater) =>
      test(inputString) {
        CypherVersion.values().foreach { version =>
          if (version isEqualOrAfter CypherVersion.Cypher25) withClue(
            """in Cypher >= 25
              |""".stripMargin
          ) {
            testPrettifier(version, inputString, expectedCypher25AndLater)
          }
          else withClue(
            """in Cypher 5
              |""".stripMargin
          ) {
            testPrettifier(version, inputString, expectedCypher5)
          }
        }
      }
    case IgnoreInCypher5(inputString, expected) =>
      test(inputString) {
        CypherVersion.values().foreach { version =>
          if (version isEqualOrAfter CypherVersion.Cypher25) withClue(
            """in Cypher >= 25
              |""".stripMargin
          ) {
            testPrettifier(version, inputString, expected)
          }
        }
      }
    case FailsInCypher5(inputString, expected) =>
      test(inputString) {
        CypherVersion.values().foreach { version =>
          if (version isEqualOrAfter CypherVersion.Cypher25) withClue(
            """in Cypher >= 25
              |""".stripMargin
          ) {
            testPrettifier(version, inputString, expected)
          }
          else withClue(
            """in Cypher 5
              |""".stripMargin
          ) {
            // Which exception to throw is out of scope for the prettifier ITs, only checking that it throws something.
            assertThrows[Throwable](parseAntlr(version, inputString))
          }
        }
      }
    case FailsInCypher25AndLater(inputString, expected) =>
      test(inputString) {
        CypherVersion.values().foreach { version =>
          if (version isEqualOrAfter CypherVersion.Cypher25) withClue("in Cypher >= 25") {
            // Which exception to throw is out of scope for the prettifier ITs, only checking that it throws something.
            assertThrows[Throwable](parseAntlr(version, inputString))
          }
          else withClue("in Cypher 5") {
            testPrettifier(version, inputString, expected)
          }
        }
      }
  }

  private def testPrettifier(version: CypherVersion, inputString: String, expected: String): Unit = {
    try {
      val statement = parseAntlr(version, inputString)
      val prettified = prettifier.asString(statement)
      withClue(
        s"""Version: $version
           |Query:
           |$inputString
           |Prettified:
           |$prettified
           |Expected:
           |$expected
           |AST:
           |${pprint.apply(statement)}
           |""".stripMargin
      )(prettified shouldBe expected)
    } catch {
      case t: Throwable if !t.isInstanceOf[TestFailedException] =>
        val stackTrace: String = {
          val sw = new java.io.StringWriter()
          val pw = new java.io.PrintWriter(sw)
          t.printStackTrace(pw)
          sw.toString
        }
        withClue(
          s"""Version: $version
             |Query:
             |$inputString
             |
             |Parsing failed with:
             |${t.getMessage}
             |$stackTrace
             |""".stripMargin
        )(fail())
    }

    // roundtrip
    val statement = parseAntlr(version, inputString)
    val prettified = prettifier.asString(statement)
    try {
      val reparsedStatement = parseAntlr(version, prettified)
      val (expected, reparsed) = rewriteASTDifferences(version, statement, reparsedStatement)
      withClue(
        s"""Version: $version
           |Query:
           |$inputString
           |Prettified:
           |$prettified
           |
           |Reparsed AST:
           |${pprint.apply(reparsed)}
           |Expected original AST:
           |${pprint.apply(expected)}
           |""".stripMargin
      )(reparsed shouldBe expected)
    } catch {
      case t: Throwable if !t.isInstanceOf[TestFailedException] =>
        val stackTrace: String = {
          val sw = new java.io.StringWriter()
          val pw = new java.io.PrintWriter(sw)
          t.printStackTrace(pw)
          sw.toString
        }
        withClue(
          s"""Version: $version
             |Query:
             |$inputString
             |Prettified:
             |$prettified
             |Original AST:
             |${pprint.apply(statement)}
             |
             |Reparsing failed with:
             |${t.getMessage}
             |$stackTrace
             |""".stripMargin
        )(fail())
    }
  }

  private def parseAntlr(version: CypherVersion, cypher: String): Statement =
    AstParserFactory(version)(
      cypher,
      Neo4jCypherExceptionFactory(cypher, None),
      None,
      Seq()
    ).singleStatement()

  /**
   * There are some AST changes done at the parser level or the prettifier itself that are expected.
   * This rewriter can be expanded to update those parts.
   */
  def rewriteASTDifferences(
    version: CypherVersion,
    originalStatement: Statement,
    reparsedStatement: Statement
  ): (Statement, Statement) = {
    val fixedAlias = "alias"
    val originalStatementAdjusted = originalStatement.endoRewrite(bottomUp(Rewriter.lift {
      case si @ SensitiveStringLiteral(_) => SensitiveStringLiteral(value = Array(42, 42, 42, 42, 42, 42))(si.position)

      case r @ GraphDirectReference(cn @ CatalogName(parts, true)) if parts.size > 1 =>
        GraphDirectReference(cn.copy(parts = List(parts.mkString("."))))(r.position)

      case da @ CreateLocalDatabaseAlias(n @ NamespacedName(nameComponents, _), _, _, _)
        if nameComponents.size > 1 && version == CypherVersion.Cypher5 =>
        val aliasName = n.copy(nameComponents = List(nameComponents.mkString(".")))(n.position)
        da.copy(aliasName = aliasName)(da.position)

      case s @ NamedDatabasesScope(dbs) if version == CypherVersion.Cypher5 =>
        val databases = dbs.map {
          case n @ NamespacedName(nameComponents, _) if nameComponents.size > 1 =>
            n.copy(nameComponents = List(nameComponents.mkString(".")))(n.position)
          case n => n
        }
        s.copy(databases = databases)(s.position)

      case ri @ UnaliasedReturnItem(_, _) =>
        // we adjust the alias of the originalStatement and the reparsedStatement to a fixed string
        ri.copy(inputText = fixedAlias)(ri.position)

      // normalized property map in PatternQualifier
      case p: PrivilegeCommand =>
        val qualifiers = p.qualifier.map {
          case q @ PatternQualifier(_, variableOpt, MapExpression(items), element) =>
            val variable = variableOpt.getOrElse({
              val variableName = element match {
                case Node         => "n"
                case Relationship => "r"
              }
              Variable(variableName)(InputPosition.NONE, Variable.isIsolatedDefault)
            })
            val expressions = items.map {
              case (key, value) => Equals(Property(variable, key)(InputPosition.NONE), value)(InputPosition.NONE)
            }
            val expression =
              if (expressions.size == 1) expressions.head
              else Ands(expressions)(InputPosition.NONE)
            q.copy(variable = Some(variable), expression = expression)
          case q => q
        }
        p match {
          case p: DenyPrivilege   => p.copy(qualifier = qualifiers)(p.position)
          case p: GrantPrivilege  => p.copy(qualifier = qualifiers)(p.position)
          case p: RevokePrivilege => p.copy(qualifier = qualifiers)(p.position)
        }

      // order NativeAuthAttribute
      case na @ Auth(AdministrationCommand.NATIVE_AUTH, attributes) =>
        val (otherAttributes, passwordChange) = attributes partition {
          case PasswordChange(_) => false
          case _                 => true
        }
        val authAttributes = otherAttributes ++ passwordChange
        na.copy(authAttributes = authAttributes)(na.position)

      // order Auths and default to SET PASSWORD CHANGE REQUIRED
      case u @ CreateUser(_, _, _, externalAuths, nativeAuths, _)
        if externalAuths.size > 1 || nativeAuths.nonEmpty =>
        val nativeAuthsWithDefault = nativeAuths.map {
          case a @ NativeAuth(attributes) if (attributes collectFirst { case PasswordChange(_) => () }).isEmpty =>
            val authAttributes = attributes :+ PasswordChange(requireChange = true)(InputPosition.NONE)
            a.copy(authAttributes = authAttributes)(a.position)
          case a => a
        }
        val oldStyleNativeAuth = nativeAuthsWithDefault.filter(_ => u.usesOldStyleNativeAuth)
        val oldStyleAuth = oldStyleNativeAuth.map { (a: NativeAuth) =>
          Auth(a.provider, a.authAttributes)(a.position)
        }
        val nonOldStyleNativeAuths = nativeAuthsWithDefault.filter(_ => !u.usesOldStyleNativeAuth).toList
        val newStyleAuth = (nonOldStyleNativeAuths ++ externalAuths.sortBy(_.provider)).map {
          case a: NativeAuth   => Auth(a.provider, a.authAttributes)(a.position)
          case a: ExternalAuth => Auth(a.provider, a.authAttributes)(a.position)
        }
        u.copy(newStyleAuth = newStyleAuth, oldStyleAuth = oldStyleAuth)(u.position)

      // order Auths and remove redundant REMOVE AUTHs
      case u @ AlterUser(_, _, _, externalAuths, nativeAuths, removeAuth, _)
        if (externalAuths.size + nativeAuths.toList.size) > 1 || removeAuth.all =>
        val oldStyleNativeAuth = nativeAuths.filter(_ => u.usesOldStyleNativeAuth)
        val oldStyleAuth = oldStyleNativeAuth.map { (a: NativeAuth) =>
          Auth(a.provider, a.authAttributes)(a.position)
        }
        val nonOldStyleNativeAuths = nativeAuths.filter(_ => !u.usesOldStyleNativeAuth).toList
        val newStyleAuth = (nonOldStyleNativeAuths ++ externalAuths.sortBy(_.provider)).map {
          case a: NativeAuth   => Auth(a.provider, a.authAttributes)(a.position)
          case a: ExternalAuth => Auth(a.provider, a.authAttributes)(a.position)
        }
        val newRemoveAuth = if (removeAuth.all) removeAuth.copy(auths = List.empty) else removeAuth
        u.copy(newStyleAuth = newStyleAuth, oldStyleAuth = oldStyleAuth, removeAuth = newRemoveAuth)(u.position)
    }))

    // Rewriter to normalize UnarySubtract applied to signed literals
    // This is needed because the prettifier may output --1 which parses to UnarySubtract(SignedDecimalIntegerLiteral("-1"))
    // but the original AST might have been UnarySubtract(SignedDecimalIntegerLiteral("-1")) from -(-1)
    // Both are semantically equivalent but have different AST representations
    val normalizeUnaryLiterals: Rewriter = bottomUp(Rewriter.lift {
      // UnarySubtract of a negative signed integer literal -> positive signed integer literal
      case UnarySubtract(SignedDecimalIntegerLiteral(stringVal)) if stringVal.startsWith("-") =>
        SignedDecimalIntegerLiteral(stringVal.substring(1))(InputPosition.NONE)
      // UnarySubtract of a positive signed integer literal -> negative signed integer literal
      case UnarySubtract(SignedDecimalIntegerLiteral(stringVal)) if !stringVal.startsWith("-") =>
        SignedDecimalIntegerLiteral("-" + stringVal)(InputPosition.NONE)
      // UnarySubtract of a negative double literal -> positive double literal
      case UnarySubtract(DecimalDoubleLiteral(stringVal)) if stringVal.startsWith("-") =>
        DecimalDoubleLiteral(stringVal.substring(1))(InputPosition.NONE)
      // UnarySubtract of a positive double literal -> negative double literal
      case UnarySubtract(DecimalDoubleLiteral(stringVal)) if !stringVal.startsWith("-") =>
        DecimalDoubleLiteral("-" + stringVal)(InputPosition.NONE)
    })

    val reparsedStatementAdjusted = reparsedStatement.endoRewrite(bottomUp(Rewriter.lift {
      case uri @ UnaliasedReturnItem(_, _) =>
        // we adjust the alias of the originalStatement and the reparsedStatement to a fixed string
        uri.copy(inputText = fixedAlias)(uri.position)
    })).endoRewrite(normalizeUnaryLiterals)

    val originalStatementNormalized = originalStatementAdjusted.endoRewrite(normalizeUnaryLiterals)

    (originalStatementNormalized, reparsedStatementAdjusted)
  }
}

object PrettifierTestSupport {

  sealed trait Test {
    def testPosition: Position
  }

  case class SameAcrossVersions(
    inputString: String,
    output: String
  )(implicit override val testPosition: Position) extends Test

  case class ChangedBetween5And25(
    inputString: String,
    outputCypher5: String,
    outputCypher25AndLater: String
  )(implicit override val testPosition: Position) extends Test

  case class IgnoreInCypher5(
    inputString: String,
    output: String
  )(implicit override val testPosition: Position) extends Test

  case class FailsInCypher5(
    inputString: String,
    output: String
  )(implicit override val testPosition: Position) extends Test

  case class FailsInCypher25AndLater(
    inputString: String,
    output: String
  )(implicit override val testPosition: Position) extends Test

  implicit class Tuple2TestConverter(tuple: (String, String))(implicit testPosition: Position)
      extends SameAcrossVersions(tuple._1, tuple._2)
}
