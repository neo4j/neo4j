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
package org.neo4j.cypher.internal.ast.factory.ddl

import org.neo4j.cypher.internal.ast.AlterAuthRule
import org.neo4j.cypher.internal.ast.AuthRuleCondition
import org.neo4j.cypher.internal.ast.AuthRuleEnabled
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5
import org.neo4j.cypher.internal.expressions.Add
import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.NotEquals
import org.neo4j.cypher.internal.expressions.Or
import org.neo4j.cypher.internal.util.FunctionName
import org.neo4j.cypher.internal.util.Namespace
import org.neo4j.cypher.internal.util.symbols.AnyType

class AlterAuthRuleAdministrationCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {

  override protected def ignorePrettifier: Boolean = true

  private val validExpressions = Seq(
    Equals(literalInt(1), literalInt(1))(pos),
    NotEquals(
      literalInt(1),
      ExplicitParameter("param", AnyType(isNullable = true)(pos))(pos)
    )(pos),
    Equals(
      FunctionInvocation(
        FunctionName(Namespace(List("abac", "oidc"))(pos), "user_attribute")(pos),
        literalString("country")
      )(pos),
      literalString("SE")
    )(pos),
    Equals(
      FunctionInvocation(
        FunctionName(Namespace(List("abac", "oidc"))(pos), "user_attribute")(pos),
        Add(literalString("hello"), literalInt(1))(pos)
      )(pos),
      literalString("SE")
    )(pos),
    Equals(
      FunctionInvocation(
        FunctionName(Namespace(List("abac", "oidc"))(pos), "user_attribute")(pos),
        Add(literalInt(1), literalInt(1))(pos)
      )(pos),
      literalString("SE")
    )(pos),
    Equals(
      FunctionInvocation(
        FunctionName(Namespace(List("unknown"))(pos), "function")(pos),
        literalString("HELLO")
      )(pos),
      literalString("SE")
    )(pos),
    NotEquals(
      FunctionInvocation(
        FunctionName(Namespace(List("graph"))(pos), "byName")(pos),
        literalString("HELLO")
      )(pos),
      literalString("SE")
    )(pos),
    Equals(
      FunctionInvocation(
        FunctionName(Namespace(List("abac", "oidc"))(pos), "user_attribute")(pos),
        FunctionInvocation(
          FunctionName(Namespace(List())(pos), "toLower")(pos),
          literalString("HELLO")
        )(pos)
      )(pos),
      literalString("SE")
    )(pos),
    Equals(
      FunctionInvocation(
        FunctionName(Namespace(List("abac", "oidc"))(pos), "user_attribute")(pos),
        listOfString("country", "city")
      )(pos),
      literalString("SE_MALMÖ")
    )(pos),
    NotEquals(
      FunctionInvocation(
        FunctionName(Namespace(List("abac", "oidc"))(pos), "user_attribute")(pos),
        literalInt(1)
      )(pos),
      literalString("SE")
    )(pos),
    NotEquals(
      FunctionInvocation(
        FunctionName(Namespace(List("abac", "oidc"))(pos), "user_attribute")(pos),
        ExplicitParameter("param", AnyType(isNullable = true)(pos))(pos)
      )(pos),
      MapExpression(Seq(
        (propName("key"), listOfString("value1", "value2")),
        (propName("key2"), literalInt(1))
      ))(pos)
    )(pos)
  )

  private val complexValidAndOrExpression =
    validExpressions.tail.foldLeft[Expression](validExpressions.head)((acc, expression) =>
      Or(And(acc, expression)(pos), expression)(pos)
    )

  (validExpressions :+ complexValidAndOrExpression)
    .map(e => (ExpressionStringifier.apply().apply(e), AuthRuleCondition(e)(pos)))
    .foreach { case (exprString, authRuleCondition) =>
      test(s"ALTER AUTH RULE foo SET CONDITION $exprString") {
        parsesIn[Statement] {
          case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'AUTH'")
          case _ => _.toAst(AlterAuthRule(
              literalFoo,
              ifExists = false,
              List(authRuleCondition)
            )(pos))
        }
      }

      test(s"ALTER AUTH RULE foo IF EXISTS SET CONDITION $exprString") {
        parsesIn[Statement] {
          case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'AUTH'")
          case _ => _.toAst(AlterAuthRule(
              literalFoo,
              ifExists = true,
              List(authRuleCondition)
            )(pos))
        }
      }

      test(s"ALTER AUTH RULE $$foo SET CONDITION $exprString") {
        parsesIn[Statement] {
          case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'AUTH'")
          case _ => _.toAst(AlterAuthRule(
              paramFoo,
              ifExists = false,
              List(authRuleCondition)
            )(pos))
        }
      }

      test(s"ALTER AUTH RULE $$foo IF EXISTS SET CONDITION $exprString") {
        parsesIn[Statement] {
          case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'AUTH'")
          case _ => _.toAst(AlterAuthRule(
              paramFoo,
              ifExists = true,
              List(authRuleCondition)
            )(pos))
        }
      }

      test(s"ALTER AUTH RULE foo SET CONDITION $exprString SET ENABLED FALSE") {
        parsesIn[Statement] {
          case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'AUTH'")
          case _ => _.toAst(AlterAuthRule(
              literalFoo,
              ifExists = false,
              List(
                authRuleCondition,
                AuthRuleEnabled(enabled = false)(pos)
              )
            )(pos))
        }
      }

      test(s"ALTER AUTH RULE foo IF EXISTS SET CONDITION $exprString SET ENABLED FALSE") {
        parsesIn[Statement] {
          case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'AUTH'")
          case _ => _.toAst(AlterAuthRule(
              literalFoo,
              ifExists = true,
              List(
                authRuleCondition,
                AuthRuleEnabled(enabled = false)(pos)
              )
            )(pos))
        }
      }

      test(s"ALTER AUTH RULE $$foo SET CONDITION $exprString SET ENABLED TRUE") {
        parsesIn[Statement] {
          case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'AUTH'")
          case _ => _.toAst(AlterAuthRule(
              paramFoo,
              ifExists = false,
              List(
                authRuleCondition,
                AuthRuleEnabled(enabled = true)(pos)
              )
            )(pos))
        }
      }

      test(s"ALTER AUTH RULE $$foo IF EXISTS SET CONDITION $exprString SET ENABLED TRUE") {
        parsesIn[Statement] {
          case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'AUTH'")
          case _ => _.toAst(AlterAuthRule(
              paramFoo,
              ifExists = true,
              List(
                authRuleCondition,
                AuthRuleEnabled(enabled = true)(pos)
              )
            )(pos))
        }
      }
    }

  Seq("date", "datetime", "localtime", "localdatetime", "time")
    .map(FunctionName(_)(pos))
    .foreach(functionName => {
      val dateFunctionInvocation = FunctionInvocation(
        functionName,
        distinct = false,
        IndexedSeq(literalString("2024-11-18"))
      )(pos)
      test(
        s"ALTER AUTH RULE foo SET CONDITION abac.oidc.user_attribute('start_date') > ${dateFunctionInvocation.name}('2024-11-18')"
      ) {
        parsesIn[Statement] {
          case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'AUTH'")
          case _ => _.toAst(AlterAuthRule(
              literalFoo,
              ifExists = false,
              List(AuthRuleCondition(GreaterThan(
                FunctionInvocation(
                  FunctionName(Namespace(List("abac", "oidc"))(pos), "user_attribute")(pos),
                  literalString("start_date")
                )(pos),
                dateFunctionInvocation
              )(pos))(pos))
            )(pos))
        }
      }

      val dateFunctionInvocationWithoutArguments = FunctionInvocation(
        functionName,
        distinct = false,
        IndexedSeq.empty
      )(pos)

      // Should fail semantic checking
      test(
        s"ALTER AUTH RULE foo SET CONDITION abac.oidc.user_attribute('start_date') > ${dateFunctionInvocationWithoutArguments.name}()"
      ) {
        parsesIn[Statement] {
          case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'AUTH'")
          case _ => _.toAst(AlterAuthRule(
              literalFoo,
              ifExists = false,
              List(AuthRuleCondition(GreaterThan(
                FunctionInvocation(
                  FunctionName(Namespace(List("abac", "oidc"))(pos), "user_attribute")(pos),
                  literalString("start_date")
                )(pos),
                dateFunctionInvocationWithoutArguments
              )(pos))(pos))
            )(pos))
        }
      }
    })

  test("ALTER AUTH RULE foo SET ENABLED true") {
    parsesIn[Statement] {
      case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'AUTH'")
      case _ => _.toAst(AlterAuthRule(
          literalFoo,
          ifExists = false,
          List(AuthRuleEnabled(enabled = true)(pos))
        )(pos))
    }
  }

  test("ALTER AUTH RULE $foo SET ENABLED false") {
    parsesIn[Statement] {
      case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'AUTH'")
      case _ => _.toAst(AlterAuthRule(
          paramFoo,
          ifExists = false,
          List(AuthRuleEnabled(enabled = false)(pos))
        )(pos))
    }
  }

  test("ALTER AUTH RULE foo IF EXISTS SET ENABLED false") {
    parsesIn[Statement] {
      case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'AUTH'")
      case _ => _.toAst(AlterAuthRule(
          literalFoo,
          ifExists = true,
          List(AuthRuleEnabled(enabled = false)(pos))
        )(pos))
    }
  }

  test("ALTER AUTH RULE $foo IF EXISTS SET ENABLED true") {
    parsesIn[Statement] {
      case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'AUTH'")
      case _ => _.toAst(AlterAuthRule(
          paramFoo,
          ifExists = true,
          List(AuthRuleEnabled(enabled = true)(pos))
        )(pos))
    }
  }

  test(s"ALTER AUTH RULE foo SET ENABLED FALSE SET CONDITION true") {
    parsesIn[Statement] {
      case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'AUTH'")
      case _ => _.toAst(AlterAuthRule(
          literalFoo,
          ifExists = false,
          List(
            AuthRuleCondition(literalBoolean(true))(pos),
            AuthRuleEnabled(enabled = false)(pos)
          )
        )(pos))
    }
  }

  // failure scenarios

  test("ALTER AUTH RULE foo") {
    parsesIn[Statement] {
      case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'AUTH'")
      case _ => _.withSyntaxError("""Invalid input '': expected 'SET' (line 1, column 20 (offset: 19))
                                    |"ALTER AUTH RULE foo"
                                    |                    ^""".stripMargin)
    }
  }

  test("ALTER AUTH RULE foo IF EXISTS") {
    parsesIn[Statement] {
      case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'AUTH'")
      case _ => _.withSyntaxError("""Invalid input '': expected 'SET' (line 1, column 30 (offset: 29))
                                    |"ALTER AUTH RULE foo IF EXISTS"
                                    |                              ^""".stripMargin)
    }
  }

  test("ALTER AUTH RULE foo SET ENABLED $param") {
    parsesIn[Statement] {
      case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'AUTH'")
      case _ => _.withSyntaxError("""Invalid input '$': expected 'FALSE' or 'TRUE' (line 1, column 33 (offset: 32))
                                    |"ALTER AUTH RULE foo SET ENABLED $param"
                                    |                                 ^""".stripMargin)
    }
  }

  test("ALTER AUTH RULE foo SET ENABLED true SET ENABLED false") {
    parsesIn[Statement] {
      case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'AUTH'")
      case _ => _.withSyntaxError("""Duplicate SET ENABLED clause (line 1, column 38 (offset: 37))
                                    |"ALTER AUTH RULE foo SET ENABLED true SET ENABLED false"
                                    |                                      ^""".stripMargin)
    }
  }

  test("ALTER AUTH RULE foo SET CONDITION 1 = 1 SET CONDITION 2 = 2") {
    parsesIn[Statement] {
      case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'AUTH'")
      case _ => _.withSyntaxError("""Duplicate SET CONDITION clause (line 1, column 41 (offset: 40))
                                    |"ALTER AUTH RULE foo SET CONDITION 1 = 1 SET CONDITION 2 = 2"
                                    |                                         ^""".stripMargin)
    }
  }

  test("ALTER AUTH RULE foo SET CONDITION 1 = 1 SET CONDITION 2 = 2 SET CONDITION 3 = 3") {
    parsesIn[Statement] {
      case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'AUTH'")
      case _ => _.withSyntaxError("""Duplicate SET CONDITION clause (line 1, column 41 (offset: 40))
                                    |"ALTER AUTH RULE foo SET CONDITION 1 = 1 SET CONDITION 2 = 2 SET CONDITION 3 = 3"
                                    |                                         ^""".stripMargin)
    }
  }

  test("ALTER AUTH RULE foo IF EXISTS SET CONDITION 1 = 1 SET CONDITION 1 = 1") {
    parsesIn[Statement] {
      case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'AUTH'")
      case _ => _.withSyntaxError("""Duplicate SET CONDITION clause (line 1, column 51 (offset: 50))
                                    |"ALTER AUTH RULE foo IF EXISTS SET CONDITION 1 = 1 SET CONDITION 1 = 1"
                                    |                                                   ^""".stripMargin)
    }
  }

  test("ALTER AUTH RULE foo SET ENABLED false SET CONDITION 1 = 1 SET ENABLED false") {
    parsesIn[Statement] {
      case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'AUTH'")
      case _ => _.withSyntaxError("""Duplicate SET ENABLED clause (line 1, column 59 (offset: 58))
                                    |"ALTER AUTH RULE foo SET ENABLED false SET CONDITION 1 = 1 SET ENABLED false"
                                    |                                                           ^""".stripMargin)
    }
  }

  test("ALTER AUTH RULE foo IF EXISTS SET ENABLED false SET CONDITION 1 = 1 SET ENABLED true") {
    parsesIn[Statement] {
      case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'AUTH'")
      case _ =>
        _.withSyntaxError("""Duplicate SET ENABLED clause (line 1, column 69 (offset: 68))
                            |"ALTER AUTH RULE foo IF EXISTS SET ENABLED false SET CONDITION 1 = 1 SET ENABLED true"
                            |                                                                     ^""".stripMargin)
    }
  }

  test("""ALTER AUTH RULE foo SET ENABLED "TRUE"""") {
    parsesIn[Statement] {
      case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'AUTH'")
      case _ => _.withSyntaxError("""Invalid input '"TRUE"': expected 'FALSE' or 'TRUE' (line 1, column 33 (offset: 32))
                                    |"ALTER AUTH RULE foo SET ENABLED "TRUE""
                                    |                                 ^""".stripMargin)
    }
  }

  test("ALTER AUTH RULE foo SET CONDITION 1 > ") {
    parsesIn[Statement] {
      case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'AUTH'")
      case _ => _.withSyntaxError("""Invalid input '': expected an expression (line 1, column 38 (offset: 37))
                                    |"ALTER AUTH RULE foo SET CONDITION 1 >"
                                    |                                      ^""".stripMargin)
    }
  }

  // pass, should fail in semantic checking

  test("ALTER AUTH RULE foo SET CONDITION $param") {
    parsesIn[Statement] {
      case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'AUTH'")
      case _ => _.toAst(AlterAuthRule(
          literalFoo,
          ifExists = false,
          List(
            AuthRuleCondition(
              ExplicitParameter("param", AnyType(isNullable = true)(pos))(pos)
            )(pos)
          )
        )(pos))
    }
  }
}
