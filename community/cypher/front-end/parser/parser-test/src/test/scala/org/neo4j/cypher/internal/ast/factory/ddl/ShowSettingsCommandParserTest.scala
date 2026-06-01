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

import org.neo4j.cypher.internal.ast.CommaSeparatedNames
import org.neo4j.cypher.internal.ast.ExpressionNames
import org.neo4j.cypher.internal.ast.NoNames
import org.neo4j.cypher.internal.ast.OrderBy
import org.neo4j.cypher.internal.ast.ShowSettingsClause
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5
import org.neo4j.cypher.internal.expressions.AllIterablePredicate
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.IntegerType

class ShowSettingsCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {

  Seq("SETTING", "SETTINGS").foreach { settingKeyword =>
    test(s"SHOW $settingKeyword") {
      assertAst(
        singleQuery(ShowSettingsClause(NoNames, None, List.empty, yieldAll = false, None)(defaultPos))
      )
    }

    test(s"SHOW $settingKeyword 'foo'") {
      assertAst(
        singleQuery(
          ShowSettingsClause(
            ExpressionNames(literalString("foo")),
            None,
            List.empty,
            yieldAll = false,
            None
          )(defaultPos)
        )
      )
    }

    test(s"SHOW $settingKeyword ''") {
      assertAst(
        singleQuery(ShowSettingsClause(
          ExpressionNames(literalString("")),
          None,
          List.empty,
          yieldAll = false,
          None
        )(defaultPos))
      )
    }

    test(s"SHOW $settingKeyword null") {
      assertAst(
        singleQuery(ShowSettingsClause(
          ExpressionNames(nullLiteral),
          None,
          List.empty,
          yieldAll = false,
          None
        )(defaultPos))
      )
    }

    test(s"SHOW $settingKeyword $$param") {
      assertAst(
        singleQuery(
          ShowSettingsClause(
            ExpressionNames(parameter("param", CTAny)),
            None,
            List.empty,
            yieldAll = false,
            None
          )(defaultPos)
        )
      )
    }

    test(s"SHOW $settingKeyword 'foo', 'bar'") {
      assertAst(
        singleQuery(ShowSettingsClause(
          CommaSeparatedNames(listOfString("foo", "bar")),
          None,
          List.empty,
          yieldAll = false,
          None
        )(defaultPos)),
        obfuscator = false
      )
    }

    test(s"SHOW $settingKeyword 'foo'+'.'+$$name") {
      assertAst(singleQuery(ShowSettingsClause(
        ExpressionNames(
          add(add(literalString("foo"), literalString(".")), parameter("name", CTAny))
        ),
        None,
        List.empty,
        yieldAll = false,
        None
      )(defaultPos)))
    }

    test(s"SHOW $settingKeyword ['foo', 'bar']") {
      assertAst(singleQuery(ShowSettingsClause(
        ExpressionNames(listOfString("foo", "bar")),
        None,
        List.empty,
        yieldAll = false,
        None
      )(defaultPos)))
    }

    test(s"USE db SHOW $settingKeyword") {
      assertAstVersionBased(cypher5 =>
        SingleQuery(
          List(
            use(List("db"), !cypher5),
            ShowSettingsClause(NoNames, None, List.empty, yieldAll = false, None)((1, 8, 7))
          )
        )((1, 8, 7))
      )
    }

  }

  // Filtering tests

  test("SHOW SETTING WHERE name = 'db.setting.sub_setting'") {
    assertAst(singleQuery(ShowSettingsClause(
      NoNames,
      Some(where(
        equals(
          varFor("name"),
          literalString("db.setting.sub_setting")
        )
      )),
      List.empty,
      yieldAll = false,
      None
    )(defaultPos)))
  }

  test("SHOW SETTING WHERE name IN ['db.setting.sub_setting', 'db.another.setting']") {
    assertAst(singleQuery(ShowSettingsClause(
      NoNames,
      Some(where(
        in(
          varFor("name", (1, 20, 19)),
          listOfString(
            "db.setting.sub_setting",
            "db.another.setting"
          )
        )
      )),
      List.empty,
      yieldAll = false,
      None
    )(defaultPos)))
  }

  test("SHOW SETTING WHERE name = $name") {
    assertAst(singleQuery(ShowSettingsClause(
      NoNames,
      Some(where(
        equals(
          varFor("name"),
          parameter("name", CTAny)
        )
      )),
      List.empty,
      yieldAll = false,
      None
    )(defaultPos)))
  }

  test("SHOW SETTINGS WHERE (`name`) = ($`s`)") {
    def expected(nameIsEscaped: Boolean) =
      singleQuery(ShowSettingsClause(
        NoNames,
        Some(where(
          equals(
            varFor("name", nameIsEscaped),
            parameter("s", CTAny)
          )
        )),
        List.empty,
        yieldAll = false,
        None
      )(defaultPos))
    parsesIn[Statement] {
      case Cypher5 => _.toAst(expected(nameIsEscaped = true))
      case _       => _.toAst(expected(nameIsEscaped = false))
    }
  }

  test("SHOW SETTING WHERE name IN $list") {
    assertAst(singleQuery(ShowSettingsClause(
      NoNames,
      Some(where(
        in(
          varFor("name"),
          parameter("list", CTAny)
        )
      )),
      List.empty,
      yieldAll = false,
      None
    )(defaultPos)))
  }

  test("SHOW SETTING 'foo' WHERE isDynamic") {
    assertAst(singleQuery(ShowSettingsClause(
      ExpressionNames(literalString("foo")),
      Some(where(varFor("isDynamic"))),
      List.empty,
      yieldAll = false,
      None
    )(defaultPos)))
  }

  test("SHOW SETTING 'foo', 'bar' WHERE isDynamic") {
    assertAst(
      singleQuery(ShowSettingsClause(
        CommaSeparatedNames(listOfString("foo", "bar")),
        Some(where(varFor("isDynamic"))),
        List.empty,
        yieldAll = false,
        None
      )(defaultPos)),
      obfuscator = false
    )
  }

  test("SHOW SETTING $foo WHERE pp < 50.0") {
    assertAst(singleQuery(ShowSettingsClause(
      ExpressionNames(parameter("foo", CTAny)),
      Some(where(lessThan(varFor("pp"), literalFloat(50.0)))),
      List.empty,
      yieldAll = false,
      None
    )(defaultPos)))
  }

  test("SHOW SETTINGS YIELD description") {
    assertAst(singleQuery(
      ShowSettingsClause(
        NoNames,
        None,
        List(commandResultItem("description")),
        yieldAll = false,
        Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("description"))))
      )(defaultPos)
    ))
  }

  test("SHOW SETTINGS YIELD * ORDER BY name SKIP 2 LIMIT 5") {
    assertAst(singleQuery(
      ShowSettingsClause(
        NoNames,
        None,
        List.empty,
        yieldAll = true,
        Some(withFromYield(
          returnAllItems((1, 23, 22)),
          Some(OrderBy(Seq(
            sortItem(varFor("name"))
          ))((1, 23, 22))),
          Some(skip(2)),
          Some(limit(5))
        ))
      )(defaultPos)
    ))
  }

  test("SHOW SETTINGS YIELD * ORDER BY name OFFSET 2 LIMIT 5") {
    assertAst(singleQuery(
      ShowSettingsClause(
        NoNames,
        None,
        List.empty,
        yieldAll = true,
        Some(withFromYield(
          returnAllItems((1, 23, 22)),
          Some(OrderBy(Seq(
            sortItem(varFor("name"))
          ))((1, 23, 22))),
          Some(skip(2)),
          Some(limit(5))
        ))
      )(defaultPos)
    ))
  }

  test("SHOW SETTING YIELD name, description, value WHERE name = 'db.setting.sub_setting'") {
    assertAst(singleQuery(
      ShowSettingsClause(
        NoNames,
        None,
        List(
          commandResultItem("name"),
          commandResultItem("description"),
          commandResultItem("value")
        ),
        yieldAll = false,
        Some(withFromYield(
          returnAllItems.withDefaultOrderOnColumns(List("name", "description", "value")),
          where = Some(where(
            equals(
              varFor("name"),
              literalString("db.setting.sub_setting")
            )
          ))
        ))
      )((1, 1, 0))
    ))
  }

  test("USE db SHOW SETTINGS YIELD name, description AS pp WHERE pp < 50.0 RETURN name") {
    assertAstVersionBased(
      cypher5 =>
        singleQuery(
          use(List("db"), !cypher5),
          ShowSettingsClause(
            NoNames,
            None,
            List(commandResultItem("name"), commandResultItem("description", Some("pp"))),
            yieldAll = false,
            Some(
              withFromYield(
                returnAllItems.withDefaultOrderOnColumns(List("name", "pp")),
                where = Some(where(lessThan(varFor("pp"), literalFloat(50.0))))
              )
            )
          )(pos),
          return_(variableReturnItem("name"))
        ),
      comparePosition = false
    )
  }

  test("SHOW SETTINGS YIELD name AS SETTING, mode AS OUTPUT") {
    assertAst(
      singleQuery(
        ShowSettingsClause(
          NoNames,
          None,
          List(commandResultItem("name", Some("SETTING")), commandResultItem("mode", Some("OUTPUT"))),
          yieldAll = false,
          Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("SETTING", "OUTPUT"))))
        )(pos)
      ),
      comparePosition = false
    )
  }

  test("SHOW SETTINGS 'db.setting.sub_setting' YIELD description") {
    assertAst(singleQuery(
      ShowSettingsClause(
        ExpressionNames(literalString("db.setting.sub_setting")),
        None,
        List(commandResultItem("description")),
        yieldAll = false,
        Some(withFromYield(
          returnAllItems.withDefaultOrderOnColumns(List("description"))
        ))
      )(defaultPos)
    ))
  }

  test("SHOW SETTINGS 'db.setting.sub_setting', 'db.another.setting' YIELD description") {
    assertAst(
      singleQuery(
        ShowSettingsClause(
          CommaSeparatedNames(listOfString("db.setting.sub_setting", "db.another.setting")),
          None,
          List(commandResultItem("description")),
          yieldAll = false,
          Some(withFromYield(
            returnAllItems.withDefaultOrderOnColumns(List("description"))
          ))
        )(defaultPos)
      ),
      obfuscator = false
    )
  }

  test("SHOW SETTINGS $list YIELD description") {
    assertAst(singleQuery(
      ShowSettingsClause(
        ExpressionNames(parameter("list", CTAny)),
        None,
        List(commandResultItem("description")),
        yieldAll = false,
        Some(withFromYield(
          returnAllItems.withDefaultOrderOnColumns(List("description"))
        ))
      )(defaultPos)
    ))
  }

  test("SHOW SETTINGS $list YIELD name, description, isExplicitlySet WHERE isExplicitlySet") {
    assertAst(singleQuery(
      ShowSettingsClause(
        ExpressionNames(parameter("list", CTAny)),
        None,
        List(
          commandResultItem("name"),
          commandResultItem("description"),
          commandResultItem("isExplicitlySet")
        ),
        yieldAll = false,
        Some(withFromYield(
          returnAllItems.withDefaultOrderOnColumns(List("name", "description", "isExplicitlySet")),
          where = Some(where(varFor("isExplicitlySet")))
        ))
      )(defaultPos)
    ))
  }

  test("SHOW SETTINGS YIELD (123 + xyz)") {
    assertAst(singleQuery(
      ShowSettingsClause(
        ExpressionNames(function("YIELD", add(literalInt(123), varFor("xyz")))),
        None,
        List.empty,
        yieldAll = false,
        None
      )(pos)
    ))
  }

  test("SHOW SETTINGS YIELD") {
    assertAst(singleQuery(
      ShowSettingsClause(ExpressionNames(varFor("YIELD")), None, List.empty, yieldAll = false, None)(pos)
    ))
  }

  test("SHOW SETTINGS YIELD a ORDER BY a WHERE a = 1") {
    assertAst(singleQuery(
      ShowSettingsClause(
        NoNames,
        None,
        List(commandResultItem("a")),
        yieldAll = false,
        Some(withFromYield(
          returnAllItems.withDefaultOrderOnColumns(List("a")),
          Some(orderBy(sortItem(varFor("a")))),
          where = Some(where(equals(varFor("a"), literalInt(1))))
        ))
      )(pos)
    ))
  }

  test("SHOW SETTINGS YIELD a AS b ORDER BY b WHERE b = 1") {
    assertAst(singleQuery(
      ShowSettingsClause(
        NoNames,
        None,
        List(commandResultItem("a", Some("b"))),
        yieldAll = false,
        Some(withFromYield(
          returnAllItems.withDefaultOrderOnColumns(List("b")),
          Some(orderBy(sortItem(varFor("b")))),
          where = Some(where(equals(varFor("b"), literalInt(1))))
        ))
      )(pos)
    ))
  }

  test("SHOW SETTINGS YIELD a AS b ORDER BY a WHERE a = 1") {
    assertAst(singleQuery(
      ShowSettingsClause(
        NoNames,
        None,
        List(commandResultItem("a", Some("b"))),
        yieldAll = false,
        Some(withFromYield(
          returnAllItems.withDefaultOrderOnColumns(List("b")),
          Some(orderBy(sortItem(varFor("b")))),
          where = Some(where(equals(varFor("b"), literalInt(1))))
        ))
      )(pos)
    ))
  }

  test("SHOW SETTINGS YIELD a ORDER BY EXISTS { (a) } WHERE EXISTS { (a) }") {
    assertAst(singleQuery(
      ShowSettingsClause(
        NoNames,
        None,
        List(commandResultItem("a")),
        yieldAll = false,
        Some(withFromYield(
          returnAllItems.withDefaultOrderOnColumns(List("a")),
          Some(orderBy(sortItem(simpleExistsExpression(patternForMatch(nodePat(Some("a"))), None)))),
          where = Some(where(simpleExistsExpression(patternForMatch(nodePat(Some("a"))), None)))
        ))
      )(pos)
    ))
  }

  test("SHOW SETTINGS YIELD a ORDER BY EXISTS { (b) } WHERE EXISTS { (b) }") {
    assertAst(singleQuery(
      ShowSettingsClause(
        NoNames,
        None,
        List(commandResultItem("a")),
        yieldAll = false,
        Some(withFromYield(
          returnAllItems.withDefaultOrderOnColumns(List("a")),
          Some(orderBy(sortItem(simpleExistsExpression(patternForMatch(nodePat(Some("b"))), None)))),
          where = Some(where(simpleExistsExpression(patternForMatch(nodePat(Some("b"))), None)))
        ))
      )(pos)
    ))
  }

  test("SHOW SETTINGS YIELD a AS b ORDER BY COUNT { (b) } WHERE EXISTS { (b) }") {
    assertAst(singleQuery(
      ShowSettingsClause(
        NoNames,
        None,
        List(commandResultItem("a", Some("b"))),
        yieldAll = false,
        Some(withFromYield(
          returnAllItems.withDefaultOrderOnColumns(List("b")),
          Some(orderBy(sortItem(simpleCountExpression(patternForMatch(nodePat(Some("b"))), None)))),
          where = Some(where(simpleExistsExpression(patternForMatch(nodePat(Some("b"))), None)))
        ))
      )(pos)
    ))
  }

  test("SHOW SETTINGS YIELD a AS b ORDER BY EXISTS { (a) } WHERE COLLECT { MATCH (a) RETURN a } <> []") {
    assertAst(singleQuery(
      ShowSettingsClause(
        NoNames,
        None,
        List(commandResultItem("a", Some("b"))),
        yieldAll = false,
        Some(withFromYield(
          returnAllItems.withDefaultOrderOnColumns(List("b")),
          Some(orderBy(sortItem(simpleExistsExpression(patternForMatch(nodePat(Some("b"))), None)))),
          where = Some(where(notEquals(
            simpleCollectExpression(patternForMatch(nodePat(Some("b"))), None, return_(returnItem(varFor("b"), "a"))),
            listOf()
          )))
        ))
      )(pos)
    ))
  }

  test("SHOW SETTINGS YIELD a AS b ORDER BY b + COUNT { () } WHERE b OR EXISTS { () }") {
    assertAst(singleQuery(
      ShowSettingsClause(
        NoNames,
        None,
        List(commandResultItem("a", Some("b"))),
        yieldAll = false,
        Some(withFromYield(
          returnAllItems.withDefaultOrderOnColumns(List("b")),
          Some(orderBy(sortItem(add(varFor("b"), simpleCountExpression(patternForMatch(nodePat()), None))))),
          where = Some(where(or(varFor("b"), simpleExistsExpression(patternForMatch(nodePat()), None))))
        ))
      )(pos)
    ))
  }

  test("SHOW SETTINGS YIELD a AS b ORDER BY a + EXISTS { () } WHERE a OR ALL (x IN [1, 2] WHERE x IS :: INT)") {
    assertAst(singleQuery(
      ShowSettingsClause(
        NoNames,
        None,
        List(commandResultItem("a", Some("b"))),
        yieldAll = false,
        Some(withFromYield(
          returnAllItems.withDefaultOrderOnColumns(List("b")),
          Some(orderBy(sortItem(add(varFor("b"), simpleExistsExpression(patternForMatch(nodePat()), None))))),
          where = Some(where(or(
            varFor("b"),
            AllIterablePredicate(
              varFor("x"),
              listOfInt(1, 2),
              Some(isTyped(varFor("x"), IntegerType(isNullable = true)(pos)))
            )(pos)
          )))
        ))
      )(pos)
    ))
  }

  test("SHOW SETTINGS YIELD name as value, value as name where size(value) > 0 RETURN value as name") {
    assertAst(singleQuery(
      ShowSettingsClause(
        NoNames,
        None,
        List(
          commandResultItem("name", Some("value")),
          commandResultItem("value", Some("name"))
        ),
        yieldAll = false,
        Some(withFromYield(
          returnAllItems.withDefaultOrderOnColumns(List("value", "name")),
          where = Some(where(
            greaterThan(size(varFor("value")), literalInt(0))
          ))
        ))
      )(pos),
      return_(aliasedReturnItem("value", "name"))
    ))
  }

  test(
    "SHOW SETTINGS YIELD name RETURN name ORDER BY name"
  ) {
    assertAst(
      singleQuery(
        ShowSettingsClause(
          NoNames,
          None,
          List(commandResultItem("name")),
          yieldAll = false,
          Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("name"))))
        )(pos),
        return_(orderBy(sortItem(varFor("name"))), variableReturnItem("name"))
      ),
      comparePosition = false
    )
  }

  test("SHOW SETTINGS WHERE name = 'db.setting' RETURN *") {
    parsesIn[Statements] {
      case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'RETURN'")
      case _ => _.toAstPositioned(singleQuery(
          ShowSettingsClause(
            NoNames,
            Some(where(
              equals(
                varFor("name"),
                literalString("db.setting")
              )
            )),
            List.empty,
            yieldAll = false,
            None
          )(defaultPos),
          returnAll
        ))
    }
  }

  test("SHOW SETTINGS WHERE true RETURN *") {
    parsesIn[Statements] {
      case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'RETURN'")
      case _ => _.toAstPositioned(singleQuery(
          ShowSettingsClause(
            NoNames,
            Some(where(trueLiteral)),
            List.empty,
            yieldAll = false,
            None
          )(defaultPos),
          returnAll
        ))
    }
  }

  test("SHOW SETTINGS RETURN *") {
    parsesIn[Statements] {
      case Cypher5 =>
        // parses `RETURN` as setting name
        _.withSyntaxErrorContaining("Invalid input '': expected an expression")
      case _ => _.toAstPositioned(singleQuery(
          ShowSettingsClause(
            NoNames,
            None,
            List.empty,
            yieldAll = false,
            None
          )(defaultPos),
          returnAll
        ))
    }
  }

  // Negative tests

  test("SHOW ALL SETTINGS") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input 'SETTINGS': expected 'CONSTRAINT', 'CONSTRAINTS', 'FUNCTION', 'FUNCTIONS', 'INDEX', 'INDEXES', 'PRIVILEGE', 'PRIVILEGES', 'ROLE' or 'ROLES' (line 1, column 10 (offset: 9))
        |"SHOW ALL SETTINGS"
        |          ^""".stripMargin
    )
  }

  test("SHOW SETTING $foo, $bar") {
    failsParsing[Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input ',': expected an expression, 'WHERE', 'YIELD' or <EOF> (line 1, column 18 (offset: 17))
            |"SHOW SETTING $foo, $bar"
            |                  ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input ',': expected an expression, 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FILTER', 'FINISH', 'FOR', 'FOREACH', 'INSERT', 'LET', 'LIMIT', 'MATCH', 'MERGE', 'NEXT', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WHERE', 'WITH', 'YIELD' or <EOF> (line 1, column 18 (offset: 17))
            |"SHOW SETTING $foo, $bar"
            |                  ^""".stripMargin
        )
    }
  }

  test("SHOW SETTING $foo $bar") {
    failsParsing[Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input '$': expected an expression, 'WHERE', 'YIELD' or <EOF> (line 1, column 19 (offset: 18))
            |"SHOW SETTING $foo $bar"
            |                   ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input '$': expected an expression, 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FILTER', 'FINISH', 'FOR', 'FOREACH', 'INSERT', 'LET', 'LIMIT', 'MATCH', 'MERGE', 'NEXT', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WHERE', 'WITH', 'YIELD' or <EOF> (line 1, column 19 (offset: 18))
            |"SHOW SETTING $foo $bar"
            |                   ^""".stripMargin
        )
    }
  }

  test("SHOW SETTING 'bar', $foo") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input '$': expected a string (line 1, column 21 (offset: 20))
        |"SHOW SETTING 'bar', $foo"
        |                     ^""".stripMargin
    )
  }

  test("SHOW SETTING 'bar', null") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input 'null': expected a string (line 1, column 21 (offset: 20))
        |"SHOW SETTING 'bar', null"
        |                     ^""".stripMargin
    )
  }

  test("SHOW SETTING $foo, 'bar'") {
    failsParsing[Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input ',': expected an expression, 'WHERE', 'YIELD' or <EOF> (line 1, column 18 (offset: 17))
            |"SHOW SETTING $foo, 'bar'"
            |                  ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input ',': expected an expression, 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FILTER', 'FINISH', 'FOR', 'FOREACH', 'INSERT', 'LET', 'LIMIT', 'MATCH', 'MERGE', 'NEXT', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WHERE', 'WITH', 'YIELD' or <EOF> (line 1, column 18 (offset: 17))
            |"SHOW SETTING $foo, 'bar'"
            |                  ^""".stripMargin
        )
    }
  }

  test("SHOW SETTING 'foo' 'bar'") {
    failsParsing[Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input ''bar'': expected an expression, ',', 'WHERE', 'YIELD' or <EOF> (line 1, column 20 (offset: 19))
            |"SHOW SETTING 'foo' 'bar'"
            |                    ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input ''bar'': expected an expression, ',', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FILTER', 'FINISH', 'FOR', 'FOREACH', 'INSERT', 'LET', 'LIMIT', 'MATCH', 'MERGE', 'NEXT', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WHERE', 'WITH', 'YIELD' or <EOF> (line 1, column 20 (offset: 19))
            |"SHOW SETTING 'foo' 'bar'"
            |                    ^""".stripMargin
        )
    }
  }

  test("SHOW SETTINGS YIELD (123 + xyz) AS foo") {
    failsParsing[Statements]
  }

  test("SHOW SETTINGS YIELD * YIELD *") {
    failsParsing[Statements]
  }

  test("SHOW SETTINGS WHERE name = 'db.setting' YIELD *") {
    failsParsing[Statements]
  }

  test("SHOW SETTINGS YIELD a b RETURN *") {
    failsParsing[Statements]
  }

}
