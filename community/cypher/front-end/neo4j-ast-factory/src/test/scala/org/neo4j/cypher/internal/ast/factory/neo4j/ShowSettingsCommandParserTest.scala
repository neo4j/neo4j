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

import org.neo4j.cypher.internal.ast.OrderBy
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.ShowSettingsClause
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.util.symbols.CTAny

class ShowSettingsCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {

  Seq("SETTING", "SETTINGS").foreach { settingKeyword =>
    test(s"SHOW $settingKeyword") {
      assertAst(singleQuery(ShowSettingsClause(Left(List.empty[String]), None, hasYield = false)(defaultPos)))
    }

    test(s"SHOW $settingKeyword 'foo'") {
      assertAst(
        singleQuery(ShowSettingsClause(Right(literalString("foo")), None, hasYield = false)(defaultPos))
      )
    }

    test(s"SHOW $settingKeyword ''") {
      assertAst(
        singleQuery(ShowSettingsClause(Right(literalString("")), None, hasYield = false)(defaultPos))
      )
    }

    test(s"SHOW $settingKeyword $$param") {
      assertAst(
        singleQuery(ShowSettingsClause(Right(parameter("param", CTAny)), None, hasYield = false)(defaultPos))
      )
    }

    test(s"SHOW $settingKeyword 'foo', 'bar'") {
      assertAst(singleQuery(ShowSettingsClause(Left(List("foo", "bar")), None, hasYield = false)(defaultPos)))
    }

    test(s"SHOW $settingKeyword 'foo'+'.'+$$name") {
      assertAst(singleQuery(ShowSettingsClause(
        Right(
          add(add(literalString("foo"), literalString(".")), parameter("name", CTAny))
        ),
        None,
        hasYield = false
      )(defaultPos)))
    }

    test(s"SHOW $settingKeyword ['foo', 'bar']") {
      assertAst(singleQuery(ShowSettingsClause(
        Right(listOfString("foo", "bar")),
        None,
        hasYield = false
      )(defaultPos)))
    }

    test(s"USE db SHOW $settingKeyword") {
      assertAst(SingleQuery(
        List(
          use(varFor("db", (1, 5, 4))),
          ShowSettingsClause(Left(List.empty[String]), None, hasYield = false)((1, 8, 7))
        )
      )((1, 8, 7)))
    }

  }

  // Filtering tests

  test("SHOW SETTING WHERE name = 'db.setting.sub_setting'") {
    assertAst(singleQuery(ShowSettingsClause(
      Left(List.empty[String]),
      Some(where(
        equals(
          varFor("name"),
          literalString("db.setting.sub_setting")
        )
      )),
      hasYield = false
    )(defaultPos)))
  }

  test("SHOW SETTING WHERE name IN ['db.setting.sub_setting', 'db.another.setting']") {
    assertAst(singleQuery(ShowSettingsClause(
      Left(List.empty[String]),
      Some(where(
        in(
          varFor("name", (1, 20, 19)),
          listOfString(
            "db.setting.sub_setting",
            "db.another.setting"
          )
        )
      )),
      hasYield = false
    )(defaultPos)))
  }

  test("SHOW SETTING WHERE name = $name") {
    assertAst(singleQuery(ShowSettingsClause(
      Left(List.empty[String]),
      Some(where(
        eq(
          varFor("name"),
          parameter("name", CTAny)
        )
      )),
      hasYield = false
    )(defaultPos)))
  }

  test("SHOW SETTING WHERE name IN $list") {
    assertAst(singleQuery(ShowSettingsClause(
      Left(List.empty[String]),
      Some(where(
        in(
          varFor("name"),
          parameter("list", CTAny)
        )
      )),
      hasYield = false
    )(defaultPos)))
  }

  test("SHOW SETTING 'foo' WHERE isDynamic") {
    assertAst(singleQuery(ShowSettingsClause(
      Right(literalString("foo")),
      Some(where(varFor("isDynamic"))),
      hasYield = false
    )(defaultPos)))
  }

  test("SHOW SETTING 'foo', 'bar' WHERE isDynamic") {
    assertAst(singleQuery(ShowSettingsClause(
      Left(List("foo", "bar")),
      Some(where(varFor("isDynamic"))),
      hasYield = false
    )(defaultPos)))
  }

  test("SHOW SETTING $foo WHERE pp < 50.0") {
    assertAst(singleQuery(ShowSettingsClause(
      Right(parameter("foo", CTAny)),
      Some(where(lessThan(varFor("pp"), literalFloat(50.0)))),
      hasYield = false
    )(defaultPos)))
  }

  test("SHOW SETTINGS YIELD description") {
    assertAst(singleQuery(
      ShowSettingsClause(Left(List.empty[String]), None, hasYield = true)(defaultPos),
      yieldClause(
        ReturnItems(includeExisting = false, Seq(variableReturnItem("description")))((1, 21, 20))
      )
    ))
  }

  test("SHOW SETTINGS YIELD * ORDER BY name SKIP 2 LIMIT 5") {
    assertAst(singleQuery(
      ShowSettingsClause(Left(List.empty[String]), None, hasYield = true)(defaultPos),
      yieldClause(
        returnAllItems((1, 23, 22)),
        Some(OrderBy(Seq(
          sortItem(varFor("name"))
        ))((1, 23, 22))),
        Some(skip(2)),
        Some(limit(5))
      )
    ))
  }

  test("SHOW SETTING YIELD name, description, value WHERE name = 'db.setting.sub_setting'") {
    assertAst(singleQuery(
      ShowSettingsClause(Left(List.empty[String]), None, hasYield = true)(defaultPos),
      yieldClause(
        ReturnItems(
          includeExisting = false,
          Seq(
            variableReturnItem("name"),
            variableReturnItem("description"),
            variableReturnItem("value")
          )
        )((1, 20, 19)),
        where = Some(where(
          eq(
            varFor("name"),
            literalString("db.setting.sub_setting")
          )
        ))
      )
    ))
  }

  test("USE db SHOW SETTINGS YIELD name, description AS pp WHERE pp < 50.0 RETURN name") {
    assertAst(
      singleQuery(
        use(varFor("db")),
        ShowSettingsClause(Left(List.empty[String]), None, hasYield = true)(pos),
        yieldClause(
          returnItems(variableReturnItem("name"), aliasedReturnItem("description", "pp")),
          where = Some(where(lessThan(varFor("pp"), literalFloat(50.0))))
        ),
        return_(variableReturnItem("name"))
      ),
      comparePosition = false
    )
  }

  test("SHOW SETTINGS YIELD name AS SETTING, mode AS OUTPUT") {
    assertAst(
      singleQuery(
        ShowSettingsClause(Left(List.empty[String]), None, hasYield = true)(pos),
        yieldClause(returnItems(aliasedReturnItem("name", "SETTING"), aliasedReturnItem("mode", "OUTPUT")))
      ),
      comparePosition = false
    )
  }

  test("SHOW SETTINGS 'db.setting.sub_setting' YIELD description") {
    assertAst(singleQuery(
      ShowSettingsClause(Right(literalString("db.setting.sub_setting")), None, hasYield = true)(defaultPos),
      yieldClause(
        ReturnItems(includeExisting = false, Seq(variableReturnItem("description")))((1, 46, 45))
      )
    ))
  }

  test("SHOW SETTINGS 'db.setting.sub_setting', 'db.another.setting' YIELD description") {
    assertAst(singleQuery(
      ShowSettingsClause(Left(List("db.setting.sub_setting", "db.another.setting")), None, hasYield = true)(
        defaultPos
      ),
      yieldClause(
        ReturnItems(includeExisting = false, Seq(variableReturnItem("description")))((1, 68, 67))
      )
    ))
  }

  test("SHOW SETTINGS $list YIELD description") {
    assertAst(singleQuery(
      ShowSettingsClause(Right(parameter("list", CTAny)), None, hasYield = true)(defaultPos),
      yieldClause(
        ReturnItems(includeExisting = false, Seq(variableReturnItem("description")))((1, 27, 26))
      )
    ))
  }

  test("SHOW SETTINGS $list YIELD name, description, isExplicitlySet WHERE isExplicitlySet") {
    assertAst(singleQuery(
      ShowSettingsClause(Right(parameter("list", CTAny)), None, hasYield = true)(defaultPos),
      yieldClause(
        ReturnItems(
          includeExisting = false,
          Seq(
            variableReturnItem("name"),
            variableReturnItem("description"),
            variableReturnItem("isExplicitlySet")
          )
        )((1, 27, 26)),
        where =
          Some(where(varFor("isExplicitlySet")))
      )
    ))
  }

  test("SHOW SETTINGS YIELD (123 + xyz)") {
    assertAst(singleQuery(
      ShowSettingsClause(
        Right(function("YIELD", add(literalInt(123), varFor("xyz")))),
        None,
        hasYield = false
      )(pos)
    ))
  }

  test("SHOW SETTINGS YIELD") {
    assertAst(singleQuery(
      ShowSettingsClause(Right(varFor("YIELD")), None, hasYield = false)(pos)
    ))
  }

  // Negative tests

  test("SHOW ALL SETTINGS") {
    assertFailsWithMessageStart(
      testName,
      """Invalid input 'SETTINGS': expected
        |  "CONSTRAINT"
        |  "CONSTRAINTS"
        |  "FUNCTION"
        |  "FUNCTIONS"
        |  "INDEX"
        |  "INDEXES"
        |  "PRIVILEGE"
        |  "PRIVILEGES"
        |  "ROLE"
        |  "ROLES"""".stripMargin
    )
  }

  test("SHOW SETTING $foo, $bar") {
    assertFailsWithMessageStart(
      testName,
      """Invalid input ',': expected
        |  "!="
        |  "%"""".stripMargin
    )
  }

  test("SHOW SETTING $foo $bar") {
    assertFailsWithMessageStart(
      testName,
      """Invalid input '$': expected
        |  "!="
        |  "%"""".stripMargin
    )
  }

  test("SHOW SETTING 'bar', $foo") {
    assertFailsWithMessageStart(testName, """Invalid input '$': expected "\"" or "\'" """)
  }

  test("SHOW SETTING $foo, 'bar'") {
    assertFailsWithMessageStart(
      testName,
      """Invalid input ',': expected
        |  "!="
        |  "%"""".stripMargin
    )
  }

  test("SHOW SETTING 'foo' 'bar'") {
    assertFailsWithMessageStart(
      testName,
      """Invalid input 'bar': expected
        |  "!="
        |  "%"""".stripMargin
    )
  }

  test("SHOW SETTINGS YIELD (123 + xyz) AS foo") {
    failsToParse
  }

  test("SHOW SETTINGS YIELD * YIELD *") {
    failsToParse
  }

  test("SHOW SETTINGS WHERE name = 'db.setting' YIELD *") {
    failsToParse
  }

  test("SHOW SETTINGS WHERE name = 'db.setting' RETURN *") {
    failsToParse
  }

  test("SHOW SETTINGS YIELD a b RETURN *") {
    failsToParse
  }

  test("SHOW SETTINGS RETURN *") {
    failsToParse
  }
}
