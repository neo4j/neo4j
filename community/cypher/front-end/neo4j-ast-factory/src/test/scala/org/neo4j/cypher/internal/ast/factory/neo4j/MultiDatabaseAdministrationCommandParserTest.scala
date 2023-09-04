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

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.NamespacedName
import org.neo4j.cypher.internal.ast.NoOptions
import org.neo4j.cypher.internal.ast.OptionsMap
import org.neo4j.cypher.internal.ast.ReadOnlyAccess
import org.neo4j.cypher.internal.ast.Topology
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.Null
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.util.symbols.CTMap

class MultiDatabaseAdministrationCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {

  // SHOW DATABASE

  Seq(
    ("DATABASE", ast.ShowDatabase.apply(ast.AllDatabasesScope()(pos), _: ast.YieldOrWhere) _),
    ("DATABASES", ast.ShowDatabase.apply(ast.AllDatabasesScope()(pos), _: ast.YieldOrWhere) _),
    ("DEFAULT DATABASE", ast.ShowDatabase.apply(ast.DefaultDatabaseScope()(pos), _: ast.YieldOrWhere) _),
    ("HOME DATABASE", ast.ShowDatabase.apply(ast.HomeDatabaseScope()(pos), _: ast.YieldOrWhere) _),
    (
      "DATABASE $db",
      ast.ShowDatabase.apply(ast.SingleNamedDatabaseScope(stringParamName("db"))(pos), _: ast.YieldOrWhere) _
    ),
    (
      "DATABASES $db",
      ast.ShowDatabase.apply(ast.SingleNamedDatabaseScope(stringParamName("db"))(pos), _: ast.YieldOrWhere) _
    ),
    (
      "DATABASE neo4j",
      ast.ShowDatabase.apply(ast.SingleNamedDatabaseScope(literal("neo4j"))(pos), _: ast.YieldOrWhere) _
    ),
    (
      "DATABASES neo4j",
      ast.ShowDatabase.apply(ast.SingleNamedDatabaseScope(literal("neo4j"))(pos), _: ast.YieldOrWhere) _
    ),
    // vvv naming the database yield/where should not fail either vvv
    (
      "DATABASE yield",
      ast.ShowDatabase.apply(ast.SingleNamedDatabaseScope(literal("yield"))(pos), _: ast.YieldOrWhere) _
    ),
    (
      "DATABASES yield",
      ast.ShowDatabase.apply(ast.SingleNamedDatabaseScope(literal("yield"))(pos), _: ast.YieldOrWhere) _
    ),
    (
      "DATABASE where",
      ast.ShowDatabase.apply(ast.SingleNamedDatabaseScope(literal("where"))(pos), _: ast.YieldOrWhere) _
    ),
    (
      "DATABASES where",
      ast.ShowDatabase.apply(ast.SingleNamedDatabaseScope(literal("where"))(pos), _: ast.YieldOrWhere) _
    )
  ).foreach { case (dbType, privilege) =>
    test(s"SHOW $dbType") {
      yields(privilege(None))
    }

    test(s"USE system SHOW $dbType") {
      yields(privilege(None))
    }

    test(s"SHOW $dbType WHERE access = 'GRANTED'") {
      yields(privilege(Some(Right(where(equals(accessVar, grantedString))))))
    }

    test(s"SHOW $dbType WHERE access = 'GRANTED' AND action = 'match'") {
      val accessPredicate = equals(accessVar, grantedString)
      val matchPredicate = equals(varFor(actionString), literalString("match"))
      yields(privilege(Some(Right(where(and(accessPredicate, matchPredicate))))))
    }

    test(s"SHOW $dbType YIELD access ORDER BY access") {
      val orderByClause = orderBy(sortItem(accessVar))
      val columns = yieldClause(returnItems(variableReturnItem(accessString)), Some(orderByClause))
      yields(privilege(Some(Left((columns, None)))))
    }

    test(s"SHOW $dbType YIELD access ORDER BY access WHERE access ='none'") {
      val orderByClause = orderBy(sortItem(accessVar))
      val whereClause = where(equals(accessVar, noneString))
      val columns =
        yieldClause(returnItems(variableReturnItem(accessString)), Some(orderByClause), where = Some(whereClause))
      yields(privilege(Some(Left((columns, None)))))
    }

    test(s"SHOW $dbType YIELD access ORDER BY access SKIP 1 LIMIT 10 WHERE access ='none'") {
      val orderByClause = orderBy(sortItem(accessVar))
      val whereClause = where(equals(accessVar, noneString))
      val columns = yieldClause(
        returnItems(variableReturnItem(accessString)),
        Some(orderByClause),
        Some(skip(1)),
        Some(limit(10)),
        Some(whereClause)
      )
      yields(privilege(Some(Left((columns, None)))))
    }

    test(s"SHOW $dbType YIELD access SKIP -1") {
      val columns = yieldClause(returnItems(variableReturnItem(accessString)), skip = Some(skip(-1)))
      yields(privilege(Some(Left((columns, None)))))
    }

    test(s"SHOW $dbType YIELD access ORDER BY access RETURN access") {
      yields(privilege(
        Some(Left((
          yieldClause(returnItems(variableReturnItem(accessString)), Some(orderBy(sortItem(accessVar)))),
          Some(returnClause(returnItems(variableReturnItem(accessString))))
        )))
      ))
    }

    test(s"SHOW $dbType WHERE access = 'GRANTED' RETURN action") {
      failsToParse
    }

    test(s"SHOW $dbType YIELD * RETURN *") {
      yields(privilege(Some(Left((yieldClause(returnAllItems), Some(returnClause(returnAllItems)))))))
    }
  }

  test("SHOW DATABASE `foo.bar`") {
    yields(ast.ShowDatabase(ast.SingleNamedDatabaseScope(namespacedName("foo.bar"))(pos), None))
  }

  test("SHOW DATABASE foo.bar") {
    yields(ast.ShowDatabase(ast.SingleNamedDatabaseScope(namespacedName("foo", "bar"))(pos), None))
  }

  test("SHOW DATABASE blah YIELD *,blah RETURN user") {
    failsToParse
  }

  test("SHOW DATABASE YIELD (123 + xyz)") {
    failsToParse
  }

  test("SHOW DATABASES YIELD (123 + xyz) AS foo") {
    failsToParse
  }

  test("SHOW DEFAULT DATABASES") {
    assertFailsWithMessage(
      testName,
      """Invalid input 'DATABASES': expected "DATABASE" (line 1, column 14 (offset: 13))"""
    )
  }

  test("SHOW DEFAULT DATABASES YIELD *") {
    assertFailsWithMessage(
      testName,
      """Invalid input 'DATABASES': expected "DATABASE" (line 1, column 14 (offset: 13))"""
    )
  }

  test("SHOW DEFAULT DATABASES WHERE name STARTS WITH 'foo'") {
    assertFailsWithMessage(
      testName,
      """Invalid input 'DATABASES': expected "DATABASE" (line 1, column 14 (offset: 13))"""
    )
  }

  test("SHOW HOME DATABASES") {
    assertFailsWithMessage(
      testName,
      """Invalid input 'DATABASES': expected "DATABASE" (line 1, column 11 (offset: 10))"""
    )
  }

  test("SHOW HOME DATABASES YIELD *") {
    assertFailsWithMessage(
      testName,
      """Invalid input 'DATABASES': expected "DATABASE" (line 1, column 11 (offset: 10))"""
    )
  }

  test("SHOW HOME DATABASES WHERE name STARTS WITH 'foo'") {
    assertFailsWithMessage(
      testName,
      """Invalid input 'DATABASES': expected "DATABASE" (line 1, column 11 (offset: 10))"""
    )
  }

  // CREATE DATABASE

  test("CREATE DATABASE foo") {
    yields(ast.CreateDatabase(literalFoo, ast.IfExistsThrowError, ast.NoOptions, ast.NoWait, None))
  }

  test("USE system CREATE DATABASE foo") {
    // can parse USE clause, but is not included in AST
    yields(ast.CreateDatabase(literalFoo, ast.IfExistsThrowError, ast.NoOptions, ast.NoWait, None))
  }

  test("CREATE DATABASE $foo") {
    yields(ast.CreateDatabase(stringParamName("foo"), ast.IfExistsThrowError, ast.NoOptions, ast.NoWait, None))
  }

  test("CREATE DATABASE $wait") {
    yields(ast.CreateDatabase(stringParamName("wait"), ast.IfExistsThrowError, ast.NoOptions, ast.NoWait, None))
  }

  test("CREATE DATABASE `nowait.sec`") {
    yields(ast.CreateDatabase(literal("nowait.sec"), ast.IfExistsThrowError, ast.NoOptions, ast.NoWait, None))
  }

  test("CREATE DATABASE second WAIT") {
    yields(ast.CreateDatabase(literal("second"), ast.IfExistsThrowError, ast.NoOptions, ast.IndefiniteWait, None))
  }

  test("CREATE DATABASE seconds WAIT 12") {
    yields(ast.CreateDatabase(literal("seconds"), ast.IfExistsThrowError, ast.NoOptions, ast.TimeoutAfter(12), None))
  }

  test("CREATE DATABASE dump WAIT 12 SEC") {
    yields(ast.CreateDatabase(literal("dump"), ast.IfExistsThrowError, ast.NoOptions, ast.TimeoutAfter(12), None))
  }

  test("CREATE DATABASE destroy WAIT 12 SECOND") {
    yields(ast.CreateDatabase(literal("destroy"), ast.IfExistsThrowError, ast.NoOptions, ast.TimeoutAfter(12), None))
  }

  test("CREATE DATABASE data WAIT 12 SECONDS") {
    yields(ast.CreateDatabase(literal("data"), ast.IfExistsThrowError, ast.NoOptions, ast.TimeoutAfter(12), None))
  }

  test("CREATE DATABASE foo NOWAIT") {
    yields(ast.CreateDatabase(literal("foo"), ast.IfExistsThrowError, ast.NoOptions, ast.NoWait, None))
  }

  test("CREATE DATABASE `foo.bar`") {
    yields(ast.CreateDatabase(literal("foo.bar"), ast.IfExistsThrowError, ast.NoOptions, ast.NoWait, None))
  }

  test("CREATE DATABASE foo.bar") {
    yields(ast.CreateDatabase(namespacedName("foo", "bar"), ast.IfExistsThrowError, ast.NoOptions, ast.NoWait, None))
  }

  test("CREATE DATABASE `graph.db`.`db.db`") {
    yields(_ =>
      ast.CreateDatabase(namespacedName("graph.db", "db.db"), ast.IfExistsThrowError, ast.NoOptions, ast.NoWait, None)(
        pos
      )
    )
  }

  test("CREATE DATABASE `foo-bar42`") {
    yields(_ => ast.CreateDatabase(literal("foo-bar42"), ast.IfExistsThrowError, ast.NoOptions, ast.NoWait, None)(pos))
  }

  test("CREATE DATABASE `_foo-bar42`") {
    yields(_ => ast.CreateDatabase(literal("_foo-bar42"), ast.IfExistsThrowError, ast.NoOptions, ast.NoWait, None)(pos))
  }

  test("CREATE DATABASE ``") {
    yields(_ => ast.CreateDatabase(literal(""), ast.IfExistsThrowError, ast.NoOptions, ast.NoWait, None)(pos))
  }

  test("CREATE DATABASE foo IF NOT EXISTS") {
    yields(ast.CreateDatabase(literalFoo, ast.IfExistsDoNothing, ast.NoOptions, ast.NoWait, None))
  }

  test("CREATE DATABASE foo IF NOT EXISTS WAIT 10 SECONDS") {
    yields(ast.CreateDatabase(literalFoo, ast.IfExistsDoNothing, ast.NoOptions, ast.TimeoutAfter(10), None))
  }

  test("CREATE DATABASE foo IF NOT EXISTS WAIT") {
    yields(ast.CreateDatabase(literalFoo, ast.IfExistsDoNothing, ast.NoOptions, ast.IndefiniteWait, None))
  }

  test("CREATE  DATABASE foo IF NOT EXISTS NOWAIT") {
    yields(ast.CreateDatabase(literalFoo, ast.IfExistsDoNothing, ast.NoOptions, ast.NoWait, None))
  }

  test("CREATE DATABASE `_foo-bar42` IF NOT EXISTS") {
    yields(_ => ast.CreateDatabase(literal("_foo-bar42"), ast.IfExistsDoNothing, ast.NoOptions, ast.NoWait, None)(pos))
  }

  test("CREATE OR REPLACE DATABASE foo") {
    yields(ast.CreateDatabase(literalFoo, ast.IfExistsReplace, ast.NoOptions, ast.NoWait, None))
  }

  test("CREATE OR REPLACE DATABASE foo WAIT 10 SECONDS") {
    yields(ast.CreateDatabase(literalFoo, ast.IfExistsReplace, ast.NoOptions, ast.TimeoutAfter(10), None))
  }

  test("CREATE OR REPLACE DATABASE foo WAIT") {
    yields(ast.CreateDatabase(literalFoo, ast.IfExistsReplace, ast.NoOptions, ast.IndefiniteWait, None))
  }

  test("CREATE OR REPLACE DATABASE foo NOWAIT") {
    yields(ast.CreateDatabase(literalFoo, ast.IfExistsReplace, ast.NoOptions, ast.NoWait, None))
  }

  test("CREATE OR REPLACE DATABASE `_foo-bar42`") {
    yields(_ => ast.CreateDatabase(literal("_foo-bar42"), ast.IfExistsReplace, ast.NoOptions, ast.NoWait, None)(pos))
  }

  test("CREATE OR REPLACE DATABASE foo IF NOT EXISTS") {
    yields(ast.CreateDatabase(literalFoo, ast.IfExistsInvalidSyntax, ast.NoOptions, ast.NoWait, None))
  }

  test("CREATE DATABASE") {
    // missing db name but parses as 'normal' cypher CREATE...
    assertFailsWithMessage(
      testName,
      s"""Invalid input '': expected a parameter or an identifier (line 1, column 16 (offset: 15))"""
    )
  }

  test("CREATE DATABASE \"foo.bar\"") {
    failsToParse
  }

  test("CREATE DATABASE foo-bar42") {
    failsToParse
  }

  test("CREATE DATABASE _foo-bar42") {
    failsToParse
  }

  test("CREATE DATABASE 42foo-bar") {
    failsToParse
  }

  test("CREATE DATABASE _foo-bar42 IF NOT EXISTS") {
    failsToParse
  }

  test("CREATE DATABASE  IF NOT EXISTS") {
    val exceptionMessage =
      s"""Invalid input 'NOT': expected
         |  "."
         |  "IF"
         |  "NOWAIT"
         |  "OPTIONS"
         |  "TOPOLOGY"
         |  "WAIT"
         |  <EOF> (line 1, column 21 (offset: 20))""".stripMargin

    assertFailsWithMessage(testName, exceptionMessage)
  }

  test("CREATE DATABASE foo IF EXISTS") {
    failsToParse
  }

  test("CREATE DATABASE foo WAIT -12") {
    failsToParse
  }

  test("CREATE DATABASE foo WAIT 3.14") {
    assertFailsWithMessage(
      testName,
      "Invalid input '3.14': expected <EOF> or <UNSIGNED_DECIMAL_INTEGER> (line 1, column 26 (offset: 25))"
    )
  }

  test("CREATE DATABASE foo WAIT bar") {
    failsToParse
  }

  test("CREATE OR REPLACE DATABASE _foo-bar42") {
    failsToParse
  }

  test("CREATE OR REPLACE DATABASE") {
    assertFailsWithMessage(
      testName,
      s"""Invalid input '': expected a parameter or an identifier (line 1, column 27 (offset: 26))"""
    )
  }

  test(
    "CREATE DATABASE foo OPTIONS {existingData: 'use', existingDataSeedInstance: '84c3ee6f-260e-47db-a4b6-589c807f2c2e'}"
  ) {
    assertAst(
      ast.CreateDatabase(
        NamespacedName("foo")((1, 17, 16)),
        ast.IfExistsThrowError,
        ast.OptionsMap(Map(
          "existingData" -> StringLiteral("use")((1, 44, 43)),
          "existingDataSeedInstance" -> StringLiteral("84c3ee6f-260e-47db-a4b6-589c807f2c2e")((1, 77, 76))
        )),
        ast.NoWait,
        None
      )(defaultPos)
    )
  }

  test(
    "CREATE DATABASE foo OPTIONS {existingData: 'use', existingDataSeedInstance: '84c3ee6f-260e-47db-a4b6-589c807f2c2e'} WAIT"
  ) {
    assertAst(
      ast.CreateDatabase(
        NamespacedName("foo")((1, 17, 16)),
        ast.IfExistsThrowError,
        ast.OptionsMap(Map(
          "existingData" -> StringLiteral("use")((1, 44, 43)),
          "existingDataSeedInstance" -> StringLiteral("84c3ee6f-260e-47db-a4b6-589c807f2c2e")((1, 77, 76))
        )),
        ast.IndefiniteWait,
        None
      )(defaultPos)
    )
  }

  test("CREATE DATABASE foo OPTIONS $param") {
    assertAst(
      ast.CreateDatabase(
        NamespacedName("foo")((1, 17, 16)),
        ast.IfExistsThrowError,
        ast.OptionsParam(ExplicitParameter("param", CTMap)((1, 29, 28))),
        ast.NoWait,
        None
      )(defaultPos)
    )
  }

  test("CREATE DATABASE foo SET OPTION key value") {
    assertFailsWithMessage(
      testName,
      """Invalid input 'SET': expected
        |  "."
        |  "IF"
        |  "NOWAIT"
        |  "OPTIONS"
        |  "TOPOLOGY"
        |  "WAIT"
        |  <EOF> (line 1, column 21 (offset: 20))""".stripMargin
    )
  }

  test("CREATE DATABASE foo OPTION {key: value}") {
    assertFailsWithMessage(
      testName,
      """Invalid input 'OPTION': expected
        |  "."
        |  "IF"
        |  "NOWAIT"
        |  "OPTIONS"
        |  "TOPOLOGY"
        |  "WAIT"
        |  <EOF> (line 1, column 21 (offset: 20))""".stripMargin
    )
  }

  test("CREATE DATABASE foo SET OPTIONS key value") {
    assertFailsWithMessage(
      testName,
      """Invalid input 'SET': expected
        |  "."
        |  "IF"
        |  "NOWAIT"
        |  "OPTIONS"
        |  "TOPOLOGY"
        |  "WAIT"
        |  <EOF> (line 1, column 21 (offset: 20))""".stripMargin
    )
  }

  test("CREATE DATABASE foo OPTIONS key value") {
    assertFailsWithMessage(
      testName,
      """Invalid input 'key': expected "{" or a parameter (line 1, column 29 (offset: 28))""".stripMargin
    )
  }

  test("CREATE DATABASE foo TOPOLOGY 1 PRIMARY") {
    assertAst(
      ast.CreateDatabase(
        literalFoo,
        ast.IfExistsThrowError,
        ast.NoOptions,
        ast.NoWait,
        Some(Topology(Some(1), None))
      )(pos)
    )
  }

  test("CREATE DATABASE foo TOPOLOGY 1 PRIMARIES") {
    assertAst(
      ast.CreateDatabase(
        literalFoo,
        ast.IfExistsThrowError,
        ast.NoOptions,
        ast.NoWait,
        Some(Topology(Some(1), None))
      )(pos)
    )
  }

  test("CREATE DATABASE foo TOPOLOGY 1 PRIMARY 1 SECONDARY") {
    assertAst(
      ast.CreateDatabase(
        literalFoo,
        ast.IfExistsThrowError,
        ast.NoOptions,
        ast.NoWait,
        Some(Topology(Some(1), Some(1)))
      )(pos)
    )
  }

  test("CREATE DATABASE foo TOPOLOGY 1 PRIMARY 2 SECONDARIES") {
    assertAst(
      ast.CreateDatabase(
        literalFoo,
        ast.IfExistsThrowError,
        ast.NoOptions,
        ast.NoWait,
        Some(Topology(Some(1), Some(2)))
      )(pos)
    )
  }

  test("CREATE DATABASE foo TOPOLOGY 1 SECONDARY 1 PRIMARY") {
    assertAst(
      ast.CreateDatabase(
        literalFoo,
        ast.IfExistsThrowError,
        ast.NoOptions,
        ast.NoWait,
        Some(Topology(Some(1), Some(1)))
      )(pos)
    )
  }

  test("CREATE DATABASE foo TOPOLOGY 1 PRIMARY TOPOLOGY 1 SECONDARY") {
    assertFailsWithMessage(
      testName,
      """Invalid input 'TOPOLOGY': expected
        |  "NOWAIT"
        |  "OPTIONS"
        |  "WAIT"
        |  <EOF>
        |  <UNSIGNED_DECIMAL_INTEGER> (line 1, column 40 (offset: 39))""".stripMargin
    )
  }

  test("CREATE DATABASE foo TOPOLOGY 1 PRIMARY 1 PRIMARY") {
    assertFailsWithMessage(
      testName,
      """Duplicate PRIMARY clause (line 1, column 42 (offset: 41))""".stripMargin
    )
  }

  test("CREATE DATABASE foo TOPOLOGY 2 PRIMARIES 1 PRIMARY") {
    assertFailsWithMessage(
      testName,
      """Duplicate PRIMARY clause (line 1, column 44 (offset: 43))""".stripMargin
    )
  }

  test("CREATE DATABASE foo TOPOLOGY 2 SECONDARIES 1 SECONDARY") {
    assertFailsWithMessage(
      testName,
      """Duplicate SECONDARY clause (line 1, column 46 (offset: 45))""".stripMargin
    )
  }

  test("CREATE DATABASE foo TOPOLOGY 1 PRIMARY 1 SECONDARY 2 SECONDARY") {
    assertFailsWithMessage(
      testName,
      """Duplicate SECONDARY clause (line 1, column 54 (offset: 53))""".stripMargin
    )
  }

  test("CREATE DATABASE foo TOPOLOGY -1 PRIMARY") {
    assertFailsWithMessage(
      testName,
      """Invalid input '-': expected <UNSIGNED_DECIMAL_INTEGER> (line 1, column 30 (offset: 29))""".stripMargin
    )
  }

  test("CREATE DATABASE foo TOPOLOGY 1 PRIMARY -1 SECONDARY") {
    assertFailsWithMessage(
      testName,
      """Invalid input '-': expected
        |  "NOWAIT"
        |  "OPTIONS"
        |  "WAIT"
        |  <EOF>
        |  <UNSIGNED_DECIMAL_INTEGER> (line 1, column 40 (offset: 39))""".stripMargin
    )
  }

  test("CREATE DATABASE foo TOPOLOGY -1 SECONDARY 1 PRIMARY") {
    assertFailsWithMessage(
      testName,
      """Invalid input '-': expected <UNSIGNED_DECIMAL_INTEGER> (line 1, column 30 (offset: 29))""".stripMargin
    )
  }

  test("CREATE DATABASE foo TOPOLOGY 1 SECONDARY 1 SECONDARY") {
    assertFailsWithMessage(
      testName,
      """Duplicate SECONDARY clause (line 1, column 44 (offset: 43))""".stripMargin
    )
  }

  test("CREATE DATABASE foo TOPOLOGY 1 SECONDARY") {
    assertAst(
      ast.CreateDatabase(
        literalFoo,
        ast.IfExistsThrowError,
        ast.NoOptions,
        ast.NoWait,
        Some(Topology(None, Some(1)))
      )(pos)
    )
  }

  test("CREATE DATABASE foo TOPOLOGY $param PRIMARY") {
    assertFailsWithMessage(
      testName,
      """Invalid input '$': expected <UNSIGNED_DECIMAL_INTEGER> (line 1, column 30 (offset: 29))"""
    )
  }

  test("CREATE DATABASE foo TOPOLOGY 1 PRIMARY $param SECONDARY") {
    assertFailsWithMessage(
      testName,
      """Invalid input '$': expected
        |  "NOWAIT"
        |  "OPTIONS"
        |  "WAIT"
        |  <EOF>
        |  <UNSIGNED_DECIMAL_INTEGER> (line 1, column 40 (offset: 39))""".stripMargin
    )
  }

  test("CREATE DATABASE foo TOPOLOGY") {
    assertFailsWithMessage(
      testName,
      """Invalid input '': expected <UNSIGNED_DECIMAL_INTEGER> (line 1, column 29 (offset: 28))"""
    )
  }

  test("CREATE DATABASE alias") {
    yields(_ => ast.CreateDatabase(literal("alias"), ast.IfExistsThrowError, ast.NoOptions, ast.NoWait, None)(pos))
  }

  test("CREATE DATABASE alias IF NOT EXISTS") {
    yields(_ => ast.CreateDatabase(literal("alias"), ast.IfExistsDoNothing, ast.NoOptions, ast.NoWait, None)(pos))
  }

  // DROP DATABASE

  test("DROP DATABASE foo") {
    yields(ast.DropDatabase(literal("foo"), ifExists = false, composite = false, ast.DestroyData, ast.NoWait))
  }

  test("DROP DATABASE alias") {
    yields(ast.DropDatabase(literal("alias"), ifExists = false, composite = false, ast.DestroyData, ast.NoWait))
  }

  test("DROP DATABASE alias WAIT") {
    yields(ast.DropDatabase(literal("alias"), ifExists = false, composite = false, ast.DestroyData, ast.IndefiniteWait))
  }

  test("DROP DATABASE alias NOWAIT") {
    yields(ast.DropDatabase(literal("alias"), ifExists = false, composite = false, ast.DestroyData, ast.NoWait))
  }

  test("DROP DATABASE $foo") {
    yields(ast.DropDatabase(stringParamName("foo"), ifExists = false, composite = false, ast.DestroyData, ast.NoWait))
  }

  test("DROP DATABASE foo WAIT") {
    yields(ast.DropDatabase(literal("foo"), ifExists = false, composite = false, ast.DestroyData, ast.IndefiniteWait))
  }

  test("DROP DATABASE foo WAIT 10") {
    yields(ast.DropDatabase(literal("foo"), ifExists = false, composite = false, ast.DestroyData, ast.TimeoutAfter(10)))
  }

  test("DROP DATABASE foo WAIT 10 SEC") {
    yields(ast.DropDatabase(literal("foo"), ifExists = false, composite = false, ast.DestroyData, ast.TimeoutAfter(10)))
  }

  test("DROP DATABASE foo WAIT 10 SECOND") {
    yields(ast.DropDatabase(literal("foo"), ifExists = false, composite = false, ast.DestroyData, ast.TimeoutAfter(10)))
  }

  test("DROP DATABASE foo WAIT 10 SECONDS") {
    yields(ast.DropDatabase(literal("foo"), ifExists = false, composite = false, ast.DestroyData, ast.TimeoutAfter(10)))
  }

  test("DROP DATABASE foo NOWAIT") {
    yields(ast.DropDatabase(literalFoo, ifExists = false, composite = false, ast.DestroyData, ast.NoWait))
  }

  test("DROP DATABASE `foo.bar`") {
    yields(_ =>
      ast.DropDatabase(literal("foo.bar"), ifExists = false, composite = false, ast.DestroyData, ast.NoWait)(pos)
    )
  }

  test("DROP DATABASE foo.bar") {
    yields(_ =>
      ast.DropDatabase(
        NamespacedName(List("bar"), Some("foo"))((1, 14, 13)),
        ifExists = false,
        composite = false,
        ast.DestroyData,
        ast.NoWait
      )(pos)
    )
  }

  test("DROP DATABASE foo IF EXISTS") {
    yields(ast.DropDatabase(literalFoo, ifExists = true, composite = false, ast.DestroyData, ast.NoWait))
  }

  test("DROP DATABASE foo IF EXISTS WAIT") {
    yields(ast.DropDatabase(literalFoo, ifExists = true, composite = false, ast.DestroyData, ast.IndefiniteWait))
  }

  test("DROP DATABASE foo IF EXISTS NOWAIT") {
    yields(ast.DropDatabase(literalFoo, ifExists = true, composite = false, ast.DestroyData, ast.NoWait))
  }

  test("DROP DATABASE foo DUMP DATA") {
    yields(ast.DropDatabase(literalFoo, ifExists = false, composite = false, ast.DumpData, ast.NoWait))
  }

  test("DROP DATABASE foo DESTROY DATA") {
    yields(ast.DropDatabase(literalFoo, ifExists = false, composite = false, ast.DestroyData, ast.NoWait))
  }

  test("DROP DATABASE foo IF EXISTS DUMP DATA") {
    yields(ast.DropDatabase(literalFoo, ifExists = true, composite = false, ast.DumpData, ast.NoWait))
  }

  test("DROP DATABASE foo IF EXISTS DESTROY DATA") {
    yields(ast.DropDatabase(literalFoo, ifExists = true, composite = false, ast.DestroyData, ast.NoWait))
  }

  test("DROP DATABASE foo IF EXISTS DESTROY DATA WAIT") {
    yields(ast.DropDatabase(literal("foo"), ifExists = true, composite = false, ast.DestroyData, ast.IndefiniteWait))
  }

  test("DROP DATABASE") {
    assertFailsWithMessage(
      testName,
      s"""Invalid input '': expected a parameter or an identifier (line 1, column 14 (offset: 13))"""
    )
  }

  test("DROP DATABASE  IF EXISTS") {
    failsToParse
  }

  test("DROP DATABASE foo IF NOT EXISTS") {
    failsToParse
  }

  test("DROP DATABASE KEEP DATA") {
    val exceptionMessage =
      s"""Invalid input 'DATA': expected
         |  "."
         |  "DESTROY"
         |  "DUMP"
         |  "IF"
         |  "NOWAIT"
         |  "WAIT"
         |  <EOF> (line 1, column 20 (offset: 19))""".stripMargin

    assertFailsWithMessage(testName, exceptionMessage)
  }

  // ALTER DATABASE
  Seq(
    ("READ ONLY", ast.ReadOnlyAccess),
    ("READ WRITE", ast.ReadWriteAccess)
  ).foreach {
    case (accessKeyword, accessType) =>
      test(s"ALTER DATABASE foo SET ACCESS $accessKeyword") {
        assertAst(
          ast.AlterDatabase(literalFoo, ifExists = false, Some(accessType), None, NoOptions, Set.empty, ast.NoWait)(
            defaultPos
          )
        )
      }

      test(s"ALTER DATABASE $$foo SET ACCESS $accessKeyword") {
        assertAst(ast.AlterDatabase(
          stringParamName("foo"),
          ifExists = false,
          Some(accessType),
          None,
          NoOptions,
          Set.empty,
          ast.NoWait
        )(
          defaultPos
        ))
      }

      test(s"ALTER DATABASE `foo.bar` SET ACCESS $accessKeyword") {
        assertAst(
          ast.AlterDatabase(
            literal("foo.bar"),
            ifExists = false,
            Some(accessType),
            None,
            NoOptions,
            Set.empty,
            ast.NoWait
          )(
            defaultPos
          )
        )
      }

      test(s"USE system ALTER DATABASE foo SET ACCESS $accessKeyword") {
        // can parse USE clause, but is not included in AST
        assertAst(ast.AlterDatabase(
          literalFoo,
          ifExists = false,
          Some(accessType),
          None,
          NoOptions,
          Set.empty,
          ast.NoWait
        )((
          1,
          12,
          11
        )))
      }

      test(s"ALTER DATABASE foo IF EXISTS SET ACCESS $accessKeyword") {
        assertAst(
          ast.AlterDatabase(literalFoo, ifExists = true, Some(accessType), None, NoOptions, Set.empty, ast.NoWait)(
            defaultPos
          )
        )
      }
  }

  test("ALTER DATABASE") {
    assertFailsWithMessage(
      testName,
      "Invalid input '': expected a parameter or an identifier (line 1, column 15 (offset: 14))"
    )
  }

  test("ALTER DATABASE foo") {
    assertFailsWithMessage(
      testName,
      "Invalid input '': expected \".\", \"IF\", \"REMOVE\" or \"SET\" (line 1, column 19 (offset: 18))"
    )
  }

  test("ALTER DATABASE foo SET READ ONLY") {
    assertFailsWithMessage(
      testName,
      "Invalid input 'READ': expected \"ACCESS\", \"OPTION\" or \"TOPOLOGY\" (line 1, column 24 (offset: 23))"
    )
  }

  test("ALTER DATABASE foo ACCESS READ WRITE") {
    assertFailsWithMessage(
      testName,
      "Invalid input 'ACCESS': expected \".\", \"IF\", \"REMOVE\" or \"SET\" (line 1, column 20 (offset: 19))"
    )
  }

  test("ALTER DATABASE foo SET ACCESS READ") {
    assertFailsWithMessage(
      testName,
      "Invalid input '': expected \"ONLY\" or \"WRITE\" (line 1, column 35 (offset: 34))"
    )
  }

  test("ALTER DATABASE foo SET ACCESS READWRITE'") {
    assertFailsWithMessage(testName, "Invalid input 'READWRITE': expected \"READ\" (line 1, column 31 (offset: 30))")
  }

  test("ALTER DATABASE foo SET ACCESS READ_ONLY") {
    assertFailsWithMessage(testName, "Invalid input 'READ_ONLY': expected \"READ\" (line 1, column 31 (offset: 30))")
  }

  test("ALTER DATABASE foo SET ACCESS WRITE") {
    assertFailsWithMessage(testName, "Invalid input 'WRITE': expected \"READ\" (line 1, column 31 (offset: 30))")
  }

  // Set ACCESS multiple times in the same command
  test("ALTER DATABASE foo SET ACCESS READ ONLY SET ACCESS READ WRITE") {
    assertFailsWithMessage(testName, "Duplicate SET ACCESS clause (line 1, column 41 (offset: 40))")
  }

  // Wrong order between IF EXISTS and SET
  test("ALTER DATABASE foo SET ACCESS READ ONLY IF EXISTS") {
    assertFailsWithMessage(
      testName,
      "Invalid input 'IF': expected \"NOWAIT\", \"SET\", \"WAIT\" or <EOF> (line 1, column 41 (offset: 40))"
    )
  }

  // IF NOT EXISTS instead of IF EXISTS
  test("ALTER DATABASE foo IF NOT EXISTS SET ACCESS READ ONLY") {
    assertFailsWithMessage(testName, "Invalid input 'NOT': expected \"EXISTS\" (line 1, column 23 (offset: 22))")
  }

  // ALTER with OPTIONS
  test("ALTER DATABASE foo SET ACCESS READ WRITE OPTIONS {existingData: 'use'}") {
    assertFailsWithMessage(
      testName,
      "Invalid input 'OPTIONS': expected \"NOWAIT\", \"SET\", \"WAIT\" or <EOF> (line 1, column 42 (offset: 41))"
    )
  }

  test("ALTER DATABASE foo SET OPTION txLogEnrichment 'FULL'") {
    assertAst(
      ast.AlterDatabase(
        literalFoo,
        ifExists = false,
        None,
        None,
        OptionsMap(Map("txLogEnrichment" -> StringLiteral("FULL")(pos))),
        Set.empty,
        ast.NoWait
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET OPTION key 1") {
    assertAst(
      ast.AlterDatabase(
        literalFoo,
        ifExists = false,
        None,
        None,
        OptionsMap(Map("key" -> SignedDecimalIntegerLiteral("1")(pos))),
        Set.empty,
        ast.NoWait
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET OPTION key -1") {
    assertAst(
      ast.AlterDatabase(
        literalFoo,
        ifExists = false,
        None,
        None,
        OptionsMap(Map("key" -> SignedDecimalIntegerLiteral("-1")(pos))),
        Set.empty,
        ast.NoWait
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET OPTION key null") {
    assertAst(
      ast.AlterDatabase(
        literalFoo,
        ifExists = false,
        None,
        None,
        OptionsMap(Map("key" -> Null()(pos))),
        Set.empty,
        ast.NoWait
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET OPTION key1 1 SET OPTION key2 'two'") {
    assertAst(
      ast.AlterDatabase(
        literalFoo,
        ifExists = false,
        None,
        None,
        OptionsMap(Map(
          "key1" -> SignedDecimalIntegerLiteral("1")(pos),
          "key2" -> StringLiteral("two")(pos)
        )),
        Set.empty,
        ast.NoWait
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET ACCESS READ ONLY SET TOPOLOGY 1 PRIMARY SET OPTION txLogEnrichment 'FULL'") {
    assertAst(
      ast.AlterDatabase(
        literalFoo,
        ifExists = false,
        Some(ReadOnlyAccess),
        Some(Topology(Some(1), None)),
        OptionsMap(Map("txLogEnrichment" -> StringLiteral("FULL")(pos))),
        Set.empty,
        ast.NoWait
      )(pos)
    )
  }

  test("ALTER DATABASE foo REMOVE OPTION key REMOVE OPTION key2") {
    assertAst(
      ast.AlterDatabase(
        literalFoo,
        ifExists = false,
        None,
        None,
        NoOptions,
        Set("key", "key2"),
        ast.NoWait
      )(pos)
    )
  }

  test("ALTER DATABASE foo REMOVE OPTION key REMOVE OPTION key") {
    assertFailsWithMessage(testName, """Duplicate 'REMOVE OPTION key' clause (line 1, column 38 (offset: 37))""")
  }

  test("ALTER DATABASE foo SET ACCESS READ ONLY REMOVE OPTION key") {
    assertFailsWithMessage(
      testName,
      """Invalid input 'REMOVE': expected "NOWAIT", "SET", "WAIT" or <EOF> (line 1, column 41 (offset: 40))"""
    )
  }

  test("ALTER DATABASE foo SET OPTIONS {key: value}") {
    assertFailsWithMessage(
      testName,
      """Invalid input 'OPTIONS': expected "ACCESS", "OPTION" or "TOPOLOGY" (line 1, column 24 (offset: 23))"""
    )
  }

  test("ALTER DATABASE foo SET OPTION {key: value}") {
    assertFailsWithMessage(testName, """Invalid input '{': expected an identifier (line 1, column 31 (offset: 30))""")
  }

  test("ALTER DATABASE foo SET OPTIONS key value") {
    assertFailsWithMessage(
      testName,
      """Invalid input 'OPTIONS': expected "ACCESS", "OPTION" or "TOPOLOGY" (line 1, column 24 (offset: 23))"""
    )
  }

  test("ALTER DATABASE foo SET OPTION key value key2 value") {
    assertFailsWithMessage(
      testName,
      """Invalid input 'key2': expected
        |  "!="
        |  "%"
        |  "*"
        |  "+"
        |  "-"
        |  "/"
        |  "::"
        |  "<"
        |  "<="
        |  "<>"
        |  "="
        |  "=~"
        |  ">"
        |  ">="
        |  "AND"
        |  "CONTAINS"
        |  "ENDS"
        |  "IN"
        |  "IS"
        |  "NOWAIT"
        |  "OR"
        |  "SET"
        |  "STARTS"
        |  "WAIT"
        |  "XOR"
        |  "^"
        |  <EOF> (line 1, column 41 (offset: 40))""".stripMargin
    )
  }

  test("ALTER DATABASE foo SET OPTION key value, key2 value") {
    assertFailsWithMessage(
      testName,
      """Invalid input ',': expected
        |  "!="
        |  "%"
        |  "*"
        |  "+"
        |  "-"
        |  "/"
        |  "::"
        |  "<"
        |  "<="
        |  "<>"
        |  "="
        |  "=~"
        |  ">"
        |  ">="
        |  "AND"
        |  "CONTAINS"
        |  "ENDS"
        |  "IN"
        |  "IS"
        |  "NOWAIT"
        |  "OR"
        |  "SET"
        |  "STARTS"
        |  "WAIT"
        |  "XOR"
        |  "^"
        |  <EOF> (line 1, column 40 (offset: 39))""".stripMargin
    )
  }

  test("ALTER DATABASE foo REMOVE OPTION key key2") {
    assertFailsWithMessage(
      testName,
      """Invalid input 'key2': expected "NOWAIT", "REMOVE", "WAIT" or <EOF> (line 1, column 38 (offset: 37))"""
    )
  }

  test("ALTER DATABASE foo REMOVE OPTION key, key2") {
    assertFailsWithMessage(
      testName,
      """Invalid input ',': expected "NOWAIT", "REMOVE", "WAIT" or <EOF> (line 1, column 37 (offset: 36))"""
    )
  }

  test("ALTER DATABASE foo REMOVE OPTIONS key") {
    assertFailsWithMessage(testName, """Invalid input 'OPTIONS': expected "OPTION" (line 1, column 27 (offset: 26))""")
  }

  test("ALTER DATABASE foo SET OPTION txLogEnrichment 'FULL' SET OPTION txLogEnrichment 'FULL'") {
    assertFailsWithMessage(testName, "Duplicate 'SET OPTION txLogEnrichment' clause (line 1, column 54 (offset: 53))")
  }

  test("ALTER DATABASE foo SET OPTION txLogEnrichment 'FULL' REMOVE OPTION txLogEnrichment") {
    assertFailsWithMessage(
      testName,
      """Invalid input 'REMOVE': expected
        |  "!="
        |  "%"
        |  "*"
        |  "+"
        |  "-"
        |  "/"
        |  "::"
        |  "<"
        |  "<="
        |  "<>"
        |  "="
        |  "=~"
        |  ">"
        |  ">="
        |  "AND"
        |  "CONTAINS"
        |  "ENDS"
        |  "IN"
        |  "IS"
        |  "NOWAIT"
        |  "OR"
        |  "SET"
        |  "STARTS"
        |  "WAIT"
        |  "XOR"
        |  "^"
        |  <EOF> (line 1, column 54 (offset: 53))""".stripMargin
    )
  }

  test("ALTER DATABASE foo REMOVE OPTION txLogEnrichment SET OPTION txLogEnrichment 'FULL'") {
    assertFailsWithMessage(
      testName,
      "Invalid input 'SET': expected \"NOWAIT\", \"REMOVE\", \"WAIT\" or <EOF> (line 1, column 50 (offset: 49))"
    )
  }

  // ALTER OR REPLACE
  test("ALTER OR REPLACE DATABASE foo SET ACCESS READ WRITE") {
    assertFailsWithMessage(
      testName,
      """Invalid input 'OR': expected
        |  "ALIAS"
        |  "CURRENT"
        |  "DATABASE"
        |  "SERVER"
        |  "USER" (line 1, column 7 (offset: 6))""".stripMargin
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY") {
    assertAst(
      ast.AlterDatabase(
        literalFoo,
        ifExists = false,
        None,
        Some(Topology(Some(1), None)),
        NoOptions,
        Set.empty,
        ast.NoWait
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY $param PRIMARY") {
    assertFailsWithMessage(
      testName,
      """Invalid input '$': expected <UNSIGNED_DECIMAL_INTEGER> (line 1, column 33 (offset: 32))"""
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY $param SECONDARY") {
    assertFailsWithMessage(
      testName,
      """Invalid input '$': expected
        |  "NOWAIT"
        |  "SET"
        |  "WAIT"
        |  <EOF>
        |  <UNSIGNED_DECIMAL_INTEGER> (line 1, column 43 (offset: 42))""".stripMargin
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY 1 SECONDARY") {
    assertAst(
      ast.AlterDatabase(
        literalFoo,
        ifExists = false,
        None,
        Some(Topology(Some(1), Some(1))),
        NoOptions,
        Set.empty,
        ast.NoWait
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 SECONDARY 1 PRIMARY") {
    assertAst(
      ast.AlterDatabase(
        literalFoo,
        ifExists = false,
        None,
        Some(Topology(Some(1), Some(1))),
        NoOptions,
        Set.empty,
        ast.NoWait
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 2 PRIMARIES 1 PRIMARY") {
    assertFailsWithMessage(
      testName,
      """Duplicate PRIMARY clause (line 1, column 47 (offset: 46))""".stripMargin
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 2 SECONDARIES 1 SECONDARY") {
    assertFailsWithMessage(
      testName,
      """Duplicate SECONDARY clause (line 1, column 49 (offset: 48))""".stripMargin
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 5 PRIMARIES 10 PRIMARIES 1 PRIMARY 2 SECONDARIES") {
    assertFailsWithMessage(
      testName,
      """Duplicate PRIMARY clause (line 1, column 48 (offset: 47))"""
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY 2 SECONDARIES 1 SECONDARIES") {
    assertFailsWithMessage(
      testName,
      """Duplicate SECONDARY clause (line 1, column 59 (offset: 58))"""
    )
  }

  test("ALTER DATABASE foo SET ACCESS READ WRITE SET TOPOLOGY 1 PRIMARY 1 SECONDARY") {
    assertAst(
      ast.AlterDatabase(
        literalFoo,
        ifExists = false,
        Some(ast.ReadWriteAccess),
        Some(Topology(Some(1), Some(1))),
        NoOptions,
        Set.empty,
        ast.NoWait
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY 1 SECONDARY SET ACCESS READ WRITE") {
    assertAst(
      ast.AlterDatabase(
        literalFoo,
        ifExists = false,
        Some(ast.ReadWriteAccess),
        Some(Topology(Some(1), Some(1))),
        NoOptions,
        Set.empty,
        ast.NoWait
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY 1 SECONDARY SET ACCESS READ WRITE WAIT") {
    assertAst(
      ast.AlterDatabase(
        literalFoo,
        ifExists = false,
        Some(ast.ReadWriteAccess),
        Some(Topology(Some(1), Some(1))),
        NoOptions,
        Set.empty,
        ast.IndefiniteWait
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY 1 SECONDARY SET ACCESS READ WRITE WAIT 5") {
    assertAst(
      ast.AlterDatabase(
        literalFoo,
        ifExists = false,
        Some(ast.ReadWriteAccess),
        Some(Topology(Some(1), Some(1))),
        NoOptions,
        Set.empty,
        ast.TimeoutAfter(5)
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY 1 SECONDARY SET ACCESS READ WRITE WAIT 5 SEC") {
    assertAst(
      ast.AlterDatabase(
        literalFoo,
        ifExists = false,
        Some(ast.ReadWriteAccess),
        Some(Topology(Some(1), Some(1))),
        NoOptions,
        Set.empty,
        ast.TimeoutAfter(5)
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY 1 SECONDARY SET ACCESS READ WRITE WAIT 5 SECOND") {
    assertAst(
      ast.AlterDatabase(
        literalFoo,
        ifExists = false,
        Some(ast.ReadWriteAccess),
        Some(Topology(Some(1), Some(1))),
        NoOptions,
        Set.empty,
        ast.TimeoutAfter(5)
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY 1 SECONDARY SET ACCESS READ WRITE WAIT 5 SECONDS") {
    assertAst(
      ast.AlterDatabase(
        literalFoo,
        ifExists = false,
        Some(ast.ReadWriteAccess),
        Some(Topology(Some(1), Some(1))),
        NoOptions,
        Set.empty,
        ast.TimeoutAfter(5)
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY 1 SECONDARY SET ACCESS READ WRITE NOWAIT") {
    assertAst(
      ast.AlterDatabase(
        literalFoo,
        ifExists = false,
        Some(ast.ReadWriteAccess),
        Some(Topology(Some(1), Some(1))),
        NoOptions,
        Set.empty,
        ast.NoWait
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY SET TOPOLOGY 1 SECONDARY") {
    assertFailsWithMessage(testName, "Duplicate SET TOPOLOGY clause (line 1, column 43 (offset: 42))")
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY 1 PRIMARY") {
    assertFailsWithMessage(
      testName,
      """Duplicate PRIMARY clause (line 1, column 45 (offset: 44))""".stripMargin
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY 1 SECONDARY 2 SECONDARY") {
    assertFailsWithMessage(
      testName,
      """Duplicate SECONDARY clause (line 1, column 57 (offset: 56))""".stripMargin
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY -1 PRIMARY") {
    assertFailsWithMessage(
      testName,
      """Invalid input '-': expected <UNSIGNED_DECIMAL_INTEGER> (line 1, column 33 (offset: 32))""".stripMargin
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY -1 SECONDARY") {
    assertFailsWithMessage(
      testName,
      """Invalid input '-': expected
        |  "NOWAIT"
        |  "SET"
        |  "WAIT"
        |  <EOF>
        |  <UNSIGNED_DECIMAL_INTEGER> (line 1, column 43 (offset: 42))""".stripMargin
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY -1 SECONDARY 1 PRIMARY") {
    assertFailsWithMessage(
      testName,
      """Invalid input '-': expected <UNSIGNED_DECIMAL_INTEGER> (line 1, column 33 (offset: 32))""".stripMargin
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 SECONDARY 1 SECONDARY") {
    assertFailsWithMessage(
      testName,
      """Duplicate SECONDARY clause (line 1, column 47 (offset: 46))""".stripMargin
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 SECONDARY") {
    assertAst(
      ast.AlterDatabase(
        literalFoo,
        ifExists = false,
        None,
        Some(Topology(None, Some(1))),
        NoOptions,
        Set.empty,
        ast.NoWait
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY") {
    assertFailsWithMessage(
      testName,
      """Invalid input '': expected <UNSIGNED_DECIMAL_INTEGER> (line 1, column 32 (offset: 31))"""
    )
  }

  // START DATABASE

  test("START DATABASE foo") {
    yields(ast.StartDatabase(literalFoo, ast.NoWait))
  }

  test("START DATABASE $foo") {
    yields(ast.StartDatabase(stringParamName("foo"), ast.NoWait))
  }

  test("START DATABASE foo WAIT") {
    yields(ast.StartDatabase(literalFoo, ast.IndefiniteWait))
  }

  test("START DATABASE foo WAIT 5") {
    yields(ast.StartDatabase(literal("foo"), ast.TimeoutAfter(5)))
  }

  test("START DATABASE foo WAIT 5 SEC") {
    yields(ast.StartDatabase(literal("foo"), ast.TimeoutAfter(5)))
  }

  test("START DATABASE foo WAIT 5 SECOND") {
    yields(ast.StartDatabase(literal("foo"), ast.TimeoutAfter(5)))
  }

  test("START DATABASE foo WAIT 5 SECONDS") {
    yields(ast.StartDatabase(literal("foo"), ast.TimeoutAfter(5)))
  }

  test("START DATABASE foo NOWAIT") {
    yields(ast.StartDatabase(literalFoo, ast.NoWait))
  }

  test("START DATABASE `foo.bar`") {
    yields(_ => ast.StartDatabase(literal("foo.bar"), ast.NoWait)(pos))
  }

  test("START DATABASE foo.bar") {
    yields(_ => ast.StartDatabase(NamespacedName(List("bar"), Some("foo"))((1, 16, 15)), ast.NoWait)(pos))
  }

  test("START DATABASE") {
    assertFailsWithMessage(
      testName,
      "Invalid input '': expected a parameter or an identifier (line 1, column 15 (offset: 14))"
    )
  }

  // STOP DATABASE

  test("STOP DATABASE foo") {
    yields(ast.StopDatabase(literalFoo, ast.NoWait))
  }

  test("STOP DATABASE $foo") {
    yields(ast.StopDatabase(stringParamName("foo"), ast.NoWait))
  }

  test("STOP DATABASE foo WAIT") {
    yields(ast.StopDatabase(literalFoo, ast.IndefiniteWait))
  }

  test("STOP DATABASE foo WAIT 99") {
    yields(ast.StopDatabase(literal("foo"), ast.TimeoutAfter(99)))
  }

  test("STOP DATABASE foo WAIT 99 SEC") {
    yields(ast.StopDatabase(literal("foo"), ast.TimeoutAfter(99)))
  }

  test("STOP DATABASE foo WAIT 99 SECOND") {
    yields(ast.StopDatabase(literal("foo"), ast.TimeoutAfter(99)))
  }

  test("STOP DATABASE foo WAIT 99 SECONDS") {
    yields(ast.StopDatabase(literal("foo"), ast.TimeoutAfter(99)))
  }

  test("STOP DATABASE foo NOWAIT") {
    yields(ast.StopDatabase(literalFoo, ast.NoWait))
  }

  test("STOP DATABASE `foo.bar`") {
    yields(_ => ast.StopDatabase(literal("foo.bar"), ast.NoWait)(pos))
  }

  test("STOP DATABASE foo.bar") {
    yields(_ => ast.StopDatabase(NamespacedName(List("bar"), Some("foo"))((1, 16, 15)), ast.NoWait)(pos))
  }

  test("STOP DATABASE") {
    assertFailsWithMessage(
      testName,
      "Invalid input '': expected a parameter or an identifier (line 1, column 14 (offset: 13))"
    )
  }
}
