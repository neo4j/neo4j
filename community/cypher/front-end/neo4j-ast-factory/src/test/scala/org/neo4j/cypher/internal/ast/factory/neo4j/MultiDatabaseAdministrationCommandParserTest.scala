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
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.Topology
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.Antlr
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.JavaCc
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.Null
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.exceptions.SyntaxException

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
      parsesTo[Statements](privilege(None)(pos))
    }

    test(s"USE system SHOW $dbType") {
      parsesTo[Statements](privilege(None)(pos).withGraph(Some(use(List("system")))))
    }

    test(s"SHOW $dbType WHERE access = 'GRANTED'") {
      parsesTo[Statements](privilege(Some(Right(where(equals(accessVar, grantedString)))))(pos))
    }

    test(s"SHOW $dbType WHERE access = 'GRANTED' AND action = 'match'") {
      val accessPredicate = equals(accessVar, grantedString)
      val matchPredicate = equals(varFor(actionString), literalString("match"))
      parsesTo[Statements](privilege(Some(Right(where(and(accessPredicate, matchPredicate)))))(pos))
    }

    test(s"SHOW $dbType YIELD access ORDER BY access") {
      val orderByClause = orderBy(sortItem(accessVar))
      val columns = yieldClause(returnItems(variableReturnItem(accessString)), Some(orderByClause))
      parsesTo[Statements](privilege(Some(Left((columns, None))))(pos))
    }

    test(s"SHOW $dbType YIELD access ORDER BY access WHERE access ='none'") {
      val orderByClause = orderBy(sortItem(accessVar))
      val whereClause = where(equals(accessVar, noneString))
      val columns =
        yieldClause(returnItems(variableReturnItem(accessString)), Some(orderByClause), where = Some(whereClause))
      parsesTo[Statements](privilege(Some(Left((columns, None))))(pos))
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
      parsesTo[Statements](privilege(Some(Left((columns, None))))(pos))
    }

    test(s"SHOW $dbType YIELD access SKIP -1") {
      val columns = yieldClause(returnItems(variableReturnItem(accessString)), skip = Some(skip(-1)))
      parsesTo[Statements](privilege(Some(Left((columns, None))))(pos))
    }

    test(s"SHOW $dbType YIELD access ORDER BY access RETURN access") {
      parsesTo[Statements](privilege(
        Some(Left((
          yieldClause(returnItems(variableReturnItem(accessString)), Some(orderBy(sortItem(accessVar)))),
          Some(returnClause(returnItems(variableReturnItem(accessString))))
        )))
      )(pos))
    }

    test(s"SHOW $dbType WHERE access = 'GRANTED' RETURN action") {
      failsParsing[Statements]
    }

    test(s"SHOW $dbType YIELD * RETURN *") {
      parsesTo[Statements](
        privilege(Some(Left((yieldClause(returnAllItems), Some(returnClause(returnAllItems))))))(pos)
      )
    }
  }

  test("SHOW DATABASE `foo.bar`") {
    parsesTo[Statements](ast.ShowDatabase(ast.SingleNamedDatabaseScope(namespacedName("foo.bar"))(pos), None)(pos))
  }

  test("SHOW DATABASE foo.bar") {
    parsesTo[Statements](ast.ShowDatabase(ast.SingleNamedDatabaseScope(namespacedName("foo", "bar"))(pos), None)(pos))
  }

  test("SHOW DATABASE blah YIELD *,blah RETURN user") {
    failsParsing[Statements]
  }

  test("SHOW DATABASE YIELD (123 + xyz)") {
    failsParsing[Statements]
  }

  test("SHOW DATABASES YIELD (123 + xyz) AS foo") {
    failsParsing[Statements]
  }

  test("SHOW DEFAULT DATABASES") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Invalid input 'DATABASES': expected "DATABASE" (line 1, column 14 (offset: 13))"""
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'DATABASES': expected 'DATABASE' (line 1, column 14 (offset: 13))
          |"SHOW DEFAULT DATABASES"
          |              ^""".stripMargin
      ))
  }

  test("SHOW DEFAULT DATABASES YIELD *") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Invalid input 'DATABASES': expected "DATABASE" (line 1, column 14 (offset: 13))"""
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'DATABASES': expected 'DATABASE' (line 1, column 14 (offset: 13))
          |"SHOW DEFAULT DATABASES YIELD *"
          |              ^""".stripMargin
      ))
  }

  test("SHOW DEFAULT DATABASES WHERE name STARTS WITH 'foo'") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Invalid input 'DATABASES': expected "DATABASE" (line 1, column 14 (offset: 13))"""
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'DATABASES': expected 'DATABASE' (line 1, column 14 (offset: 13))
          |"SHOW DEFAULT DATABASES WHERE name STARTS WITH 'foo'"
          |              ^""".stripMargin
      ))
  }

  test("SHOW HOME DATABASES") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Invalid input 'DATABASES': expected "DATABASE" (line 1, column 11 (offset: 10))"""
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'DATABASES': expected 'DATABASE' (line 1, column 11 (offset: 10))
          |"SHOW HOME DATABASES"
          |           ^""".stripMargin
      ))
  }

  test("SHOW HOME DATABASES YIELD *") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Invalid input 'DATABASES': expected "DATABASE" (line 1, column 11 (offset: 10))"""
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'DATABASES': expected 'DATABASE' (line 1, column 11 (offset: 10))
          |"SHOW HOME DATABASES YIELD *"
          |           ^""".stripMargin
      ))
  }

  test("SHOW HOME DATABASES WHERE name STARTS WITH 'foo'") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Invalid input 'DATABASES': expected "DATABASE" (line 1, column 11 (offset: 10))"""
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'DATABASES': expected 'DATABASE' (line 1, column 11 (offset: 10))
          |"SHOW HOME DATABASES WHERE name STARTS WITH 'foo'"
          |           ^""".stripMargin
      ))
  }

  // CREATE DATABASE

  test("CREATE DATABASE foo") {
    parsesTo[Statements](ast.CreateDatabase(literalFoo, ast.IfExistsThrowError, ast.NoOptions, ast.NoWait, None)(pos))
  }

  test("USE system CREATE DATABASE foo") {
    // can parse USE clause, but is not included in AST
    parsesTo[Statements] {
      ast.CreateDatabase(literalFoo, ast.IfExistsThrowError, ast.NoOptions, ast.NoWait, None)(pos)
        .withGraph(Some(use(List("system"))))
    }
  }

  test("CREATE DATABASE $foo") {
    parsesTo[Statements](ast.CreateDatabase(
      stringParamName("foo"),
      ast.IfExistsThrowError,
      ast.NoOptions,
      ast.NoWait,
      None
    )(pos))
  }

  test("CREATE DATABASE $wait") {
    parsesTo[Statements](ast.CreateDatabase(
      stringParamName("wait"),
      ast.IfExistsThrowError,
      ast.NoOptions,
      ast.NoWait,
      None
    )(pos))
  }

  test("CREATE DATABASE `nowait.sec`") {
    parsesTo[Statements](ast.CreateDatabase(
      literal("nowait.sec"),
      ast.IfExistsThrowError,
      ast.NoOptions,
      ast.NoWait,
      None
    )(pos))
  }

  test("CREATE DATABASE second WAIT") {
    parsesTo[Statements](ast.CreateDatabase(
      literal("second"),
      ast.IfExistsThrowError,
      ast.NoOptions,
      ast.IndefiniteWait,
      None
    )(pos))
  }

  test("CREATE DATABASE seconds WAIT 12") {
    parsesTo[Statements](ast.CreateDatabase(
      literal("seconds"),
      ast.IfExistsThrowError,
      ast.NoOptions,
      ast.TimeoutAfter(12),
      None
    )(pos))
  }

  test("CREATE DATABASE dump WAIT 12 SEC") {
    parsesTo[Statements](ast.CreateDatabase(
      literal("dump"),
      ast.IfExistsThrowError,
      ast.NoOptions,
      ast.TimeoutAfter(12),
      None
    )(pos))
  }

  test("CREATE DATABASE destroy WAIT 12 SECOND") {
    parsesTo[Statements](ast.CreateDatabase(
      literal("destroy"),
      ast.IfExistsThrowError,
      ast.NoOptions,
      ast.TimeoutAfter(12),
      None
    )(pos))
  }

  test("CREATE DATABASE data WAIT 12 SECONDS") {
    parsesTo[Statements](ast.CreateDatabase(
      literal("data"),
      ast.IfExistsThrowError,
      ast.NoOptions,
      ast.TimeoutAfter(12),
      None
    )(pos))
  }

  test("CREATE DATABASE foo NOWAIT") {
    parsesTo[Statements](
      ast.CreateDatabase(literal("foo"), ast.IfExistsThrowError, ast.NoOptions, ast.NoWait, None)(pos)
    )
  }

  test("CREATE DATABASE `foo.bar`") {
    parsesTo[Statements](ast.CreateDatabase(
      literal("foo.bar"),
      ast.IfExistsThrowError,
      ast.NoOptions,
      ast.NoWait,
      None
    )(pos))
  }

  test("CREATE DATABASE foo.bar") {
    parsesTo[Statements](ast.CreateDatabase(
      namespacedName("foo", "bar"),
      ast.IfExistsThrowError,
      ast.NoOptions,
      ast.NoWait,
      None
    )(pos))
  }

  test("CREATE DATABASE `graph.db`.`db.db`") {
    parsesTo[Statements](
      ast.CreateDatabase(
        namespacedName("graph.db", "db.db"),
        ast.IfExistsThrowError,
        ast.NoOptions,
        ast.NoWait,
        None
      )(pos)
    )
  }

  test("CREATE DATABASE `foo-bar42`") {
    parsesTo[Statements](
      ast.CreateDatabase(literal("foo-bar42"), ast.IfExistsThrowError, ast.NoOptions, ast.NoWait, None)(pos)
    )
  }

  test("CREATE DATABASE `_foo-bar42`") {
    parsesTo[Statements](
      ast.CreateDatabase(literal("_foo-bar42"), ast.IfExistsThrowError, ast.NoOptions, ast.NoWait, None)(pos)
    )
  }

  test("CREATE DATABASE ``") {
    parsesTo[Statements](
      ast.CreateDatabase(literal(""), ast.IfExistsThrowError, ast.NoOptions, ast.NoWait, None)(pos)
    )
  }

  test("CREATE DATABASE foo IF NOT EXISTS") {
    parsesTo[Statements](ast.CreateDatabase(literalFoo, ast.IfExistsDoNothing, ast.NoOptions, ast.NoWait, None)(pos))
  }

  test("CREATE DATABASE foo IF NOT EXISTS WAIT 10 SECONDS") {
    parsesTo[Statements](ast.CreateDatabase(
      literalFoo,
      ast.IfExistsDoNothing,
      ast.NoOptions,
      ast.TimeoutAfter(10),
      None
    )(pos))
  }

  test("CREATE DATABASE foo IF NOT EXISTS WAIT") {
    parsesTo[Statements](
      ast.CreateDatabase(literalFoo, ast.IfExistsDoNothing, ast.NoOptions, ast.IndefiniteWait, None)(pos)
    )
  }

  test("CREATE  DATABASE foo IF NOT EXISTS NOWAIT") {
    parsesTo[Statements](ast.CreateDatabase(literalFoo, ast.IfExistsDoNothing, ast.NoOptions, ast.NoWait, None)(pos))
  }

  test("CREATE DATABASE `_foo-bar42` IF NOT EXISTS") {
    parsesTo[Statements](
      ast.CreateDatabase(literal("_foo-bar42"), ast.IfExistsDoNothing, ast.NoOptions, ast.NoWait, None)(pos)
    )
  }

  test("CREATE OR REPLACE DATABASE foo") {
    parsesTo[Statements](ast.CreateDatabase(literalFoo, ast.IfExistsReplace, ast.NoOptions, ast.NoWait, None)(pos))
  }

  test("CREATE OR REPLACE DATABASE foo WAIT 10 SECONDS") {
    parsesTo[Statements](
      ast.CreateDatabase(literalFoo, ast.IfExistsReplace, ast.NoOptions, ast.TimeoutAfter(10), None)(pos)
    )
  }

  test("CREATE OR REPLACE DATABASE foo WAIT") {
    parsesTo[Statements](
      ast.CreateDatabase(literalFoo, ast.IfExistsReplace, ast.NoOptions, ast.IndefiniteWait, None)(pos)
    )
  }

  test("CREATE OR REPLACE DATABASE foo NOWAIT") {
    parsesTo[Statements](ast.CreateDatabase(literalFoo, ast.IfExistsReplace, ast.NoOptions, ast.NoWait, None)(pos))
  }

  test("CREATE OR REPLACE DATABASE `_foo-bar42`") {
    parsesTo[Statements](
      ast.CreateDatabase(literal("_foo-bar42"), ast.IfExistsReplace, ast.NoOptions, ast.NoWait, None)(pos)
    )
  }

  test("CREATE OR REPLACE DATABASE foo IF NOT EXISTS") {
    parsesTo[Statements](
      ast.CreateDatabase(literalFoo, ast.IfExistsInvalidSyntax, ast.NoOptions, ast.NoWait, None)(pos)
    )
  }

  test("CREATE DATABASE") {
    // missing db name but parses as 'normal' cypher CREATE...
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        s"""Invalid input '': expected a parameter or an identifier (line 1, column 16 (offset: 15))"""
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input '': expected a database name, a graph pattern or a parameter (line 1, column 16 (offset: 15))
          |"CREATE DATABASE"
          |                ^""".stripMargin
      ))
  }

  test("CREATE DATABASE \"foo.bar\"") {
    failsParsing[Statements]
  }

  test("CREATE DATABASE foo-bar42") {
    failsParsing[Statements]
  }

  test("CREATE DATABASE _foo-bar42") {
    failsParsing[Statements]
  }

  test("CREATE DATABASE 42foo-bar") {
    failsParsing[Statements]
  }

  test("CREATE DATABASE _foo-bar42 IF NOT EXISTS") {
    failsParsing[Statements]
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

    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(exceptionMessage))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'NOT': expected a database name, 'IF NOT EXISTS', 'NOWAIT', 'OPTIONS', 'TOPOLOGY', 'WAIT' or <EOF> (line 1, column 21 (offset: 20))
          |"CREATE DATABASE  IF NOT EXISTS"
          |                     ^""".stripMargin
      ))
  }

  test("CREATE DATABASE foo IF EXISTS") {
    failsParsing[Statements]
  }

  test("CREATE DATABASE foo WAIT -12") {
    failsParsing[Statements]
  }

  test("CREATE DATABASE foo WAIT 3.14") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input '3.14': expected <EOF> or <UNSIGNED_DECIMAL_INTEGER> (line 1, column 26 (offset: 25))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input '3.14': expected <EOF> or an integer value (line 1, column 26 (offset: 25))
          |"CREATE DATABASE foo WAIT 3.14"
          |                          ^""".stripMargin
      ))
  }

  test("CREATE DATABASE foo WAIT bar") {
    failsParsing[Statements]
  }

  test("CREATE OR REPLACE DATABASE _foo-bar42") {
    failsParsing[Statements]
  }

  test("CREATE OR REPLACE DATABASE") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        s"""Invalid input '': expected a parameter or an identifier (line 1, column 27 (offset: 26))"""
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input '': expected a database name or a parameter (line 1, column 27 (offset: 26))
          |"CREATE OR REPLACE DATABASE"
          |                           ^""".stripMargin
      ))
  }

  test(
    "CREATE DATABASE foo OPTIONS {existingData: 'use', existingDataSeedInstance: '84c3ee6f-260e-47db-a4b6-589c807f2c2e'}"
  ) {
    assertAst(
      ast.CreateDatabase(
        NamespacedName("foo")((1, 17, 16)),
        ast.IfExistsThrowError,
        ast.OptionsMap(Map(
          "existingData" -> StringLiteral("use")((1, 44, 43).withInputLength(5)),
          "existingDataSeedInstance" -> StringLiteral("84c3ee6f-260e-47db-a4b6-589c807f2c2e")(
            (1, 77, 76).withInputLength(38)
          )
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
          "existingData" -> StringLiteral("use")((1, 44, 43).withInputLength(5)),
          "existingDataSeedInstance" -> StringLiteral("84c3ee6f-260e-47db-a4b6-589c807f2c2e")(
            (1, 77, 76).withInputLength(38)
          )
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
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Invalid input 'SET': expected
          |  "."
          |  "IF"
          |  "NOWAIT"
          |  "OPTIONS"
          |  "TOPOLOGY"
          |  "WAIT"
          |  <EOF> (line 1, column 21 (offset: 20))""".stripMargin
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'SET': expected a database name, 'IF NOT EXISTS', 'NOWAIT', 'OPTIONS', 'TOPOLOGY', 'WAIT' or <EOF> (line 1, column 21 (offset: 20))
          |"CREATE DATABASE foo SET OPTION key value"
          |                     ^""".stripMargin
      ))
  }

  test("CREATE DATABASE foo OPTION {key: value}") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Invalid input 'OPTION': expected
          |  "."
          |  "IF"
          |  "NOWAIT"
          |  "OPTIONS"
          |  "TOPOLOGY"
          |  "WAIT"
          |  <EOF> (line 1, column 21 (offset: 20))""".stripMargin
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'OPTION': expected a database name, 'IF NOT EXISTS', 'NOWAIT', 'OPTIONS', 'TOPOLOGY', 'WAIT' or <EOF> (line 1, column 21 (offset: 20))
          |"CREATE DATABASE foo OPTION {key: value}"
          |                     ^""".stripMargin
      ))
  }

  test("CREATE DATABASE foo SET OPTIONS key value") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Invalid input 'SET': expected
          |  "."
          |  "IF"
          |  "NOWAIT"
          |  "OPTIONS"
          |  "TOPOLOGY"
          |  "WAIT"
          |  <EOF> (line 1, column 21 (offset: 20))""".stripMargin
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'SET': expected a database name, 'IF NOT EXISTS', 'NOWAIT', 'OPTIONS', 'TOPOLOGY', 'WAIT' or <EOF> (line 1, column 21 (offset: 20))
          |"CREATE DATABASE foo SET OPTIONS key value"
          |                     ^""".stripMargin
      ))
  }

  test("CREATE DATABASE foo OPTIONS key value") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Invalid input 'key': expected "{" or a parameter (line 1, column 29 (offset: 28))""".stripMargin
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'key': expected a parameter or '{' (line 1, column 29 (offset: 28))
          |"CREATE DATABASE foo OPTIONS key value"
          |                             ^""".stripMargin
      ))
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
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Invalid input 'TOPOLOGY': expected
          |  "NOWAIT"
          |  "OPTIONS"
          |  "WAIT"
          |  <EOF>
          |  <UNSIGNED_DECIMAL_INTEGER> (line 1, column 40 (offset: 39))""".stripMargin
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'TOPOLOGY': expected 'NOWAIT', 'OPTIONS', 'WAIT', <EOF> or an integer value (line 1, column 40 (offset: 39))
          |"CREATE DATABASE foo TOPOLOGY 1 PRIMARY TOPOLOGY 1 SECONDARY"
          |                                        ^""".stripMargin
      ))
  }

  test("CREATE DATABASE foo TOPOLOGY 1 PRIMARY 1 PRIMARY") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Duplicate PRIMARY clause (line 1, column 42 (offset: 41))""".stripMargin
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
        """Duplicate PRIMARY clause (line 1, column 40 (offset: 39))""".stripMargin
      ))
  }

  test("CREATE DATABASE foo TOPOLOGY 2 PRIMARIES 1 PRIMARY") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Duplicate PRIMARY clause (line 1, column 44 (offset: 43))""".stripMargin
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
        """Duplicate PRIMARY clause (line 1, column 42 (offset: 41))"""
      ))
  }

  test("CREATE DATABASE foo TOPOLOGY 2 SECONDARIES 1 SECONDARY") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Duplicate SECONDARY clause (line 1, column 46 (offset: 45))""".stripMargin
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
        """Duplicate SECONDARY clause (line 1, column 44 (offset: 43))""".stripMargin
      ))
  }

  test("CREATE DATABASE foo TOPOLOGY 1 PRIMARY 1 SECONDARY 2 SECONDARY") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Duplicate SECONDARY clause (line 1, column 54 (offset: 53))""".stripMargin
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
        """Duplicate SECONDARY clause (line 1, column 52 (offset: 51))""".stripMargin
      ))
  }

  test("CREATE DATABASE foo TOPOLOGY -1 PRIMARY") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Invalid input '-': expected <UNSIGNED_DECIMAL_INTEGER> (line 1, column 30 (offset: 29))""".stripMargin
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input '-': expected an integer value (line 1, column 30 (offset: 29))
          |"CREATE DATABASE foo TOPOLOGY -1 PRIMARY"
          |                              ^""".stripMargin
      ))
  }

  test("CREATE DATABASE foo TOPOLOGY 1 PRIMARY -1 SECONDARY") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Invalid input '-': expected
          |  "NOWAIT"
          |  "OPTIONS"
          |  "WAIT"
          |  <EOF>
          |  <UNSIGNED_DECIMAL_INTEGER> (line 1, column 40 (offset: 39))""".stripMargin
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input '-': expected 'NOWAIT', 'OPTIONS', 'WAIT', <EOF> or an integer value (line 1, column 40 (offset: 39))
          |"CREATE DATABASE foo TOPOLOGY 1 PRIMARY -1 SECONDARY"
          |                                        ^""".stripMargin
      ))
  }

  test("CREATE DATABASE foo TOPOLOGY -1 SECONDARY 1 PRIMARY") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Invalid input '-': expected <UNSIGNED_DECIMAL_INTEGER> (line 1, column 30 (offset: 29))""".stripMargin
      ))
      // Modify update error message. -1 is an integer...
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input '-': expected an integer value (line 1, column 30 (offset: 29))
          |"CREATE DATABASE foo TOPOLOGY -1 SECONDARY 1 PRIMARY"
          |                              ^""".stripMargin
      ))
  }

  test("CREATE DATABASE foo TOPOLOGY 1 SECONDARY 1 SECONDARY") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Duplicate SECONDARY clause (line 1, column 44 (offset: 43))""".stripMargin
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
        """Duplicate SECONDARY clause (line 1, column 42 (offset: 41))""".stripMargin
      ))
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
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Invalid input '$': expected <UNSIGNED_DECIMAL_INTEGER> (line 1, column 30 (offset: 29))"""
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input '$': expected an integer value (line 1, column 30 (offset: 29))
          |"CREATE DATABASE foo TOPOLOGY $param PRIMARY"
          |                              ^""".stripMargin
      ))
  }

  test("CREATE DATABASE foo TOPOLOGY 1 PRIMARY $param SECONDARY") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Invalid input '$': expected
          |  "NOWAIT"
          |  "OPTIONS"
          |  "WAIT"
          |  <EOF>
          |  <UNSIGNED_DECIMAL_INTEGER> (line 1, column 40 (offset: 39))""".stripMargin
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input '$': expected 'NOWAIT', 'OPTIONS', 'WAIT', <EOF> or an integer value (line 1, column 40 (offset: 39))
          |"CREATE DATABASE foo TOPOLOGY 1 PRIMARY $param SECONDARY"
          |                                        ^""".stripMargin
      ))
  }

  test("CREATE DATABASE foo TOPOLOGY") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Invalid input '': expected <UNSIGNED_DECIMAL_INTEGER> (line 1, column 29 (offset: 28))"""
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input '': expected an integer value (line 1, column 29 (offset: 28))
          |"CREATE DATABASE foo TOPOLOGY"
          |                             ^""".stripMargin
      ))
  }

  test("CREATE DATABASE alias") {
    parsesTo[Statements](
      ast.CreateDatabase(literal("alias"), ast.IfExistsThrowError, ast.NoOptions, ast.NoWait, None)(pos)
    )
  }

  test("CREATE DATABASE alias IF NOT EXISTS") {
    parsesTo[Statements](
      ast.CreateDatabase(literal("alias"), ast.IfExistsDoNothing, ast.NoOptions, ast.NoWait, None)(pos)
    )
  }

  // DROP DATABASE

  test("DROP DATABASE foo") {
    parsesTo[Statements](ast.DropDatabase(
      literal("foo"),
      ifExists = false,
      composite = false,
      ast.DestroyData,
      ast.NoWait
    )(pos))
  }

  test("DROP DATABASE alias") {
    parsesTo[Statements](ast.DropDatabase(
      literal("alias"),
      ifExists = false,
      composite = false,
      ast.DestroyData,
      ast.NoWait
    )(pos))
  }

  test("DROP DATABASE alias WAIT") {
    parsesTo[Statements](ast.DropDatabase(
      literal("alias"),
      ifExists = false,
      composite = false,
      ast.DestroyData,
      ast.IndefiniteWait
    )(pos))
  }

  test("DROP DATABASE alias NOWAIT") {
    parsesTo[Statements](ast.DropDatabase(
      literal("alias"),
      ifExists = false,
      composite = false,
      ast.DestroyData,
      ast.NoWait
    )(pos))
  }

  test("DROP DATABASE $foo") {
    parsesTo[Statements](ast.DropDatabase(
      stringParamName("foo"),
      ifExists = false,
      composite = false,
      ast.DestroyData,
      ast.NoWait
    )(pos))
  }

  test("DROP DATABASE foo WAIT") {
    parsesTo[Statements](ast.DropDatabase(
      literal("foo"),
      ifExists = false,
      composite = false,
      ast.DestroyData,
      ast.IndefiniteWait
    )(pos))
  }

  test("DROP DATABASE foo WAIT 10") {
    parsesTo[Statements](ast.DropDatabase(
      literal("foo"),
      ifExists = false,
      composite = false,
      ast.DestroyData,
      ast.TimeoutAfter(10)
    )(pos))
  }

  test("DROP DATABASE foo WAIT 10 SEC") {
    parsesTo[Statements](ast.DropDatabase(
      literal("foo"),
      ifExists = false,
      composite = false,
      ast.DestroyData,
      ast.TimeoutAfter(10)
    )(pos))
  }

  test("DROP DATABASE foo WAIT 10 SECOND") {
    parsesTo[Statements](ast.DropDatabase(
      literal("foo"),
      ifExists = false,
      composite = false,
      ast.DestroyData,
      ast.TimeoutAfter(10)
    )(pos))
  }

  test("DROP DATABASE foo WAIT 10 SECONDS") {
    parsesTo[Statements](ast.DropDatabase(
      literal("foo"),
      ifExists = false,
      composite = false,
      ast.DestroyData,
      ast.TimeoutAfter(10)
    )(pos))
  }

  test("DROP DATABASE foo NOWAIT") {
    parsesTo[Statements](
      ast.DropDatabase(literalFoo, ifExists = false, composite = false, ast.DestroyData, ast.NoWait)(pos)
    )
  }

  test("DROP DATABASE `foo.bar`") {
    parsesTo[Statements](
      ast.DropDatabase(literal("foo.bar"), ifExists = false, composite = false, ast.DestroyData, ast.NoWait)(pos)
    )
  }

  test("DROP DATABASE foo.bar") {
    parsesTo[Statements](
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
    parsesTo[Statements](
      ast.DropDatabase(literalFoo, ifExists = true, composite = false, ast.DestroyData, ast.NoWait)(pos)
    )
  }

  test("DROP DATABASE foo IF EXISTS WAIT") {
    parsesTo[Statements](ast.DropDatabase(
      literalFoo,
      ifExists = true,
      composite = false,
      ast.DestroyData,
      ast.IndefiniteWait
    )(pos))
  }

  test("DROP DATABASE foo IF EXISTS NOWAIT") {
    parsesTo[Statements](
      ast.DropDatabase(literalFoo, ifExists = true, composite = false, ast.DestroyData, ast.NoWait)(pos)
    )
  }

  test("DROP DATABASE foo DUMP DATA") {
    parsesTo[Statements](
      ast.DropDatabase(literalFoo, ifExists = false, composite = false, ast.DumpData, ast.NoWait)(pos)
    )
  }

  test("DROP DATABASE foo DESTROY DATA") {
    parsesTo[Statements](
      ast.DropDatabase(literalFoo, ifExists = false, composite = false, ast.DestroyData, ast.NoWait)(pos)
    )
  }

  test("DROP DATABASE foo IF EXISTS DUMP DATA") {
    parsesTo[Statements](
      ast.DropDatabase(literalFoo, ifExists = true, composite = false, ast.DumpData, ast.NoWait)(pos)
    )
  }

  test("DROP DATABASE foo IF EXISTS DESTROY DATA") {
    parsesTo[Statements](
      ast.DropDatabase(literalFoo, ifExists = true, composite = false, ast.DestroyData, ast.NoWait)(pos)
    )
  }

  test("DROP DATABASE foo IF EXISTS DESTROY DATA WAIT") {
    parsesTo[Statements](ast.DropDatabase(
      literal("foo"),
      ifExists = true,
      composite = false,
      ast.DestroyData,
      ast.IndefiniteWait
    )(pos))
  }

  test("DROP DATABASE") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        s"""Invalid input '': expected a parameter or an identifier (line 1, column 14 (offset: 13))"""
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input '': expected a database name or a parameter (line 1, column 14 (offset: 13))
          |"DROP DATABASE"
          |              ^""".stripMargin
      ))
  }

  test("DROP DATABASE  IF EXISTS") {
    failsParsing[Statements]
  }

  test("DROP DATABASE foo IF NOT EXISTS") {
    failsParsing[Statements]
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

    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(exceptionMessage))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'DATA': expected a database name, 'DESTROY', 'DUMP', 'IF EXISTS', 'NOWAIT', 'WAIT' or <EOF> (line 1, column 20 (offset: 19))
          |"DROP DATABASE KEEP DATA"
          |                    ^""".stripMargin
      ))
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
        )).withGraph(Some(use(List("system")))))
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
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input '': expected a parameter or an identifier (line 1, column 15 (offset: 14))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input '': expected a database name or a parameter (line 1, column 15 (offset: 14))
          |"ALTER DATABASE"
          |               ^""".stripMargin
      ))
  }

  test("ALTER DATABASE foo") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input '': expected \".\", \"IF\", \"REMOVE\" or \"SET\" (line 1, column 19 (offset: 18))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input '': expected a database name, 'IF EXISTS', 'REMOVE OPTION' or 'SET' (line 1, column 19 (offset: 18))
          |"ALTER DATABASE foo"
          |                   ^""".stripMargin
      ))
  }

  test("ALTER DATABASE foo SET READ ONLY") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input 'READ': expected \"ACCESS\", \"OPTION\" or \"TOPOLOGY\" (line 1, column 24 (offset: 23))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'READ': expected 'OPTION', 'ACCESS READ' or 'TOPOLOGY' (line 1, column 24 (offset: 23))
          |"ALTER DATABASE foo SET READ ONLY"
          |                        ^""".stripMargin
      ))
  }

  test("ALTER DATABASE foo ACCESS READ WRITE") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input 'ACCESS': expected \".\", \"IF\", \"REMOVE\" or \"SET\" (line 1, column 20 (offset: 19))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'ACCESS': expected a database name, 'IF EXISTS', 'REMOVE OPTION' or 'SET' (line 1, column 20 (offset: 19))
          |"ALTER DATABASE foo ACCESS READ WRITE"
          |                    ^""".stripMargin
      ))
  }

  test("ALTER DATABASE foo SET ACCESS READ") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input '': expected \"ONLY\" or \"WRITE\" (line 1, column 35 (offset: 34))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input '': expected 'ONLY' or 'WRITE' (line 1, column 35 (offset: 34))
          |"ALTER DATABASE foo SET ACCESS READ"
          |                                   ^""".stripMargin
      ))
  }

  test("ALTER DATABASE foo SET ACCESS READWRITE'") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input 'READWRITE': expected \"READ\" (line 1, column 31 (offset: 30))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'READWRITE': expected 'READ' (line 1, column 31 (offset: 30))
          |"ALTER DATABASE foo SET ACCESS READWRITE'"
          |                               ^""".stripMargin
      ))
  }

  test("ALTER DATABASE foo SET ACCESS READ_ONLY") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input 'READ_ONLY': expected \"READ\" (line 1, column 31 (offset: 30))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'READ_ONLY': expected 'READ' (line 1, column 31 (offset: 30))
          |"ALTER DATABASE foo SET ACCESS READ_ONLY"
          |                               ^""".stripMargin
      ))
  }

  test("ALTER DATABASE foo SET ACCESS WRITE") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input 'WRITE': expected \"READ\" (line 1, column 31 (offset: 30))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'WRITE': expected 'READ' (line 1, column 31 (offset: 30))
          |"ALTER DATABASE foo SET ACCESS WRITE"
          |                               ^""".stripMargin
      ))
  }

  // Set ACCESS multiple times in the same command
  test("ALTER DATABASE foo SET ACCESS READ ONLY SET ACCESS READ WRITE") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart("Duplicate SET ACCESS clause (line 1, column 41 (offset: 40))"))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
        """Duplicate ACCESS clause (line 1, column 45 (offset: 44))"""
      ))
  }

  // Wrong order between IF EXISTS and SET
  test("ALTER DATABASE foo SET ACCESS READ ONLY IF EXISTS") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input 'IF': expected \"NOWAIT\", \"SET\", \"WAIT\" or <EOF> (line 1, column 41 (offset: 40))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'IF': expected 'NOWAIT', 'SET', 'WAIT' or <EOF> (line 1, column 41 (offset: 40))
          |"ALTER DATABASE foo SET ACCESS READ ONLY IF EXISTS"
          |                                         ^""".stripMargin
      ))
  }

  // IF NOT EXISTS instead of IF EXISTS
  test("ALTER DATABASE foo IF NOT EXISTS SET ACCESS READ ONLY") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input 'NOT': expected \"EXISTS\" (line 1, column 23 (offset: 22))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'NOT': expected 'EXISTS' (line 1, column 23 (offset: 22))
          |"ALTER DATABASE foo IF NOT EXISTS SET ACCESS READ ONLY"
          |                       ^""".stripMargin
      ))
  }

  // ALTER with OPTIONS
  test("ALTER DATABASE foo SET ACCESS READ WRITE OPTIONS {existingData: 'use'}") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input 'OPTIONS': expected \"NOWAIT\", \"SET\", \"WAIT\" or <EOF> (line 1, column 42 (offset: 41))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'OPTIONS': expected 'NOWAIT', 'SET', 'WAIT' or <EOF> (line 1, column 42 (offset: 41))
          |"ALTER DATABASE foo SET ACCESS READ WRITE OPTIONS {existingData: 'use'}"
          |                                          ^""".stripMargin
      ))
  }

  test("ALTER DATABASE foo SET OPTION txLogEnrichment 'FULL'") {
    assertAst(
      ast.AlterDatabase(
        literalFoo,
        ifExists = false,
        None,
        None,
        OptionsMap(Map("txLogEnrichment" -> StringLiteral("FULL")(pos.withInputLength(0)))),
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
    parsesTo[Statements](
      ast.AlterDatabase(
        literalFoo,
        ifExists = false,
        None,
        None,
        OptionsMap(Map(
          "key1" -> SignedDecimalIntegerLiteral("1")(pos),
          "key2" -> StringLiteral("two")(pos.withInputLength(0))
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
        OptionsMap(Map("txLogEnrichment" -> StringLiteral("FULL")(pos.withInputLength(0)))),
        Set.empty,
        ast.NoWait
      )(pos)
    )
  }

  test("ALTER DATABASE foo REMOVE OPTION key REMOVE OPTION key2") {
    parsesTo[Statements](
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
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Duplicate 'REMOVE OPTION key' clause (line 1, column 38 (offset: 37))"""
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
        """Duplicate 'REMOVE OPTION key' clause (line 1, column 52 (offset: 51))"""
      ))
  }

  test("ALTER DATABASE foo SET ACCESS READ ONLY REMOVE OPTION key") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Invalid input 'REMOVE': expected "NOWAIT", "SET", "WAIT" or <EOF> (line 1, column 41 (offset: 40))"""
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'REMOVE': expected 'NOWAIT', 'SET', 'WAIT' or <EOF> (line 1, column 41 (offset: 40))
          |"ALTER DATABASE foo SET ACCESS READ ONLY REMOVE OPTION key"
          |                                         ^""".stripMargin
      ))
  }

  test("ALTER DATABASE foo SET OPTIONS {key: value}") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Invalid input 'OPTIONS': expected "ACCESS", "OPTION" or "TOPOLOGY" (line 1, column 24 (offset: 23))"""
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'OPTIONS': expected 'OPTION', 'ACCESS READ' or 'TOPOLOGY' (line 1, column 24 (offset: 23))
          |"ALTER DATABASE foo SET OPTIONS {key: value}"
          |                        ^""".stripMargin
      ))
  }

  test("ALTER DATABASE foo SET OPTION {key: value}") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Invalid input '{': expected an identifier (line 1, column 31 (offset: 30))"""
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input '{': expected an identifier (line 1, column 31 (offset: 30))
          |"ALTER DATABASE foo SET OPTION {key: value}"
          |                               ^""".stripMargin
      ))
  }

  test("ALTER DATABASE foo SET OPTIONS key value") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Invalid input 'OPTIONS': expected "ACCESS", "OPTION" or "TOPOLOGY" (line 1, column 24 (offset: 23))"""
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'OPTIONS': expected 'OPTION', 'ACCESS READ' or 'TOPOLOGY' (line 1, column 24 (offset: 23))
          |"ALTER DATABASE foo SET OPTIONS key value"
          |                        ^""".stripMargin
      ))
  }

  test("ALTER DATABASE foo SET OPTION key value key2 value") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
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
          |  "||"
          |  <EOF> (line 1, column 41 (offset: 40))""".stripMargin
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'key2': expected an expression, 'NOWAIT', 'SET', 'WAIT' or <EOF> (line 1, column 41 (offset: 40))
          |"ALTER DATABASE foo SET OPTION key value key2 value"
          |                                         ^""".stripMargin
      ))
  }

  test("ALTER DATABASE foo SET OPTION key value, key2 value") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
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
          |  "||"
          |  <EOF> (line 1, column 40 (offset: 39))""".stripMargin
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input ',': expected an expression, 'NOWAIT', 'SET', 'WAIT' or <EOF> (line 1, column 40 (offset: 39))
          |"ALTER DATABASE foo SET OPTION key value, key2 value"
          |                                        ^""".stripMargin
      ))
  }

  test("ALTER DATABASE foo REMOVE OPTION key key2") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Invalid input 'key2': expected "NOWAIT", "REMOVE", "WAIT" or <EOF> (line 1, column 38 (offset: 37))"""
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'key2': expected 'NOWAIT', 'REMOVE OPTION', 'WAIT' or <EOF> (line 1, column 38 (offset: 37))
          |"ALTER DATABASE foo REMOVE OPTION key key2"
          |                                      ^""".stripMargin
      ))
  }

  test("ALTER DATABASE foo REMOVE OPTION key, key2") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Invalid input ',': expected "NOWAIT", "REMOVE", "WAIT" or <EOF> (line 1, column 37 (offset: 36))"""
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input ',': expected 'NOWAIT', 'REMOVE OPTION', 'WAIT' or <EOF> (line 1, column 37 (offset: 36))
          |"ALTER DATABASE foo REMOVE OPTION key, key2"
          |                                     ^""".stripMargin
      ))
  }

  test("ALTER DATABASE foo REMOVE OPTIONS key") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Invalid input 'OPTIONS': expected "OPTION" (line 1, column 27 (offset: 26))"""
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'OPTIONS': expected 'OPTION' (line 1, column 27 (offset: 26))
          |"ALTER DATABASE foo REMOVE OPTIONS key"
          |                           ^""".stripMargin
      ))
  }

  test("ALTER DATABASE foo SET OPTION txLogEnrichment 'FULL' SET OPTION txLogEnrichment 'FULL'") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Duplicate 'SET OPTION txLogEnrichment' clause (line 1, column 54 (offset: 53))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
        """Duplicate 'SET OPTION txLogEnrichment' clause (line 1, column 58 (offset: 57))"""
      ))
  }

  test("ALTER DATABASE foo SET OPTION txLogEnrichment 'FULL' REMOVE OPTION txLogEnrichment") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
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
          |  "||"
          |  <EOF> (line 1, column 54 (offset: 53))""".stripMargin
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'REMOVE': expected an expression, 'NOWAIT', 'SET', 'WAIT' or <EOF> (line 1, column 54 (offset: 53))
          |"ALTER DATABASE foo SET OPTION txLogEnrichment 'FULL' REMOVE OPTION txLogEnrichment"
          |                                                      ^""".stripMargin
      ))
  }

  test("ALTER DATABASE foo REMOVE OPTION txLogEnrichment SET OPTION txLogEnrichment 'FULL'") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input 'SET': expected \"NOWAIT\", \"REMOVE\", \"WAIT\" or <EOF> (line 1, column 50 (offset: 49))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'SET': expected 'NOWAIT', 'REMOVE OPTION', 'WAIT' or <EOF> (line 1, column 50 (offset: 49))
          |"ALTER DATABASE foo REMOVE OPTION txLogEnrichment SET OPTION txLogEnrichment 'FULL'"
          |                                                  ^""".stripMargin
      ))
  }

  // ALTER OR REPLACE
  test("ALTER OR REPLACE DATABASE foo SET ACCESS READ WRITE") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Invalid input 'OR': expected
          |  "ALIAS"
          |  "CURRENT"
          |  "DATABASE"
          |  "SERVER"
          |  "USER" (line 1, column 7 (offset: 6))""".stripMargin
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'OR': expected 'ALIAS', 'DATABASE', 'CURRENT USER SET PASSWORD FROM', 'SERVER' or 'USER' (line 1, column 7 (offset: 6))
          |"ALTER OR REPLACE DATABASE foo SET ACCESS READ WRITE"
          |       ^""".stripMargin
      ))
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
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Invalid input '$': expected <UNSIGNED_DECIMAL_INTEGER> (line 1, column 33 (offset: 32))"""
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input '$': expected an integer value (line 1, column 33 (offset: 32))
          |"ALTER DATABASE foo SET TOPOLOGY $param PRIMARY"
          |                                 ^""".stripMargin
      ))
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY $param SECONDARY") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Invalid input '$': expected
          |  "NOWAIT"
          |  "SET"
          |  "WAIT"
          |  <EOF>
          |  <UNSIGNED_DECIMAL_INTEGER> (line 1, column 43 (offset: 42))""".stripMargin
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input '$': expected 'NOWAIT', 'SET', 'WAIT', <EOF> or an integer value (line 1, column 43 (offset: 42))
          |"ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY $param SECONDARY"
          |                                           ^""".stripMargin
      ))
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
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Duplicate PRIMARY clause (line 1, column 47 (offset: 46))""".stripMargin
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
        """Duplicate PRIMARY clause (line 1, column 45 (offset: 44))""".stripMargin
      ))
  }

  test("ALTER DATABASE foo SET TOPOLOGY 2 SECONDARIES 1 SECONDARY") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Duplicate SECONDARY clause (line 1, column 49 (offset: 48))""".stripMargin
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
        """Duplicate SECONDARY clause (line 1, column 47 (offset: 46))""".stripMargin
      ))
  }

  test("ALTER DATABASE foo SET TOPOLOGY 5 PRIMARIES 10 PRIMARIES 1 PRIMARY 2 SECONDARIES") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Duplicate PRIMARY clause (line 1, column 48 (offset: 47))"""
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
        """Duplicate PRIMARY clause (line 1, column 45 (offset: 44))"""
      ))
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY 2 SECONDARIES 1 SECONDARIES") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Duplicate SECONDARY clause (line 1, column 59 (offset: 58))"""
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
        """Duplicate SECONDARY clause (line 1, column 57 (offset: 56))"""
      ))
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
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart("Duplicate SET TOPOLOGY clause (line 1, column 43 (offset: 42))"))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
        """Duplicate TOPOLOGY clause (line 1, column 47 (offset: 46))"""
      ))
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY 1 PRIMARY") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Duplicate PRIMARY clause (line 1, column 45 (offset: 44))""".stripMargin
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
        """Duplicate PRIMARY clause (line 1, column 43 (offset: 42))"""
      ))
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY 1 SECONDARY 2 SECONDARY") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Duplicate SECONDARY clause (line 1, column 57 (offset: 56))""".stripMargin
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
        """Duplicate SECONDARY clause (line 1, column 55 (offset: 54))"""
      ))
  }

  test("ALTER DATABASE foo SET TOPOLOGY -1 PRIMARY") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Invalid input '-': expected <UNSIGNED_DECIMAL_INTEGER> (line 1, column 33 (offset: 32))""".stripMargin
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input '-': expected an integer value (line 1, column 33 (offset: 32))
          |"ALTER DATABASE foo SET TOPOLOGY -1 PRIMARY"
          |                                 ^""".stripMargin
      ))
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY -1 SECONDARY") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Invalid input '-': expected
          |  "NOWAIT"
          |  "SET"
          |  "WAIT"
          |  <EOF>
          |  <UNSIGNED_DECIMAL_INTEGER> (line 1, column 43 (offset: 42))""".stripMargin
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input '-': expected 'NOWAIT', 'SET', 'WAIT', <EOF> or an integer value (line 1, column 43 (offset: 42))
          |"ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY -1 SECONDARY"
          |                                           ^""".stripMargin
      ))
  }

  test("ALTER DATABASE foo SET TOPOLOGY -1 SECONDARY 1 PRIMARY") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Invalid input '-': expected <UNSIGNED_DECIMAL_INTEGER> (line 1, column 33 (offset: 32))""".stripMargin
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input '-': expected an integer value (line 1, column 33 (offset: 32))
          |"ALTER DATABASE foo SET TOPOLOGY -1 SECONDARY 1 PRIMARY"
          |                                 ^""".stripMargin
      ))
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 SECONDARY 1 SECONDARY") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Duplicate SECONDARY clause (line 1, column 47 (offset: 46))""".stripMargin
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
        """Duplicate SECONDARY clause (line 1, column 45 (offset: 44))""".stripMargin
      ))
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
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Invalid input '': expected <UNSIGNED_DECIMAL_INTEGER> (line 1, column 32 (offset: 31))"""
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input '': expected an integer value (line 1, column 32 (offset: 31))
          |"ALTER DATABASE foo SET TOPOLOGY"
          |                                ^""".stripMargin
      ))
  }

  // START DATABASE

  test("START DATABASE foo") {
    parsesTo[Statements](ast.StartDatabase(literalFoo, ast.NoWait)(pos))
  }

  test("START DATABASE $foo") {
    parsesTo[Statements](ast.StartDatabase(stringParamName("foo"), ast.NoWait)(pos))
  }

  test("START DATABASE foo WAIT") {
    parsesTo[Statements](ast.StartDatabase(literalFoo, ast.IndefiniteWait)(pos))
  }

  test("START DATABASE foo WAIT 5") {
    parsesTo[Statements](ast.StartDatabase(literal("foo"), ast.TimeoutAfter(5))(pos))
  }

  test("START DATABASE foo WAIT 5 SEC") {
    parsesTo[Statements](ast.StartDatabase(literal("foo"), ast.TimeoutAfter(5))(pos))
  }

  test("START DATABASE foo WAIT 5 SECOND") {
    parsesTo[Statements](ast.StartDatabase(literal("foo"), ast.TimeoutAfter(5))(pos))
  }

  test("START DATABASE foo WAIT 5 SECONDS") {
    parsesTo[Statements](ast.StartDatabase(literal("foo"), ast.TimeoutAfter(5))(pos))
  }

  test("START DATABASE foo NOWAIT") {
    parsesTo[Statements](ast.StartDatabase(literalFoo, ast.NoWait)(pos))
  }

  test("START DATABASE `foo.bar`") {
    parsesTo[Statements](ast.StartDatabase(literal("foo.bar"), ast.NoWait)(pos))
  }

  test("START DATABASE foo.bar") {
    parsesTo[Statements](ast.StartDatabase(NamespacedName(List("bar"), Some("foo"))((1, 16, 15)), ast.NoWait)(pos))
  }

  test("START DATABASE") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input '': expected a parameter or an identifier (line 1, column 15 (offset: 14))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input '': expected a database name or a parameter (line 1, column 15 (offset: 14))
          |"START DATABASE"
          |               ^""".stripMargin
      ))
  }

  // STOP DATABASE

  test("STOP DATABASE foo") {
    parsesTo[Statements](ast.StopDatabase(literalFoo, ast.NoWait)(pos))
  }

  test("STOP DATABASE $foo") {
    parsesTo[Statements](ast.StopDatabase(stringParamName("foo"), ast.NoWait)(pos))
  }

  test("STOP DATABASE foo WAIT") {
    parsesTo[Statements](ast.StopDatabase(literalFoo, ast.IndefiniteWait)(pos))
  }

  test("STOP DATABASE foo WAIT 99") {
    parsesTo[Statements](ast.StopDatabase(literal("foo"), ast.TimeoutAfter(99))(pos))
  }

  test("STOP DATABASE foo WAIT 99 SEC") {
    parsesTo[Statements](ast.StopDatabase(literal("foo"), ast.TimeoutAfter(99))(pos))
  }

  test("STOP DATABASE foo WAIT 99 SECOND") {
    parsesTo[Statements](ast.StopDatabase(literal("foo"), ast.TimeoutAfter(99))(pos))
  }

  test("STOP DATABASE foo WAIT 99 SECONDS") {
    parsesTo[Statements](ast.StopDatabase(literal("foo"), ast.TimeoutAfter(99))(pos))
  }

  test("STOP DATABASE foo NOWAIT") {
    parsesTo[Statements](ast.StopDatabase(literalFoo, ast.NoWait)(pos))
  }

  test("STOP DATABASE `foo.bar`") {
    parsesTo[Statements](ast.StopDatabase(literal("foo.bar"), ast.NoWait)(pos))
  }

  test("STOP DATABASE foo.bar") {
    parsesTo[Statements](ast.StopDatabase(NamespacedName(List("bar"), Some("foo"))((1, 16, 15)), ast.NoWait)(pos))
  }

  test("STOP DATABASE") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input '': expected a parameter or an identifier (line 1, column 14 (offset: 13))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input '': expected a database name or a parameter (line 1, column 14 (offset: 13))
          |"STOP DATABASE"
          |              ^""".stripMargin
      ))
  }
}
