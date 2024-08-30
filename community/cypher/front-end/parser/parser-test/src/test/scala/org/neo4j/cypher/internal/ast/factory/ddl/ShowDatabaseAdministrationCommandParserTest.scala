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

import org.neo4j.cypher.internal.ast.AllDatabasesScope
import org.neo4j.cypher.internal.ast.DefaultDatabaseScope
import org.neo4j.cypher.internal.ast.HomeDatabaseScope
import org.neo4j.cypher.internal.ast.ShowDatabase
import org.neo4j.cypher.internal.ast.SingleNamedDatabaseScope
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.YieldOrWhere
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5JavaCc

class ShowDatabaseAdministrationCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {

  Seq(
    ("DATABASE", ShowDatabase.apply(AllDatabasesScope()(pos), _: YieldOrWhere) _),
    ("DATABASES", ShowDatabase.apply(AllDatabasesScope()(pos), _: YieldOrWhere) _),
    ("DEFAULT DATABASE", ShowDatabase.apply(DefaultDatabaseScope()(pos), _: YieldOrWhere) _),
    ("HOME DATABASE", ShowDatabase.apply(HomeDatabaseScope()(pos), _: YieldOrWhere) _),
    (
      "DATABASE $db",
      ShowDatabase.apply(SingleNamedDatabaseScope(stringParamName("db"))(pos), _: YieldOrWhere) _
    ),
    (
      "DATABASES $db",
      ShowDatabase.apply(SingleNamedDatabaseScope(stringParamName("db"))(pos), _: YieldOrWhere) _
    ),
    (
      "DATABASE neo4j",
      ShowDatabase.apply(SingleNamedDatabaseScope(literal("neo4j"))(pos), _: YieldOrWhere) _
    ),
    (
      "DATABASES neo4j",
      ShowDatabase.apply(SingleNamedDatabaseScope(literal("neo4j"))(pos), _: YieldOrWhere) _
    ),
    // vvv naming the database yield/where should not fail either vvv
    (
      "DATABASE yield",
      ShowDatabase.apply(SingleNamedDatabaseScope(literal("yield"))(pos), _: YieldOrWhere) _
    ),
    (
      "DATABASES yield",
      ShowDatabase.apply(SingleNamedDatabaseScope(literal("yield"))(pos), _: YieldOrWhere) _
    ),
    (
      "DATABASE where",
      ShowDatabase.apply(SingleNamedDatabaseScope(literal("where"))(pos), _: YieldOrWhere) _
    ),
    (
      "DATABASES where",
      ShowDatabase.apply(SingleNamedDatabaseScope(literal("where"))(pos), _: YieldOrWhere) _
    )
  ).foreach { case (dbType, command) =>
    test(s"SHOW $dbType") {
      parsesTo[Statements](command(None)(pos))
    }

    test(s"USE system SHOW $dbType") {
      parsesTo[Statements](command(None)(pos).withGraph(Some(use(List("system")))))
    }

    test(s"SHOW $dbType WHERE access = 'GRANTED'") {
      parsesTo[Statements](command(Some(Right(where(equals(accessVar, grantedString)))))(pos))
    }

    test(s"SHOW $dbType WHERE access = 'GRANTED' AND action = 'match'") {
      val accessPredicate = equals(accessVar, grantedString)
      val matchPredicate = equals(varFor(actionString), literalString("match"))
      parsesTo[Statements](command(Some(Right(where(and(accessPredicate, matchPredicate)))))(pos))
    }

    test(s"SHOW $dbType YIELD access ORDER BY access") {
      val orderByClause = orderBy(sortItem(accessVar))
      val columns = yieldClause(returnItems(variableReturnItem(accessString)), Some(orderByClause))
      parsesTo[Statements](command(Some(Left((columns, None))))(pos))
    }

    test(s"SHOW $dbType YIELD access ORDER BY access WHERE access ='none'") {
      val orderByClause = orderBy(sortItem(accessVar))
      val whereClause = where(equals(accessVar, noneString))
      val columns =
        yieldClause(returnItems(variableReturnItem(accessString)), Some(orderByClause), where = Some(whereClause))
      parsesTo[Statements](command(Some(Left((columns, None))))(pos))
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
      parsesTo[Statements](command(Some(Left((columns, None))))(pos))
    }

    test(s"SHOW $dbType YIELD access ORDER BY access OFFSET 1 LIMIT 10 WHERE access ='none'") {
      val orderByClause = orderBy(sortItem(accessVar))
      val whereClause = where(equals(accessVar, noneString))
      val columns = yieldClause(
        returnItems(variableReturnItem(accessString)),
        Some(orderByClause),
        Some(skip(1)),
        Some(limit(10)),
        Some(whereClause)
      )
      parsesTo[Statements](command(Some(Left((columns, None))))(pos))
    }

    test(s"SHOW $dbType YIELD access SKIP -1") {
      val columns = yieldClause(returnItems(variableReturnItem(accessString)), skip = Some(skip(-1)))
      parsesTo[Statements](command(Some(Left((columns, None))))(pos))
    }

    test(s"SHOW $dbType YIELD access OFFSET -1") {
      val columns = yieldClause(returnItems(variableReturnItem(accessString)), skip = Some(skip(-1)))
      parsesTo[Statements](command(Some(Left((columns, None))))(pos))
    }

    test(s"SHOW $dbType YIELD access ORDER BY access RETURN access") {
      parsesTo[Statements](command(
        Some(Left((
          yieldClause(returnItems(variableReturnItem(accessString)), Some(orderBy(sortItem(accessVar)))),
          Some(returnClause(returnItems(variableReturnItem(accessString))))
        )))
      )(pos))
    }

    test(s"SHOW $dbType YIELD * RETURN *") {
      parsesTo[Statements](
        command(Some(Left((yieldClause(returnAllItems), Some(returnClause(returnAllItems))))))(pos)
      )
    }

    test(s"SHOW $dbType WHERE access = 'GRANTED' RETURN action") {
      failsParsing[Statements]
    }
  }

  test("SHOW DATABASE `foo.bar`") {
    parsesTo[Statements](ShowDatabase(SingleNamedDatabaseScope(namespacedName("foo.bar"))(pos), None)(pos))
  }

  test("SHOW DATABASE foo.bar") {
    parsesTo[Statements](ShowDatabase(SingleNamedDatabaseScope(namespacedName("foo", "bar"))(pos), None)(pos))
  }

  test("SHOW DATABASE `foo`.`bar`.`baz`") {
    failsParsing[Statements].withMessageStart(
      "Invalid input ``foo`.`bar`.`baz`` for name. Expected name to contain at most two components separated by `.`."
    )
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
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("""Invalid input 'DATABASES': expected "DATABASE" (line 1, column 14 (offset: 13))""")
      case _ => _.withSyntaxError(
          """Invalid input 'DATABASES': expected 'DATABASE' (line 1, column 14 (offset: 13))
            |"SHOW DEFAULT DATABASES"
            |              ^""".stripMargin
        )
    }
  }

  test("SHOW DEFAULT DATABASES YIELD *") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("""Invalid input 'DATABASES': expected "DATABASE" (line 1, column 14 (offset: 13))""")
      case _ => _.withSyntaxError(
          """Invalid input 'DATABASES': expected 'DATABASE' (line 1, column 14 (offset: 13))
            |"SHOW DEFAULT DATABASES YIELD *"
            |              ^""".stripMargin
        )
    }
  }

  test("SHOW DEFAULT DATABASES WHERE name STARTS WITH 'foo'") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("""Invalid input 'DATABASES': expected "DATABASE" (line 1, column 14 (offset: 13))""")
      case _ => _.withSyntaxError(
          """Invalid input 'DATABASES': expected 'DATABASE' (line 1, column 14 (offset: 13))
            |"SHOW DEFAULT DATABASES WHERE name STARTS WITH 'foo'"
            |              ^""".stripMargin
        )
    }
  }

  test("SHOW HOME DATABASES") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("""Invalid input 'DATABASES': expected "DATABASE" (line 1, column 11 (offset: 10))""")
      case _ => _.withSyntaxError(
          """Invalid input 'DATABASES': expected 'DATABASE' (line 1, column 11 (offset: 10))
            |"SHOW HOME DATABASES"
            |           ^""".stripMargin
        )
    }
  }

  test("SHOW HOME DATABASES YIELD *") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("""Invalid input 'DATABASES': expected "DATABASE" (line 1, column 11 (offset: 10))""")
      case _ => _.withSyntaxError(
          """Invalid input 'DATABASES': expected 'DATABASE' (line 1, column 11 (offset: 10))
            |"SHOW HOME DATABASES YIELD *"
            |           ^""".stripMargin
        )
    }
  }

  test("SHOW HOME DATABASES WHERE name STARTS WITH 'foo'") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("""Invalid input 'DATABASES': expected "DATABASE" (line 1, column 11 (offset: 10))""")
      case _ => _.withSyntaxError(
          """Invalid input 'DATABASES': expected 'DATABASE' (line 1, column 11 (offset: 10))
            |"SHOW HOME DATABASES WHERE name STARTS WITH 'foo'"
            |           ^""".stripMargin
        )
    }
  }
}
