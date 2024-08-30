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
package org.neo4j.cypher.internal.ast.factory.ddl.privilege

import org.neo4j.cypher.internal.ast.ShowAllPrivileges
import org.neo4j.cypher.internal.ast.ShowPrivilegeCommands
import org.neo4j.cypher.internal.ast.ShowPrivilegeScope
import org.neo4j.cypher.internal.ast.ShowPrivileges
import org.neo4j.cypher.internal.ast.ShowRolesPrivileges
import org.neo4j.cypher.internal.ast.ShowSupportedPrivilegeCommand
import org.neo4j.cypher.internal.ast.ShowUserPrivileges
import org.neo4j.cypher.internal.ast.ShowUsersPrivileges
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.factory.ddl.AdministrationAndSchemaCommandParserTestBase
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5JavaCc
import org.neo4j.cypher.internal.expressions.Expression

class ShowPrivilegesAdministrationCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {

  // Show supported privileges

  test("SHOW SUPPORTED PRIVILEGES") {
    parsesTo[Statements](ShowSupportedPrivilegeCommand(None)(pos))
  }

  test("use system show supported privileges") {
    parsesTo[Statements](ShowSupportedPrivilegeCommand(None)(pos).withGraph(Some(use(List("system")))))
  }

  test("show supported privileges YIELD *") {
    parsesTo[Statements](ShowSupportedPrivilegeCommand(Some(Left((yieldClause(returnAllItems, None), None))))(pos))
  }

  test("show supported privileges YIELD action") {
    parsesTo[Statements](ShowSupportedPrivilegeCommand(Some(Left((
      yieldClause(returnItems(variableReturnItem("action"))),
      None
    ))))(pos))
  }

  test("show supported privileges WHERE action = 'read'") {
    parsesTo[Statements](ShowSupportedPrivilegeCommand(Some(Right(where(equals(
      varFor("action"),
      literalString("read")
    )))))(pos))
  }

  test(
    "show supported privileges YIELD action, target, description ORDER BY action SKIP 1 LIMIT 10 WHERE target ='graph' RETURN *"
  ) {
    val orderByClause = orderBy(sortItem(varFor("action")))
    val whereClause = where(equals(varFor("target"), literalString("graph")))
    val columns = yieldClause(
      returnItems(variableReturnItem("action"), variableReturnItem("target"), variableReturnItem("description")),
      Some(orderByClause),
      Some(skip(1)),
      Some(limit(10)),
      Some(whereClause)
    )
    parsesTo[Statements](
      ShowSupportedPrivilegeCommand(Some(Left((columns, Some(returnClause(returnAllItems))))))(pos)
    )
  }

  // Show privileges

  test("SHOW PRIVILEGES") {
    parsesTo[Statements](ShowPrivileges(ShowAllPrivileges()(pos), None)(pos))
  }

  test("SHOW PRIVILEGE") {
    parsesTo[Statements](ShowPrivileges(ShowAllPrivileges()(pos), None)(pos))
  }

  test("use system show privileges") {
    parsesTo[Statements](ShowPrivileges(ShowAllPrivileges()(pos), None)(pos)
      .withGraph(Some(use(List("system")))))
  }

  test("SHOW ALL PRIVILEGES") {
    parsesTo[Statements](ShowPrivileges(ShowAllPrivileges()(pos), None)(pos))
  }

  // Show user privileges

  test("SHOW USER user PRIVILEGES") {
    parsesTo[Statements](ShowPrivileges(ShowUsersPrivileges(List(literalUser))(pos), None)(pos))
  }

  test("SHOW USERS $user PRIVILEGES") {
    parsesTo[Statements](ShowPrivileges(ShowUsersPrivileges(List(paramUser))(pos), None)(pos))
  }

  test("SHOW USER `us%er` PRIVILEGES") {
    parsesTo[Statements](ShowPrivileges(ShowUsersPrivileges(List(literal("us%er")))(pos), None)(pos))
  }

  test("SHOW USER user, $user PRIVILEGES") {
    parsesTo[Statements](ShowPrivileges(ShowUsersPrivileges(List(literalUser, paramUser))(pos), None)(pos))
  }

  test("SHOW USER user, $user PRIVILEGE") {
    parsesTo[Statements](ShowPrivileges(ShowUsersPrivileges(List(literalUser, paramUser))(pos), None)(pos))
  }

  test("SHOW USERS user1, $user, user2 PRIVILEGES") {
    parsesTo[Statements](ShowPrivileges(
      ShowUsersPrivileges(List(literalUser1, paramUser, literal("user2")))(pos),
      None
    )(pos))
  }

  test("SHOW USER PRIVILEGES") {
    parsesTo[Statements](ShowPrivileges(ShowUserPrivileges(None)(pos), None)(pos))
  }

  test("SHOW USERS PRIVILEGES") {
    parsesTo[Statements](ShowPrivileges(ShowUserPrivileges(None)(pos), None)(pos))
  }

  test("SHOW USER PRIVILEGE") {
    parsesTo[Statements](ShowPrivileges(ShowUserPrivileges(None)(pos), None)(pos))
  }

  test("SHOW USER privilege PRIVILEGE") {
    parsesTo[Statements](ShowPrivileges(ShowUsersPrivileges(List(literal("privilege")))(pos), None)(pos))
  }

  test("SHOW USER privilege, privileges PRIVILEGES") {
    parsesTo[Statements](ShowPrivileges(
      ShowUsersPrivileges(List(literal("privilege"), literal("privileges")))(pos),
      None
    )(pos))
  }

  test("SHOW USER defined PRIVILEGES") {
    parsesTo[Statements](ShowPrivileges(ShowUsersPrivileges(List(literal("defined")))(pos), None)(pos))
  }

  test("SHOW USERS yield, where PRIVILEGES") {
    parsesTo[Statements](ShowPrivileges(
      ShowUsersPrivileges(List(literal("yield"), literal("where")))(pos),
      None
    )(pos))
  }

  test("SHOW USERS where PRIVILEGES") {
    parsesTo[Statements](ShowPrivileges(
      ShowUsersPrivileges(List(literal("where")))(pos),
      None
    )(pos))
  }

  test("SHOW USERS with PRIVILEGES") {
    parsesTo[Statements](ShowPrivileges(
      ShowUsersPrivileges(List(literal("with")))(pos),
      None
    )(pos))
  }

  test("SHOW USERS with, yield PRIVILEGES") {
    parsesTo[Statements](ShowPrivileges(
      ShowUsersPrivileges(List(literal("with"), literal("yield")))(pos),
      None
    )(pos))
  }

  // Show role privileges

  test("SHOW ROLE role PRIVILEGES") {
    parsesTo[Statements](ShowPrivileges(ShowRolesPrivileges(List(literalRole))(pos), None)(pos))
  }

  test("SHOW ROLE role PRIVILEGE") {
    parsesTo[Statements](ShowPrivileges(ShowRolesPrivileges(List(literalRole))(pos), None)(pos))
  }

  test("SHOW ROLE $role PRIVILEGES") {
    parsesTo[Statements](ShowPrivileges(ShowRolesPrivileges(List(paramRole))(pos), None)(pos))
  }

  test("SHOW ROLES `ro%le` PRIVILEGES") {
    parsesTo[Statements](ShowPrivileges(ShowRolesPrivileges(List(literal("ro%le")))(pos), None)(pos))
  }

  test("SHOW ROLE role1, $roleParam, role2, role3 PRIVILEGES") {
    parsesTo[Statements](ShowPrivileges(
      ShowRolesPrivileges(List(literalRole1, stringParam("roleParam"), literalRole2, literal("role3")))(pos),
      None
    )(pos))
  }

  test("SHOW ROLES role1, $roleParam1, role2, $roleParam2 PRIVILEGES") {
    parsesTo[Statements](ShowPrivileges(
      ShowRolesPrivileges(List(literalRole1, stringParam("roleParam1"), literalRole2, stringParam("roleParam2")))(pos),
      None
    )(pos))
  }

  test("SHOW ROLES privilege PRIVILEGE") {
    parsesTo[Statements](ShowPrivileges(ShowRolesPrivileges(List(literal("privilege")))(pos), None)(pos))
  }

  test("SHOW ROLE privilege, privileges PRIVILEGES") {
    parsesTo[Statements](ShowPrivileges(
      ShowRolesPrivileges(List(literal("privilege"), literal("privileges")))(pos),
      None
    )(pos))
  }

  test(s"SHOW ROLES yield, where PRIVILEGES") {
    parsesTo[Statements](ShowPrivileges(
      ShowRolesPrivileges(List(literal("yield"), literal("where")))(pos),
      None
    )(pos))
  }

  test(s"SHOW ROLES with PRIVILEGES") {
    parsesTo[Statements](ShowPrivileges(ShowRolesPrivileges(List(literal("with")))(pos), None)(pos))
  }

  // Show privileges as commands

  test("SHOW PRIVILEGES AS COMMAND") {
    parsesTo[Statements](ShowPrivilegeCommands(ShowAllPrivileges()(pos), asRevoke = false, None)(pos))
  }

  test("SHOW PRIVILEGES AS COMMANDS") {
    parsesTo[Statements](ShowPrivilegeCommands(ShowAllPrivileges()(pos), asRevoke = false, None)(pos))
  }

  test("SHOW PRIVILEGES AS REVOKE COMMAND") {
    parsesTo[Statements](ShowPrivilegeCommands(ShowAllPrivileges()(pos), asRevoke = true, None)(pos))
  }

  test("SHOW PRIVILEGES AS REVOKE COMMANDS") {
    parsesTo[Statements](ShowPrivilegeCommands(ShowAllPrivileges()(pos), asRevoke = true, None)(pos))
  }

  test("SHOW ALL PRIVILEGES AS COMMAND") {
    parsesTo[Statements](ShowPrivilegeCommands(ShowAllPrivileges()(pos), asRevoke = false, None)(pos))
  }

  test("SHOW ALL PRIVILEGE AS COMMAND") {
    parsesTo[Statements](ShowPrivilegeCommands(ShowAllPrivileges()(pos), asRevoke = false, None)(pos))
  }

  test("SHOW ALL PRIVILEGES AS REVOKE COMMANDS") {
    parsesTo[Statements](ShowPrivilegeCommands(ShowAllPrivileges()(pos), asRevoke = true, None)(pos))
  }

  test("SHOW USER user PRIVILEGES AS COMMANDS") {
    parsesTo[Statements](ShowPrivilegeCommands(
      ShowUsersPrivileges(List(literalUser))(pos),
      asRevoke = false,
      None
    )(pos))
  }

  test("SHOW USERS $user PRIVILEGES AS REVOKE COMMAND") {
    parsesTo[Statements](ShowPrivilegeCommands(
      ShowUsersPrivileges(List(paramUser))(pos),
      asRevoke = true,
      None
    )(pos))
  }

  test("SHOW USER `us%er` PRIVILEGES AS COMMANDS") {
    parsesTo[Statements](ShowPrivilegeCommands(
      ShowUsersPrivileges(List(literal("us%er")))(pos),
      asRevoke = false,
      None
    )(pos))
  }

  test("SHOW USER `us%er` PRIVILEGE AS COMMANDS") {
    parsesTo[Statements](ShowPrivilegeCommands(
      ShowUsersPrivileges(List(literal("us%er")))(pos),
      asRevoke = false,
      None
    )(pos))
  }

  test("SHOW USER user, $user PRIVILEGES AS REVOKE COMMANDS") {
    parsesTo[Statements](ShowPrivilegeCommands(
      ShowUsersPrivileges(List(literalUser, paramUser))(pos),
      asRevoke = true,
      None
    )(pos))
  }

  test("SHOW USER PRIVILEGES AS COMMAND") {
    parsesTo[Statements](ShowPrivilegeCommands(ShowUserPrivileges(None)(pos), asRevoke = false, None)(pos))
  }

  test("SHOW USERS PRIVILEGES AS REVOKE COMMANDS") {
    parsesTo[Statements](ShowPrivilegeCommands(ShowUserPrivileges(None)(pos), asRevoke = true, None)(pos))
  }

  test("SHOW USERS PRIVILEGE AS REVOKE COMMANDS") {
    parsesTo[Statements](ShowPrivilegeCommands(ShowUserPrivileges(None)(pos), asRevoke = true, None)(pos))
  }

  test("SHOW ROLE role PRIVILEGES AS COMMANDS") {
    parsesTo[Statements](ShowPrivilegeCommands(
      ShowRolesPrivileges(List(literalRole))(pos),
      asRevoke = false,
      None
    )(pos))
  }

  test("SHOW ROLE role PRIVILEGE AS COMMANDS") {
    parsesTo[Statements](ShowPrivilegeCommands(
      ShowRolesPrivileges(List(literalRole))(pos),
      asRevoke = false,
      None
    )(pos))
  }

  test("SHOW ROLE $role PRIVILEGES AS REVOKE COMMAND") {
    parsesTo[Statements](ShowPrivilegeCommands(
      ShowRolesPrivileges(List(paramRole))(pos),
      asRevoke = true,
      None
    )(pos))
  }

  // yield / skip / limit / order by / where

  Seq(
    (" AS COMMANDS", false),
    (" AS REVOKE COMMANDS", true),
    ("", false)
  ).foreach { case (optionalAsRev: String, asRev) =>
    Seq(
      ("", ShowAllPrivileges()(pos)),
      ("ALL", ShowAllPrivileges()(pos)),
      ("USER", ShowUserPrivileges(None)(pos)),
      ("USER neo4j", ShowUsersPrivileges(List(literal("neo4j")))(pos)),
      ("USERS neo4j, $user", ShowUsersPrivileges(List(literal("neo4j"), paramUser))(pos)),
      ("ROLES $role", ShowRolesPrivileges(List(paramRole))(pos)),
      ("ROLE $role, reader", ShowRolesPrivileges(List(paramRole, literal("reader")))(pos))
    ).foreach { case (privType, privilege) =>
      Seq(
        "PRIVILEGE",
        "PRIVILEGES"
      ).foreach { privilegeOrPrivileges =>
        test(s"SHOW $privType $privilegeOrPrivileges$optionalAsRev WHERE access = 'GRANTED'") {
          if (optionalAsRev.isEmpty) {
            parsesTo[Statements](
              ShowPrivileges(privilege, Some(Right(where(equals(accessVar, grantedString)))))(pos)
            )
          } else {
            parsesTo[Statements](ShowPrivilegeCommands(
              privilege,
              asRev,
              Some(Right(where(equals(accessVar, grantedString))))
            )(pos))
          }
        }

        test(s"SHOW $privType $privilegeOrPrivileges$optionalAsRev WHERE access = 'GRANTED' AND action = 'match'") {
          val accessPredicate = equals(accessVar, grantedString)
          val matchPredicate = equals(varFor(actionString), literalString("match"))
          if (optionalAsRev.isEmpty) {
            parsesTo[Statements](ShowPrivileges(
              privilege,
              Some(Right(where(and(accessPredicate, matchPredicate))))
            )(pos))
          } else {
            parsesTo[Statements](ShowPrivilegeCommands(
              privilege,
              asRev,
              Some(Right(where(and(accessPredicate, matchPredicate))))
            )(pos))
          }
        }

        test(s"SHOW $privType $privilegeOrPrivileges$optionalAsRev YIELD access ORDER BY access") {
          val orderByClause = orderBy(sortItem(accessVar))
          val columns = yieldClause(returnItems(variableReturnItem(accessString)), Some(orderByClause))
          if (optionalAsRev.isEmpty) {
            parsesTo[Statements](ShowPrivileges(privilege, Some(Left((columns, None))))(pos))
          } else {
            parsesTo[Statements](ShowPrivilegeCommands(privilege, asRev, Some(Left((columns, None))))(pos))
          }
        }

        test(
          s"SHOW $privType $privilegeOrPrivileges$optionalAsRev YIELD access ORDER BY access WHERE access ='none'"
        ) {
          val orderByClause = orderBy(sortItem(accessVar))
          val whereClause = where(equals(accessVar, noneString))
          val columns =
            yieldClause(returnItems(variableReturnItem(accessString)), Some(orderByClause), where = Some(whereClause))
          if (optionalAsRev.isEmpty) {
            parsesTo[Statements](ShowPrivileges(privilege, Some(Left((columns, None))))(pos))
          } else {
            parsesTo[Statements](ShowPrivilegeCommands(privilege, asRev, Some(Left((columns, None))))(pos))
          }
        }

        test(
          s"SHOW $privType $privilegeOrPrivileges$optionalAsRev YIELD access ORDER BY access SKIP 1 LIMIT 10 WHERE access ='none'"
        ) {
          val orderByClause = orderBy(sortItem(accessVar))
          val whereClause = where(equals(accessVar, noneString))
          val columns = yieldClause(
            returnItems(variableReturnItem(accessString)),
            Some(orderByClause),
            Some(skip(1)),
            Some(limit(10)),
            Some(whereClause)
          )
          if (optionalAsRev.isEmpty) {
            parsesTo[Statements](ShowPrivileges(privilege, Some(Left((columns, None))))(pos))
          } else {
            parsesTo[Statements](ShowPrivilegeCommands(privilege, asRev, Some(Left((columns, None))))(pos))
          }
        }

        test(s"SHOW $privType $privilegeOrPrivileges$optionalAsRev YIELD access SKIP -1") {
          val columns = yieldClause(returnItems(variableReturnItem(accessString)), skip = Some(skip(-1)))
          if (optionalAsRev.isEmpty) {
            parsesTo[Statements](ShowPrivileges(privilege, Some(Left((columns, None))))(pos))
          } else {
            parsesTo[Statements](ShowPrivilegeCommands(privilege, asRev, Some(Left((columns, None))))(pos))
          }
        }

        test(
          s"SHOW $privType $privilegeOrPrivileges$optionalAsRev YIELD access, action RETURN access, count(action) ORDER BY access"
        ) {
          val orderByClause = orderBy(sortItem(accessVar))
          val accessColumn = variableReturnItem(accessString)
          val actionColumn = variableReturnItem(actionString)
          val countColumn = returnItem(count(varFor(actionString)), "count(action)")
          val yieldColumns = yieldClause(returnItems(accessColumn, actionColumn))
          val returns = returnClause(returnItems(accessColumn, countColumn), Some(orderByClause))
          if (optionalAsRev.isEmpty) {
            parsesTo[Statements](ShowPrivileges(privilege, Some(Left((yieldColumns, Some(returns)))))(pos))
          } else {
            parsesTo[Statements](
              ShowPrivilegeCommands(privilege, asRev, Some(Left((yieldColumns, Some(returns)))))(pos)
            )
          }
        }

        test(
          s"SHOW $privType $privilegeOrPrivileges$optionalAsRev YIELD access, action SKIP 1 RETURN access, action"
        ) {
          val returnItemsPart = returnItems(variableReturnItem(accessString), variableReturnItem(actionString))
          if (optionalAsRev.isEmpty) {
            parsesTo[Statements](ShowPrivileges(
              privilege,
              Some(Left((yieldClause(returnItemsPart, skip = Some(skip(1))), Some(returnClause(returnItemsPart)))))
            )(pos))
          } else {
            parsesTo[Statements](ShowPrivilegeCommands(
              privilege,
              asRev,
              Some(Left((yieldClause(returnItemsPart, skip = Some(skip(1))), Some(returnClause(returnItemsPart)))))
            )(pos))
          }
        }

        test(
          s"SHOW $privType $privilegeOrPrivileges$optionalAsRev YIELD access, action WHERE access = 'none' RETURN action"
        ) {
          val accessColumn = variableReturnItem(accessString)
          val actionColumn = variableReturnItem(actionString)
          val whereClause = where(equals(accessVar, noneString))
          if (optionalAsRev.isEmpty) {
            parsesTo[Statements](ShowPrivileges(
              privilege,
              Some(Left((
                yieldClause(returnItems(accessColumn, actionColumn), where = Some(whereClause)),
                Some(returnClause(returnItems(actionColumn)))
              )))
            )(pos))
          } else {
            parsesTo[Statements](ShowPrivilegeCommands(
              privilege,
              asRev,
              Some(Left((
                yieldClause(returnItems(accessColumn, actionColumn), where = Some(whereClause)),
                Some(returnClause(returnItems(actionColumn)))
              )))
            )(pos))
          }
        }

        test(s"SHOW $privType $privilegeOrPrivileges$optionalAsRev YIELD * RETURN *") {
          if (optionalAsRev.isEmpty) {
            parsesTo[Statements](ShowPrivileges(
              privilege,
              Some(Left((yieldClause(returnAllItems), Some(returnClause(returnAllItems)))))
            )(pos))
          } else {
            parsesTo[Statements](ShowPrivilegeCommands(
              privilege,
              asRev,
              Some(Left((yieldClause(returnAllItems), Some(returnClause(returnAllItems)))))
            )(pos))
          }
        }

        test(s"SHOW $privType $privilegeOrPrivileges$optionalAsRev YIELD `access`") {
          if (optionalAsRev.isEmpty) {
            parsesTo[Statements](ShowPrivileges(
              privilege,
              Some(Left((yieldClause(returnItems(returnItem(varFor("access"), "`access`"))), None)))
            )(pos))
          } else {
            parsesTo[Statements](ShowPrivilegeCommands(
              privilege,
              asRev,
              Some(Left((yieldClause(returnItems(returnItem(varFor("access"), "`access`"))), None)))
            )(pos))
          }
        }
      }
    }

    // yield and where edge cases

    type privilegeFunc = List[String] => ShowPrivilegeScope

    def userPrivilegeFunc(users: List[String]): ShowPrivilegeScope = {
      val literalUsers: List[Expression] = users.map(u => literal(u))
      ShowUsersPrivileges(literalUsers)(pos)
    }

    def rolePrivilegeFunc(roles: List[String]): ShowPrivilegeScope = {
      val literalRoles: List[Expression] = roles.map(r => literal(r))
      ShowRolesPrivileges(literalRoles)(pos)
    }

    Seq(
      ("USER", userPrivilegeFunc: privilegeFunc),
      ("USERS", userPrivilegeFunc: privilegeFunc),
      ("ROLE", rolePrivilegeFunc: privilegeFunc),
      ("ROLES", rolePrivilegeFunc: privilegeFunc)
    ).foreach {
      case (privType: String, func: privilegeFunc) =>
        test(s"SHOW $privType yield PRIVILEGES$optionalAsRev YIELD access RETURN *") {
          val accessColumn = returnItems(variableReturnItem(accessString))
          if (optionalAsRev.isEmpty) {
            parsesTo[Statements](ShowPrivileges(
              func(List("yield")),
              Some(Left((yieldClause(accessColumn), Some(returnClause(returnAllItems)))))
            )(pos))
          } else {
            parsesTo[Statements](ShowPrivilegeCommands(
              func(List("yield")),
              asRev,
              Some(Left((yieldClause(accessColumn), Some(returnClause(returnAllItems)))))
            )(pos))
          }
        }

        test(s"SHOW $privType yield, where PRIVILEGES$optionalAsRev YIELD access RETURN *") {
          val accessColumn = returnItems(variableReturnItem(accessString))
          if (optionalAsRev.isEmpty) {
            parsesTo[Statements](ShowPrivileges(
              func(List("yield", "where")),
              Some(Left((yieldClause(accessColumn), Some(returnClause(returnAllItems)))))
            )(pos))
          } else {
            parsesTo[Statements](ShowPrivilegeCommands(
              func(List("yield", "where")),
              asRev,
              Some(Left((yieldClause(accessColumn), Some(returnClause(returnAllItems)))))
            )(pos))
          }
        }

        test(s"SHOW $privType where PRIVILEGE$optionalAsRev WHERE access = 'none'") {
          if (optionalAsRev.isEmpty) {
            parsesTo[Statements](ShowPrivileges(
              func(List("where")),
              Some(Right(where(equals(accessVar, noneString))))
            )(pos))
          } else {
            parsesTo[Statements](ShowPrivilegeCommands(
              func(List("where")),
              asRev,
              Some(Right(where(equals(accessVar, noneString))))
            )(pos))
          }
        }

        test(s"SHOW $privType privilege PRIVILEGE$optionalAsRev YIELD access RETURN *") {
          val accessColumn = returnItems(variableReturnItem(accessString))
          if (optionalAsRev.isEmpty) {
            parsesTo[Statements](ShowPrivileges(
              func(List("privilege")),
              Some(Left((yieldClause(accessColumn), Some(returnClause(returnAllItems)))))
            )(pos))
          } else {
            parsesTo[Statements](ShowPrivilegeCommands(
              func(List("privilege")),
              asRev,
              Some(Left((yieldClause(accessColumn), Some(returnClause(returnAllItems)))))
            )(pos))
          }
        }

        test(s"SHOW $privType privileges PRIVILEGES$optionalAsRev YIELD access RETURN *") {
          val accessColumn = returnItems(variableReturnItem(accessString))
          if (optionalAsRev.isEmpty) {
            parsesTo[Statements](ShowPrivileges(
              func(List("privileges")),
              Some(Left((yieldClause(accessColumn), Some(returnClause(returnAllItems)))))
            )(pos))
          } else {
            parsesTo[Statements](ShowPrivilegeCommands(
              func(List("privileges")),
              asRev,
              Some(Left((yieldClause(accessColumn), Some(returnClause(returnAllItems)))))
            )(pos))
          }
        }

        test(s"SHOW $privType privilege, privileges PRIVILEGES$optionalAsRev YIELD access RETURN *") {
          val accessColumn = returnItems(variableReturnItem(accessString))
          if (optionalAsRev.isEmpty) {
            parsesTo[Statements](ShowPrivileges(
              func(List("privilege", "privileges")),
              Some(Left((yieldClause(accessColumn), Some(returnClause(returnAllItems)))))
            )(pos))
          } else {
            parsesTo[Statements](ShowPrivilegeCommands(
              func(List("privilege", "privileges")),
              asRev,
              Some(Left((yieldClause(accessColumn), Some(returnClause(returnAllItems)))))
            )(pos))
          }
        }
    }
  }

  // Fails to parse

  test("SHOW PRIVILAGES") {
    val exceptionMessage =
      s"""Invalid input 'PRIVILAGES': expected
         |  "ALIAS"
         |  "ALIASES"
         |  "ALL"
         |  "BTREE"
         |  "BUILT"
         |  "CONSTRAINT"
         |  "CONSTRAINTS"
         |  "CURRENT"
         |  "DATABASE"
         |  "DATABASES"
         |  "DEFAULT"
         |  "EXIST"
         |  "EXISTENCE"
         |  "EXISTS"
         |  "FULLTEXT"
         |  "FUNCTION"
         |  "FUNCTIONS"
         |  "HOME"
         |  "INDEX"
         |  "INDEXES"
         |  "KEY"
         |  "LOOKUP"
         |  "NODE"
         |  "POINT"
         |  "POPULATED"
         |  "PRIVILEGE"
         |  "PRIVILEGES"
         |  "PROCEDURE"
         |  "PROCEDURES"
         |  "PROPERTY"
         |  "RANGE"
         |  "REL"
         |  "RELATIONSHIP"
         |  "ROLE"
         |  "ROLES"
         |  "SERVER"
         |  "SERVERS"
         |  "SETTING"
         |  "SETTINGS"
         |  "SUPPORTED"
         |  "TEXT"
         |  "TRANSACTION"
         |  "TRANSACTIONS"
         |  "UNIQUE"
         |  "UNIQUENESS"
         |  "USER"
         |  "USERS"
         |  "VECTOR" (line 1, column 6 (offset: 5))""".stripMargin

    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessage(exceptionMessage)
      case Cypher5 => _.withSyntaxError(
          """Invalid input 'PRIVILAGES': expected 'ALIAS', 'ALIASES', 'ALL', 'BTREE', 'CONSTRAINT', 'CONSTRAINTS', 'DATABASE', 'DEFAULT DATABASE', 'HOME DATABASE', 'DATABASES', 'EXIST', 'EXISTENCE', 'EXISTS', 'FULLTEXT', 'FUNCTION', 'FUNCTIONS', 'BUILT IN', 'INDEX', 'INDEXES', 'KEY', 'LOOKUP', 'NODE', 'POINT', 'POPULATED', 'PRIVILEGE', 'PRIVILEGES', 'PROCEDURE', 'PROCEDURES', 'PROPERTY', 'RANGE', 'REL', 'RELATIONSHIP', 'ROLE', 'ROLES', 'SERVER', 'SERVERS', 'SETTING', 'SETTINGS', 'SUPPORTED', 'TEXT', 'TRANSACTION', 'TRANSACTIONS', 'UNIQUE', 'UNIQUENESS', 'USER', 'CURRENT USER', 'USERS' or 'VECTOR' (line 1, column 6 (offset: 5))
            |"SHOW PRIVILAGES"
            |      ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input 'PRIVILAGES': expected 'ALIAS', 'ALIASES', 'ALL', 'BTREE', 'CONSTRAINT', 'CONSTRAINTS', 'DATABASE', 'DEFAULT DATABASE', 'HOME DATABASE', 'DATABASES', 'EXIST', 'EXISTENCE', 'FULLTEXT', 'FUNCTION', 'FUNCTIONS', 'BUILT IN', 'INDEX', 'INDEXES', 'KEY', 'LOOKUP', 'NODE', 'POINT', 'POPULATED', 'PRIVILEGE', 'PRIVILEGES', 'PROCEDURE', 'PROCEDURES', 'PROPERTY', 'RANGE', 'REL', 'RELATIONSHIP', 'ROLE', 'ROLES', 'SERVER', 'SERVERS', 'SETTING', 'SETTINGS', 'SUPPORTED', 'TEXT', 'TRANSACTION', 'TRANSACTIONS', 'UNIQUE', 'UNIQUENESS', 'USER', 'CURRENT USER', 'USERS' or 'VECTOR' (line 1, column 6 (offset: 5))
            |"SHOW PRIVILAGES"
            |      ^""".stripMargin
        )
    }
  }

  test("SHOW PRIVELAGES") {
    failsParsing[Statements]
  }

  test("SHOW privalages") {
    failsParsing[Statements]
  }

  test("SHOW ALL USER user PRIVILEGES") {
    val exceptionMessage =
      s"""Invalid input 'USER': expected
         |  "CONSTRAINT"
         |  "CONSTRAINTS"
         |  "FUNCTION"
         |  "FUNCTIONS"
         |  "INDEX"
         |  "INDEXES"
         |  "PRIVILEGE"
         |  "PRIVILEGES"
         |  "ROLE"
         |  "ROLES" (line 1, column 10 (offset: 9))""".stripMargin

    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessage(exceptionMessage)
      case _ => _.withSyntaxError(
          """|Invalid input 'USER': expected 'CONSTRAINT', 'CONSTRAINTS', 'FUNCTION', 'FUNCTIONS', 'INDEX', 'INDEXES', 'PRIVILEGE', 'PRIVILEGES', 'ROLE' or 'ROLES' (line 1, column 10 (offset: 9))
             |"SHOW ALL USER user PRIVILEGES"
             |          ^""".stripMargin
        )
    }
  }

  test("SHOW USER us%er PRIVILEGES") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessage(
          """Invalid input '%': expected ",", "PRIVILEGE" or "PRIVILEGES" (line 1, column 13 (offset: 12))"""
        )
      case _ => _.withSyntaxError(
          """Invalid input '%': expected 'PRIVILEGE' or 'PRIVILEGES' (line 1, column 13 (offset: 12))
            |"SHOW USER us%er PRIVILEGES"
            |             ^""".stripMargin
        )
    }
  }

  test("SHOW ROLE PRIVILEGES") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessage(
          """Invalid input '': expected ",", "PRIVILEGE" or "PRIVILEGES" (line 1, column 21 (offset: 20))"""
        )
      case _ => _.withSyntaxError(
          """Invalid input '': expected 'PRIVILEGE' or 'PRIVILEGES' (line 1, column 21 (offset: 20))
            |"SHOW ROLE PRIVILEGES"
            |                     ^""".stripMargin
        )
    }
  }

  test("SHOW ALL ROLE role PRIVILEGES") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessage(
          s"""Invalid input 'role': expected "WHERE", "WITH", "YIELD" or <EOF> (line 1, column 15 (offset: 14))"""
        )
      case _ => _.withSyntaxError(
          """Invalid input 'role': expected 'WHERE', 'WITH', 'YIELD' or <EOF> (line 1, column 15 (offset: 14))
            |"SHOW ALL ROLE role PRIVILEGES"
            |               ^""".stripMargin
        )
    }
  }

  test("SHOW ROLE ro%le PRIVILEGES") {
    failsParsing[Statements]
  }

  test("SHOW USER user PRIVILEGES YIELD *, blah RETURN user") {
    val exceptionMessage =
      s"""Invalid input ',': expected
         |  "LIMIT"
         |  "OFFSET"
         |  "ORDER"
         |  "RETURN"
         |  "SKIP"
         |  "WHERE"
         |  <EOF> (line 1, column 34 (offset: 33))""".stripMargin

    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessage(exceptionMessage)
      case _ => _.withSyntaxError(
          """Invalid input ',': expected 'ORDER BY', 'LIMIT', 'OFFSET', 'RETURN', 'SKIP', 'WHERE' or <EOF> (line 1, column 34 (offset: 33))
            |"SHOW USER user PRIVILEGES YIELD *, blah RETURN user"
            |                                  ^""".stripMargin
        )
    }
  }

  test("SHOW USER user PRIVILEGES YIELD # RETURN user") {
    failsParsing[Statements]
  }

  test("SHOW PRIVILEGES COMMANDS") {
    failsParsing[Statements]
  }

  test("SHOW PRIVILEGES REVOKE") {
    failsParsing[Statements]
  }

  test("SHOW PRIVILEGES AS REVOKE COMMAND COMMANDS") {
    failsParsing[Statements]
  }

  test("SHOW PRIVILEGES AS COMMANDS REVOKE") {
    failsParsing[Statements]
  }

  test("SHOW PRIVILEGES AS COMMANDS USER user") {
    failsParsing[Statements]
  }
}
