/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.parser.privilege

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.UnaliasedReturnItem
import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.parser.AdministrationCommandParserTestBase

class ShowPrivilegesAdministrationCommandParserTest extends AdministrationCommandParserTestBase {

  test("SHOW PRIVILEGES") {
    yields(ast.ShowPrivileges(ast.ShowAllPrivileges() _, None, None))
  }

  test("catalog show privileges") {
    yields(ast.ShowPrivileges(ast.ShowAllPrivileges() _, None, None))
  }

  test("SHOW ALL PRIVILEGES") {
    yields(ast.ShowPrivileges(ast.ShowAllPrivileges() _, None, None))
  }

  // yield / skip / limit / order by / where

  Seq(
    ("ALL", ast.ShowAllPrivileges() _),
    ("USER", ast.ShowUserPrivileges(None) _),
    ("USER neo4j", ast.ShowUsersPrivileges(List(literal("neo4j"))) _),
    ("USERS neo4j, $user", ast.ShowUsersPrivileges(List(literal("neo4j"), paramUser)) _),
    ("ROLES $role", ast.ShowRolesPrivileges(List(paramRole)) _),
    ("ROLE $role, reader", ast.ShowRolesPrivileges(List(paramRole, literal("reader"))) _)
  ).foreach{ case (privType, privilege) =>

    test(s"SHOW $privType PRIVILEGES WHERE access = 'GRANTED'") {
      yields(ast.ShowPrivileges(privilege, Some(Right(ast.Where(equals(varFor(accessString), grantedString)) _)), None))
    }

    test(s"SHOW $privType PRIVILEGES WHERE access = 'GRANTED' AND action = 'match'") {
      val accessPredicate = equals(varFor(accessString), grantedString)
      val matchPredicate = equals(varFor("action"), literalString("match"))
      yields(ast.ShowPrivileges(privilege, Some(Right(ast.Where(and(accessPredicate, matchPredicate)) _)), None))
    }

    test(s"SHOW $privType PRIVILEGES YIELD access ORDER BY access") {
      val orderBy = ast.OrderBy(List(ast.AscSortItem(varFor(accessString)) _)) _
      val columns = ast.Yield(ast.ReturnItems(includeExisting = false, List(UnaliasedReturnItem(varFor(accessString), accessString) _)) _, Some(orderBy), None, None, None) _
      yields(ast.ShowPrivileges(privilege, Some(Left(columns)), None))
    }

    test(s"SHOW $privType PRIVILEGES YIELD access ORDER BY access WHERE access ='none'") {
      val orderBy = ast.OrderBy(List(ast.AscSortItem(varFor(accessString)) _)) _
      val where = ast.Where(equals(varFor(accessString), literalString("none"))) _
      val columns = ast.Yield(ast.ReturnItems(includeExisting = false, List(UnaliasedReturnItem(varFor(accessString), accessString) _)) _, Some(orderBy), None, None, Some(where)) _
      yields(ast.ShowPrivileges(privilege, Some(Left(columns)), None))
    }

    test(s"SHOW $privType PRIVILEGES YIELD access ORDER BY access SKIP 1 LIMIT 10 WHERE access ='none'") {
      val orderBy = ast.OrderBy(List(ast.AscSortItem(varFor(accessString)) _)) _
      val where = ast.Where(equals(varFor(accessString), literalString("none"))) _
      val columns = ast.Yield(ast.ReturnItems(includeExisting = false, List(UnaliasedReturnItem(varFor(accessString), accessString) _)) _, Some(orderBy),
        Some(ast.Skip(literalInt(1)) _), Some(ast.Limit(literalInt(10)) _), Some(where)) _
      yields(ast.ShowPrivileges(privilege, Some(Left(columns)), None))
    }

    test(s"SHOW $privType PRIVILEGES YIELD access SKIP -1") {
      val columns = ast.Yield(ast.ReturnItems(includeExisting = false, List(UnaliasedReturnItem(varFor(accessString), accessString) _)) _, None,
        Some(ast.Skip(literalInt(-1)) _), None, None) _
      yields(ast.ShowPrivileges(privilege, Some(Left(columns)), None))
    }

    test(s"SHOW $privType PRIVILEGES YIELD access, action RETURN access, count(action) ORDER BY access") {
      val orderBy = ast.OrderBy(List(ast.AscSortItem(varFor(accessString)) _)) _
      val accessColumn = UnaliasedReturnItem(varFor(accessString), accessString) _
      val actionColumn: UnaliasedReturnItem = UnaliasedReturnItem(varFor("action"), "action") _
      val countFunction = astFunctionInvocation("count", varFor("action"))
      val countColumn = UnaliasedReturnItem(countFunction, "count(action)") _
      val yieldColumns = ast.Yield(ast.ReturnItems(includeExisting = false, List(accessColumn, actionColumn)) _, None, None, None, None) _
      val returns = ast.Return(distinct = false, ast.ReturnItems(includeExisting = false, List(accessColumn, countColumn)) _, Some(orderBy), None, None) _
      yields(ast.ShowPrivileges(privilege, Some(Left(yieldColumns)), Some(returns)))
    }

    test(s"SHOW $privType PRIVILEGES YIELD access, action SKIP 1 RETURN access, action") {
      val accessColumn = UnaliasedReturnItem(varFor(accessString), accessString) _
      val actionColumn: UnaliasedReturnItem = UnaliasedReturnItem(varFor("action"), "action") _
      val returnItems: ast.ReturnItems =  ast.ReturnItems(includeExisting = false, List(accessColumn, actionColumn)) _
      yields(ast.ShowPrivileges(privilege,
        Some(Left(ast.Yield(returnItems, None, Some(ast.Skip(literalInt(1)) _), None, None) _ )),
        Some(ast.Return(distinct = false, returnItems, None, None, None) _ )))
    }

    test(s"SHOW $privType PRIVILEGES YIELD access, action WHERE access = 'none' RETURN action") {
      val accessColumn = UnaliasedReturnItem(varFor(accessString), accessString) _
      val actionColumn: UnaliasedReturnItem = UnaliasedReturnItem(varFor("action"), "action") _
      val where = ast.Where(equals(varFor(accessString), literalString("none"))) _
      yields(ast.ShowPrivileges(privilege,
        Some(Left(ast.Yield(ast.ReturnItems(includeExisting = false, List(accessColumn, actionColumn)) _, None, None, None, Some(where)) _ )),
        Some(ast.Return(distinct = false, ast.ReturnItems(includeExisting = false, List(actionColumn)) _, None, None, None) _ )))
    }

    test(s"SHOW $privType PRIVILEGES YIELD * RETURN *") {
      yields(ast.ShowPrivileges(privilege,
        Some(Left(ast.Yield(ast.ReturnItems(includeExisting = true,List()) _,None,None,None,None)_)),
        Some(ast.Return(false,ast.ReturnItems(includeExisting = true,List()) _,None,None,None,Set()) _)))
    }
  }

  test("SHOW USER user PRIVILEGES YIELD *, blah RETURN user") {
    failsToParse
  }

  test("SHOW USER user PRIVILEGES YIELD # RETURN user") {
    failsToParse
  }

  test("SHOW USER user PRIVILEGES") {
    yields(ast.ShowPrivileges(ast.ShowUsersPrivileges(List(literalUser)) _, None, None))
  }

  test("SHOW USERS $user PRIVILEGES") {
    yields(ast.ShowPrivileges(ast.ShowUsersPrivileges(List(paramUser)) _, None,  None))
  }

  test("SHOW USER `us%er` PRIVILEGES") {
    yields(ast.ShowPrivileges(ast.ShowUsersPrivileges(List(literal("us%er"))) _, None,  None))
  }

  test("SHOW USER user, $user PRIVILEGES") {
    yields(ast.ShowPrivileges(ast.ShowUsersPrivileges(List(literalUser, paramUser)) _, None, None))
  }

  test("SHOW USERS user1, $user, user2 PRIVILEGES") {
    yields(ast.ShowPrivileges(ast.ShowUsersPrivileges(List(literalUser1, paramUser, literal("user2"))) _, None, None))
  }

  test("SHOW USER PRIVILEGES") {
    yields(ast.ShowPrivileges(ast.ShowUserPrivileges(None) _, None, None))
  }

  test("SHOW USERS PRIVILEGES") {
    yields(ast.ShowPrivileges(ast.ShowUserPrivileges(None) _, None, None))
  }

  test("SHOW ROLE role PRIVILEGES") {
    yields(ast.ShowPrivileges(ast.ShowRolesPrivileges(List(literalRole)) _, None, None))
  }

  test("SHOW ROLE $role PRIVILEGES") {
    yields(ast.ShowPrivileges(ast.ShowRolesPrivileges(List(paramRole)) _, None, None))
  }

  test("SHOW ROLES `ro%le` PRIVILEGES") {
    yields(ast.ShowPrivileges(ast.ShowRolesPrivileges(List(literal("ro%le"))) _, None, None))
  }

  test("SHOW ROLE role1, $roleParam, role2, role3 PRIVILEGES") {
    yields(ast.ShowPrivileges(ast.ShowRolesPrivileges(List(literalRole1, param("roleParam"), literalRole2, literal("role3"))) _, None, None))
  }

  test("SHOW ROLES role1, $roleParam1, role2, $roleParam2 PRIVILEGES") {
    yields(ast.ShowPrivileges(ast.ShowRolesPrivileges(List(literalRole1, param("roleParam1"), literalRole2, param("roleParam2"))) _, None, None))
  }

  test("SHOW PRIVILEGE") {
    failsToParse
  }

  test("SHOW PRIVILAGES") {
    failsToParse
  }

  test("SHOW PRIVELAGES") {
    failsToParse
  }

  test("SHOW privalages") {
    failsToParse
  }

  test("SHOW ALL USER user PRIVILEGES") {
    failsToParse
  }

  test("SHOW USER us%er PRIVILEGES") {
    failsToParse
  }

  test("SHOW ROLE PRIVILEGES") {
    failsToParse
  }

  test("SHOW ALL ROLE role PRIVILEGES") {
    failsToParse
  }

  test("SHOW ROLE ro%le PRIVILEGES") {
    failsToParse
  }

  private def astFunctionInvocation(functionName: String, parameters: expressions.Expression*): expressions.FunctionInvocation =
    expressions.FunctionInvocation(expressions.FunctionName(functionName) _, distinct = false, parameters.toIndexedSeq) _

}
