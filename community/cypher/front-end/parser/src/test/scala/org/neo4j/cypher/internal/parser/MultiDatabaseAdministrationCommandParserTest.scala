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
package org.neo4j.cypher.internal.parser

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.AllDatabasesScope
import org.neo4j.cypher.internal.ast.DefaultDatabaseScope
import org.neo4j.cypher.internal.ast.DestroyData
import org.neo4j.cypher.internal.ast.DumpData
import org.neo4j.cypher.internal.ast.NamedDatabaseScope
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.UnaliasedReturnItem
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.ast.Yield

class MultiDatabaseAdministrationCommandParserTest extends AdministrationCommandParserTestBase {
  private val literalFooBar = literal("foo.bar")

  // SHOW DATABASE

  Seq(
    ("DATABASES", ast.ShowDatabase.apply(AllDatabasesScope()(pos), _: Option[Either[Yield, Where]], _: Option[Return]) _  ),
    ("DEFAULT DATABASE", ast.ShowDatabase.apply(DefaultDatabaseScope()(pos), _: Option[Either[Yield, Where]], _: Option[Return]) _  ),
    ("DATABASE $db",  ast.ShowDatabase.apply(NamedDatabaseScope(param("db"))(pos), _: Option[Either[Yield, Where]], _: Option[Return]) _  ),
    ("DATABASE neo4j",  ast.ShowDatabase.apply(NamedDatabaseScope(literal("neo4j"))(pos), _: Option[Either[Yield, Where]], _: Option[Return]) _  )
  ).foreach{ case (dbType, privilege) =>

    test(s"SHOW $dbType") {
      yields(privilege(None, None))
    }

    test(s"SHOW $dbType WHERE access = 'GRANTED'") {
      yields(privilege(Some(Right(ast.Where(equals(varFor(accessString), grantedString)) _)), None))
    }

    test(s"SHOW $dbType WHERE access = 'GRANTED' AND action = 'match'") {
      val accessPredicate = equals(varFor(accessString), grantedString)
      val matchPredicate = equals(varFor("action"), literalString("match"))
      yields(privilege(Some(Right(ast.Where(and(accessPredicate, matchPredicate)) _)), None))
    }

    test(s"SHOW $dbType YIELD access ORDER BY access") {
      val orderBy = ast.OrderBy(List(ast.AscSortItem(varFor(accessString)) _)) _
      val columns = ast.Yield(ast.ReturnItems(includeExisting = false, List(UnaliasedReturnItem(varFor(accessString), accessString) _)) _, Some(orderBy), None, None, None) _
      yields(privilege( Some(Left(columns)), None))
    }

    test(s"SHOW $dbType YIELD access ORDER BY access WHERE access ='none'") {
      val orderBy = ast.OrderBy(List(ast.AscSortItem(varFor(accessString)) _)) _
      val where = ast.Where(equals(varFor(accessString), literalString("none"))) _
      val columns = ast.Yield(ast.ReturnItems(includeExisting = false, List(UnaliasedReturnItem(varFor(accessString), accessString) _)) _, Some(orderBy), None, None, Some(where)) _
      yields(privilege(Some(Left(columns)), None))
    }

    test(s"SHOW $dbType YIELD access ORDER BY access SKIP 1 LIMIT 10 WHERE access ='none'") {
      val orderBy = ast.OrderBy(List(ast.AscSortItem(varFor(accessString)) _)) _
      val where = ast.Where(equals(varFor(accessString), literalString("none"))) _
      val columns = ast.Yield(ast.ReturnItems(includeExisting = false, List(UnaliasedReturnItem(varFor(accessString), accessString) _)) _, Some(orderBy),
        Some(ast.Skip(literalInt(1)) _), Some(ast.Limit(literalInt(10)) _), Some(where)) _
      yields(privilege(Some(Left(columns)), None))
    }

    test(s"SHOW $dbType YIELD access SKIP -1") {
      val columns = ast.Yield(ast.ReturnItems(includeExisting = false, List(UnaliasedReturnItem(varFor(accessString), accessString) _)) _, None,
        Some(ast.Skip(literalInt(-1)) _), None, None) _
      yields(privilege(Some(Left(columns)), None))
    }

    test(s"SHOW $dbType YIELD access ORDER BY access RETURN access") {
      yields(privilege(
        Some(Left(ast.Yield(ast.ReturnItems(includeExisting = false, List(UnaliasedReturnItem(varFor(accessString), accessString) _)) _, Some(ast.OrderBy(List(ast.AscSortItem(varFor(accessString)) _)) _), None, None, None) _)),
        Some(ast.Return(ast.ReturnItems(includeExisting = false, List(UnaliasedReturnItem(varFor(accessString), accessString) _)) _) _)
      ))
    }

    test(s"SHOW $dbType WHERE access = 'GRANTED' RETURN action") {
      yields(privilege(
        Some(Right(ast.Where(equals(varFor(accessString), grantedString)) _)),
        Some(ast.Return(ast.ReturnItems(includeExisting = false, List(UnaliasedReturnItem(varFor("action"), "action") _))_ )_)
      ))
    }

    test(s"SHOW $dbType YIELD * RETURN *") {
      yields(privilege(
        Some(Left(ast.Yield(ast.ReturnItems(includeExisting = true,List()) _,None,None,None,None)_)),
        Some(ast.Return(distinct = false,ast.ReturnItems(includeExisting = true,List()) _,None,None,None,Set()) _)))
    }
  }

  test("SHOW DATABASE blah YIELD *,blah RETURN user") {
    failsToParse
  }

  test("SHOW DATABASE `foo.bar`") {
    yields(ast.ShowDatabase(NamedDatabaseScope(literalFooBar)(pos), None, None))
  }

  test("SHOW DATABASE foo.bar") {
    yields(ast.ShowDatabase(NamedDatabaseScope(literalFooBar)(pos), None, None))
  }

  test("SHOW DATABASE") {
    failsToParse
  }

  // CREATE DATABASE

  test("CREATE DATABASE foo") {
    yields(ast.CreateDatabase(literalFoo, ast.IfExistsThrowError))
  }

  test("USE system CREATE DATABASE foo") {
    // can parse USE clause, but is not included in AST
    yields(ast.CreateDatabase(literalFoo, ast.IfExistsThrowError))
  }

  test("CREATE DATABASE $foo") {
    yields(ast.CreateDatabase(paramFoo, ast.IfExistsThrowError))
  }

  test("CREATE DATABASE `foo.bar`") {
    yields(ast.CreateDatabase(literalFooBar, ast.IfExistsThrowError))
  }

  test("CATALOG CREATE DATABASE `foo.bar`") {
    yields(ast.CreateDatabase(literalFooBar, ast.IfExistsThrowError))
  }

  test("CREATE DATABASE foo.bar") {
    yields(ast.CreateDatabase(literalFooBar, ast.IfExistsThrowError))
  }

  test("CATALOG CREATE DATABASE foo.bar") {
    yields(ast.CreateDatabase(literalFooBar, ast.IfExistsThrowError))
  }

  test("CATALOG CREATE DATABASE `graph.db`.`db.db`") {
    yields(ast.CreateDatabase(literal("graph.db.db.db"), ast.IfExistsThrowError))
  }

  test("CATALOG CREATE DATABASE `foo-bar42`") {
    yields(ast.CreateDatabase(literal("foo-bar42"), ast.IfExistsThrowError))
  }

  test("CATALOG CREATE DATABASE `_foo-bar42`") {
    yields(ast.CreateDatabase(literal("_foo-bar42"), ast.IfExistsThrowError))
  }

  test("CATALOG CREATE DATABASE ``") {
    yields(ast.CreateDatabase(literalEmpty, ast.IfExistsThrowError))
  }

  test("CREATE DATABASE foo IF NOT EXISTS") {
    yields(ast.CreateDatabase(literalFoo, ast.IfExistsDoNothing))
  }

  test("CATALOG CREATE DATABASE `_foo-bar42` IF NOT EXISTS") {
    yields(ast.CreateDatabase(literal("_foo-bar42"), ast.IfExistsDoNothing))
  }

  test("CREATE OR REPLACE DATABASE foo") {
    yields(ast.CreateDatabase(literalFoo, ast.IfExistsReplace))
  }

  test("CATALOG CREATE OR REPLACE DATABASE `_foo-bar42`") {
    yields(ast.CreateDatabase(literal("_foo-bar42"), ast.IfExistsReplace))
  }

  test("CREATE OR REPLACE DATABASE foo IF NOT EXISTS") {
    yields(ast.CreateDatabase(literalFoo, ast.IfExistsInvalidSyntax))
  }

  test("CREATE DATABASE \"foo.bar\"") {
    failsToParse  }

  test("CATALOG CREATE DATABASE foo-bar42") {
    failsToParse
  }

  test("CATALOG CREATE DATABASE _foo-bar42") {
    failsToParse
  }

  test("CATALOG CREATE DATABASE 42foo-bar") {
    failsToParse
  }

  test("CATALOG CREATE DATABASE") {
    failsToParse
  }

  test("CATALOG CREATE DATABASE _foo-bar42 IF NOT EXISTS") {
    failsToParse
  }

  test("CREATE DATABASE  IF NOT EXISTS") {
    failsToParse
  }

  test("CREATE DATABASE foo IF EXISTS") {
    failsToParse
  }

  test("CATALOG CREATE OR REPLACE DATABASE _foo-bar42") {
    failsToParse
  }

  test("CREATE OR REPLACE DATABASE") {
    failsToParse
  }

  // DROP DATABASE

  test("DROP DATABASE foo") {
    yields(ast.DropDatabase(literalFoo, ifExists = false, DestroyData))
  }

  test("DROP DATABASE $foo") {
    yields(ast.DropDatabase(paramFoo, ifExists = false, DestroyData))
  }

  test("CATALOG DROP DATABASE `foo.bar`") {
    yields(ast.DropDatabase(literalFooBar, ifExists = false, DestroyData))
  }

  test("CATALOG DROP DATABASE foo.bar") {
    yields(ast.DropDatabase(literalFooBar, ifExists = false, DestroyData))
  }

  test("DROP DATABASE foo IF EXISTS") {
    yields(ast.DropDatabase(literalFoo, ifExists = true, DestroyData))
  }

  test("DROP DATABASE") {
    failsToParse
  }

  test("DROP DATABASE  IF EXISTS") {
    failsToParse
  }

  test("DROP DATABASE foo IF NOT EXISTS") {
    failsToParse
  }

  test("DROP DATABASE foo DUMP DATA") {
    yields(ast.DropDatabase(literalFoo, ifExists = false, DumpData))
  }

  test("DROP DATABASE foo DESTROY DATA") {
    yields(ast.DropDatabase(literalFoo, ifExists = false, DestroyData))
  }

  test("DROP DATABASE foo IF EXISTS DUMP DATA") {
    yields(ast.DropDatabase(literalFoo, ifExists = true, DumpData))
  }

  test("DROP DATABASE foo IF EXISTS DESTROY DATA") {
    yields(ast.DropDatabase(literalFoo, ifExists = true, DestroyData))
  }

  test("DROP DATABASE  KEEP DATA") {
    failsToParse
  }

  // START DATABASE

  test("START DATABASE foo") {
    yields(ast.StartDatabase(literalFoo))
  }

  test("START DATABASE $foo") {
    yields(ast.StartDatabase(paramFoo))
  }

  test("CATALOG START DATABASE `foo.bar`") {
    yields(ast.StartDatabase(literalFooBar))
  }

  test("CATALOG START DATABASE foo.bar") {
    yields(ast.StartDatabase(literalFooBar))
  }

  test("START DATABASE") {
    failsToParse
  }

  // STOP DATABASE

  test("STOP DATABASE foo") {
    yields(ast.StopDatabase(literalFoo))
  }

  test("STOP DATABASE $foo") {
    yields(ast.StopDatabase(paramFoo))
  }

  test("CATALOG STOP DATABASE `foo.bar`") {
    yields(ast.StopDatabase(literalFooBar))
  }

  test("CATALOG STOP DATABASE foo.bar") {
    yields(ast.StopDatabase(literalFooBar))
  }

  test("STOP DATABASE") {
    failsToParse
  }
}
