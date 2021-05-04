/*
 * Copyright (c) "Neo4j"
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
import org.parboiled.scala.Rule1

/* Tests for listing procedures */
class ShowProceduresCommandParserTest  extends ParserAstTest[ast.Statement]
                                       with Statement
                                       with ast.AstConstructionTestSupport {
  implicit val parser: Rule1[ast.Statement] = Statement

  Seq("PROCEDURE", "PROCEDURES").foreach { procKeyword =>

    test(s"SHOW $procKeyword") {
      yields(_ => query(ast.ShowProceduresClause(None, None, hasYield = false)(pos)))
    }

    test(s"SHOW $procKeyword EXECUTABLE") {
      yields(_ => query(ast.ShowProceduresClause(Some(ast.ShowProceduresClause.CurrentUser), None, hasYield = false)(pos)))
    }

    test(s"SHOW $procKeyword EXECUTABLE BY CURRENT USER") {
      yields(_ => query(ast.ShowProceduresClause(Some(ast.ShowProceduresClause.CurrentUser), None, hasYield = false)(pos)))
    }

    test(s"SHOW $procKeyword EXECUTABLE BY user") {
      yields(_ => query(ast.ShowProceduresClause(Some(ast.ShowProceduresClause.User("user")), None, hasYield = false)(pos)))
    }

    test(s"USE db SHOW $procKeyword") {
      yields(_ => query(use(varFor("db")), ast.ShowProceduresClause(None, None, hasYield = false)(pos)))
    }

  }

  // Filtering tests

  test("SHOW PROCEDURE WHERE name = 'my.proc'") {
    yields(_ => query(ast.ShowProceduresClause(None, Some(where(equals(varFor("name"), literalString("my.proc")))), hasYield = false)(pos)))
  }

  test("SHOW PROCEDURES YIELD description") {
    yields(_ => query(ast.ShowProceduresClause(None, None, hasYield = true)(pos), yieldClause(returnItems(variableReturnItem("description")))))
  }

  test("SHOW PROCEDURES EXECUTABLE BY user YIELD *") {
    yields(_ => query(ast.ShowProceduresClause(Some(ast.ShowProceduresClause.User("user")), None, hasYield = true)(pos), yieldClause(returnAllItems)))
  }

  test("SHOW PROCEDURES YIELD * ORDER BY name SKIP 2 LIMIT 5") {
    yields(_ => query(ast.ShowProceduresClause(None, None, hasYield = true)(pos),
      yieldClause(returnAllItems, Some(orderBy(sortItem(varFor("name")))), Some(skip(2)), Some(limit(5)))
    ))
  }

  test("USE db SHOW PROCEDURES YIELD name, description AS pp WHERE pp < 50.0 RETURN name") {
    yields(_ => query(
      use(varFor("db")),
      ast.ShowProceduresClause(None, None, hasYield = true)(pos),
      yieldClause(returnItems(variableReturnItem("name"), aliasedReturnItem("description", "pp")),
        where = Some(where(lessThan(varFor("pp"), literalFloat(50.0))))),
      return_(variableReturnItem("name"))
    ))
  }

  test("USE db SHOW PROCEDURES EXECUTABLE YIELD name, description AS pp ORDER BY pp SKIP 2 LIMIT 5 WHERE pp < 50.0 RETURN name") {
    yields(_ => query(
      use(varFor("db")),
      ast.ShowProceduresClause(Some(ast.ShowProceduresClause.CurrentUser), None, hasYield = true)(pos),
      yieldClause(returnItems(variableReturnItem("name"), aliasedReturnItem("description", "pp")),
        Some(orderBy(sortItem(varFor("pp")))),
        Some(skip(2)),
        Some(limit(5)),
        Some(where(lessThan(varFor("pp"), literalFloat(50.0))))),
      return_(variableReturnItem("name"))
    ))
  }

  test("SHOW PROCEDURES YIELD name AS PROCEDURE, mode AS OUTPUT") {
    yields(_ => query(ast.ShowProceduresClause(None, None, hasYield = true)(pos),
      yieldClause(returnItems(aliasedReturnItem("name", "PROCEDURE"), aliasedReturnItem("mode", "OUTPUT")))))
  }

  // Negative tests

  test("SHOW PROCEDURES YIELD") {
    failsToParse
  }

  test("SHOW PROCEDURES YIELD * YIELD *") {
    failsToParse
  }

  test("SHOW PROCEDURES WHERE name = 'my.proc' YIELD *") {
    failsToParse
  }

  test("SHOW PROCEDURES WHERE name = 'my.proc' RETURN *") {
    failsToParse
  }

  test("SHOW PROCEDURES YIELD a b RETURN *") {
    failsToParse
  }

  test("SHOW PROCEDURES RETURN *") {
    failsToParse
  }

  test("SHOW EXECUTABLE PROCEDURE") {
    failsToParse
  }

  test("SHOW PROCEDURE EXECUTABLE user") {
    failsToParse
  }

  test("SHOW PROCEDURE EXECUTABLE CURRENT USER") {
    failsToParse
  }

  test("SHOW PROCEDURE EXEC") {
    failsToParse
  }

  test("SHOW PROCEDURE EXECUTABLE BY") {
    failsToParse
  }

  test("SHOW PROCEDURE EXECUTABLE BY user1, user2") {
    failsToParse
  }

  test("SHOW PROCEDURE EXECUTABLE BY CURRENT USER user") {
    failsToParse
  }

  test("SHOW PROCEDURE EXECUTABLE BY CURRENT USER, user") {
    failsToParse
  }

  test("SHOW PROCEDURE EXECUTABLE BY user CURRENT USER") {
    failsToParse
  }

  test("SHOW PROCEDURE EXECUTABLE BY user, CURRENT USER") {
    failsToParse
  }

  test("SHOW PROCEDURE CURRENT USER") {
    failsToParse
  }

  test("SHOW PROCEDURE user") {
    failsToParse
  }

  test("SHOW CURRENT USER PROCEDURE") {
    failsToParse
  }

  test("SHOW user PROCEDURE") {
    failsToParse
  }

  test("SHOW USER user PROCEDURE") {
    failsToParse
  }

  test("SHOW PROCEDURE EXECUTABLE BY USER user") {
    failsToParse
  }

  test("SHOW PROCEDURE EXECUTABLE USER user") {
    failsToParse
  }

  test("SHOW PROCEDURE USER user") {
    failsToParse
  }

  // Invalid clause order

  for (prefix <- Seq("USE neo4j", "")) {
    test(s"$prefix SHOW PROCEDURES YIELD * WITH * MATCH (n) RETURN n") {
      // Can't parse WITH after SHOW
      failsToParse
    }

    test(s"$prefix UNWIND range(1,10) as b SHOW PROCEDURES YIELD * RETURN *") {
      // Can't parse SHOW  after UNWIND
      failsToParse
    }

    test(s"$prefix SHOW PROCEDURES WITH name, type RETURN *") {
      // Can't parse WITH after SHOW
      failsToParse
    }

    test(s"$prefix WITH 'n' as n SHOW PROCEDURES YIELD name RETURN name as numIndexes") {
      failsToParse
    }

    test(s"$prefix SHOW PROCEDURES RETURN name as numIndexes") {
      failsToParse
    }

    test(s"$prefix SHOW PROCEDURES WITH 1 as c RETURN name as numIndexes") {
      failsToParse
    }

    test(s"$prefix SHOW PROCEDURES WITH 1 as c") {
      failsToParse
    }

    test(s"$prefix SHOW PROCEDURES YIELD a WITH a RETURN a") {
      failsToParse
    }

    test(s"$prefix SHOW PROCEDURES YIELD as UNWIND as as a RETURN a") {
      failsToParse
    }

    test(s"$prefix SHOW PROCEDURES YIELD name SHOW PROCEDURES YIELD name2 RETURN name2") {
      failsToParse
    }

    test(s"$prefix SHOW PROCEDURES RETURN name2 YIELD name2") {
      failsToParse
    }
  }

  // Brief/verbose not allowed

  test("SHOW PROCEDURE BRIEF") {
    failsToParse
  }

  test("SHOW PROCEDURE BRIEF OUTPUT") {
    failsToParse
  }

  test("SHOW PROCEDURES BRIEF YIELD *") {
    failsToParse
  }

  test("SHOW PROCEDURES BRIEF RETURN *") {
    failsToParse
  }

  test("SHOW PROCEDURES BRIEF WHERE name = 'my.proc'") {
    failsToParse
  }

  test("SHOW PROCEDURE VERBOSE") {
    failsToParse
  }

  test("SHOW PROCEDURE VERBOSE OUTPUT") {
    failsToParse
  }

  test("SHOW PROCEDURES VERBOSE YIELD *") {
    failsToParse
  }

  test("SHOW PROCEDURES VERBOSE RETURN *") {
    failsToParse
  }

  test("SHOW PROCEDURES VERBOSE WHERE name = 'my.proc'") {
    failsToParse
  }

  test("SHOW PROCEDURE OUTPUT") {
    failsToParse
  }

}
