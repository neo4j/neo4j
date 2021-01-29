/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.util.test_helpers.TestName
import org.scalatest.FunSuiteLike

class RoleAdministrationCommandJavaCcParserTest extends ParserComparisonTestBase with FunSuiteLike with TestName {

  test("USE GRAPH SYSTEM SHOW ROLES") {
    assertSameAST(testName)
  }

  test("SHOW ROLES") {
    assertSameAST(testName)
  }

  test("CATALOG SHOW ALL ROLES") {
    assertSameAST(testName)
  }

  test("CATALOG SHOW POPULATED ROLES") {
    assertSameAST(testName)
  }

  test("SHOW ROLES WITH USERS") {
    assertSameAST(testName)
  }

  test("CATALOG SHOW ALL ROLES WITH USERS") {
    assertSameAST(testName)
  }

  test("SHOW POPULATED ROLES WITH USERS") {
    assertSameAST(testName)
  }

  test("CATALOG SHOW ALL ROLES YIELD role") {
    assertSameAST(testName)
  }

  test("CATALOG SHOW ALL ROLES WHERE role='PUBLIC'") {
    assertSameAST(testName)
  }

  test("SHOW ALL ROLES YIELD role RETURN role") {
    assertSameAST(testName)
  }

  test("SHOW POPULATED ROLES YIELD role WHERE role='PUBLIC' RETURN role") {
    assertSameAST(testName)
  }

  test("SHOW POPULATED ROLES YIELD * RETURN *") {
    assertSameAST(testName)
  }

  test("SHOW ROLES WITH USERS YIELD * LIMIT 10 WHERE foo='bar' RETURN some,columns LIMIT 10") {
    assertSameAST(testName)
  }

  test("CATALOG SHOW ROLE") {
    assertSyntaxException(testName)
  }

  test("SHOW ALL ROLE") {
    assertSyntaxException(testName)
  }

  test("SHOW POPULATED ROLE") {
    assertSyntaxException(testName)
  }

  test("SHOW ROLE role") {
    assertSyntaxException(testName)
  }

  test("SHOW ROLE WITH USERS") {
    assertSyntaxException(testName)
  }

  test("CATALOG SHOW ROLES WITH USER") {
    assertSyntaxException(testName)
  }

  test("SHOW ROLE WITH USER") {
    assertSyntaxException(testName)
  }

  test("SHOW ALL ROLE WITH USERS") {
    assertSyntaxException(testName)
  }

  test("SHOW ALL ROLES WITH USER") {
    assertSyntaxException(testName)
  }

  test("SHOW ALL ROLE WITH USER") {
    assertSyntaxException(testName)
  }

  test("YIELD a, b, c WHERE a = b") {
    assertSyntaxException(testName)
  }

  test("SHOW POPULATED ROLES YIELD role ORDER BY role SKIP -1") {
    assertSameAST(testName)
  }

  test("SHOW POPULATED ROLES YIELD role ORDER BY role SKIP -1*4 + 2") {
    assertSameAST(testName)
  }

  test("SHOW POPULATED ROLES YIELD role ORDER BY role LIMIT -1") {
    assertSameAST(testName)
  }

  test("CATALOG SHOW POPULATED ROLE WITH USERS") {
    assertSyntaxException(testName)
  }

  test("CATALOG SHOW POPULATED ROLES WITH USER") {
    assertSyntaxException(testName)
  }

  test("CATALOG SHOW POPULATED ROLE WITH USER") {
    assertSyntaxException(testName)
  }

  test("CATALOG SHOW ROLES WITH USER user") {
    assertSyntaxException(testName)
  }

  test("SHOW POPULATED ROLES YIELD *,blah RETURN role") {
    assertSameAST(testName)
  }

  //  Creating roles

  test("CREATE ROLE foo") {
    assertSameAST(testName)
  }

  test("CREATE ROLE $foo") {
    assertSameAST(testName)
  }

  test("CATALOG CREATE ROLE `fo!$o`") {
    assertSameAST(testName)
  }

  test("CREATE ROLE ``") {
    assertSameAST(testName)
  }

  test("CREATE ROLE foo AS COPY OF bar") {
    assertSameAST(testName)
  }

  test("CREATE ROLE foo AS COPY OF $bar") {
    assertSameAST(testName)
  }

  test("CREATE ROLE foo AS COPY OF ``") {
    assertSameAST(testName)
  }

  test("CREATE ROLE `` AS COPY OF bar") {
    assertSameAST(testName)
  }

  test("CREATE ROLE foo IF NOT EXISTS") {
    assertSameAST(testName)
  }

  test("CREATE ROLE foo IF NOT EXISTS AS COPY OF bar") {
    assertSameAST(testName)
  }

  test("CREATE OR REPLACE ROLE foo") {
    assertSameAST(testName)
  }

  test("CREATE OR REPLACE ROLE foo AS COPY OF bar") {
    assertSameAST(testName)
  }

  test("CREATE OR REPLACE ROLE foo IF NOT EXISTS") {
    assertSameAST(testName)
  }

  test("CREATE OR REPLACE ROLE foo IF NOT EXISTS AS COPY OF bar") {
    assertSameAST(testName)
  }

  test("CATALOG CREATE ROLE \"foo\"") {
    assertSameAST(testName)
  }

  test("CREATE ROLE f%o") {
    assertSameAST(testName)
  }

  test("CREATE ROLE  IF NOT EXISTS") {
    assertSameAST(testName)
  }

  test("CREATE ROLE foo IF EXISTS") {
    assertSameAST(testName)
  }

  test("CREATE OR REPLACE ROLE ") {
    assertSyntaxException(testName)
  }

  test("CREATE ROLE foo AS COPY OF") {
    assertSyntaxException(testName)
  }

  test("CREATE ROLE foo IF NOT EXISTS AS COPY OF") {
    assertSyntaxException(testName)
  }

  test("CREATE OR REPLACE ROLE foo AS COPY OF") {
    assertSyntaxException(testName)
  }

  //  Dropping role

  test("DROP ROLE foo") {
    assertSameAST(testName)
  }

  test("DROP ROLE $foo") {
    assertSameAST(testName)
  }

  test("DROP ROLE ``") {
    assertSameAST(testName)
  }

  test("DROP ROLE foo IF EXISTS") {
    assertSameAST(testName)
  }

  test("DROP ROLE `` IF EXISTS") {
    assertSameAST(testName)
  }

  test("DROP ROLE ") {
    assertSyntaxException(testName)
  }

  test("DROP ROLE  IF EXISTS") {
    assertSameAST(testName)
  }

  test("DROP ROLE foo IF NOT EXISTS") {
    assertSameAST(testName)
  }

  //  Granting and Revoking role(s)

  Seq("ROLE", "ROLES").foreach {
    roleKeyword =>

      Seq(
        ("GRANT", "TO"),
        ("REVOKE", "FROM")
      ).foreach {
        case (verb: String, preposition: String) =>

          test(s"$verb $roleKeyword foo $preposition abc") {
            assertSameAST(testName)
          }

          test(s"CATALOG $verb $roleKeyword foo $preposition abc") {
            assertSameAST(testName)
          }

          test(s"$verb $roleKeyword " +
            s"catalog, show, populated, roles, role, users, replace, grant, revoke, if, copy, of, to " +
            s"$preposition abc") {
            assertSameAST(testName)
          }

          test(s"$verb $roleKeyword foo $preposition abc, def") {
            assertSameAST(testName)
          }

          test(s"$verb $roleKeyword foo,bla,roo $preposition bar, baz,abc,  def") {
            assertSameAST(testName)
          }

          test(s"$verb $roleKeyword `fo:o` $preposition bar") {
            assertSameAST(testName)
          }

          test(s"$verb $roleKeyword foo $preposition `b:ar`") {
            assertSameAST(testName)
          }

          test(s"$verb $roleKeyword `$$f00`,bar $preposition abc,`$$a&c`") {
            assertSameAST(testName)
          }

          // Should fail to parse if not following the pattern $command $roleKeyword role(s) $preposition user(s)

          test(s"$verb $roleKeyword") {
            assertSyntaxException(testName)
          }

          test(s"$verb $roleKeyword foo") {
            assertSyntaxException(testName)
          }

          test(s"$verb $roleKeyword foo $preposition") {
            assertSyntaxException(testName)
          }

          test(s"$verb $roleKeyword $preposition abc") {
            assertSameAST(testName)
          }

          // Should fail to parse when invalid user or role name

          test(s"$verb $roleKeyword fo:o $preposition bar") {
            assertSameAST(testName)
          }

          test(s"$verb $roleKeyword foo $preposition b:ar") {
            assertSameAST(testName)
          }
      }

      // Should fail to parse when mixing TO and FROM

      test(s"GRANT $roleKeyword foo FROM abc") {
        assertSameAST(testName)
      }

      test(s"REVOKE $roleKeyword foo TO abc") {
        assertSameAST(testName)
      }

      // ROLES TO USER only have GRANT and REVOKE and not DENY

      test(s"DENY $roleKeyword foo TO abc") {
        assertSyntaxException(testName)
      }
  }

  test("GRANT ROLE $a TO $x") {
    assertSameAST(testName)
  }

  test("REVOKE ROLE $a FROM $x") {
    assertSameAST(testName)
  }

  test("GRANT ROLES a, $b, $c TO $x, y, z") {
    assertSameAST(testName)
  }

  test("REVOKE ROLES a, $b, $c FROM $x, y, z") {
    assertSameAST(testName)
  }
}
