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
package org.neo4j.cypher.internal.ast.factory.neo4j.privilege

import org.neo4j.cypher.internal.ast.factory.neo4j.ParserComparisonTestBase
import org.neo4j.cypher.internal.util.test_helpers.TestName
import org.scalatest.FunSuiteLike

class DatabasePrivilegeAdministrationCommandJavaCcParserTest extends ParserComparisonTestBase with FunSuiteLike with TestName {

  Seq(
    ("GRANT", "TO"),
    ("DENY", "TO"),
    ("REVOKE GRANT", "FROM"),
    ("REVOKE DENY", "FROM"),
    ("REVOKE", "FROM")
  ).foreach {
    case (verb: String, preposition: String) =>

      Seq(
        "ACCESS",
        "START",
        "STOP",
        "CREATE INDEX",
        "CREATE INDEXES",
        "DROP INDEX",
        "DROP INDEXES",
        "SHOW INDEX",
        "SHOW INDEXES",
        "INDEX",
        "INDEXES",
        "INDEX MANAGEMENT",
        "INDEXES MANAGEMENT",
        "CREATE CONSTRAINT",
        "CREATE CONSTRAINTS",
        "DROP CONSTRAINT",
        "DROP CONSTRAINTS",
        "SHOW CONSTRAINT",
        "SHOW CONSTRAINTS",
        "CONSTRAINT",
        "CONSTRAINTS",
        "CONSTRAINT MANAGEMENT",
        "CONSTRAINTS MANAGEMENT",
        "CREATE NEW LABEL",
        "CREATE NEW LABELS",
        "CREATE NEW NODE LABEL",
        "CREATE NEW NODE LABELS",
        "CREATE NEW TYPE",
        "CREATE NEW TYPES",
        "CREATE NEW RELATIONSHIP TYPE",
        "CREATE NEW RELATIONSHIP TYPES",
        "CREATE NEW NAME",
        "CREATE NEW NAMES",
        "CREATE NEW PROPERTY NAME",
        "CREATE NEW PROPERTY NAMES",
        "NAME",
        "NAME MANAGEMENT",
        "ALL",
        "ALL PRIVILEGES",
        "ALL DATABASE PRIVILEGES"
      ).foreach {
        privilege: String =>

          test(s"$verb $privilege ON DATABASE * $preposition $$role") {
            assertSameAST(testName)
          }

          test(s"$verb $privilege ON DATABASES * $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb $privilege ON DATABASE * $preposition role1, role2") {
            assertSameAST(testName)
          }

          test(s"$verb $privilege ON DATABASE foo $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb $privilege ON DATABASE `fo:o` $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb $privilege ON DATABASE more.Dots.more.Dots $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb $privilege ON DATABASE foo $preposition `r:ole`") {
            assertSameAST(testName)
          }

          test(s"$verb $privilege ON DATABASE foo $preposition role1, $$role2") {
            assertSameAST(testName)
          }

          test(s"$verb $privilege ON DATABASE $$foo $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb $privilege ON DATABASE foo, bar $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb $privilege ON DATABASES foo, $$bar $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb $privilege ON HOME DATABASE $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb $privilege ON HOME DATABASE $preposition $$role1, role2") {
            assertSameAST(testName)
          }

          test(s"$verb $privilege ON DEFAULT DATABASE $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb $privilege ON DEFAULT DATABASE $preposition $$role1, role2") {
            assertSameAST(testName)
          }

          test(s"$verb $privilege ON GRAPH * $preposition role") {
            // GRAPH instead of DATABASE
            if (!(privilege.equals("ALL") || privilege.equals("ALL PRIVILEGES"))) {
              assertSameAST(testName)
            }
          }

          test(s"$verb $privilege ON DATABASE fo:o $preposition role") {
            // invalid database name
            assertSameAST(testName)
          }

          test(s"$verb $privilege ON DATABASE foo, * $preposition role") {
            // specific database followed by *
            assertSameAST(testName)
          }

          test(s"$verb $privilege ON DATABASE *, foo $preposition role") {
            // * followed by specific database
            assertSameAST(testName)
          }

          test(s"$verb $privilege ON DATABASE foo $preposition r:ole") {
            // invalid role name
            assertSameAST(testName)
          }

          test(s"$verb $privilege ON DATABASES * $preposition") {
            // Missing role
            assertSameAST(testName)
          }

          test(s"$verb $privilege ON DATABASES *") {
            // Missing role and preposition
            assertSameAST(testName)
          }

          test(s"$verb $privilege ON DATABASES $preposition role") {
            // Missing dbName
            assertSameAST(testName)
          }

          test(s"$verb $privilege ON * $preposition role") {
            // Missing DATABASE keyword
            assertSameAST(testName)
          }

          test(s"$verb $privilege DATABASE foo $preposition role") {
            // Missing ON keyword
            assertSameAST(testName)
          }

          test(s"$verb $privilege ON HOME DATABASES $preposition role") {
            // 'databases' instead of 'database'
            assertJavaCCExceptionStart(testName, """Invalid input 'DATABASES': expected "DATABASE"""")
          }

          test(s"$verb $privilege ON HOME DATABASE foo $preposition role") {
            // both home and database name
            assertJavaCCExceptionStart(testName, s"""Invalid input 'foo': expected "$preposition"""")
          }

          test(s"$verb $privilege ON HOME DATABASE * $preposition role") {
            // both home and *
            assertJavaCCExceptionStart(testName, s"""Invalid input '*': expected "$preposition"""")
          }

          test(s"$verb $privilege ON DEFAULT DATABASES $preposition role") {
            // 'databases' instead of 'database'
            assertJavaCCExceptionStart(testName, """Invalid input 'DATABASES': expected "DATABASE"""")
          }

          test(s"$verb $privilege ON DEFAULT DATABASE foo $preposition role") {
            // both default and database name
            assertJavaCCExceptionStart(testName, s"""Invalid input 'foo': expected "$preposition"""")
          }

          test(s"$verb $privilege ON DEFAULT DATABASE * $preposition role") {
            // both default and *
            assertJavaCCExceptionStart(testName, s"""Invalid input '*': expected "$preposition"""")
          }
      }

      // Dropping instead of creating name management privileges

      test(s"$verb DROP NEW LABEL ON DATABASE * $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb DROP NEW TYPE ON DATABASE * $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb DROP NEW NAME ON DATABASE * $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb DROP LABEL ON DATABASE * $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb DROP TYPE ON DATABASE * $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb DROP NAME ON DATABASE * $preposition role") {
        assertSameAST(testName)
      }

  }

  // transaction management

  Seq(
    ("GRANT", "TO"),
    ("DENY", "TO"),
    ("REVOKE GRANT", "FROM"),
    ("REVOKE DENY", "FROM"),
    ("REVOKE", "FROM")
  ).foreach {
    case (verb: String, preposition: String) =>

      test(s"$verb SHOW TRANSACTION (*) ON DATABASE * $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb SHOW TRANSACTIONS (*) ON DATABASES foo $preposition role1, role2") {
        assertSameAST(testName)
      }

      test(s"$verb SHOW TRANSACTIONS (*) ON DATABASES $$foo $preposition $$role1, $$role2") {
        assertSameAST(testName)
      }

      test(s"$verb SHOW TRANSACTION (user) ON HOME DATABASE $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb SHOW TRANSACTION ($$user) ON HOME DATABASE $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb SHOW TRANSACTION (user) ON DEFAULT DATABASE $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb SHOW TRANSACTION ($$user) ON DEFAULT DATABASE $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb SHOW TRANSACTIONS (user1,user2) ON DATABASES * $preposition role1, role2") {
        assertSameAST(testName)
      }

      test(s"$verb SHOW TRANSACTIONS ON DATABASES * $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb SHOW TRANSACTION ON DATABASE foo, bar $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb SHOW TRANSACTION (user) ON DATABASES foo, $$bar $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb TERMINATE TRANSACTION (*) ON DATABASE * $preposition $$role") {
        assertSameAST(testName)
      }

      test(s"$verb TERMINATE TRANSACTIONS (*) ON DATABASES foo $preposition role1, role2") {
        assertSameAST(testName)
      }

      test(s"$verb TERMINATE TRANSACTIONS (*) ON DATABASES $$foo $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb TERMINATE TRANSACTION (user) ON HOME DATABASE $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb TERMINATE TRANSACTION (user) ON DEFAULT DATABASE $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb TERMINATE TRANSACTIONS (user1,user2) ON DATABASES * $preposition role1, role2") {
        assertSameAST(testName)
      }

      test(s"$verb TERMINATE TRANSACTIONS ($$user1,$$user2) ON DATABASES * $preposition role1, role2") {
        assertSameAST(testName)
      }

      test(s"$verb TERMINATE TRANSACTIONS ON DATABASES * $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb TERMINATE TRANSACTION ON DATABASE foo, bar $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb TERMINATE TRANSACTION (user) ON DATABASES foo, $$bar $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb TRANSACTION ON DATABASES * $preposition role1, role2") {
        assertSameAST(testName)
      }

      test(s"$verb TRANSACTION (*) ON DATABASES foo $preposition role1, $$role2") {
        assertSameAST(testName)
      }

      test(s"$verb TRANSACTION (*) ON DATABASES $$foo $preposition role1, role2") {
        assertSameAST(testName)
      }

      test(s"$verb TRANSACTION (user) ON DATABASES * $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb TRANSACTION ON DATABASE foo, bar $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb TRANSACTION (user) ON DATABASES foo, $$bar $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb TRANSACTION MANAGEMENT ON HOME DATABASE $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb TRANSACTION MANAGEMENT ON DEFAULT DATABASE $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb TRANSACTION MANAGEMENT (*) ON DATABASE * $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb TRANSACTION MANAGEMENT (user) ON DATABASES * $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb TRANSACTION MANAGEMENT (user1, $$user2) ON DATABASES * $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb TRANSACTION MANAGEMENT ON DATABASE foo, bar $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb TRANSACTION MANAGEMENT (user) ON DATABASES foo, $$bar $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb TRANSACTION ON DATABASE foo, * $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb TRANSACTION ON DATABASE *, foo $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb TRANSACTIONS ON DATABASES * $preposition role") {
        // can't have the complete error message, since grant and revoke have more accepted keywords than deny
        val expected =
          """Invalid input 'TRANSACTIONS': expected
            |  "ACCESS"""".stripMargin
        assertJavaCCExceptionStart(testName, expected)
      }

      test(s"$verb TRANSACTIONS (*) ON DATABASES * $preposition role") {
        // can't have the complete error message, since grant and revoke have more accepted keywords than deny
        val expected =
          """Invalid input 'TRANSACTIONS': expected
            |  "ACCESS"""".stripMargin
        assertJavaCCExceptionStart(testName, expected)
      }

      test(s"$verb TRANSACTIONS MANAGEMENT ON DATABASES * $preposition role") {
        // can't have the complete error message, since grant and revoke have more accepted keywords than deny
        val expected =
          """Invalid input 'TRANSACTIONS': expected
            |  "ACCESS"""".stripMargin
        assertJavaCCExceptionStart(testName, expected)
      }

      test(s"$verb TRANSACTIONS MANAGEMENT (*) ON DATABASES * $preposition role") {
        // can't have the complete error message, since grant and revoke have more accepted keywords than deny
        val expected =
          """Invalid input 'TRANSACTIONS': expected
            |  "ACCESS"""".stripMargin
        assertJavaCCExceptionStart(testName, expected)
      }
  }
}
