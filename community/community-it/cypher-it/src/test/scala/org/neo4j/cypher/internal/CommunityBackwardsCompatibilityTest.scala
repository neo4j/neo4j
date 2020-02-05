/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.exceptions.SecurityAdministrationException
import org.neo4j.exceptions.SyntaxException

class CommunityBackwardsCompatibilityTest extends ExecutionEngineFunSuite {

  // Additions in 4.0

  test("community administration commands should not work with CYPHER 3.5") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    val exception = the[SyntaxException] thrownBy {
      execute("CYPHER 3.5 SHOW DATABASES")
    }
    exception.getMessage should include("Commands towards system database are not supported in this Cypher version.")
  }

  test("enterprise administration commands should fail correctly with CYPHER 3.5") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    val exception_35 = the[SyntaxException] thrownBy {
      execute("CYPHER 3.5 CREATE ROLE role")
    }
    exception_35.getMessage should include("Commands towards system database are not supported in this Cypher version.")

    // WHEN
    val exception_40 = the[SecurityAdministrationException] thrownBy {
      execute("CYPHER 4.0 CREATE ROLE role")
    }
    exception_40.getMessage should include("Unsupported administration command: CREATE ROLE role")
  }

  test("procedures towards system database should not work with CYPHER 3.5") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    val exception = the[SyntaxException] thrownBy {
      execute("CYPHER 3.5 dbms.security.createUser('Alice', '1234', true)")
    }
    exception.getMessage should include("Commands towards system database are not supported in this Cypher version.")
  }

  test("new create index syntax should not work in community with CYPHER 3.5") {
    // WHEN
    val exception = the[SyntaxException] thrownBy {
      execute("CYPHER 3.5 CREATE INDEX my_index FOR (n:Label) ON (n.prop)")
    }
    exception.getMessage should include("Creating index using this syntax is not supported in this Cypher version.")

    // THEN
    graph.getMaybeIndex("Label", Seq("prop")).isEmpty should be(true)
  }

  test("existential subquery should not work with CYPHER 3.5") {
    // WHEN
    val exception = the[SyntaxException] thrownBy {
      execute("CYPHER 3.5 MATCH (n) WHERE EXISTS { (n)-->() } RETURN n")
    }
    // THEN
    exception.getMessage should include("Existential subquery is not supported in this Cypher version.")
  }

  // Additions in 4.1

  test("user management administration commands should fail correctly with CYPHER 3.5 and 4.0") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN 3.5
    val exception_35 = the[SyntaxException] thrownBy {
      execute(s"CYPHER 3.5 GRANT CREATE USER ON DBMS TO reader")
    }
    exception_35.getMessage should include("Commands towards system database are not supported in this Cypher version.")

    // WHEN 4.0
    val exception_40 = the[SyntaxException] thrownBy {
      execute(s"CYPHER 4.0 GRANT CREATE USER ON DBMS TO reader")
    }
    exception_40.getMessage should include("User administration privileges are not supported in this Cypher version.")

    // WHEN 4.1
    val exception_41 = the[SecurityAdministrationException] thrownBy {
      execute("GRANT CREATE USER ON DBMS TO reader")
    }
    exception_41.getMessage should include("Unsupported administration command: GRANT CREATE USER ON DBMS TO reader")
  }
}
