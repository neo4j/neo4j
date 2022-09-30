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

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.DestroyData
import org.neo4j.cypher.internal.ast.IfExistsDoNothing
import org.neo4j.cypher.internal.ast.IfExistsReplace
import org.neo4j.cypher.internal.ast.IfExistsThrowError
import org.neo4j.cypher.internal.ast.IndefiniteWait
import org.neo4j.cypher.internal.ast.NoWait
import org.neo4j.cypher.internal.ast.TimeoutAfter

class CompositeDatabaseParserTest extends AdministrationAndSchemaCommandParserTestBase {

  test("CREATE COMPOSITE DATABASE name") {
    yields(ast.CreateCompositeDatabase(namespacedName("name"), IfExistsThrowError, NoWait))
  }

  test("CREATE COMPOSITE DATABASE $name") {
    yields(ast.CreateCompositeDatabase(stringParamName("name"), IfExistsThrowError, NoWait))
  }

  test("CREATE COMPOSITE DATABASE `db.name`") {
    yields(ast.CreateCompositeDatabase(namespacedName("db.name"), IfExistsThrowError, NoWait))
  }

  test("CREATE COMPOSITE DATABASE db.name") {
    yields(ast.CreateCompositeDatabase(namespacedName("db", "name"), IfExistsThrowError, NoWait))
  }

  test("CREATE COMPOSITE DATABASE name IF NOT EXISTS") {
    yields(ast.CreateCompositeDatabase(namespacedName("name"), IfExistsDoNothing, NoWait))
  }

  test("CREATE OR REPLACE COMPOSITE DATABASE name") {
    yields(ast.CreateCompositeDatabase(namespacedName("name"), IfExistsReplace, NoWait))
  }

  test("CREATE COMPOSITE DATABASE name OPTIONS {}") {
    assertFailsWithMessage(
      testName,
      """Invalid input 'OPTIONS': expected
        |  "."
        |  "IF"
        |  "NOWAIT"
        |  "WAIT"
        |  <EOF> (line 1, column 32 (offset: 31))""".stripMargin
    )
  }

  test("CREATE COMPOSITE DATABASE name TOPOLOGY 1 PRIMARY") {
    assertFailsWithMessage(
      testName,
      """Invalid input 'TOPOLOGY': expected
        |  "."
        |  "IF"
        |  "NOWAIT"
        |  "WAIT"
        |  <EOF> (line 1, column 32 (offset: 31))""".stripMargin
    )
  }

  test("CREATE COMPOSITE DATABASE name WAIT") {
    yields(ast.CreateCompositeDatabase(namespacedName("name"), IfExistsThrowError, IndefiniteWait))
  }

  test("CREATE COMPOSITE DATABASE name NOWAIT") {
    yields(ast.CreateCompositeDatabase(namespacedName("name"), IfExistsThrowError, NoWait))
  }

  test("CREATE COMPOSITE DATABASE name WAIT 10 SECONDS") {
    yields(ast.CreateCompositeDatabase(namespacedName("name"), IfExistsThrowError, TimeoutAfter(10)))
  }

  test("DROP COMPOSITE DATABASE name") {
    yields(ast.DropDatabase(namespacedName("name"), ifExists = false, composite = true, DestroyData, NoWait))
  }

  test("DROP COMPOSITE DATABASE `db.name`") {
    yields(ast.DropDatabase(namespacedName("db.name"), ifExists = false, composite = true, DestroyData, NoWait))
  }

  test("DROP COMPOSITE DATABASE db.name") {
    yields(ast.DropDatabase(namespacedName("db", "name"), ifExists = false, composite = true, DestroyData, NoWait))
  }

  test("DROP COMPOSITE DATABASE $name") {
    yields(ast.DropDatabase(stringParamName("name"), ifExists = false, composite = true, DestroyData, NoWait))
  }

  test("DROP COMPOSITE DATABASE name IF EXISTS") {
    yields(ast.DropDatabase(namespacedName("name"), ifExists = true, composite = true, DestroyData, NoWait))
  }

  test("DROP COMPOSITE DATABASE name WAIT") {
    yields(ast.DropDatabase(
      namespacedName("name"),
      ifExists = false,
      composite = true,
      DestroyData,
      IndefiniteWait
    ))
  }

  test("DROP COMPOSITE DATABASE name WAIT 10 SECONDS") {
    yields(ast.DropDatabase(
      namespacedName("name"),
      ifExists = false,
      composite = true,
      DestroyData,
      TimeoutAfter(10)
    ))
  }

  test("DROP COMPOSITE DATABASE name NOWAIT") {
    yields(ast.DropDatabase(
      namespacedName("name"),
      ifExists = false,
      composite = true,
      DestroyData,
      NoWait
    ))
  }
}
