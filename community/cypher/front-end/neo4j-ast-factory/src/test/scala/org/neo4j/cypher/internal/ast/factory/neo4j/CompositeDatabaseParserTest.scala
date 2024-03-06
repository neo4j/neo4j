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
import org.neo4j.cypher.internal.ast.DestroyData
import org.neo4j.cypher.internal.ast.IfExistsDoNothing
import org.neo4j.cypher.internal.ast.IfExistsReplace
import org.neo4j.cypher.internal.ast.IfExistsThrowError
import org.neo4j.cypher.internal.ast.IndefiniteWait
import org.neo4j.cypher.internal.ast.NoOptions
import org.neo4j.cypher.internal.ast.NoWait
import org.neo4j.cypher.internal.ast.OptionsMap
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.TimeoutAfter

class CompositeDatabaseParserTest extends AdministrationAndSchemaCommandParserTestBase {

  test("CREATE COMPOSITE DATABASE name") {
    yields[Statements](ast.CreateCompositeDatabase(namespacedName("name"), IfExistsThrowError, NoOptions, NoWait))
  }

  test("CREATE COMPOSITE DATABASE $name") {
    yields[Statements](ast.CreateCompositeDatabase(stringParamName("name"), IfExistsThrowError, NoOptions, NoWait))
  }

  test("CREATE COMPOSITE DATABASE `db.name`") {
    yields[Statements](ast.CreateCompositeDatabase(namespacedName("db.name"), IfExistsThrowError, NoOptions, NoWait))
  }

  test("CREATE COMPOSITE DATABASE db.name") {
    yields[Statements](ast.CreateCompositeDatabase(namespacedName("db", "name"), IfExistsThrowError, NoOptions, NoWait))
  }

  test("CREATE COMPOSITE DATABASE name IF NOT EXISTS") {
    yields[Statements](ast.CreateCompositeDatabase(namespacedName("name"), IfExistsDoNothing, NoOptions, NoWait))
  }

  test("CREATE OR REPLACE COMPOSITE DATABASE name") {
    yields[Statements](ast.CreateCompositeDatabase(namespacedName("name"), IfExistsReplace, NoOptions, NoWait))
  }

  test("CREATE COMPOSITE DATABASE name OPTIONS {}") {
    yields[Statements](ast.CreateCompositeDatabase(
      namespacedName("name"),
      IfExistsThrowError,
      OptionsMap(Map.empty),
      NoWait
    ))
  }

  test("CREATE COMPOSITE DATABASE name OPTIONS {someKey: 'someValue'} NOWAIT") {
    yields[Statements](ast.CreateCompositeDatabase(
      namespacedName("name"),
      IfExistsThrowError,
      OptionsMap(Map(
        "someKey" -> literalString("someValue")
      )),
      NoWait
    ))
  }

  test("CREATE COMPOSITE DATABASE name TOPOLOGY 1 PRIMARY") {
    assertFailsWithMessage[Statements](
      testName,
      """Invalid input 'TOPOLOGY': expected
        |  "."
        |  "IF"
        |  "NOWAIT"
        |  "OPTIONS"
        |  "WAIT"
        |  <EOF> (line 1, column 32 (offset: 31))""".stripMargin
    )
  }

  test("CREATE COMPOSITE DATABASE name WAIT") {
    yields[Statements](ast.CreateCompositeDatabase(
      namespacedName("name"),
      IfExistsThrowError,
      NoOptions,
      IndefiniteWait
    ))
  }

  test("CREATE COMPOSITE DATABASE name NOWAIT") {
    yields[Statements](ast.CreateCompositeDatabase(namespacedName("name"), IfExistsThrowError, NoOptions, NoWait))
  }

  test("CREATE COMPOSITE DATABASE name WAIT 10 SECONDS") {
    yields[Statements](ast.CreateCompositeDatabase(
      namespacedName("name"),
      IfExistsThrowError,
      NoOptions,
      TimeoutAfter(10)
    ))
  }

  test("DROP COMPOSITE DATABASE name") {
    yields[Statements](ast.DropDatabase(
      namespacedName("name"),
      ifExists = false,
      composite = true,
      DestroyData,
      NoWait
    ))
  }

  test("DROP COMPOSITE DATABASE `db.name`") {
    yields[Statements](ast.DropDatabase(
      namespacedName("db.name"),
      ifExists = false,
      composite = true,
      DestroyData,
      NoWait
    ))
  }

  test("DROP COMPOSITE DATABASE db.name") {
    yields[Statements](ast.DropDatabase(
      namespacedName("db", "name"),
      ifExists = false,
      composite = true,
      DestroyData,
      NoWait
    ))
  }

  test("DROP COMPOSITE DATABASE $name") {
    yields[Statements](ast.DropDatabase(
      stringParamName("name"),
      ifExists = false,
      composite = true,
      DestroyData,
      NoWait
    ))
  }

  test("DROP COMPOSITE DATABASE name IF EXISTS") {
    yields[Statements](ast.DropDatabase(namespacedName("name"), ifExists = true, composite = true, DestroyData, NoWait))
  }

  test("DROP COMPOSITE DATABASE name WAIT") {
    yields[Statements](ast.DropDatabase(
      namespacedName("name"),
      ifExists = false,
      composite = true,
      DestroyData,
      IndefiniteWait
    ))
  }

  test("DROP COMPOSITE DATABASE name WAIT 10 SECONDS") {
    yields[Statements](ast.DropDatabase(
      namespacedName("name"),
      ifExists = false,
      composite = true,
      DestroyData,
      TimeoutAfter(10)
    ))
  }

  test("DROP COMPOSITE DATABASE name NOWAIT") {
    yields[Statements](ast.DropDatabase(
      namespacedName("name"),
      ifExists = false,
      composite = true,
      DestroyData,
      NoWait
    ))
  }
}
