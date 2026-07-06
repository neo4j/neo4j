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

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.CreateReplicaDatabase
import org.neo4j.cypher.internal.ast.IfExistsDo
import org.neo4j.cypher.internal.ast.IfExistsDoNothing
import org.neo4j.cypher.internal.ast.IfExistsInvalidSyntax
import org.neo4j.cypher.internal.ast.IfExistsReplace
import org.neo4j.cypher.internal.ast.IfExistsThrowError
import org.neo4j.cypher.internal.ast.IndefiniteWait
import org.neo4j.cypher.internal.ast.NoOptions
import org.neo4j.cypher.internal.ast.NoWait
import org.neo4j.cypher.internal.ast.Options
import org.neo4j.cypher.internal.ast.OptionsMap
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.TimeoutAfter
import org.neo4j.cypher.internal.ast.Topology
import org.neo4j.cypher.internal.ast.WaitUntilComplete
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5
import org.neo4j.gqlstatus.GqlStatusInfoCodes

class CreateReplicaDatabaseParserTest extends AdministrationAndSchemaCommandParserTestBase {

  test("CREATE REPLICA DATABASE foo") {
    assertCreateReplicaDatabase(NoOptions)
  }

  test("CREATE REPLICA DATABASE foo OPTIONS {replicaConfig: {remote: 'bar'}}") {
    assertCreateReplicaDatabase(
      OptionsMap(Map("replicaConfig" -> mapOf("remote" -> literalString("bar"))))(pos)
    )
  }

  test("CREATE REPLICA DATABASE foo IF NOT EXISTS") {
    assertCreateReplicaDatabase(NoOptions, ifExists = IfExistsDoNothing)
  }

  test("CREATE OR REPLACE REPLICA DATABASE foo") {
    assertCreateReplicaDatabase(NoOptions, ifExists = IfExistsReplace)
  }

  test("CREATE OR REPLACE REPLICA DATABASE foo IF NOT EXISTS") {
    assertCreateReplicaDatabase(NoOptions, ifExists = IfExistsInvalidSyntax)
  }

  test("CREATE REPLICA DATABASE foo WAIT") {
    assertCreateReplicaDatabase(NoOptions, wait = IndefiniteWait()(defaultPos))
  }

  test("CREATE REPLICA DATABASE foo WAIT 10 SECONDS") {
    assertCreateReplicaDatabase(NoOptions, wait = TimeoutAfter("10")(defaultPos))
  }

  test("CREATE REPLICA DATABASE foo DEFAULT LANGUAGE CYPHER 25") {
    assertCreateReplicaDatabase(NoOptions, defaultLanguage = Some(CypherVersion.Cypher25))
  }

  test("CREATE REPLICA DATABASE foo SET DEFAULT LANGUAGE CYPHER 25 OPTIONS {replicaConfig: {remote: 'bar'}} WAIT") {
    assertCreateReplicaDatabase(
      OptionsMap(Map("replicaConfig" -> mapOf("remote" -> literalString("bar"))))(pos),
      wait = IndefiniteWait()(defaultPos),
      defaultLanguage = Some(CypherVersion.Cypher25)
    )
  }

  test("CREATE REPLICA DATABASE foo TOPOLOGY 1 PRIMARY") {
    assertCreateReplicaDatabase(NoOptions, topology = Some(Topology(Some(Left(1)), None)))
  }

  test("CREATE REPLICA DATABASE foo TOPOLOGY 1 PRIMARY 2 SECONDARIES") {
    assertCreateReplicaDatabase(
      NoOptions,
      topology = Some(Topology(Some(Left(1)), Some(Left(2))))
    )
  }

  test("CREATE REPLICA DATABASE foo TOPOLOGY 1 PRIMARY OPTIONS {replicaConfig: {remote: 'bar'}}") {
    assertCreateReplicaDatabase(
      OptionsMap(Map("replicaConfig" -> mapOf("remote" -> literalString("bar"))))(pos),
      topology = Some(Topology(Some(Left(1)), None))
    )
  }

  test("USE system CREATE REPLICA DATABASE foo OPTIONS {replicaConfig: {remote: 'bar'}}") {
    parsesIn[Statement] {
      case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'DATABASE': expected a graph pattern")
      case _ => _.toAst(
          CreateReplicaDatabase(
            literalFoo,
            IfExistsThrowError,
            OptionsMap(Map("replicaConfig" -> mapOf("remote" -> literalString("bar"))))(pos),
            NoWait()(pos),
            None,
            None
          )(pos).withGraph(Some(use(List("system"), resolveStrictly = true)))
        )
    }
  }

  test("CREATE REPLICA DATABASE foo SET GRAPH SHARD { TOPOLOGY 1 PRIMARY }") {
    failsParsing[Statements].in {
      case Cypher5 => _.withSyntaxErrorContaining(
          "Invalid input 'DATABASE': expected a graph pattern"
        )
      case _ => _.withSyntaxErrorContaining(
          "Invalid input 'GRAPH': expected 'DEFAULT LANGUAGE CYPHER' or 'TOPOLOGY'",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input 'GRAPH', expected: 'DEFAULT LANGUAGE CYPHER' or 'TOPOLOGY'."
        )
    }
  }

  test("CREATE REPLICA DATABASE foo SET PROPERTY SHARDS { COUNT 1 }") {
    failsParsing[Statements].in {
      case Cypher5 => _.withSyntaxErrorContaining(
          "Invalid input 'DATABASE': expected a graph pattern"
        )
      case _ => _.withSyntaxErrorContaining(
          "Invalid input 'PROPERTY': expected 'DEFAULT LANGUAGE CYPHER' or 'TOPOLOGY'",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input 'PROPERTY', expected: 'DEFAULT LANGUAGE CYPHER' or 'TOPOLOGY'."
        )
    }
  }

  private def assertCreateReplicaDatabase(
    options: Options,
    ifExists: IfExistsDo = IfExistsThrowError,
    wait: WaitUntilComplete = NoWait()(pos),
    topology: Option[Topology] = None,
    defaultLanguage: Option[CypherVersion] = None
  ): Unit =
    assertAst(
      CreateReplicaDatabase(
        literalFoo,
        ifExists,
        options,
        wait,
        topology,
        defaultLanguage
      )(pos),
      comparePosition = false,
      supportedInCypher5 = false
    )
}
