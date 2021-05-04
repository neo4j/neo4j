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
import org.parboiled.scala.Parser
import org.parboiled.scala.Rule1

//noinspection ConvertibleToMethodValue
// Can't convert since that breaks parsing
trait Statement extends Parser
  with GraphSelection
  with Query
  with ShowCommand
  with SchemaCommand
  with AdministrationCommand
  with Base {

  def Statement: Rule1[ast.Statement] = ShowSchemaCommand | AdministrationCommand | SchemaCommand | Query
}
