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
import org.neo4j.cypher.internal.ast.Options
import org.neo4j.cypher.internal.ast.OptionsMap
import org.neo4j.cypher.internal.ast.OptionsParam
import org.neo4j.cypher.internal.expressions.Expression
import org.parboiled.scala.Parser
import org.parboiled.scala.Rule0
import org.parboiled.scala.Rule1
import org.parboiled.scala.group

// Common methods for schema and administration commands
trait CommandHelper extends Parser
                    with Base
                    with Query {
  def IndexKeyword: Rule0 = keyword("INDEXES") | keyword("INDEX")

  def ConstraintKeyword: Rule0 = keyword("CONSTRAINTS") | keyword("CONSTRAINT")

  def ProcedureKeyword: Rule0 = keyword("PROCEDURES") | keyword("PROCEDURE")

  def FunctionKeyword: Rule0 = keyword("FUNCTIONS") | keyword("FUNCTION")

  def TransactionKeyword: Rule0 = keyword("TRANSACTIONS") | keyword("TRANSACTION")

  def optionsMapOrParameter: Rule1[Options] = rule {
    keyword("OPTIONS") ~~ optionsMap ~~> (map => OptionsMap(map)) |
    keyword("OPTIONS") ~~ MapParameter ~~> (mapParam => OptionsParam(mapParam))
  }

  private def optionsMap: Rule1[Map[String, Expression]] = rule {
    group(ch('{') ~~ zeroOrMore(SymbolicNameString ~~ ch(':') ~~ Expression, separator = CommaSep) ~~ ch('}')) ~~>> (l => _ => l.toMap)
  }

  def ShowCommandClauses: Rule1[Either[(ast.Yield, Option[ast.Return]), ast.Where]] = rule("YIELD, WHERE") {
    (Yield ~~ optional(Return)) ~~> ((y, r) => Left(y -> r)) |
    (Where ~~>> (where => _ => Right(where)))
  }
}
