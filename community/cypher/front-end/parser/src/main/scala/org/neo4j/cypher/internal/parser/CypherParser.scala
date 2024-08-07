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
import org.neo4j.cypher.internal.util.CypherException
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.parboiled.scala.EOI
import org.parboiled.scala.Parser
import org.parboiled.scala.Rule1

/**
 * Parser for Cypher queries.
 */
class CypherParser extends Parser
  with Statement
  with Expressions {

  @throws(classOf[CypherException])
  def parse(queryText: String, cypherExceptionFactory: CypherExceptionFactory): ast.Statement =
    parseOrThrow(queryText, cypherExceptionFactory, CypherParser.Statements)
}

object CypherParser extends Parser with Statement with Expressions {
  val Statements: Rule1[Seq[ast.Statement]] = rule {
    oneOrMore(WS ~ Statement ~ WS, separator = ch(';')) ~~ optional(ch(';')) ~~ EOI.label("end of input")
  }
}
