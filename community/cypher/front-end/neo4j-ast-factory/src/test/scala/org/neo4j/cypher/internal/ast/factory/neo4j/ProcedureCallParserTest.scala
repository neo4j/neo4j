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

import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.cst.factory.neo4j.AntlrRule
import org.neo4j.cypher.internal.cst.factory.neo4j.Cst
import org.neo4j.cypher.internal.parser.CypherParser

class ProcedureCallParserTest extends ParserSyntaxTreeBase[Cst.CallClause, Clause] {

  implicit private val javaccRule: JavaccRule[Clause] = JavaccRule.CallClause
  implicit private val antlrRule: AntlrRule[CypherParser.CallClauseContext] = AntlrRule.CallClause

  test("CALL foo") {
    gives(call(Seq.empty, "foo", None))
  }

  test("CALL foo()") {
    gives(call(Seq.empty, "foo", Some(Seq.empty)))
  }

  test("CALL foo('Test', 1+2)") {
    gives(call(Seq.empty, "foo", Some(Vector(literalString("Test"), add(literalInt(1), literalInt(2))))))
  }

  test("CALL foo.bar.baz('Test', 1+2)") {
    gives(call(List("foo", "bar"), "baz", Some(Vector(literalString("Test"), add(literalInt(1), literalInt(2))))))
  }

  test("CALL foo YIELD bar") {
    gives(call(Seq.empty, "foo", None, Some(Seq(varFor("bar")))))
  }

  test("CALL foo YIELD bar, baz") {
    gives(call(Seq.empty, "foo", None, Some(Seq(varFor("bar"), varFor("baz")))))
  }

  test("CALL foo() YIELD bar") {
    gives(call(Seq.empty, "foo", Some(Seq.empty), Some(Seq(varFor("bar")))))
  }

  test("CALL foo() YIELD bar, baz") {
    gives(call(Seq.empty, "foo", Some(Seq.empty), Some(Seq(varFor("bar"), varFor("baz")))))
  }
}
