/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.v3_5.it

import org.neo4j.cypher.internal.v3_5.ast
import org.neo4j.cypher.internal.v3_5.ast.PeriodicCommitHint
import org.neo4j.cypher.internal.v3_5.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.v3_5.parser.ParserTest
import org.neo4j.cypher.internal.v3_5.parser.Query
import org.neo4j.cypher.internal.v3_5.util.DummyPosition
import org.parboiled.scala._

class PeriodicCommitHintIT extends ParserTest[ast.PeriodicCommitHint, Any] with Query {

  implicit val parserToTest: Rule1[PeriodicCommitHint] = PeriodicCommitHint ~ EOI

  val t = DummyPosition(0)

  test("tests") {
    parsing("USING PERIODIC COMMIT") shouldGive ast.PeriodicCommitHint(None)(t)
    parsing("USING PERIODIC COMMIT 300") shouldGive ast.PeriodicCommitHint(Some(SignedDecimalIntegerLiteral("300")(t)))(t)
  }

  override def convert(astNode: ast.PeriodicCommitHint): Any = astNode
}
