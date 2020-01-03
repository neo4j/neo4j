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
package org.neo4j.cypher.internal.v4_0.parser

import org.neo4j.cypher.internal.v4_0.ast
import org.neo4j.cypher.internal.v4_0.ast.{AstConstructionTestSupport, SubQuery}
import org.parboiled.scala.Rule1

class SubQueriesTest
  extends ParserAstTest[ast.SubQuery]
    with Query
    with Clauses
    with AstConstructionTestSupport {

  implicit val parser: Rule1[SubQuery] = SubQuery

  test("CALL { RETURN 1 }") {
    gives(subQuery(return_(literalInt(1).unaliased)))
  }

  test("CALL { CALL { RETURN 1 as a } }") {
    gives(subQuery(subQuery(return_(literalInt(1).as("a")))))
  }

  test("CALL { RETURN 1 AS a UNION RETURN 2 AS a }") {
    gives(subQuery(unionDistinct(
      singleQuery(return_(literalInt(1).as("a"))),
      singleQuery(return_(literalInt(2).as("a")))
    )))
  }

  test("CALL { }") {
    failsToParse
  }

}
