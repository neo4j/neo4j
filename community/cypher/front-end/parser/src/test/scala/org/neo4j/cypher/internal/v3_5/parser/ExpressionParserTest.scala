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
package org.neo4j.cypher.internal.v3_5.parser


import org.neo4j.cypher.internal.v3_5.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.v3_5.{expressions => exp}
import org.parboiled.scala.Rule1

import scala.language.implicitConversions

class ExpressionParserTest
  extends ParserAstTest[exp.Expression]
    with Expressions
    with AstConstructionTestSupport {

  implicit val parser: Rule1[exp.Expression] = Expression

  test("a ~ b") {
    yields(exp.Equivalent(varFor("a"), varFor("b")))
  }

  test("[] ~ []") {
    yields(exp.Equivalent(exp.ListLiteral(Seq.empty)(pos), exp.ListLiteral(Seq.empty)(pos)))
  }

}
