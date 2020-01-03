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

import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.util.DummyPosition
import org.neo4j.cypher.internal.v4_0.{expressions => ast}
import org.parboiled.scala._

class ListComprehensionTest extends ParserTest[ast.ListComprehension, Any] with Expressions {
  implicit val parserToTest: Rule1[ListComprehension] = ListComprehension ~ EOI
  val t = DummyPosition(0)

  test("tests") {

    parsing("[ a in p WHERE a.foo > 123 ]") shouldGive
      ast.ListComprehension(ExtractScope(ast.Variable("a")(t),
                                         Some(GreaterThan(
                                           Property(ast.Variable("a")(t),
                                                    ast.PropertyKeyName("foo")(t))(t),
                                           SignedDecimalIntegerLiteral("123")(t))(t)),
                                         None)(t),
                            ast.Variable("p")(t))(t)

    parsing("[ a in p | a.foo ]") shouldGive
      ast.ListComprehension(ExtractScope(ast.Variable("a")(t),
                                         None,
                                         Some(Property(ast.Variable("a")(t),ast.PropertyKeyName("foo")(t))(t))
                                        )(t),
                            ast.Variable("p")(t))(t)

    parsing("[ a in p WHERE a.foo > 123 | a.foo ]") shouldGive
      ast.ListComprehension(ExtractScope(ast.Variable("a")(t),
                                         Some(GreaterThan(
                                           Property(ast.Variable("a")(t),
                                                    ast.PropertyKeyName("foo")(t))(t),
                                           SignedDecimalIntegerLiteral("123")(t))(t)),
                                         Some(Property(ast.Variable("a")(t),ast.PropertyKeyName("foo")(t))(t))
                                        )(t),
                            ast.Variable("p")(t))(t)
  }

  def convert(result: ast.ListComprehension): Any = result
}
