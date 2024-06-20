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
package org.neo4j.cypher.internal.ast.factory.expression

import org.neo4j.cypher.internal.ast.AscSortItem
import org.neo4j.cypher.internal.ast.OrderBy
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.expressions.StringLiteral

class StringLiteralTest extends AstParsingTestBase {

  val escapes = List("\\\\", "\\\'", "\\\"", "\\b", "\\f", "\\n", "\\r", "\\t", "\\u0000")
    .zip(List("\\", "'", "\"", "\b", "\f", "\n", "\r", "\t", "\u0000"))

  escapes.foreach { case (c, t) =>
    test(s"Double Quotes - Correct escaping of $c") {
      val query = s"""RETURN * ORDER BY "b$c", "s""""
      query should parseTo[Statements](
        Statements(
          statements = List(
            SingleQuery(
              clauses = List(
                Return(
                  distinct = false,
                  returnItems = ReturnItems(
                    includeExisting = true,
                    items = List(),
                    defaultOrderOnColumns = None
                  )(pos),
                  orderBy = Some(
                    value = OrderBy(
                      sortItems = List(
                        AscSortItem(expression = StringLiteral(value = s"b$t")(pos.withInputLength(0)))(pos),
                        AscSortItem(expression = StringLiteral(value = "s")(pos.withInputLength(0)))(pos)
                      )
                    )(pos)
                  ),
                  skip = None,
                  limit = None,
                  excludedNames = Set()
                )(pos)
              )
            )(pos)
          )
        )
      )
    }
  }

  escapes.foreach { case (c1, t1) =>
    escapes.foreach { case (c2, t2) =>
      test(s"Double Quotes - Correct escaping of $c1 and $c2") {
        val query = s"""RETURN * ORDER BY "b$c1$c2", "s""""
        query should parseTo[Statements](
          Statements(
            statements = List(
              SingleQuery(
                clauses = List(
                  Return(
                    distinct = false,
                    returnItems = ReturnItems(
                      includeExisting = true,
                      items = List(),
                      defaultOrderOnColumns = None
                    )(pos),
                    orderBy = Some(
                      value = OrderBy(
                        sortItems = List(
                          AscSortItem(expression = StringLiteral(value = s"b$t1$t2")(pos.withInputLength(0)))(pos),
                          AscSortItem(expression = StringLiteral(value = "s")(pos.withInputLength(0)))(pos)
                        )
                      )(pos)
                    ),
                    skip = None,
                    limit = None,
                    excludedNames = Set()
                  )(pos)
                )
              )(pos)
            )
          )
        )
      }
    }
  }

  escapes.foreach { case (c, t) =>
    test(s"Single Quotes - Correct escaping of $c") {
      val query = s"""RETURN * ORDER BY 'b$c', 's'"""
      query should parseTo[Statements](
        Statements(
          statements = List(
            SingleQuery(
              clauses = List(
                Return(
                  distinct = false,
                  returnItems = ReturnItems(
                    includeExisting = true,
                    items = List(),
                    defaultOrderOnColumns = None
                  )(pos),
                  orderBy = Some(
                    value = OrderBy(
                      sortItems = List(
                        AscSortItem(expression = StringLiteral(value = s"b$t")(pos.withInputLength(0)))(pos),
                        AscSortItem(expression = StringLiteral(value = "s")(pos.withInputLength(0)))(pos)
                      )
                    )(pos)
                  ),
                  skip = None,
                  limit = None,
                  excludedNames = Set()
                )(pos)
              )
            )(pos)
          )
        )
      )
    }
  }

  escapes.foreach { case (c1, t1) =>
    escapes.foreach { case (c2, t2) =>
      test(s"Single Quotes - Correct escaping of $c1 and $c2") {
        val query = s"""RETURN * ORDER BY 'b$c1$c2', 's'"""
        query should parseTo[Statements](
          Statements(
            statements = List(
              SingleQuery(
                clauses = List(
                  Return(
                    distinct = false,
                    returnItems = ReturnItems(
                      includeExisting = true,
                      items = List(),
                      defaultOrderOnColumns = None
                    )(pos),
                    orderBy = Some(
                      value = OrderBy(
                        sortItems = List(
                          AscSortItem(expression = StringLiteral(value = s"b$t1$t2")(pos.withInputLength(0)))(pos),
                          AscSortItem(expression = StringLiteral(value = "s")(pos.withInputLength(0)))(pos)
                        )
                      )(pos)
                    ),
                    skip = None,
                    limit = None,
                    excludedNames = Set()
                  )(pos)
                )
              )(pos)
            )
          )
        )
      }
    }
  }
}
