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
package org.neo4j.cypher.internal.ast.factory.query

import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.Limit
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Skip
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.expressions.DecimalDoubleLiteral
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.Namespace

class OrderByAndPageStatementParserTest extends AstParsingTestBase {

  test("ORDER BY a.prop SKIP 1 LIMIT 1") {
    parsesTo[Clause](
      withAll(Some(orderBy(sortItem(prop(varFor("a"), "prop")))), Some(skip(1)), Some(limit(1)))
    )
  }

  test("ORDER BY a.prop SKIP 1") {
    parsesTo[Clause](
      withAll(Some(orderBy(sortItem(prop(varFor("a"), "prop")))), Some(skip(1)), None)
    )
  }

  test("ORDER BY a.prop") {
    parsesTo[Clause](
      withAll(Some(orderBy(sortItem(prop(varFor("a"), "prop")))), None, None)
    )
  }

  test("SKIP 1 LIMIT 1") {
    parsesTo[Clause](
      withAll(None, Some(skip(1)), Some(limit(1)))
    )
  }

  test("SKIP 1") {
    parsesTo[Clause](
      withAll(None, Some(skip(1)), None)
    )
  }

  test("LIMIT 1") {
    parsesTo[Clause](
      withAll(None, None, Some(limit(1)))
    )
  }

  test("ORDER BY a.prop OFFSET 1 LIMIT 1") {
    parsesTo[Clause](
      withAll(Some(orderBy(sortItem(prop(varFor("a"), "prop")))), Some(skip(1)), Some(limit(1)))
    )
  }

  test("ORDER BY a.prop OFFSET 1") {
    parsesTo[Clause](
      withAll(Some(orderBy(sortItem(prop(varFor("a"), "prop")))), Some(skip(1)), None)
    )
  }

  test("OFFSET 1 LIMIT 1") {
    parsesTo[Clause](
      withAll(None, Some(skip(1)), Some(limit(1)))
    )
  }

  test("OFFSET 1") {
    parsesTo[Clause](
      withAll(None, Some(skip(1)), None)
    )
  }

  test("LIMIT 1 ORDER BY a.prop OFFSET 1") {
    parsesTo[Statements](
      Statements(Seq(SingleQuery(Seq(
        withAll(None, None, Some(limit(1))),
        withAll(Some(orderBy(sortItem(prop(varFor("a"), "prop")))), Some(skip(1)), None)
      ))(pos)))
    )
  }

  test("LIMIT 1 OFFSET 1 ORDER BY a.prop ") {
    parsesTo[Statements](
      Statements(Seq(SingleQuery(Seq(
        withAll(None, None, Some(limit(1))),
        withAll(None, Some(skip(1)), None),
        withAll(Some(orderBy(sortItem(prop(varFor("a"), "prop")))), None, None)
      ))(pos)))
    )
  }

  test("ORDER BY a.prop LIMIT 1 OFFSET 1") {
    parsesTo[Statements](
      Statements(Seq(SingleQuery(Seq(
        withAll(Some(orderBy(sortItem(prop(varFor("a"), "prop")))), None, Some(limit(1))),
        withAll(None, Some(skip(1)), None)
      ))(pos)))
    )
  }

  test("ORDER BY a.prop LIMIT 1 SKIP 1") {
    parsesTo[Statements](
      Statements(Seq(SingleQuery(Seq(
        withAll(
          Some(orderBy(sortItem(prop(varFor("a"), "prop")))),
          None,
          Some(limit(1))
        ),
        withAll(None, Some(skip(1)), None)
      ))(pos)))
    )
  }

  test("ORDER BY a.prop OFFSET 1 LIMIT 1 OFFSET 1 ORDER BY a.prop OFFSET 1 LIMIT 1") {
    parsesTo[Statements](
      Statements(Seq(SingleQuery(Seq(
        withAll(Some(orderBy(sortItem(prop(varFor("a"), "prop")))), Some(skip(1)), Some(limit(1))),
        withAll(None, Some(skip(1)), None),
        withAll(Some(orderBy(sortItem(prop(varFor("a"), "prop")))), Some(skip(1)), Some(limit(1)))
      ))(pos)))
    )
  }

  test("ORDER BY a.prop SKIP 1 + 9 LIMIT 1 * 9") {
    parsesTo[Clause](
      withAll(
        Some(orderBy(sortItem(prop(varFor("a"), "prop")))),
        Some(Skip(add(literalInt(1), literalInt(9)))(pos)),
        Some(Limit(multiply(literalInt(1), literalInt(9)))(pos))
      )
    )
  }

  test("ORDER BY a.prop SKIP sin(0.5) LIMIT 8 / 2") {
    parsesTo[Clause](
      withAll(
        Some(orderBy(sortItem(prop(varFor("a"), "prop")))),
        Some(Skip(
          FunctionInvocation(
            functionName = FunctionName(namespace = Namespace(parts = List())(pos), name = "sin")(pos),
            distinct = false,
            args = Vector(DecimalDoubleLiteral(stringVal = "0.5")(pos))
          )(pos)
        )(pos)),
        Some(Limit(divide(literalInt(8), literalInt(2)))(pos))
      )
    )
  }

  test("MATCH (a) ORDER BY a.prop OFFSET 1 LIMIT 1 OFFSET 1 ORDER BY a.prop OFFSET 1 LIMIT 1 RETURN a") {
    parsesTo[Statements](
      Statements(Seq(SingleQuery(Seq(
        match_(Seq(nodePat(Some("a"))), None),
        withAll(Some(orderBy(sortItem(prop(varFor("a"), "prop")))), Some(skip(1)), Some(limit(1))),
        withAll(None, Some(skip(1)), None),
        withAll(Some(orderBy(sortItem(prop(varFor("a"), "prop")))), Some(skip(1)), Some(limit(1))),
        return_(variableReturnItem("a"))
      ))(pos)))
    )
  }

}
