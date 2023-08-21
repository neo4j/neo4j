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
package org.neo4j.cypher.internal.rewriting.rewriters

import org.neo4j.cypher.internal.ast.SetClause
import org.neo4j.cypher.internal.ast.SetItem
import org.neo4j.cypher.internal.ast.SetPropertyItem
import org.neo4j.cypher.internal.ast.SetPropertyItems
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalProperty
import org.neo4j.cypher.internal.rewriting.AstRewritingTestSupport
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class CombineSetPropertyTest extends CypherFunSuite with AstRewritingTestSupport {

  test("should not rewrite set operation update") {
    // given
    val clause = setClause(setProperty(prop(varFor("n"), "prop"), literal(42)))

    // when
    val rewritten = combineSetProperty.instance(clause)

    // then
    rewritten should equal(clause)
  }

  test("should rewrite set operation on repeated entity") {
    // given
    val clause = setClause(
      setProperty(prop(varFor("n"), "prop1"), literal(1)),
      setProperty(prop(varFor("n"), "prop2"), literal(2))
    )

    // when
    val rewritten = combineSetProperty.instance(clause)

    // then
    rewritten should equal(setClause(setProperties(varFor("n"), ("prop1", literal(1)), ("prop2", literal(2)))))
  }

  test("should not rewrite set operation on different entities") {
    // given
    val clause = setClause(
      setProperty(prop(varFor("n"), "prop1"), literal(1)),
      setProperty(prop(varFor("m"), "prop2"), literal(2))
    )

    // when
    val rewritten = combineSetProperty.instance(clause)

    // then
    rewritten should equal(clause)
  }

  test("should not rewrite multiple set with potential dependencies") {
    // given
    val clause = setClause(
      setProperty(prop(varFor("m"), "prop1"), literal(1)),
      setProperty(prop(varFor("n"), "prop1"), literal(1)),
      setProperty(prop(varFor("m"), "prop2"), prop(varFor("n"), "prop1")),
      setProperty(prop(varFor("n"), "prop2"), prop(varFor("m"), "prop2")),
      setProperty(prop(varFor("o"), "prop1"), prop(varFor("n"), "prop1"))
    )

    // when
    val rewritten = combineSetProperty.instance(clause)

    // then
    rewritten should equal(clause)
  }

  test("should rewrite multiple set with sequential property ops") {
    // given
    val clause = setClause(
      setProperty(prop(varFor("m"), "prop1"), literal(1)),
      setProperty(prop(varFor("m"), "prop2"), prop(varFor("m"), "prop1")),
      setProperty(prop(varFor("m"), "prop3"), prop(varFor("m"), "prop2")),
      setProperty(prop(varFor("n"), "prop1"), literal(1)),
      setProperty(prop(varFor("n"), "prop2"), prop(varFor("m"), "prop2")),
      setProperty(prop(varFor("o"), "prop1"), prop(varFor("n"), "prop1"))
    )

    // when
    val rewritten = combineSetProperty.instance(clause)

    // then
    rewritten should equal(
      setClause(
        setProperties(
          varFor("m"),
          ("prop1", literal(1)),
          ("prop2", prop(varFor("m"), "prop1")),
          ("prop3", prop(varFor("m"), "prop2"))
        ),
        setProperties(varFor("n"), ("prop1", literal(1)), ("prop2", prop(varFor("m"), "prop2"))),
        setProperty(prop(varFor("o"), "prop1"), prop(varFor("n"), "prop1"))
      )
    )
  }

  private def setClause(items: SetItem*) = SetClause(items)(pos)

  private def setProperty(property: LogicalProperty, expression: Expression): SetPropertyItem =
    SetPropertyItem(property, expression)(pos)

  private def setProperties(map: Expression, items: (String, Expression)*) = {
    val mapped = items.map {
      case (k, v) => (propName(k), v)
    }
    SetPropertyItems(map, mapped)(pos)
  }
}
