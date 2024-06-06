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

import org.neo4j.cypher.internal.ast.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.ast.test.util.LegacyAstParsingTestSupport
import org.neo4j.cypher.internal.label_expressions.LabelExpression

trait PatternParserTestBase extends AstParsingTestBase with LegacyAstParsingTestSupport {

  val labelExpressions: Seq[(String, LabelExpression, LabelExpression, LabelExpression)] =
    createLabelExpression("IS", containsIs = true) ++
      createLabelExpression(":", containsIs = false)

  def createLabelExpression(keyword: String, containsIs: Boolean) = Seq(
    (
      s"$keyword A",
      labelLeaf("A", containsIs = containsIs),
      labelRelTypeLeaf("A", containsIs = containsIs),
      labelOrRelTypeLeaf("A", containsIs = containsIs)
    ),
    (
      s"$keyword A&B",
      labelConjunction(
        labelLeaf("A", containsIs = containsIs),
        labelLeaf("B", containsIs = containsIs),
        containsIs = containsIs
      ),
      labelConjunction(
        labelRelTypeLeaf("A", containsIs = containsIs),
        labelRelTypeLeaf("B", containsIs = containsIs),
        containsIs = containsIs
      ),
      labelConjunction(
        labelOrRelTypeLeaf("A", containsIs = containsIs),
        labelOrRelTypeLeaf("B", containsIs = containsIs),
        containsIs = containsIs
      )
    ),
    (
      s"$keyword A|B",
      labelDisjunction(
        labelLeaf("A", containsIs = containsIs),
        labelLeaf("B", containsIs = containsIs),
        containsIs = containsIs
      ),
      labelDisjunction(
        labelRelTypeLeaf("A", containsIs = containsIs),
        labelRelTypeLeaf("B", containsIs = containsIs),
        containsIs = containsIs
      ),
      labelDisjunction(
        labelOrRelTypeLeaf("A", containsIs = containsIs),
        labelOrRelTypeLeaf("B", containsIs = containsIs),
        containsIs = containsIs
      )
    ),
    (
      s"$keyword %",
      labelWildcard(containsIs = containsIs),
      labelWildcard(containsIs = containsIs),
      labelWildcard(containsIs = containsIs)
    ),
    (
      s"$keyword !A",
      labelNegation(labelLeaf("A", containsIs = containsIs), containsIs = containsIs),
      labelNegation(labelRelTypeLeaf("A", containsIs = containsIs), containsIs = containsIs),
      labelNegation(labelOrRelTypeLeaf("A", containsIs = containsIs), containsIs = containsIs)
    ),
    (
      s"$keyword !(A|B)",
      labelNegation(
        labelDisjunction(
          labelLeaf("A", containsIs = containsIs),
          labelLeaf("B", containsIs = containsIs),
          containsIs = containsIs
        ),
        containsIs = containsIs
      ),
      labelNegation(
        labelDisjunction(
          labelRelTypeLeaf("A", containsIs = containsIs),
          labelRelTypeLeaf("B", containsIs = containsIs),
          containsIs = containsIs
        ),
        containsIs = containsIs
      ),
      labelNegation(
        labelDisjunction(
          labelOrRelTypeLeaf("A", containsIs = containsIs),
          labelOrRelTypeLeaf("B", containsIs = containsIs),
          containsIs = containsIs
        ),
        containsIs = containsIs
      )
    ),
    (
      s"$keyword A&!B",
      labelConjunction(
        labelLeaf("A", containsIs = containsIs),
        labelNegation(labelLeaf("B", containsIs = containsIs), containsIs = containsIs),
        containsIs = containsIs
      ),
      labelConjunction(
        labelRelTypeLeaf("A", containsIs = containsIs),
        labelNegation(labelRelTypeLeaf("B", containsIs = containsIs), containsIs = containsIs),
        containsIs = containsIs
      ),
      labelConjunction(
        labelOrRelTypeLeaf("A", containsIs = containsIs),
        labelNegation(labelOrRelTypeLeaf("B", containsIs = containsIs), containsIs = containsIs),
        containsIs = containsIs
      )
    )
  )

  val variable = Seq(("", None), ("x", Some("x")))
  val properties = Seq(("", None), ("{prop:1}", Some(mapOf(("prop", literalInt(1))))))
  val where = Seq(("", None), ("WHERE x.prop = 1", Some(equals(prop("x", "prop"), literalInt(1)))))
}
