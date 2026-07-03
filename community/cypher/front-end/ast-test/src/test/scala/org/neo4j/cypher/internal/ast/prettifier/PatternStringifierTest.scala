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
package org.neo4j.cypher.internal.ast.prettifier

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.TestName

class PatternStringifierTest extends CypherFunSuite with TestName with AstConstructionTestSupport {

  private val expressionStringifier = ExpressionStringifier(preferSingleQuotes = true)
  private val patternStringifier = PatternStringifier(expressionStringifier)

  test("(n:Foo:Bar {prop: 'test'} WHERE r.otherProp > 123)") {
    val pattern = nodePat(
      name = Some("n"),
      labelExpression = Some(labelColonConjunction(
        labelLeaf("Foo"),
        labelLeaf("Bar")
      )),
      properties = Some(mapOf("prop" -> literalString("test"))),
      predicates = Some(greaterThan(prop("r", "otherProp"), literalInt(123)))
    )

    patternStringifier(pattern) shouldEqual testName
  }

  test("(n:Foo:Bar:Baz)") {
    val pattern = nodePat(
      name = Some("n"),
      labelExpression =
        Some(labelColonConjunction(labelColonConjunction(labelLeaf("Foo"), labelLeaf("Bar")), labelLeaf("Baz")))
    )

    patternStringifier(pattern) shouldEqual testName
  }

  test("(n:(Foo|Bar)&Baz)") {
    val pattern =
      nodePat(Some("n"), Some(labelConjunction(labelDisjunction(labelLeaf("Foo"), labelLeaf("Bar")), labelLeaf("Baz"))))

    patternStringifier(pattern) shouldEqual testName
  }

  test("(n IS (Foo|Bar)&Baz)") {
    val pattern =
      nodePat(
        Some("n"),
        Some(labelConjunction(
          labelDisjunction(labelLeaf("Foo", containsIs = true), labelLeaf("Bar", containsIs = true), containsIs = true),
          labelLeaf("Baz", containsIs = true),
          containsIs = true
        ))
      )

    patternStringifier(pattern) shouldEqual testName
  }

  test("(n:(Foo&Bar)|Baz)") {
    val pattern =
      nodePat(Some("n"), Some(labelDisjunction(labelConjunction(labelLeaf("Foo"), labelLeaf("Bar")), labelLeaf("Baz"))))

    patternStringifier(pattern) shouldEqual testName
  }

  test("({prop: 'test'})") {
    val pattern = nodePat(
      properties = Some(mapOf("prop" -> literalString("test")))
    )

    patternStringifier(pattern) shouldEqual testName
  }

  // A bit extreme but ensures that the space before WHERE is only added when necessary
  test("(WHERE false)") {
    val pattern = nodePat(
      predicates = Some(falseLiteral)
    )

    patternStringifier(pattern) shouldEqual testName
  }

  test("()") {
    val pattern = nodePat()

    patternStringifier(pattern) shouldEqual testName
  }

  test("(:A&B)") {
    val pattern = nodePat(
      labelExpression = Some(
        labelConjunction(
          labelLeaf("A"),
          labelLeaf("B")
        )
      )
    )

    patternStringifier(pattern) shouldEqual testName
  }

  test("(:!A&B)") {
    val pattern = nodePat(
      labelExpression = Some(
        labelConjunction(
          labelNegation(labelLeaf("A")),
          labelLeaf("B")
        )
      )
    )

    patternStringifier(pattern) shouldEqual testName
  }

  test("(:!(A&B))") {
    val pattern = nodePat(
      labelExpression = Some(
        labelNegation(
          labelConjunction(
            labelLeaf("A"),
            labelLeaf("B")
          )
        )
      )
    )

    patternStringifier(pattern) shouldEqual testName
  }

  test("(:(A|A)&B)") {
    val pattern = nodePat(
      labelExpression = Some(
        labelConjunction(
          labelDisjunction(
            labelLeaf("A"),
            labelLeaf("A")
          ),
          labelLeaf("B")
        )
      )
    )

    patternStringifier(pattern) shouldEqual testName
  }

  test("(:!!(A&B)|(C&B))") {
    val pattern = nodePat(
      labelExpression = Some(
        labelDisjunction(
          labelNegation(
            labelNegation(
              labelConjunction(labelLeaf("A"), labelLeaf("B"))
            )
          ),
          labelConjunction(
            labelLeaf("C"),
            labelLeaf("B")
          )
        )
      )
    )

    patternStringifier(pattern) shouldEqual testName
  }

  test("(:!!(((A&B)|C)&B))") {
    val pattern = nodePat(
      labelExpression = Some(
        labelNegation(
          labelNegation(
            labelConjunction(
              labelDisjunction(
                labelConjunction(labelLeaf("A"), labelLeaf("B")),
                labelLeaf("C")
              ),
              labelLeaf("B")
            )
          )
        )
      )
    )

    patternStringifier(pattern) shouldEqual testName
  }

  test("-[r:Foo|Bar*1..5 {prop: 'test'} WHERE r.otherProp > 123]->") {
    val pattern = relPat(
      name = Some("r"),
      labelExpression = Some(labelDisjunction(labelRelTypeLeaf("Foo"), labelRelTypeLeaf("Bar"))),
      length = Some(Some(range(Some(1), Some(5)))),
      properties = Some(mapOf("prop" -> literalString("test"))),
      predicates = Some(greaterThan(prop("r", "otherProp"), literalInt(123))),
      direction = OUTGOING
    )

    patternStringifier(pattern) shouldEqual testName
  }

  test("<-[r:Foo|Bar|Baz*]-") {
    val pattern = relPat(
      name = Some("r"),
      labelExpression = Some(labelDisjunctions(Seq(
        labelRelTypeLeaf("Foo"),
        labelRelTypeLeaf("Bar"),
        labelRelTypeLeaf("Baz")
      ))),
      length = Some(None),
      direction = INCOMING
    )

    patternStringifier(pattern) shouldEqual testName
  }

  test("<-[r IS Foo|Bar|Baz*]-") {
    val pattern = relPat(
      name = Some("r"),
      labelExpression = Some(labelDisjunctions(
        Seq(
          labelRelTypeLeaf("Foo", containsIs = true),
          labelRelTypeLeaf("Bar", containsIs = true),
          labelRelTypeLeaf("Baz", containsIs = true)
        ),
        containsIs = true
      )),
      length = Some(None),
      direction = INCOMING
    )

    patternStringifier(pattern) shouldEqual testName
  }

  test("-[{prop: 'test'}]-") {
    val pattern = relPat(
      properties = Some(mapOf("prop" -> literalString("test"))),
      direction = BOTH
    )

    patternStringifier(pattern) shouldEqual testName
  }

  // A bit extreme but ensures that the space before WHERE is only added when necessary
  test("-[WHERE false]-") {
    val pattern = relPat(
      predicates = Some(falseLiteral),
      direction = BOTH
    )

    patternStringifier(pattern) shouldEqual testName
  }

  test("((n)-[r]->(m) WHERE n.prop = m.prop)*") {
    val pattern = quantifiedPath(
      relationshipChain(
        nodePat(Some("n")),
        relPat(Some("r")),
        nodePat(Some("m"))
      ),
      starQuantifier,
      optionalWhereExpression = Some(equals(prop("n", "prop"), prop("m", "prop")))
    )

    patternStringifier(pattern) shouldEqual testName
  }

  test("((n)-[r]->(m) WHERE n.prop = m.prop)") {
    val pattern = parenthesizedPath(
      relChain = relationshipChain(
        nodePat(Some("n")),
        relPat(Some("r")),
        nodePat(Some("m"))
      ),
      optionalWhereExpression = Some(equals(prop("n", "prop"), prop("m", "prop")))
    )

    patternStringifier(pattern) shouldEqual testName
  }

}
