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

import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.ast.test.util.LegacyAstParsingTestSupport

/**
 * This test class was created due to a bug in Javacc code generation and does not cover general pattern comprehensions
 */
class PatternComprehensionParserTest extends AstParsingTestBase with LegacyAstParsingTestSupport {
  private val variable = Seq("", "x")
  private val labelExpressions = Seq("", ":A", "IS A")
  private val properties = Seq("", "{prop:1}")
  private val pathLength = Seq("", "*1..5")
  private val where = Seq("", "WHERE x.prop = 1")

  for {
    maybeVariable <- variable
    maybeLabelExpr <- labelExpressions
    maybeProperties <- properties
    maybeWhere <- where
  } yield {
    val nodeReturnText = s"[($maybeVariable $maybeLabelExpr $maybeProperties $maybeWhere)-->() | 1]"
    test(s"RETURN $nodeReturnText") {
      parsesTo[Statements](
        singleQuery(
          return_(
            returnItem(
              patternComprehension(
                relationshipChain(
                  nodePat(
                    if (maybeVariable.equals("")) None else Some(maybeVariable),
                    if (maybeLabelExpr.equals("")) None
                    else if (maybeLabelExpr.equals(":A")) Some(labelLeaf("A"))
                    else Some(labelLeaf("A", containsIs = true)),
                    if (maybeProperties.equals("")) None else Some(mapOf(("prop", literalInt(1)))),
                    if (maybeWhere.equals("")) None else Some(eq(prop("x", "prop"), literalInt(1)))
                  ),
                  relPat(),
                  nodePat()
                ),
                literalInt(1)
              ),
              nodeReturnText
            )
          )
        )
      )
    }

    for {
      maybePathLength <- pathLength
    } yield {
      val relReturnText = s"[()-[$maybeVariable $maybeLabelExpr $maybePathLength $maybeProperties $maybeWhere]->() | 1]"
      test(s"RETURN $relReturnText") {
        parsesTo[Statements](
          singleQuery(
            return_(
              returnItem(
                patternComprehension(
                  relationshipChain(
                    nodePat(),
                    relPat(
                      if (maybeVariable.equals("")) None else Some(maybeVariable),
                      if (maybeLabelExpr.equals("")) None
                      else if (maybeLabelExpr.equals(":A")) Some(labelRelTypeLeaf("A"))
                      else Some(labelRelTypeLeaf("A", containsIs = true)),
                      if (maybePathLength.equals("")) None else Some(Some(range(Some(1), Some(5)))),
                      if (maybeProperties.equals("")) None else Some(mapOf(("prop", literalInt(1)))),
                      if (maybeWhere.equals("")) None else Some(eq(prop("x", "prop"), literalInt(1)))
                    ),
                    nodePat()
                  ),
                  literalInt(1)
                ),
                relReturnText
              )
            )
          )
        )
      }
    }
  }
}
