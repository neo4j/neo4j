/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.cst.factory.neo4j.AntlrRule
import org.neo4j.cypher.internal.cst.factory.neo4j.Cst

/**
 * This test class was created due to a bug in Javacc code generation and does not cover general pattern comprehensions
 */
class PatternComprehensionParserTest extends ParserSyntaxTreeBase[Cst.Statement, Statement] {

  implicit private val javaccRule = JavaccRule.Statements
  implicit private val antlrRule = AntlrRule.Statements()

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
      gives(
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
        gives(
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
