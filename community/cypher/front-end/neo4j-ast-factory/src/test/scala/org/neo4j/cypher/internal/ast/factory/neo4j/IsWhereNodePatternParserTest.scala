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

import org.neo4j.cypher.internal.cst.factory.neo4j.AntlrRule
import org.neo4j.cypher.internal.cst.factory.neo4j.Cst
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.label_expressions.LabelExpressionPredicate

/**
 * The aim of this class is to test parsing for all combinations of
 * IS and WHERE used in node patterns e.g. (WHERE IS WHERE WHERE IS)
 */
class IsWhereNodePatternParserTest extends ParserSyntaxTreeBase[Cst.NodePattern, NodePattern] {

  implicit val javaccRule = JavaccRule.NodePattern
  implicit val antlrRule = AntlrRule.NodePattern

  for {
    (maybeVariable, maybeVariableName) <-
      Seq(("", None), ("IS", Some("IS")), ("WHERE", Some("WHERE")))
  } yield {
    test(s"($maybeVariable)") {
      gives(nodePat(maybeVariableName))
    }

    for {
      isOrWhere <- Seq("IS", "WHERE")
    } yield {
      test(s"($maybeVariable IS $isOrWhere)") {
        gives(
          nodePat(
            maybeVariableName,
            labelExpression = Some(labelLeaf(isOrWhere, containsIs = true))
          )
        )
      }

      test(s"($maybeVariable WHERE $isOrWhere)") {
        gives(
          nodePat(
            maybeVariableName,
            predicates = Some(varFor(isOrWhere))
          )
        )
      }

      for {
        isOrWhere2 <- Seq("IS", "WHERE")
      } yield {
        test(s"($maybeVariable IS $isOrWhere WHERE $isOrWhere2)") {
          gives(
            nodePat(
              maybeVariableName,
              labelExpression = Some(labelLeaf(isOrWhere, containsIs = true)),
              predicates = Some(varFor(isOrWhere2))
            )
          )
        }

        test(s"($maybeVariable WHERE $isOrWhere IS $isOrWhere2)") {
          gives(
            nodePat(
              maybeVariableName,
              predicates = Some(LabelExpressionPredicate(
                varFor(isOrWhere),
                labelOrRelTypeLeaf(isOrWhere2, containsIs = true)
              )(pos))
            )
          )
        }

        test(s"($maybeVariable WHERE $isOrWhere WHERE $isOrWhere2)") {
          failsToParse
        }

        test(s"($maybeVariable IS $isOrWhere IS $isOrWhere2)") {
          failsToParse
        }
        for {
          isOrWhere3 <- Seq("IS", "WHERE")
        } yield {
          test(s"($maybeVariable IS $isOrWhere WHERE $isOrWhere2 IS $isOrWhere3)") {
            gives(
              nodePat(
                maybeVariableName,
                labelExpression = Some(labelLeaf(isOrWhere, containsIs = true)),
                predicates = Some(LabelExpressionPredicate(
                  varFor(isOrWhere2),
                  labelOrRelTypeLeaf(isOrWhere3, containsIs = true)
                )(pos))
              )
            )
          }
        }
      }
    }
  }
}
