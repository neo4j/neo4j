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
import org.neo4j.cypher.internal.label_expressions.LabelExpression

trait PatternParserTestBase extends ParserSyntaxTreeBase[Cst.Statement, Statement] {

  implicit val javaccRule = JavaccRule.Statements
  implicit val antlrRule = AntlrRule.Statements()

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
