/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.phases

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LabelOrRelTypeName
import org.neo4j.cypher.internal.expressions.MatchMode
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.frontend.phases.ValidSymbolicNamesInLabelExpressions
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Disjunctions
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Leaf
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Wildcard
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.InputPosition

class ValidSymbolicNamesInLabelExpressionsTest extends CypherPlannerTestSuite with AstConstructionTestSupport {

  val condition: Any => Seq[String] = ValidSymbolicNamesInLabelExpressions(_)(CancellationChecker.NeverCancelled)

  test("A NodePattern can contain a Label in its label expression") {
    val labelName = LabelName("A")(pos)
    val nodePattern = NodePattern(None, Some(Leaf(labelName)), None, None)(pos)
    condition(nodePattern) shouldBe empty
  }

  test("A NodePattern cannot contain a RelType in its label expression") {
    val relTypeName = RelTypeName("A")(InputPosition(1, 3, 2))
    val nodePattern = NodePattern(None, Some(Leaf(relTypeName)), None, None)(pos)
    condition(nodePattern) shouldEqual List(
      "Illegal symbolic name RelTypeName(A) inside a node pattern at position: line 3, column 2 (offset: 1)"
    )
  }

  test("A RelationshipPattern can contain a RelType in its label expression") {
    val relTypeName = RelTypeName("A")(pos)
    val relationshipPattern =
      RelationshipPattern(None, Some(Leaf(relTypeName)), None, None, None, SemanticDirection.BOTH)(pos)
    condition(relationshipPattern) shouldBe empty
  }

  test("A RelationshipPattern cannot contain a LabelOrRelType in its label expression") {
    val labelOrRelTypeName = LabelOrRelTypeName("A")(InputPosition(1, 3, 2))
    val relationshipPattern =
      RelationshipPattern(None, Some(Leaf(labelOrRelTypeName)), None, None, None, SemanticDirection.BOTH)(
        pos
      )
    condition(relationshipPattern) shouldEqual List(
      "Illegal symbolic name LabelOrRelTypeName(A) inside a relationship pattern at position: line 3, column 2 (offset: 1)"
    )
  }

  test("A LabelExpressionPredicate can contain a LabelOrRelType in its label expression") {
    val entity = varFor("n", pos)
    val labelOrRelTypeName = LabelOrRelTypeName("A")(pos)
    val labelExpressionPred = labelExpressionPredicate(entity, Leaf(labelOrRelTypeName))
    condition(labelExpressionPred) shouldBe empty
  }

  test("A LabelExpressionPredicate cannot contain a Label in its label expression") {
    val entity = varFor("n")
    val labelName = LabelName("A")(InputPosition(1, 3, 2))
    val labelExpressionPred = labelExpressionPredicate(entity, Leaf(labelName))
    condition(labelExpressionPred) shouldEqual List(
      "Illegal symbolic name LabelName(A) inside a label expression predicate at position: line 3, column 2 (offset: 1)"
    )
  }

  /*
   * These two tests construct ASTs resembling:
   * MATCH (n:A|%) WHERE n:B
   */
  test("Allow a more complex valid AST") {
    val variable = varFor("n")
    val labelName = LabelName("A")(pos)
    val wildcard = Wildcard()(pos)
    val disjunction = Disjunctions(Seq(Leaf(labelName), wildcard))(pos)
    val nodePattern = NodePattern(Some(variable), Some(disjunction), None, None)(pos)
    val pattern = patternForMatch(nodePattern)
    val labelOrRelTypeName = LabelOrRelTypeName("B")(pos)
    val labelExpressionPred = labelExpressionPredicate(variable, Leaf(labelOrRelTypeName))
    val where = Where(labelExpressionPred)(pos)
    val matchClause = Match(optional = false, MatchMode.default(pos), pattern, Seq.empty, Some(where), None)(
      pos
    )
    condition(matchClause) shouldBe empty
  }

  test("Report all errors in a more complex invalid AST") {
    val variable = varFor("n")
    val relTypeName = RelTypeName("A")(InputPosition(1, 3, 2))
    val wildcard = Wildcard()(pos)
    val disjunction = Disjunctions(Seq(Leaf(relTypeName), wildcard))(pos)
    val nodePattern = NodePattern(Some(variable), Some(disjunction), None, None)(pos)
    val pattern = patternForMatch(nodePattern)
    val labelName = LabelName("B")(InputPosition(41, 10, 42))
    val labelExpressionPred = labelExpressionPredicate(variable, Leaf(labelName))
    val where = Where(labelExpressionPred)(pos)
    val matchClause = Match(optional = false, MatchMode.default(pos), pattern, Seq.empty, Some(where), None)(
      pos
    )
    condition(matchClause) shouldEqual List(
      "Illegal symbolic name RelTypeName(A) inside a node pattern at position: line 3, column 2 (offset: 1)",
      "Illegal symbolic name LabelName(B) inside a label expression predicate at position: line 10, column 42 (offset: 41)"
    )
  }
}
