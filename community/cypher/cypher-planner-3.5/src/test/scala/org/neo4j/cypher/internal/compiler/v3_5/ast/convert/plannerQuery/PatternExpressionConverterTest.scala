/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_5.ast.convert.plannerQuery

import org.opencypher.v9_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v3_5.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.ir.v3_5._
import org.opencypher.v9_0.expressions._
import org.neo4j.cypher.internal.ir.v3_5.helpers.ExpressionConverters._

class PatternExpressionConverterTest extends CypherFunSuite with LogicalPlanningTestSupport {

  val aNode: NodePattern = NodePattern(Some(Variable("a")(pos)), Seq.empty, None)_
  val bNode: NodePattern = NodePattern(Some(Variable("b")(pos)), Seq.empty, None)_
  val unnamedVariable: Variable = Variable("  UNNAMED1")_
  val anonymousNode: NodePattern = NodePattern(Some(unnamedVariable), Seq.empty, None)_
  val rRel: RelationshipPattern = RelationshipPattern(Some(Variable("r")(pos)), Seq.empty, None, None, SemanticDirection.OUTGOING)_
  val TYP: RelTypeName = RelTypeName("TYP")_

  val rRelWithType: RelationshipPattern = rRel.copy(types = Seq(TYP)) _
  val planRel = PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
  val planRelWithType = PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq(TYP), SimplePatternLength)

  private def projections(names: String*): Map[String, Expression] = names.map {
    case x => x -> Variable(x)(pos)
  }.toMap

  test("(a)-[r]->(b)") {
    // Given
    val patternExpression = createPatternExpression(aNode, rRel, bNode)

    // When
    val qg = patternExpression.asQueryGraph

    // Then
    qg.selections should equal(Selections())
    qg.patternRelationships should equal(Set(planRel))
    qg.argumentIds should equal(Set("a", "r", "b"))
    qg.patternNodes should equal(Set("a", "b"))
  }

  test("(a)-[r:TYP]->(b)") {
    // Given
    val patternExpression = createPatternExpression(aNode, rRelWithType, bNode)

    // When
    val qg = patternExpression.asQueryGraph

    // Then
    qg.selections should equal(Selections())
    qg.patternRelationships should equal(Set(planRelWithType))
    qg.argumentIds should equal(Set("a", "r", "b"))
    qg.patternNodes should equal(Set("a", "b"))
  }

  test("(a)-[r]->(  UNNAMED1)") {
    // Given
    val patternExpression = createPatternExpression(aNode, rRel, anonymousNode)

    // When
    val qg = patternExpression.asQueryGraph

    // Then
    qg.selections should equal(Selections())
    qg.patternRelationships should equal(Set(planRel.copy(nodes = ("a", "  UNNAMED1"))))
    qg.argumentIds should equal(Set("a", "r"))
    qg.patternNodes should equal(Set("a", "  UNNAMED1"))
  }

  test("(a)-[r]->(b:Label)") {
    val labelName: LabelName = LabelName("Label")_
    // Given
    val patternExpression = createPatternExpression(aNode, rRel, bNode.copy(labels = Seq(labelName))(pos))

    // When
    val qg = patternExpression.asQueryGraph

    // Then
    val predicate: HasLabels = HasLabels(Variable("b")(pos), Seq(labelName))_
    qg.selections should equal(Selections(Set(Predicate(Set("b"), predicate))))
    qg.patternRelationships should equal(Set(planRel))
    qg.argumentIds should equal(Set("a", "r", "b"))
    qg.patternNodes should equal(Set("a", "b"))
  }

  def createPatternExpression(n1: NodePattern, r: RelationshipPattern, n2: NodePattern): PatternExpression =
    PatternExpression(RelationshipsPattern(RelationshipChain(n1, r, n2) _) _)
}
