/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.ast.convert.plannerQuery

import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.ir.{PatternRelationship, Predicate, Selections, SimplePatternLength}
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.ir.helpers.ExpressionConverters._
import org.neo4j.cypher.internal.v4_0.rewriting.rewriters.GeneratingNamer

class PatternExpressionConverterTest extends CypherFunSuite with LogicalPlanningTestSupport {

  private val aNode = NodePattern(Some(varFor("a")), Seq.empty, None)_
  private val bNode = NodePattern(Some(varFor("b")), Seq.empty, None)_
  private val anonymousNode = NodePattern(Some(varFor("  UNNAMED1")), Seq.empty, None)_
  private val rRel = RelationshipPattern(Some(varFor("r")), Seq.empty, None, None, SemanticDirection.OUTGOING)_
  private val TYP: RelTypeName = RelTypeName("TYP")_

  private val rRelWithType = rRel.copy(types = Seq(TYP)) _
  private val planRel = PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
  private val planRelWithType = PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq(TYP), SimplePatternLength)

  test("(a)-[r]->(b)") {
    // Given
    val patternExpression = createPatternExpression(aNode, rRel, bNode)

    // When
    val qg = asQueryGraph(patternExpression, new GeneratingNamer)

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
    val qg = asQueryGraph(patternExpression, new GeneratingNamer)

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
    val qg = asQueryGraph(patternExpression, new GeneratingNamer)

    // Then
    qg.selections should equal(Selections())
    qg.patternRelationships should equal(Set(planRel.copy(nodes = ("a", "  UNNAMED1"))))
    qg.argumentIds should equal(Set("a", "r"))
    qg.patternNodes should equal(Set("a", "  UNNAMED1"))
  }

  test("(a)-[r]->(b:Label)") {
    // Given
    val patternExpression = createPatternExpression(aNode, rRel, bNode.copy(labels = Seq(labelName("Label")))(pos))

    // When
    val qg = asQueryGraph(patternExpression, new GeneratingNamer)

    // Then
    val predicate = hasLabels("b", "Label")
    qg.selections should equal(Selections(Set(Predicate(Set("b"), predicate))))
    qg.patternRelationships should equal(Set(planRel))
    qg.argumentIds should equal(Set("a", "r", "b"))
    qg.patternNodes should equal(Set("a", "b"))
  }

  def createPatternExpression(n1: NodePattern, r: RelationshipPattern, n2: NodePattern): PatternExpression =
    PatternExpression(RelationshipsPattern(RelationshipChain(n1, r, n2) _) _)
}
