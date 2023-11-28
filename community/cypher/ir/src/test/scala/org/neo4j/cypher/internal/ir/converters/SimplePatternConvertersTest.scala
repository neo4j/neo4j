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
package org.neo4j.cypher.internal.ir.converters

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir.ExhaustivePathPattern.NodeConnections
import org.neo4j.cypher.internal.ir.ExhaustivePathPattern.SingleNode
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.ir.converters.SimplePatternConverters.convertRelationshipLength
import org.neo4j.cypher.internal.ir.converters.SimplePatternConverters.convertSimplePattern
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class SimplePatternConvertersTest extends CypherFunSuite with AstConstructionTestSupport {

  // (a)
  test("single node") {
    convertSimplePattern(nodePat(Some("a"))) shouldEqual SingleNode(v"a")
  }

  // ()
  test("single node – missing variable") {
    the[IllegalArgumentException] thrownBy {
      convertSimplePattern(nodePat(None))
    } should have message "Missing variable in node pattern"
  }

  // (a)-[r:R]->(b)
  test("relationship pattern") {
    val ast = relationshipChain(
      nodePat(Some("a")),
      relPat(
        name = Some("r"),
        labelExpression = Some(labelRelTypeLeaf("R")),
        length = None,
        direction = SemanticDirection.OUTGOING
      ),
      nodePat(Some("b"))
    )

    val ir = NodeConnections(NonEmptyList(PatternRelationship(
      variable = v"r",
      boundaryNodes = (v"a", v"b"),
      dir = SemanticDirection.OUTGOING,
      types = List(relTypeName("R")),
      length = SimplePatternLength
    )))

    convertSimplePattern(ast) shouldEqual ir
  }

  // (a)-[r:R*0..5]->(b)
  test("var-length relationship pattern") {
    val ast = relationshipChain(
      nodePat(Some("a")),
      relPat(
        name = Some("r"),
        labelExpression = Some(labelRelTypeLeaf("R")),
        length = Some(Some(range(Some(0), Some(5)))),
        direction = SemanticDirection.OUTGOING
      ),
      nodePat(Some("b"))
    )

    val ir = NodeConnections(NonEmptyList(PatternRelationship(
      variable = v"r",
      boundaryNodes = (v"a", v"b"),
      dir = SemanticDirection.OUTGOING,
      types = List(relTypeName("R")),
      length = VarPatternLength(0, Some(5))
    )))

    convertSimplePattern(ast) shouldEqual ir
  }

  // (a)-[r:R]->()
  test("relationship pattern – missing node variable") {
    val ast = relationshipChain(
      nodePat(Some("a")),
      relPat(
        name = Some("r"),
        labelExpression = Some(labelRelTypeLeaf("R")),
        length = None,
        direction = SemanticDirection.OUTGOING
      ),
      nodePat(None)
    )

    the[IllegalArgumentException] thrownBy {
      convertSimplePattern(ast)
    } should have message "Missing variable in node pattern"
  }

  // (a)-[:R]->(b)
  test("relationship pattern – missing relationship variable") {
    val ast = relationshipChain(
      nodePat(Some("a")),
      relPat(
        name = None,
        labelExpression = Some(labelRelTypeLeaf("R")),
        length = None,
        direction = SemanticDirection.OUTGOING
      ),
      nodePat(Some("b"))
    )

    the[IllegalArgumentException] thrownBy {
      convertSimplePattern(ast)
    } should have message "Missing variable in relationship pattern"
  }

  // (a)-[r:R]->(b)<-[s]-(a)
  test("path pattern") {
    val ast = relationshipChain(
      nodePat(Some("a")),
      relPat(
        name = Some("r"),
        labelExpression = Some(labelRelTypeLeaf("R")),
        length = None,
        direction = SemanticDirection.OUTGOING
      ),
      nodePat(Some("b")),
      relPat(
        name = Some("s"),
        labelExpression = None,
        length = None,
        direction = SemanticDirection.INCOMING
      ),
      nodePat(Some("a"))
    )

    val ir = NodeConnections(NonEmptyList(
      PatternRelationship(
        variable = v"r",
        boundaryNodes = (v"a", v"b"),
        dir = SemanticDirection.OUTGOING,
        types = List(relTypeName("R")),
        length = SimplePatternLength
      ),
      PatternRelationship(
        variable = v"s",
        boundaryNodes = (v"b", v"a"),
        dir = SemanticDirection.INCOMING,
        types = Nil,
        length = SimplePatternLength
      )
    ))

    convertSimplePattern(ast) shouldEqual ir
  }

  test("relationship length: [*0..3]") {
    convertRelationshipLength(Some(Some(range(Some(0), Some(3))))) shouldEqual VarPatternLength(0, Some(3))
  }

  test("relationship length: [*0..]") {
    convertRelationshipLength(Some(Some(range(Some(0), None)))) shouldEqual VarPatternLength(0, None)
  }

  test("relationship length: [*..3]") {
    convertRelationshipLength(Some(Some(range(None, Some(3))))) shouldEqual VarPatternLength(1, Some(3))
  }

  test("relationship length: [*..]") {
    convertRelationshipLength(Some(Some(range(None, None)))) shouldEqual VarPatternLength(1, None)
  }

  test("relationship length: [*]") {
    convertRelationshipLength(Some(None)) shouldEqual VarPatternLength(1, None)
  }

  test("relationship length: []") {
    convertRelationshipLength(None) shouldEqual SimplePatternLength
  }
}
