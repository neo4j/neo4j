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
package org.neo4j.cypher.internal.ir

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.Repetition
import org.neo4j.cypher.internal.util.UpperBound
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class NodeConnectionTest extends CypherFunSuite with AstConstructionTestSupport {

  private val `(foo)-[x]->(start)` = PatternRelationship(
    v"x",
    (v"foo", v"start"),
    SemanticDirection.OUTGOING,
    Seq.empty,
    SimplePatternLength
  )

  private val `(start)-[y]->(bar)` = PatternRelationship(
    v"y",
    (v"start", v"bar"),
    SemanticDirection.OUTGOING,
    Seq.empty,
    SimplePatternLength
  )

  private val `(start) ((a)-[r]->(b)-[s]->(c))+ (end)` = QuantifiedPathPattern(
    leftBinding = NodeBinding(v"a", v"start"),
    rightBinding = NodeBinding(v"c", v"end"),
    patternRelationships = NonEmptyList(
      PatternRelationship(
        variable = v"r",
        boundaryNodes = (v"a", v"b"),
        dir = SemanticDirection.OUTGOING,
        types = Nil,
        length = SimplePatternLength
      ),
      PatternRelationship(
        variable = v"s",
        boundaryNodes = (v"b", v"c"),
        dir = SemanticDirection.OUTGOING,
        types = Nil,
        length = SimplePatternLength
      )
    ),
    argumentIds = Set.empty,
    selections = Selections.empty,
    repetition = Repetition(0, UpperBound.Unlimited),
    nodeVariableGroupings = Set("a", "b", "c").map(name => variableGrouping(varFor(name), varFor(name))),
    relationshipVariableGroupings = Set("r", "s").map(name => variableGrouping(varFor(name), varFor(name)))
  )

  test("pathVariables of a relationship") {
    `(foo)-[x]->(start)`.pathVariables should equal(Seq(
      NodePathVariable(v"foo"),
      RelationshipPathVariable(v"x"),
      NodePathVariable(v"start")
    ))
  }

  test("pathVariables of a QPP") {
    `(start) ((a)-[r]->(b)-[s]->(c))+ (end)`.pathVariables should equal(Seq(
      NodePathVariable(v"start"),
      NodePathVariable(v"a"),
      RelationshipPathVariable(v"r"),
      NodePathVariable(v"b"),
      RelationshipPathVariable(v"s"),
      NodePathVariable(v"c"),
      NodePathVariable(v"end")
    ))
  }

  test("pathVariables of a QPP with gaps in group variables") {
    `(start) ((a)-[r]->(b)-[s]->(c))+ (end)`
      .copy(
        nodeVariableGroupings = Set("a", "c").map(name => variableGrouping(varFor(name), varFor(name))),
        relationshipVariableGroupings = Set("s").map(name => variableGrouping(varFor(name), varFor(name)))
      )
      .pathVariables should equal(Seq(
      NodePathVariable(v"start"),
      NodePathVariable(v"a"),
      RelationshipPathVariable(v"s"),
      NodePathVariable(v"c"),
      NodePathVariable(v"end")
    ))
  }

  test("pathVariables of a QPP with no group variables") {
    `(start) ((a)-[r]->(b)-[s]->(c))+ (end)`
      .copy(
        nodeVariableGroupings = Set.empty,
        relationshipVariableGroupings = Set.empty
      )
      .pathVariables should equal(Seq(NodePathVariable(v"start"), NodePathVariable(v"end")))
  }

  test("pathVariables of an SPP") {
    val spp =
      SelectivePathPattern(
        pathPattern = ExhaustivePathPattern.NodeConnections(NonEmptyList(
          `(foo)-[x]->(start)`,
          `(start) ((a)-[r]->(b)-[s]->(c))+ (end)`
        )),
        selections = Selections.empty,
        selector = SelectivePathPattern.Selector.ShortestGroups(1)
      )

    spp.pathVariables should equal(Seq(
      NodePathVariable(v"foo"),
      RelationshipPathVariable(v"x"),
      NodePathVariable(v"start"),
      NodePathVariable(v"a"),
      RelationshipPathVariable(v"r"),
      NodePathVariable(v"b"),
      RelationshipPathVariable(v"s"),
      NodePathVariable(v"c"),
      NodePathVariable(v"end")
    ))
  }

  test("pathVariables of an SPP without QPP") {
    val spp =
      SelectivePathPattern(
        pathPattern = ExhaustivePathPattern.NodeConnections(NonEmptyList(
          `(foo)-[x]->(start)`,
          `(start)-[y]->(bar)`
        )),
        selections = Selections.empty,
        selector = SelectivePathPattern.Selector.ShortestGroups(1)
      )

    spp.pathVariables should equal(Seq(
      NodePathVariable(v"foo"),
      RelationshipPathVariable(v"x"),
      NodePathVariable(v"start"),
      RelationshipPathVariable(v"y"),
      NodePathVariable(v"bar")
    ))
  }

  test("asQueryGraph of an SPP") {
    val selections = Selections.from(List(
      not(hasLabels("a", "Label")),
      unique(varFor("s")),
      unique(varFor("r"))
    ))
    val spp =
      SelectivePathPattern(
        pathPattern = ExhaustivePathPattern.NodeConnections(NonEmptyList(
          `(foo)-[x]->(start)`,
          `(start) ((a)-[r]->(b)-[s]->(c))+ (end)`
        )),
        selections = selections,
        selector = SelectivePathPattern.Selector.ShortestGroups(1)
      )
    val `(a)-[r]->(b)` = PatternRelationship(
      v"r",
      (v"a", v"b"),
      SemanticDirection.OUTGOING,
      Seq.empty,
      SimplePatternLength
    )
    val `(b)-[s]->(c)` = PatternRelationship(
      v"s",
      (v"b", v"c"),
      SemanticDirection.OUTGOING,
      Seq.empty,
      SimplePatternLength
    )

    spp.asQueryGraph should equal(QueryGraph(
      patternRelationships = Set(`(foo)-[x]->(start)`, `(a)-[r]->(b)`, `(b)-[s]->(c)`),
      patternNodes = Set(v"a", v"b", v"c", v"start", v"foo"),
      selections = selections
    ))
  }

}
