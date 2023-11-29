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
import org.neo4j.cypher.internal.expressions.NamedPatternPart
import org.neo4j.cypher.internal.expressions.ParenthesizedPath
import org.neo4j.cypher.internal.expressions.PathPatternPart
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternPart
import org.neo4j.cypher.internal.expressions.PatternPartWithSelector
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.ShortestPathsPatternPart
import org.neo4j.cypher.internal.ir.ExhaustivePathPattern.NodeConnections
import org.neo4j.cypher.internal.ir.ExhaustivePathPattern.SingleNode
import org.neo4j.cypher.internal.ir.NodeBinding
import org.neo4j.cypher.internal.ir.PathPattern
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QuantifiedPathPattern
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SelectivePathPattern
import org.neo4j.cypher.internal.ir.ShortestRelationshipPattern
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.Repetition
import org.neo4j.cypher.internal.util.UpperBound
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class PatternConvertersTest extends CypherFunSuite with AstConstructionTestSupport {

  private def convertPatternParts(patternParts: PatternPartWithSelector*): List[PathPattern] =
    new PatternConverters(new AnonymousVariableNameGenerator())
      .convertPattern(Pattern.ForMatch(patternParts)(pos))
      .pathPatterns

  private val a_r_b = relationshipChain(
    nodePat(Some("a")),
    relPat(
      name = Some("r"),
      labelExpression = Some(labelRelTypeLeaf("R")),
      length = None,
      direction = SemanticDirection.OUTGOING
    ),
    nodePat(Some("b"))
  )

  private val longElement = pathConcatenation(
    nodePat(Some("start")),
    quantifiedPath(a_r_b, plusQuantifier),
    relationshipChain(
      nodePat(Some("c")),
      relPat(
        name = Some("s"),
        labelExpression = None,
        length = None,
        direction = SemanticDirection.BOTH
      ),
      nodePat(Some("end"))
    )
  )

  private val longPathPattern =
    NodeConnections(NonEmptyList(
      QuantifiedPathPattern(
        leftBinding = NodeBinding(v"a", v"start"),
        rightBinding = NodeBinding(v"b", v"c"),
        patternRelationships = NonEmptyList(PatternRelationship(
          variable = v"r",
          boundaryNodes = (v"a", v"b"),
          dir = SemanticDirection.OUTGOING,
          types = List(relTypeName("R")),
          length = SimplePatternLength
        )),
        argumentIds = Set.empty,
        selections = Selections.empty,
        repetition = Repetition(1, UpperBound.unlimited),
        nodeVariableGroupings = Set(variableGrouping(v"a", v"a"), variableGrouping(v"b", v"b")),
        relationshipVariableGroupings = Set(variableGrouping(v"r", v"r"))
      ),
      PatternRelationship(
        variable = v"s",
        boundaryNodes = (v"c", v"end"),
        dir = SemanticDirection.BOTH,
        types = Nil,
        length = SimplePatternLength
      )
    ))

  private val shortestRelationship =
    ShortestPathsPatternPart(
      element = relationshipChain(
        nodePat(Some("a")),
        relPat(
          name = Some("r"),
          labelExpression = Some(labelRelTypeLeaf("R")),
          length = Some(Some(range(Some(0), Some(3)))),
          direction = SemanticDirection.OUTGOING
        ),
        nodePat(Some("b"))
      ),
      single = true
    )(pos)

  // Note that this is not semantically valid
  test("empty pattern") {
    convertPatternParts() shouldEqual Nil
  }

  test("All paths: relationship pattern") {
    val ast = PatternPartWithSelector(
      part = PathPatternPart(a_r_b),
      selector = allPathsSelector()
    )

    val ir = NodeConnections(NonEmptyList(
      PatternRelationship(
        variable = v"r",
        boundaryNodes = (v"a", v"b"),
        dir = SemanticDirection.OUTGOING,
        types = List(relTypeName("R")),
        length = SimplePatternLength
      )
    ))

    convertPatternParts(ast) shouldEqual List(ir)
  }

  test("All paths: invalid empty concatenation") {
    val ast = PatternPartWithSelector(
      part = PathPatternPart(pathConcatenation()),
      selector = allPathsSelector()
    )

    the[IllegalArgumentException] thrownBy {
      convertPatternParts(ast)
    } should have message "Cannot concatenate an empty list of path factors"
  }

  test("All paths: invalid concatenation starting with parenthesised path") {
    val ast = PatternPartWithSelector(
      part = PathPatternPart(pathConcatenation(parenthesizedPath(a_r_b))),
      selector = allPathsSelector()
    )

    the[IllegalArgumentException] thrownBy {
      convertPatternParts(ast)
    } should have message "Concatenated path factors must start with a simple pattern, not a ParenthesizedPath"
  }

  test("All paths: single node in concatenation") {
    val ast = PatternPartWithSelector(
      part = PathPatternPart(pathConcatenation(nodePat(Some("start")))),
      selector = allPathsSelector()
    )

    val ir = SingleNode(v"start")

    convertPatternParts(ast) shouldEqual List(ir)
  }

  test("All paths: concatenated path patterns") {
    val ast = PatternPartWithSelector(
      part = PathPatternPart(pathConcatenation(
        nodePat(Some("start")),
        quantifiedPath(a_r_b, plusQuantifier),
        relationshipChain(
          nodePat(Some("c")),
          relPat(
            name = Some("s"),
            labelExpression = None,
            length = None,
            direction = SemanticDirection.BOTH
          ),
          nodePat(Some("end"))
        )
      )),
      selector = allPathsSelector()
    )

    val ir = NodeConnections(NonEmptyList(
      QuantifiedPathPattern(
        leftBinding = NodeBinding(v"a", v"start"),
        rightBinding = NodeBinding(v"b", v"c"),
        patternRelationships = NonEmptyList(PatternRelationship(
          variable = v"r",
          boundaryNodes = (v"a", v"b"),
          dir = SemanticDirection.OUTGOING,
          types = List(relTypeName("R")),
          length = SimplePatternLength
        )),
        argumentIds = Set.empty,
        selections = Selections.empty,
        repetition = Repetition(1, UpperBound.unlimited),
        nodeVariableGroupings = Set(variableGrouping(v"a", v"a"), variableGrouping(v"b", v"b")),
        relationshipVariableGroupings = Set(variableGrouping(v"r", v"r"))
      ),
      PatternRelationship(
        variable = v"s",
        boundaryNodes = (v"c", v"end"),
        dir = SemanticDirection.BOTH,
        types = Nil,
        length = SimplePatternLength
      )
    ))

    convertPatternParts(ast) shouldEqual List(ir)
  }

  test("All paths: missing simple pattern after quantified path pattern") {
    val ast = PatternPartWithSelector(
      part = PathPatternPart(pathConcatenation(
        nodePat(Some("start")),
        quantifiedPath(a_r_b, plusQuantifier)
      )),
      selector = allPathsSelector()
    )

    the[IllegalArgumentException] thrownBy {
      convertPatternParts(ast)
    } should have message "A quantified path pattern must be concatenated with a simple path pattern"
  }

  test("All paths: quantified path pattern concatenated with a parenthesised path pattern") {
    val ast = PatternPartWithSelector(
      part = PathPatternPart(pathConcatenation(
        nodePat(Some("start")),
        quantifiedPath(a_r_b, plusQuantifier),
        parenthesizedPath(a_r_b)
      )),
      selector = allPathsSelector()
    )

    the[IllegalArgumentException] thrownBy {
      convertPatternParts(ast)
    } should have message "A quantified path pattern must be concatenated with a simple path pattern, not with a ParenthesizedPath"
  }

  test("All paths: simple pattern concatenated with a parenthesised path pattern") {
    val ast = PatternPartWithSelector(
      part = PathPatternPart(pathConcatenation(
        nodePat(Some("start")),
        parenthesizedPath(a_r_b)
      )),
      selector = allPathsSelector()
    )

    the[IllegalArgumentException] thrownBy {
      convertPatternParts(ast)
    } should have message "A simple path pattern may only be concatenated with a quantified path pattern, not with a ParenthesizedPath"
  }

  test("All paths: invalid quantified path pattern outside concatenation") {
    val ast = PatternPartWithSelector(
      part = PathPatternPart(quantifiedPath(a_r_b, plusQuantifier)),
      selector = allPathsSelector()
    )

    the[IllegalArgumentException] thrownBy {
      convertPatternParts(ast)
    } should have message "Quantified path patterns must be concatenated with outer node patterns"
  }

  test("All paths: invalid parenthesised path pattern") {
    val ast = PatternPartWithSelector(
      part = PathPatternPart(parenthesizedPath(a_r_b)),
      selector = allPathsSelector()
    )

    the[IllegalArgumentException] thrownBy {
      convertPatternParts(ast)
    } should have message "Parenthesised path patterns are only supported at the top level with a selective path selector"
  }

  test("All shortest paths") {
    val ast = PatternPartWithSelector(
      part = PathPatternPart(longElement),
      selector = allShortestPathsSelector()
    )

    val ir = SelectivePathPattern(
      pathPattern = longPathPattern,
      selections = Selections.empty,
      selector = SelectivePathPattern.Selector.ShortestGroups(1)
    )

    convertPatternParts(ast) shouldEqual List(ir)
  }

  test("All shortest paths with selection") {
    val predicate = hasLabels("start", "Start")

    val ast = PatternPartWithSelector(
      part = PathPatternPart(ParenthesizedPath(
        part = PathPatternPart(longElement),
        optionalWhereClause = Some(predicate)
      )(pos)),
      selector = allShortestPathsSelector()
    )

    val ir = SelectivePathPattern(
      pathPattern = longPathPattern,
      selections = Selections.from(predicate),
      selector = SelectivePathPattern.Selector.ShortestGroups(1)
    )

    convertPatternParts(ast) shouldEqual List(ir)
  }

  test("All shortest: nested shortest var-length relationship") {
    val ast = PatternPartWithSelector(
      part = PathPatternPart(ParenthesizedPath(
        part = ShortestPathsPatternPart(a_r_b, single = true)(pos),
        optionalWhereClause = None
      )(pos)),
      selector = allShortestPathsSelector()
    )

    the[IllegalArgumentException] thrownBy {
      convertPatternParts(ast)
    } should have message "shortestPath() is not allowed inside of a parenthesised path pattern"
  }

  test("All shortest: sub-path assignment") {
    val ast = PatternPartWithSelector(
      part = PathPatternPart(ParenthesizedPath(
        part = NamedPatternPart(
          variable = varFor("p"),
          patternPart = PathPatternPart(longElement)
        )(pos),
        optionalWhereClause = None
      )(pos)),
      selector = allShortestPathsSelector()
    )

    the[IllegalArgumentException] thrownBy {
      convertPatternParts(ast)
    } should have message "Sub-path assignment is currently not supported"
  }

  test("Any path with selection") {
    val predicate = hasLabels("start", "Start")

    val ast = PatternPartWithSelector(
      part = PathPatternPart(ParenthesizedPath(
        part = PathPatternPart(longElement),
        optionalWhereClause = Some(predicate)
      )(pos)),
      selector = PatternPart.AnyPath(literalUnsignedInt(1))(pos)
    )

    val ir = SelectivePathPattern(
      pathPattern = longPathPattern,
      selections = Selections.from(predicate),
      selector = SelectivePathPattern.Selector.Any(1)
    )

    convertPatternParts(ast) shouldEqual List(ir)
  }

  test("Any 2 paths") {
    val predicate = hasLabels("start", "Start")

    val ast = PatternPartWithSelector(
      part = PathPatternPart(ParenthesizedPath(
        part = PathPatternPart(longElement),
        optionalWhereClause = Some(predicate)
      )(pos)),
      selector = PatternPart.AnyPath(literalUnsignedInt(2))(pos)
    )

    val ir = SelectivePathPattern(
      pathPattern = longPathPattern,
      selections = Selections.from(predicate),
      selector = SelectivePathPattern.Selector.Any(2)
    )

    convertPatternParts(ast) shouldEqual List(ir)
  }

  test("Any shortest path with selection") {
    val predicate = hasLabels("start", "Start")

    val ast = PatternPartWithSelector(
      part = PathPatternPart(ParenthesizedPath(
        part = PathPatternPart(longElement),
        optionalWhereClause = Some(predicate)
      )(pos)),
      selector = PatternPart.AnyShortestPath(literalUnsignedInt(1))(pos)
    )

    val ir = SelectivePathPattern(
      pathPattern = longPathPattern,
      selections = Selections.from(predicate),
      selector = SelectivePathPattern.Selector.Shortest(1)
    )

    convertPatternParts(ast) shouldEqual List(ir)
  }

  test("Shortest 2 paths") {
    val predicate = hasLabels("start", "Start")

    val ast = PatternPartWithSelector(
      part = PathPatternPart(ParenthesizedPath(
        part = PathPatternPart(longElement),
        optionalWhereClause = Some(predicate)
      )(pos)),
      selector = PatternPart.AnyShortestPath(literalUnsignedInt(2))(pos)
    )

    val ir = SelectivePathPattern(
      pathPattern = longPathPattern,
      selections = Selections.from(predicate),
      selector = SelectivePathPattern.Selector.Shortest(2)
    )

    convertPatternParts(ast) shouldEqual List(ir)
  }

  // Note that SHORTEST 1 GROUP is the same as ALL SHORTEST
  test("Shortest 1 group with selection") {
    val predicate = hasLabels("start", "Start")

    val ast = PatternPartWithSelector(
      part = PathPatternPart(ParenthesizedPath(
        part = PathPatternPart(longElement),
        optionalWhereClause = Some(predicate)
      )(pos)),
      selector = PatternPart.ShortestGroups(literalUnsignedInt(1))(pos)
    )

    val ir = SelectivePathPattern(
      pathPattern = longPathPattern,
      selections = Selections.from(predicate),
      selector = SelectivePathPattern.Selector.ShortestGroups(1)
    )

    convertPatternParts(ast) shouldEqual List(ir)
  }

  test("Shortest 2 path groups") {
    val predicate = hasLabels("start", "Start")

    val ast = PatternPartWithSelector(
      part = PathPatternPart(ParenthesizedPath(
        part = PathPatternPart(longElement),
        optionalWhereClause = Some(predicate)
      )(pos)),
      selector = PatternPart.ShortestGroups(literalUnsignedInt(2))(pos)
    )

    val ir = SelectivePathPattern(
      pathPattern = longPathPattern,
      selections = Selections.from(predicate),
      selector = SelectivePathPattern.Selector.ShortestGroups(2)
    )

    convertPatternParts(ast) shouldEqual List(ir)
  }

  test("shortest relationship pattern") {
    val ast = PatternPartWithSelector(
      allPathsSelector(),
      NamedPatternPart(variable = varFor("p"), patternPart = shortestRelationship)(pos)
    )

    val ir = ShortestRelationshipPattern(
      maybePathVar = Some(v"p"),
      rel = PatternRelationship(
        variable = v"r",
        boundaryNodes = (v"a", v"b"),
        dir = SemanticDirection.OUTGOING,
        types = List(relTypeName("R")),
        length = VarPatternLength(0, Some(3))
      ),
      single = true
    )(expr = shortestRelationship)

    convertPatternParts(ast) shouldEqual List(ir)
  }

  test("anonymous shortest relationship pattern") {
    val ir = ShortestRelationshipPattern(
      maybePathVar = Some(v"  UNNAMED0"),
      rel = PatternRelationship(
        variable = v"r",
        boundaryNodes = (v"a", v"b"),
        dir = SemanticDirection.OUTGOING,
        types = List(relTypeName("R")),
        length = VarPatternLength(0, Some(3))
      ),
      single = true
    )(expr = shortestRelationship)

    convertPatternParts(shortestRelationship.withAllPathsSelector) shouldEqual List(ir)
  }

  test("invalid shortest relationship pattern") {
    val ast = ShortestPathsPatternPart(
      element = longElement,
      single = false
    )(pos)

    the[IllegalArgumentException] thrownBy {
      convertPatternParts(ast.withAllPathsSelector)
    } should have message "allShortestPaths() must contain a single relationship, it cannot contain a PathConcatenation"
  }

  test("multiple pattern parts") {
    val part1 = PatternPartWithSelector(
      part = PathPatternPart(a_r_b),
      selector = allPathsSelector()
    )

    val ir1 = NodeConnections(NonEmptyList(
      PatternRelationship(
        variable = v"r",
        boundaryNodes = (v"a", v"b"),
        dir = SemanticDirection.OUTGOING,
        types = List(relTypeName("R")),
        length = SimplePatternLength
      )
    ))

    val part2 = PatternPartWithSelector(
      part = PathPatternPart(ParenthesizedPath(
        part = PathPatternPart(longElement),
        optionalWhereClause = Some(hasLabels("start", "Start"))
      )(pos)),
      selector = PatternPart.AnyShortestPath(literalUnsignedInt(1))(pos)
    )

    val ir2 = SelectivePathPattern(
      pathPattern = longPathPattern,
      selections = Selections.from(hasLabels("start", "Start")),
      selector = SelectivePathPattern.Selector.Shortest(1)
    )

    val part3 = shortestRelationship.withAllPathsSelector

    val ir3 = ShortestRelationshipPattern(
      maybePathVar = Some(v"  UNNAMED0"),
      rel = PatternRelationship(
        variable = v"r",
        boundaryNodes = (v"a", v"b"),
        dir = SemanticDirection.OUTGOING,
        types = List(relTypeName("R")),
        length = VarPatternLength(0, Some(3))
      ),
      single = true
    )(expr = shortestRelationship)

    convertPatternParts(part1, part2, part3) shouldEqual List(ir1, ir2, ir3)
  }
}
