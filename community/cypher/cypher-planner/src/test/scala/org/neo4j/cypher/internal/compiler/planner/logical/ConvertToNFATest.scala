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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir.ExhaustivePathPattern
import org.neo4j.cypher.internal.ir.NodeBinding
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QuantifiedPathPattern
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SelectivePathPattern
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.ir.VariableGrouping
import org.neo4j.cypher.internal.logical.builder.TestNFABuilder
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.Repetition
import org.neo4j.cypher.internal.util.UpperBound
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.exceptions.InternalException

class ConvertToNFATest extends CypherFunSuite with AstConstructionTestSupport {

  private val `(start) ((a)-[r]->(b))+ (end)` = QuantifiedPathPattern(
    leftBinding = NodeBinding(v"a", v"start"),
    rightBinding = NodeBinding(v"b", v"end"),
    patternRelationships = NonEmptyList(PatternRelationship(
      variable = v"r",
      boundaryNodes = (v"a", v"b"),
      dir = SemanticDirection.OUTGOING,
      types = Nil,
      length = SimplePatternLength
    )),
    argumentIds = Set.empty,
    selections = Selections.empty,
    repetition = Repetition(1, UpperBound.Unlimited),
    nodeVariableGroupings = Set(v"a", v"b").map(name => VariableGrouping(name, name)),
    relationshipVariableGroupings = Set(VariableGrouping(v"r", v"r"))
  )

  private val `(start) ((a)-[r]->(b)-[r2]->(c))+ (end)` = QuantifiedPathPattern(
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
        variable = v"r2",
        boundaryNodes = (v"b", v"c"),
        dir = SemanticDirection.OUTGOING,
        types = Nil,
        length = SimplePatternLength
      )
    ),
    argumentIds = Set.empty,
    selections = Selections.empty,
    repetition = Repetition(1, UpperBound.Unlimited),
    nodeVariableGroupings = Set(v"a", v"b", v"c").map(name => VariableGrouping(name, name)),
    relationshipVariableGroupings = Set(v"r", v"r2").map(name => VariableGrouping(name, name))
  )

  // QPP with internal predicates
  private val `(start) ((a)-[r]->(b))+ (end) [with predicates]` =
    QuantifiedPathPattern(
      leftBinding = NodeBinding(v"a", v"start"),
      rightBinding = NodeBinding(v"b", v"end"),
      patternRelationships = NonEmptyList(PatternRelationship(
        variable = v"r",
        boundaryNodes = (v"a", v"b"),
        dir = SemanticDirection.OUTGOING,
        types = Seq(relTypeName("T")),
        length = SimplePatternLength
      )),
      argumentIds = Set.empty,
      selections = Selections.from(Seq(
        hasLabels("a", "A"),
        hasLabels("b", "B"),
        equals(prop("a", "prop"), literalInt(1)),
        equals(prop("b", "prop"), literalInt(2))
      )),
      repetition = Repetition(1, UpperBound.Unlimited),
      nodeVariableGroupings = Set(v"a", v"b").map(name => VariableGrouping(name, name)),
      relationshipVariableGroupings = Set(VariableGrouping(v"r", v"r"))
    )

  test("create simple NFA") {
    val spp =
      SelectivePathPattern(
        pathPattern = ExhaustivePathPattern.NodeConnections(NonEmptyList(`(start) ((a)-[r]->(b))+ (end)`)),
        selections = Selections.empty,
        selector = SelectivePathPattern.Selector.ShortestGroups(1)
      )

    val expectedNfa = new TestNFABuilder(0, "start")
      .addTransition(0, 1, "(start) (a)")
      .addTransition(1, 2, "(a)-[r]->(b)")
      .addTransition(2, 1, "(b) (a)")
      .addTransition(2, 3, "(b) (end)")
      .addFinalState(3)
      .build()

    ConvertToNFA.convertToNfa(
      spp,
      fromLeft = true,
      Set.empty,
      Seq.empty,
      new AnonymousVariableNameGenerator
    ) should equal((expectedNfa, Selections.empty, Map.empty))
  }

  test("create simple NFA and report non-inlineable predicate (depends on two variables)") {
    val nonInlineablePredicate = equals(prop("start", "prop"), prop("end", "prop"))
    val spp =
      SelectivePathPattern(
        pathPattern = ExhaustivePathPattern.NodeConnections(NonEmptyList(`(start) ((a)-[r]->(b))+ (end)`)),
        selections = Selections.from(nonInlineablePredicate),
        selector = SelectivePathPattern.Selector.ShortestGroups(1)
      )

    val expectedNfa = new TestNFABuilder(0, "start")
      .addTransition(0, 1, "(start) (a)")
      .addTransition(1, 2, "(a)-[r]->(b)")
      .addTransition(2, 1, "(b) (a)")
      .addTransition(2, 3, "(b) (end)")
      .addFinalState(3)
      .build()

    ConvertToNFA.convertToNfa(
      spp,
      fromLeft = true,
      Set.empty,
      Seq.empty,
      new AnonymousVariableNameGenerator
    ) should equal((
      expectedNfa,
      Selections.from(nonInlineablePredicate),
      Map.empty
    ))
  }

  test("create simple NFA and report non-inlineable predicate (depends on group variable)") {
    val nonInlineablePredicate = equals(size(varFor("a")), literalInt(3))

    val spp =
      SelectivePathPattern(
        pathPattern = ExhaustivePathPattern.NodeConnections(NonEmptyList(`(start) ((a)-[r]->(b))+ (end)`)),
        selections = Selections.from(nonInlineablePredicate),
        selector = SelectivePathPattern.Selector.ShortestGroups(1)
      )

    val expectedNfa = new TestNFABuilder(0, "start")
      .addTransition(0, 1, "(start) (a)")
      .addTransition(1, 2, "(a)-[r]->(b)")
      .addTransition(2, 1, "(b) (a)")
      .addTransition(2, 3, "(b) (end)")
      .addFinalState(3)
      .build()

    ConvertToNFA.convertToNfa(
      spp,
      fromLeft = true,
      Set.empty,
      Seq.empty,
      new AnonymousVariableNameGenerator
    ) should equal((
      expectedNfa,
      Selections.from(nonInlineablePredicate),
      Map.empty
    ))
  }

  test("does not support predicates that depends on multiple singleton variables in QPP") {
    val failingPredicate = equals(prop("a", "prop"), prop("c", "prop"))

    val qpp = `(start) ((a)-[r]->(b)-[r2]->(c))+ (end)`.copy(selections = Selections.from(failingPredicate))

    val spp =
      SelectivePathPattern(
        pathPattern = ExhaustivePathPattern.NodeConnections(NonEmptyList(qpp)),
        selections = Selections.empty,
        selector = SelectivePathPattern.Selector.ShortestGroups(1)
      )

    an[InternalException] should be thrownBy {
      ConvertToNFA.convertToNfa(
        spp,
        fromLeft = true,
        Set.empty,
        Seq.empty,
        new AnonymousVariableNameGenerator
      )
    }
  }

  test("create simple NFA reversed") {
    val spp =
      SelectivePathPattern(
        pathPattern = ExhaustivePathPattern.NodeConnections(NonEmptyList(`(start) ((a)-[r]->(b))+ (end)`)),
        selections = Selections.empty,
        selector = SelectivePathPattern.Selector.ShortestGroups(1)
      )

    val expectedNfa = new TestNFABuilder(0, "end")
      .addTransition(0, 1, "(end) (b)")
      .addTransition(1, 2, "(b)<-[r]-(a)")
      .addTransition(2, 1, "(a) (b)")
      .addTransition(2, 3, "(a) (start)")
      .addFinalState(3)
      .build()

    ConvertToNFA.convertToNfa(
      spp,
      fromLeft = false,
      Set.empty,
      Seq.empty,
      new AnonymousVariableNameGenerator
    ) should equal((expectedNfa, Selections.empty, Map.empty))
  }

  test("create NFA reversed for (start) ((a)-[r]->(b)-[s]-(c))* (d)<-[t]-(end)") {
    val qpp = QuantifiedPathPattern(
      leftBinding = NodeBinding(v"a", v"start"),
      rightBinding = NodeBinding(v"c", v"d"),
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
          dir = SemanticDirection.BOTH,
          types = Nil,
          length = SimplePatternLength
        )
      ),
      argumentIds = Set.empty,
      selections = Selections.from(differentRelationships(varFor("r"), varFor("s"))),
      repetition = Repetition(0, UpperBound.Unlimited),
      nodeVariableGroupings = Set(v"a", v"b", v"c").map(name => VariableGrouping(name, name)),
      relationshipVariableGroupings = Set(v"r", v"s").map(name => VariableGrouping(name, name))
    )
    val rel = PatternRelationship(
      variable = v"t",
      boundaryNodes = (v"d", v"end"),
      dir = SemanticDirection.INCOMING,
      types = Seq.empty,
      length = SimplePatternLength
    )
    val spp =
      SelectivePathPattern(
        pathPattern = ExhaustivePathPattern.NodeConnections(NonEmptyList(qpp, rel)),
        selections = Selections.empty,
        selector = SelectivePathPattern.Selector.ShortestGroups(1)
      )

    val expectedNfa = new TestNFABuilder(0, "end")
      .addTransition(0, 1, "(end)-[t]->(d)")
      .addTransition(1, 2, "(d) (c)")
      .addTransition(2, 3, "(c)-[s]-(b)")
      .addTransition(3, 4, "(b)<-[r]-(a)")
      .addTransition(4, 2, "(a) (c)")
      .addTransition(1, 5, "(d) (start)")
      .addTransition(4, 5, "(a) (start)")
      .addFinalState(5)
      .build()

    ConvertToNFA.convertToNfa(
      spp,
      fromLeft = false,
      Set.empty,
      Seq.empty,
      new AnonymousVariableNameGenerator
    ) should equal((expectedNfa, Selections.empty, Map.empty))
  }

  test("create NFA with predicates") {
    val spp =
      SelectivePathPattern(
        pathPattern =
          ExhaustivePathPattern.NodeConnections(NonEmptyList(`(start) ((a)-[r]->(b))+ (end) [with predicates]`)),
        selections = Selections.empty,
        selector = SelectivePathPattern.Selector.ShortestGroups(1)
      )

    val expectedNfa = new TestNFABuilder(0, "start")
      .addTransition(0, 1, "(start) (a:A WHERE a.prop = 1)")
      .addTransition(1, 2, "(a)-[r:T]->(b:B WHERE b.prop = 2)")
      .addTransition(2, 1, "(b) (a:A WHERE a.prop = 1)")
      .addTransition(2, 3, "(b) (end:E)")
      .addFinalState(3)
      .build()

    ConvertToNFA.convertToNfa(
      spp,
      fromLeft = true,
      Set.empty,
      Seq(hasLabels("end", "E")),
      new AnonymousVariableNameGenerator
    ) should equal((
      expectedNfa,
      Selections.empty,
      Map.empty
    ))
  }

  test("create NFA with predicates that depend on interior and other available symbol") {
    val qpp = QuantifiedPathPattern(
      leftBinding = NodeBinding(v"a", v"start"),
      rightBinding = NodeBinding(v"b", v"c"),
      patternRelationships = NonEmptyList(PatternRelationship(
        variable = v"r",
        boundaryNodes = (v"a", v"b"),
        dir = SemanticDirection.OUTGOING,
        types = Seq.empty,
        length = SimplePatternLength
      )),
      argumentIds = Set.empty,
      selections = Selections.from(Seq(
        equals(prop("a", "prop"), varFor("foo")),
        equals(prop("r", "prop"), varFor("foo")),
        equals(prop("b", "prop"), varFor("foo"))
      )),
      repetition = Repetition(1, UpperBound.Unlimited),
      nodeVariableGroupings = Set(v"a", v"b").map(name => VariableGrouping(name, name)),
      relationshipVariableGroupings = Set(VariableGrouping(v"r", v"r"))
    )
    val rel = PatternRelationship(
      variable = v"r2",
      boundaryNodes = (v"c", v"end"),
      dir = SemanticDirection.INCOMING,
      types = Seq.empty,
      length = SimplePatternLength
    )
    val spp =
      SelectivePathPattern(
        pathPattern =
          ExhaustivePathPattern.NodeConnections(NonEmptyList(qpp, rel)),
        selections = Selections.from(
          equals(prop("r2", "prop"), varFor("foo"))
        ),
        selector = SelectivePathPattern.Selector.ShortestGroups(1)
      )

    val expectedNfa = new TestNFABuilder(0, "start")
      .addTransition(0, 1, "(start) (a WHERE a.prop = foo)")
      .addTransition(1, 2, "(a)-[r WHERE r.prop = foo]->(b WHERE b.prop = foo)")
      .addTransition(2, 1, "(b) (a WHERE a.prop = foo)")
      .addTransition(2, 3, "(b) (c)")
      .addTransition(3, 4, "(c)<-[r2 WHERE r2.prop = foo]-(end WHERE end.prop = foo)")
      .addFinalState(4)
      .build()

    ConvertToNFA.convertToNfa(
      spp,
      fromLeft = true,
      Set("foo"),
      Seq(equals(prop("end", "prop"), varFor("foo"))),
      new AnonymousVariableNameGenerator
    ) should equal((
      expectedNfa,
      Selections.empty,
      Map.empty
    ))
  }

  test("create NFA with lower bound > 1") {
    // (start) ((a)-[r]->(b)){2, } (end)
    val qpp = `(start) ((a)-[r]->(b))+ (end)`
      .copy(repetition = Repetition(2, UpperBound.Unlimited))
    val spp =
      SelectivePathPattern(
        pathPattern = ExhaustivePathPattern.NodeConnections(NonEmptyList(qpp)),
        selections = Selections.empty,
        selector = SelectivePathPattern.Selector.ShortestGroups(1)
      )

    val expectedNfa = new TestNFABuilder(0, "start")
      .addTransition(0, 1, "(start) (a)")
      .addTransition(1, 2, "(a)-[r]->(b)")
      .addTransition(2, 3, "(b) (a)")
      .addTransition(3, 4, "(a)-[r]->(b)")
      .addTransition(4, 3, "(b) (a)")
      .addTransition(4, 5, "(b) (end)")
      .addFinalState(5)
      .build()

    ConvertToNFA.convertToNfa(
      spp,
      fromLeft = true,
      Set.empty,
      Seq.empty,
      new AnonymousVariableNameGenerator
    ) should equal((expectedNfa, Selections.empty, Map.empty))
  }

  test("create NFA with lower bound == 0 and finite upper bound") {
    // (start) ((a)-[r]->(b)){0, 2} (end)
    val qpp = `(start) ((a)-[r]->(b))+ (end)`
      .copy(repetition = Repetition(0, UpperBound.Limited(2)))
    val spp =
      SelectivePathPattern(
        pathPattern = ExhaustivePathPattern.NodeConnections(NonEmptyList(qpp)),
        selections = Selections.empty,
        selector = SelectivePathPattern.Selector.ShortestGroups(1)
      )

    val expectedNfa = new TestNFABuilder(0, "start")
      .addTransition(0, 1, "(start) (a)")
      .addTransition(1, 2, "(a)-[r]->(b)")
      .addTransition(2, 3, "(b) (a)")
      .addTransition(3, 4, "(a)-[r]->(b)")
      .addTransition(0, 5, "(start) (end)")
      .addTransition(2, 5, "(b) (end)")
      .addTransition(4, 5, "(b) (end)")
      .addFinalState(5)
      .build()

    ConvertToNFA.convertToNfa(
      spp,
      fromLeft = true,
      Set.empty,
      Seq.empty,
      new AnonymousVariableNameGenerator
    ) should equal((expectedNfa, Selections.empty, Map.empty))
  }

  test("create NFA with lower bound == 1 and finite upper bound") {
    // (start) ((a)-[r]->(b)){1, 3} (end)
    val qpp = `(start) ((a)-[r]->(b))+ (end)`
      .copy(repetition = Repetition(1, UpperBound.Limited(3)))

    val spp =
      SelectivePathPattern(
        pathPattern = ExhaustivePathPattern.NodeConnections(NonEmptyList(qpp)),
        selections = Selections.empty,
        selector = SelectivePathPattern.Selector.ShortestGroups(1)
      )

    val expectedNfa = new TestNFABuilder(0, "start")
      .addTransition(0, 1, "(start) (a)")
      .addTransition(1, 2, "(a)-[r]->(b)")
      .addTransition(2, 7, "(b) (end)")
      .addTransition(2, 3, "(b) (a)")
      .addTransition(3, 4, "(a)-[r]->(b)")
      .addTransition(4, 7, "(b) (end)")
      .addTransition(4, 5, "(b) (a)")
      .addTransition(5, 6, "(a)-[r]->(b)")
      .addTransition(6, 7, "(b) (end)")
      .addFinalState(7)
      .build()

    ConvertToNFA.convertToNfa(
      spp,
      fromLeft = true,
      Set.empty,
      Seq.empty,
      new AnonymousVariableNameGenerator
    ) should equal((expectedNfa, Selections.empty, Map.empty))
  }

  test("create NFA with lower bound > 1 and finite upper bound") {
    // (start) ((a)-[r]->(b)){2, 3} (end)
    val qpp = `(start) ((a)-[r]->(b))+ (end)`
      .copy(repetition = Repetition(2, UpperBound.Limited(3)))

    val spp =
      SelectivePathPattern(
        pathPattern = ExhaustivePathPattern.NodeConnections(NonEmptyList(qpp)),
        selections = Selections.empty,
        selector = SelectivePathPattern.Selector.ShortestGroups(1)
      )

    val expectedNfa = new TestNFABuilder(0, "start")
      .addTransition(0, 1, "(start) (a)")
      .addTransition(1, 2, "(a)-[r]->(b)")
      .addTransition(2, 3, "(b) (a)")
      .addTransition(3, 4, "(a)-[r]->(b)")
      .addTransition(4, 7, "(b) (end)")
      .addTransition(4, 5, "(b) (a)")
      .addTransition(5, 6, "(a)-[r]->(b)")
      .addTransition(6, 7, "(b) (end)")
      .addFinalState(7)
      .build()

    ConvertToNFA.convertToNfa(
      spp,
      fromLeft = true,
      Set.empty,
      Seq.empty,
      new AnonymousVariableNameGenerator
    ) should equal((expectedNfa, Selections.empty, Map.empty))
  }

  test("create NFA with lower bound > 1 and finite upper bound (with predicates)") {
    val qpp = `(start) ((a)-[r]->(b))+ (end) [with predicates]`
      .copy(repetition = Repetition(2, UpperBound.Limited(3)))

    val spp =
      SelectivePathPattern(
        pathPattern = ExhaustivePathPattern.NodeConnections(NonEmptyList(qpp)),
        selections = Selections.empty,
        selector = SelectivePathPattern.Selector.ShortestGroups(1)
      )

    val expectedNfa = new TestNFABuilder(0, "start")
      .addTransition(0, 1, "(start) (a:A WHERE a.prop = 1)")
      .addTransition(1, 2, "(a)-[r:T]->(b:B WHERE b.prop = 2)")
      .addTransition(2, 3, "(b) (a:A WHERE a.prop = 1)")
      .addTransition(3, 4, "(a)-[r:T]->(b:B WHERE b.prop = 2)")
      .addTransition(4, 7, "(b) (end:E)")
      .addTransition(4, 5, "(b) (a:A WHERE a.prop = 1)")
      .addTransition(5, 6, "(a)-[r:T]->(b:B WHERE b.prop = 2)")
      .addTransition(6, 7, "(b) (end:E)")
      .addFinalState(7)
      .build()

    ConvertToNFA.convertToNfa(
      spp,
      fromLeft = true,
      Set.empty,
      Seq(hasLabels("end", "E")),
      new AnonymousVariableNameGenerator
    ) should equal((
      expectedNfa,
      Selections.empty,
      Map.empty
    ))
  }

  test("create NFA with lower bound == 0 and infinite upper bound") {
    // (start) ((a)-[r]->(b))* (end)
    val qpp = `(start) ((a)-[r]->(b))+ (end)`
      .copy(repetition = Repetition(0, UpperBound.Unlimited))

    val spp =
      SelectivePathPattern(
        pathPattern = ExhaustivePathPattern.NodeConnections(NonEmptyList(qpp)),
        selections = Selections.empty,
        selector = SelectivePathPattern.Selector.ShortestGroups(1)
      )

    val expectedNfa = new TestNFABuilder(0, "start")
      .addTransition(0, 1, "(start) (a)")
      .addTransition(1, 2, "(a)-[r]->(b)")
      .addTransition(2, 1, "(b) (a)")
      .addTransition(2, 3, "(b) (end)")
      .addTransition(0, 3, "(start) (end)")
      .addFinalState(3)
      .build()

    ConvertToNFA.convertToNfa(
      spp,
      fromLeft = true,
      Set.empty,
      Seq.empty,
      new AnonymousVariableNameGenerator
    ) should equal((expectedNfa, Selections.empty, Map.empty))
  }

  // Tests for var-length relationship
  test("(start)-[r:R*0..]->(end)") {
    val varRel =
      PatternRelationship(
        v"r",
        (v"start", v"end"),
        SemanticDirection.OUTGOING,
        Seq(relTypeName("R")),
        VarPatternLength(0, None)
      )
    val spp =
      SelectivePathPattern(
        pathPattern = ExhaustivePathPattern.NodeConnections(NonEmptyList(varRel)),
        selections = Selections.empty,
        selector = SelectivePathPattern.Selector.ShortestGroups(1)
      )

    val expectedNfa = new TestNFABuilder(0, "start")
      .addTransition(0, 1, "(start) (`  UNNAMED1`)")
      .addTransition(1, 2, "(`  UNNAMED1`) (end)")
      .addTransition(1, 1, "(`  UNNAMED1`)-[`  r@0`:R]->(`  UNNAMED1`)")
      .addFinalState(2)
      .build()

    ConvertToNFA.convertToNfa(
      spp,
      fromLeft = true,
      Set.empty,
      Seq.empty,
      new AnonymousVariableNameGenerator
    ) should equal((expectedNfa, Selections.empty, Map("r" -> "  r@0")))
  }

  test("(start)-[r:R*1..]->(end)") {
    val varRel =
      PatternRelationship(
        v"r",
        (v"start", v"end"),
        SemanticDirection.OUTGOING,
        Seq(relTypeName("R")),
        VarPatternLength(1, None)
      )
    val spp =
      SelectivePathPattern(
        pathPattern = ExhaustivePathPattern.NodeConnections(NonEmptyList(varRel)),
        selections = Selections.empty,
        selector = SelectivePathPattern.Selector.ShortestGroups(1)
      )

    val expectedNfa = new TestNFABuilder(0, "start")
      .addTransition(0, 1, "(start) (`  UNNAMED1`)")
      .addTransition(1, 2, "(`  UNNAMED1`)-[`  r@0`:R]->(`  UNNAMED2`)")
      .addTransition(2, 2, "(`  UNNAMED2`)-[`  r@0`:R]->(`  UNNAMED2`)")
      .addTransition(2, 3, "(`  UNNAMED2`) (end)")
      .addFinalState(3)
      .build()

    ConvertToNFA.convertToNfa(
      spp,
      fromLeft = true,
      Set.empty,
      Seq.empty,
      new AnonymousVariableNameGenerator
    ) should equal((expectedNfa, Selections.empty, Map("r" -> "  r@0")))
  }

  test("(start)-[r:R*2..]->(end)") {
    val varRel =
      PatternRelationship(
        v"r",
        (v"start", v"end"),
        SemanticDirection.OUTGOING,
        Seq(relTypeName("R")),
        VarPatternLength(2, None)
      )
    val spp =
      SelectivePathPattern(
        pathPattern = ExhaustivePathPattern.NodeConnections(NonEmptyList(varRel)),
        selections = Selections.empty,
        selector = SelectivePathPattern.Selector.ShortestGroups(1)
      )

    val expectedNfa = new TestNFABuilder(0, "start")
      .addTransition(0, 1, "(start) (`  UNNAMED1`)")
      .addTransition(1, 2, "(`  UNNAMED1`)-[`  r@0`:R]->(`  UNNAMED2`)")
      .addTransition(2, 3, "(`  UNNAMED2`)-[`  r@0`:R]->(`  UNNAMED3`)")
      .addTransition(3, 3, "(`  UNNAMED3`)-[`  r@0`:R]->(`  UNNAMED3`)")
      .addTransition(3, 4, "(`  UNNAMED3`) (end)")
      .addFinalState(4)
      .build()

    ConvertToNFA.convertToNfa(
      spp,
      fromLeft = true,
      Set.empty,
      Seq.empty,
      new AnonymousVariableNameGenerator
    ) should equal((expectedNfa, Selections.empty, Map("r" -> "  r@0")))
  }

  test("(start)-[r:R*3..]->(end)") {
    val varRel =
      PatternRelationship(
        v"r",
        (v"start", v"end"),
        SemanticDirection.OUTGOING,
        Seq(relTypeName("R")),
        VarPatternLength(3, None)
      )
    val spp =
      SelectivePathPattern(
        pathPattern = ExhaustivePathPattern.NodeConnections(NonEmptyList(varRel)),
        selections = Selections.empty,
        selector = SelectivePathPattern.Selector.ShortestGroups(1)
      )

    val expectedNfa = new TestNFABuilder(0, "start")
      .addTransition(0, 1, "(start) (`  UNNAMED1`)")
      .addTransition(1, 2, "(`  UNNAMED1`)-[`  r@0`:R]->(`  UNNAMED2`)")
      .addTransition(2, 3, "(`  UNNAMED2`)-[`  r@0`:R]->(`  UNNAMED3`)")
      .addTransition(3, 4, "(`  UNNAMED3`)-[`  r@0`:R]->(`  UNNAMED4`)")
      .addTransition(4, 4, "(`  UNNAMED4`)-[`  r@0`:R]->(`  UNNAMED4`)")
      .addTransition(4, 5, "(`  UNNAMED4`) (end)")
      .addFinalState(5)
      .build()

    ConvertToNFA.convertToNfa(
      spp,
      fromLeft = true,
      Set.empty,
      Seq.empty,
      new AnonymousVariableNameGenerator
    ) should equal((expectedNfa, Selections.empty, Map("r" -> "  r@0")))
  }

  test("(start)-[r:R*2..3]->(end)") {
    val varRel =
      PatternRelationship(
        v"r",
        (v"start", v"end"),
        SemanticDirection.OUTGOING,
        Seq(relTypeName("R")),
        VarPatternLength(2, Some(3))
      )
    val spp =
      SelectivePathPattern(
        pathPattern = ExhaustivePathPattern.NodeConnections(NonEmptyList(varRel)),
        selections = Selections.empty,
        selector = SelectivePathPattern.Selector.ShortestGroups(1)
      )

    val expectedNfa = new TestNFABuilder(0, "start")
      .addTransition(0, 1, "(start) (`  UNNAMED1`)")
      .addTransition(1, 2, "(`  UNNAMED1`)-[`  r@0`:R]->(`  UNNAMED2`)")
      .addTransition(2, 3, "(`  UNNAMED2`)-[`  r@0`:R]->(`  UNNAMED3`)")
      .addTransition(3, 4, "(`  UNNAMED3`)-[`  r@0`:R]->(`  UNNAMED4`)")
      .addTransition(3, 5, "(`  UNNAMED3`) (end)")
      .addTransition(4, 5, "(`  UNNAMED4`) (end)")
      .addFinalState(5)
      .build()

    ConvertToNFA.convertToNfa(
      spp,
      fromLeft = true,
      Set.empty,
      Seq.empty,
      new AnonymousVariableNameGenerator
    ) should equal((expectedNfa, Selections.empty, Map("r" -> "  r@0")))
  }

  test("(start)-[r:R*0..3]->(end)") {
    val varRel =
      PatternRelationship(
        v"r",
        (v"start", v"end"),
        SemanticDirection.OUTGOING,
        Seq(relTypeName("R")),
        VarPatternLength(0, Some(3))
      )
    val spp =
      SelectivePathPattern(
        pathPattern = ExhaustivePathPattern.NodeConnections(NonEmptyList(varRel)),
        selections = Selections.empty,
        selector = SelectivePathPattern.Selector.ShortestGroups(1)
      )

    val expectedNfa = new TestNFABuilder(0, "start")
      .addTransition(0, 1, "(start) (`  UNNAMED1`)")
      .addTransition(1, 2, "(`  UNNAMED1`)-[`  r@0`:R]->(`  UNNAMED2`)")
      .addTransition(2, 3, "(`  UNNAMED2`)-[`  r@0`:R]->(`  UNNAMED3`)")
      .addTransition(3, 4, "(`  UNNAMED3`)-[`  r@0`:R]->(`  UNNAMED4`)")
      .addTransition(1, 5, "(`  UNNAMED1`) (end)")
      .addTransition(2, 5, "(`  UNNAMED2`) (end)")
      .addTransition(3, 5, "(`  UNNAMED3`) (end)")
      .addTransition(4, 5, "(`  UNNAMED4`) (end)")
      .addFinalState(5)
      .build()

    ConvertToNFA.convertToNfa(
      spp,
      fromLeft = true,
      Set.empty,
      Seq.empty,
      new AnonymousVariableNameGenerator
    ) should equal((expectedNfa, Selections.empty, Map("r" -> "  r@0")))
  }

  // with predicates
  test("(start:Label)-[r:R|T*2..3 {prop: 2}]->(end {prop: 42})") {
    val varRel =
      PatternRelationship(
        v"r",
        (v"start", v"end"),
        SemanticDirection.OUTGOING,
        Seq(relTypeName("R"), relTypeName("T")),
        VarPatternLength(0, Some(3))
      )
    val relationshipPredicate =
      allInList(varFor("r_inner"), varFor("r"), in(prop("r_inner", "prop"), listOf(literalInt(2))))
    val startNodePredicate = hasLabels("start", "Label")
    val endNodePredicate = in(prop("end", "prop"), listOf(literalInt(42)))
    val spp =
      SelectivePathPattern(
        pathPattern = ExhaustivePathPattern.NodeConnections(NonEmptyList(varRel)),
        selections = Selections.from(Seq(
          startNodePredicate,
          relationshipPredicate,
          endNodePredicate
        )),
        selector = SelectivePathPattern.Selector.ShortestGroups(1)
      )

    val expectedNfa = new TestNFABuilder(0, "start")
      .addTransition(0, 1, "(start) (`  UNNAMED1`)")
      .addTransition(1, 2, "(`  UNNAMED1`)-[`  r@0`:R|T]->(`  UNNAMED2`)")
      .addTransition(2, 3, "(`  UNNAMED2`)-[`  r@0`:R|T]->(`  UNNAMED3`)")
      .addTransition(3, 4, "(`  UNNAMED3`)-[`  r@0`:R|T]->(`  UNNAMED4`)")
      .addTransition(1, 5, "(`  UNNAMED1`) (end WHERE end.prop IN [42])")
      .addTransition(2, 5, "(`  UNNAMED2`) (end WHERE end.prop IN [42])")
      .addTransition(3, 5, "(`  UNNAMED3`) (end WHERE end.prop IN [42])")
      .addTransition(4, 5, "(`  UNNAMED4`) (end WHERE end.prop IN [42])")
      .addFinalState(5)
      .build()

    ConvertToNFA.convertToNfa(
      spp,
      fromLeft = true,
      Set.empty,
      Seq.empty,
      new AnonymousVariableNameGenerator
    ) should equal((
      expectedNfa,
      Selections.from(Seq(startNodePredicate, relationshipPredicate)),
      Map("r" -> "  r@0")
    ))
  }
}
