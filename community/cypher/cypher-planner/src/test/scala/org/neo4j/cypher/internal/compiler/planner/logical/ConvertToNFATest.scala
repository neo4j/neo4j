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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
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
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.Repetition
import org.neo4j.cypher.internal.util.UpperBound
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.exceptions.InternalException

class ConvertToNFATest extends CypherFunSuite with AstConstructionTestSupport {

  private val `(start) ((a)-[r]->(b))+ (end)` = QuantifiedPathPattern(
    leftBinding = NodeBinding("a", "start"),
    rightBinding = NodeBinding("b", "end"),
    patternRelationships = List(PatternRelationship(
      name = "r",
      boundaryNodes = ("a", "b"),
      dir = SemanticDirection.OUTGOING,
      types = Nil,
      length = SimplePatternLength
    )),
    patternNodes = Set("a", "b"),
    argumentIds = Set.empty,
    selections = Selections.empty,
    repetition = Repetition(1, UpperBound.Unlimited),
    nodeVariableGroupings = Set("a", "b").map(name => VariableGrouping(name, name)),
    relationshipVariableGroupings = Set(VariableGrouping("r", "r"))
  )

  // QPP with internal predicates
  private val `(start) ((a)-[r]->(b))+ (end) [with predicates]` =
    QuantifiedPathPattern(
      leftBinding = NodeBinding("a", "start"),
      rightBinding = NodeBinding("b", "end"),
      patternRelationships = List(PatternRelationship(
        name = "r",
        boundaryNodes = ("a", "b"),
        dir = SemanticDirection.OUTGOING,
        types = Seq(relTypeName("T")),
        length = SimplePatternLength
      )),
      patternNodes = Set("a", "b"),
      argumentIds = Set.empty,
      selections = Selections.from(Seq(
        hasLabels("a", "A"),
        hasLabels("b", "B"),
        equals(prop("a", "prop"), literalInt(1)),
        equals(prop("b", "prop"), literalInt(2))
      )),
      repetition = Repetition(1, UpperBound.Unlimited),
      nodeVariableGroupings = Set("a", "b").map(name => VariableGrouping(name, name)),
      relationshipVariableGroupings = Set(VariableGrouping("r", "r"))
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

    ConvertToNFA.convertToNfa(spp, fromLeft = true, Set.empty, Seq.empty) should equal((expectedNfa, Selections.empty))
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

    ConvertToNFA.convertToNfa(spp, fromLeft = true, Set.empty, Seq.empty) should equal((
      expectedNfa,
      Selections.from(nonInlineablePredicate)
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

    ConvertToNFA.convertToNfa(spp, fromLeft = true, Set.empty, Seq.empty) should equal((
      expectedNfa,
      Selections.from(nonInlineablePredicate)
    ))
  }

  test("does not support predicates that depends on multiple singleton variables in QPP") {
    val failingPredicate = equals(prop("a", "prop"), prop("b", "prop"))

    val qpp = `(start) ((a)-[r]->(b))+ (end)`.copy(selections = Selections.from(failingPredicate))

    val spp =
      SelectivePathPattern(
        pathPattern = ExhaustivePathPattern.NodeConnections(NonEmptyList(qpp)),
        selections = Selections.empty,
        selector = SelectivePathPattern.Selector.ShortestGroups(1)
      )

    an[InternalException] should be thrownBy {
      ConvertToNFA.convertToNfa(spp, fromLeft = true, Set.empty, Seq.empty)
    }
  }

  test("does not support legacy var-length relationships") {
    val spp =
      SelectivePathPattern(
        pathPattern = ExhaustivePathPattern.NodeConnections(NonEmptyList(
          PatternRelationship(
            "r",
            ("a", "b"),
            SemanticDirection.OUTGOING,
            Seq.empty,
            VarPatternLength(1, None)
          )
        )),
        selections = Selections.empty,
        selector = SelectivePathPattern.Selector.ShortestGroups(1)
      )

    an[InternalException] should be thrownBy {
      ConvertToNFA.convertToNfa(spp, fromLeft = true, Set.empty, Seq.empty)
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

    ConvertToNFA.convertToNfa(spp, fromLeft = false, Set.empty, Seq.empty) should equal((expectedNfa, Selections.empty))
  }

  test("create NFA reversed for (start) ((a)-[r]->(b)-[s]-(c))* (d)<-[t]-(end)") {
    val qpp = QuantifiedPathPattern(
      leftBinding = NodeBinding("a", "start"),
      rightBinding = NodeBinding("c", "d"),
      patternRelationships = List(
        PatternRelationship(
          name = "r",
          boundaryNodes = ("a", "b"),
          dir = SemanticDirection.OUTGOING,
          types = Nil,
          length = SimplePatternLength
        ),
        PatternRelationship(
          name = "s",
          boundaryNodes = ("b", "c"),
          dir = SemanticDirection.BOTH,
          types = Nil,
          length = SimplePatternLength
        )
      ),
      patternNodes = Set("a", "b", "c"),
      argumentIds = Set.empty,
      selections = Selections.from(differentRelationships(varFor("r"), varFor("s"))),
      repetition = Repetition(0, UpperBound.Unlimited),
      nodeVariableGroupings = Set("a", "b", "c").map(name => VariableGrouping(name, name)),
      relationshipVariableGroupings = Set("r", "s").map(name => VariableGrouping(name, name))
    )
    val rel = PatternRelationship(
      name = "t",
      boundaryNodes = ("d", "end"),
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

    ConvertToNFA.convertToNfa(spp, fromLeft = false, Set.empty, Seq.empty) should equal((expectedNfa, Selections.empty))
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

    ConvertToNFA.convertToNfa(spp, fromLeft = true, Set.empty, Seq(hasLabels("end", "E"))) should equal((
      expectedNfa,
      Selections.empty
    ))
  }

  test("create NFA with predicates that depend on interior and other available symbol") {
    val qpp = QuantifiedPathPattern(
      leftBinding = NodeBinding("a", "start"),
      rightBinding = NodeBinding("b", "c"),
      patternRelationships = List(PatternRelationship(
        name = "r",
        boundaryNodes = ("a", "b"),
        dir = SemanticDirection.OUTGOING,
        types = Seq.empty,
        length = SimplePatternLength
      )),
      patternNodes = Set("a", "b"),
      argumentIds = Set.empty,
      selections = Selections.from(Seq(
        equals(prop("a", "prop"), varFor("foo")),
        equals(prop("r", "prop"), varFor("foo")),
        equals(prop("b", "prop"), varFor("foo"))
      )),
      repetition = Repetition(1, UpperBound.Unlimited),
      nodeVariableGroupings = Set("a", "b").map(name => VariableGrouping(name, name)),
      relationshipVariableGroupings = Set(VariableGrouping("r", "r"))
    )
    val rel = PatternRelationship(
      name = "r2",
      boundaryNodes = ("c", "end"),
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
      Seq(equals(prop("end", "prop"), varFor("foo")))
    ) should equal((
      expectedNfa,
      Selections.empty
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

    ConvertToNFA.convertToNfa(spp, fromLeft = true, Set.empty, Seq.empty) should equal((expectedNfa, Selections.empty))
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

    ConvertToNFA.convertToNfa(spp, fromLeft = true, Set.empty, Seq.empty) should equal((expectedNfa, Selections.empty))
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

    ConvertToNFA.convertToNfa(spp, fromLeft = true, Set.empty, Seq.empty) should equal((expectedNfa, Selections.empty))
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

    ConvertToNFA.convertToNfa(spp, fromLeft = true, Set.empty, Seq.empty) should equal((expectedNfa, Selections.empty))
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

    ConvertToNFA.convertToNfa(spp, fromLeft = true, Set.empty, Seq(hasLabels("end", "E"))) should equal((
      expectedNfa,
      Selections.empty
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

    ConvertToNFA.convertToNfa(spp, fromLeft = true, Set.empty, Seq.empty) should equal((expectedNfa, Selections.empty))
  }
}
