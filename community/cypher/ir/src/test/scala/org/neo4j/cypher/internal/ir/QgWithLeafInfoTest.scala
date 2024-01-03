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
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.ir.QgWithLeafInfo.StableIdentifier
import org.neo4j.cypher.internal.ir.QgWithLeafInfo.UnstableIdentifier
import org.neo4j.cypher.internal.ir.ast.ExistsIRExpression
import org.neo4j.cypher.internal.ir.ast.ListIRExpression
import org.neo4j.cypher.internal.ir.helpers.ExpressionConverters.PredicateConverter
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class QgWithLeafInfoTest extends CypherFunSuite with AstConstructionTestSupport {

  private val semanticTable = SemanticTable()

  test("unstablePatternNodes includes only nodes excluding stable identifier") {
    val r = PatternRelationship(v"r", (v"a", v"b"), OUTGOING, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(
      patternNodes = Set(v"a", v"b", v"c"),
      patternRelationships = Set(r)
    )
    val qgWithLeafInfo =
      QgWithLeafInfo(qg, Set.empty, Set.empty, Some(StableIdentifier(v"b")), isTerminatingProjection = false)
    qgWithLeafInfo.unstablePatternNodes should equal(Set(v"a", v"c"))
  }

  test("unstablePatternNodes includes optional match nodes") {
    val r = PatternRelationship(v"r", (v"a", v"b"), OUTGOING, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(
      patternNodes = Set(v"a"),
      patternRelationships = Set(r),
      optionalMatches = IndexedSeq(QueryGraph(patternNodes = Set(v"b")))
    )
    val qgWithLeafInfo = QgWithLeafInfo(qg, Set.empty, Set.empty, None, isTerminatingProjection = false)
    qgWithLeafInfo.unstablePatternNodes should equal(Set(v"a", v"b"))
  }

  test("unstablePatternRelationships excludes stable identifier") {
    val r = PatternRelationship(v"r", (v"a", v"b"), OUTGOING, Seq.empty, SimplePatternLength)
    val r2 = PatternRelationship(v"r2", (v"b", v"c"), OUTGOING, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(
      patternNodes = Set(v"a", v"b", v"c"),
      patternRelationships = Set(r, r2)
    )
    val qgWithLeafInfo =
      QgWithLeafInfo(qg, Set.empty, Set.empty, Some(StableIdentifier(v"r")), isTerminatingProjection = false)
    qgWithLeafInfo.unstablePatternRelationships should equal(Set(r2))
  }

  test("unstablePatternRelationships includes optional match relationships") {
    val r = PatternRelationship(v"r", (v"a", v"b"), OUTGOING, Seq.empty, SimplePatternLength)
    val r2 = PatternRelationship(v"r2", (v"b", v"c"), OUTGOING, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(
      patternNodes = Set(v"a", v"b", v"c"),
      patternRelationships = Set(r),
      optionalMatches = IndexedSeq(QueryGraph(patternRelationships = Set(r2)))
    )
    val qgWithLeafInfo = QgWithLeafInfo(qg, Set.empty, Set.empty, None, isTerminatingProjection = false)
    qgWithLeafInfo.unstablePatternRelationships should equal(Set(r, r2))
  }

  test("unstablePatternRelationships includes shortest path relationships") {
    val r = PatternRelationship(v"r", (v"a", v"b"), OUTGOING, Seq.empty, SimplePatternLength)
    val spp = ShortestRelationshipPattern(None, r, single = true)(null)

    val qg = QueryGraph(
      patternNodes = Set(v"a", v"b", v"c"),
      shortestRelationshipPatterns = Set(spp)
    )
    val qgWithLeafInfo = QgWithLeafInfo(qg, Set.empty, Set.empty, None, isTerminatingProjection = false)
    qgWithLeafInfo.unstablePatternRelationships should equal(Set(r))
  }

  test("patternNodes includes unstable and stable identifiers") {
    val a = UnstableIdentifier(v"a")
    val b = StableIdentifier(v"b")
    val c = UnstableIdentifier(v"c")
    val qg = QueryGraph(patternNodes = Set(v"a", v"b", v"c"))
    val qgWithLeafInfo = QgWithLeafInfo(qg, Set.empty, Set.empty, Some(b), isTerminatingProjection = false)
    qgWithLeafInfo.patternNodes should equal(Set(a, b, c))
  }

  test("patternNodes should not include stable relationship identifiers") {
    val a = UnstableIdentifier(v"a")
    val b = UnstableIdentifier(v"b")
    val r = PatternRelationship(v"r", (v"a", v"b"), OUTGOING, Seq.empty, SimplePatternLength)
    val rIdent = StableIdentifier(v"r")
    val qg = QueryGraph(
      patternNodes = Set(v"a", v"b"),
      patternRelationships = Set(r)
    )
    val qgWithLeafInfo = QgWithLeafInfo(qg, Set.empty, Set.empty, Some(rIdent), isTerminatingProjection = false)

    qgWithLeafInfo.patternNodes should equal(Set(a, b))
  }

  test("leafPatternNodes should include only leaves (unstable and stable)") {
    val a = UnstableIdentifier(v"a")
    val b = StableIdentifier(v"b")
    val r = PatternRelationship(v"r", (v"a", v"b"), OUTGOING, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(
      patternNodes = Set(v"a", v"b", v"c"),
      patternRelationships = Set(r)
    )
    val qgWithLeafInfo = QgWithLeafInfo(qg, Set.empty, Set(v"a"), Some(b), isTerminatingProjection = false)

    qgWithLeafInfo.leafPatternNodes should equal(Set(a, b))
  }

  test("nonArgumentPatternNodes should exclude arguments") {
    val a = UnstableIdentifier(v"a")
    val b = StableIdentifier(v"b")
    val r = PatternRelationship(v"r", (v"a", v"b"), OUTGOING, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(
      argumentIds = Set(v"b"),
      patternNodes = Set(v"a", v"b", v"c"),
      patternRelationships = Set(r)
    )
    val qgWithLeafInfo = QgWithLeafInfo(qg, Set.empty, Set(v"a"), Some(b), isTerminatingProjection = false)

    qgWithLeafInfo.leafPatternNodes should equal(Set(a, b))
  }

  test("patternRelationships should not include stable node identifiers") {
    val b = StableIdentifier(v"b")
    val r = PatternRelationship(v"r", (v"a", v"b"), OUTGOING, Seq.empty, SimplePatternLength)
    val r2 = PatternRelationship(v"r2", (v"b", v"c"), OUTGOING, Seq.empty, SimplePatternLength)
    val rIdent = UnstableIdentifier(v"r")
    val r2Ident = UnstableIdentifier(v"r2")

    val qg = QueryGraph(
      patternNodes = Set(v"a", v"b", v"c"),
      patternRelationships = Set(r, r2)
    )
    val qgWithLeafInfo = QgWithLeafInfo(qg, Set.empty, Set.empty, Some(b), isTerminatingProjection = false)
    qgWithLeafInfo.patternRelationships should equal(Set(rIdent, r2Ident))
  }

  test("patternRelationships includes unstable and stable identifiers") {
    val r = PatternRelationship(v"r", (v"a", v"b"), OUTGOING, Seq.empty, SimplePatternLength)
    val r2 = PatternRelationship(v"r2", (v"b", v"c"), OUTGOING, Seq.empty, SimplePatternLength)
    val rIdent = StableIdentifier(v"r")
    val r2Ident = UnstableIdentifier(v"r2")

    val qg = QueryGraph(
      patternNodes = Set(v"a", v"b"),
      patternRelationships = Set(r, r2)
    )
    val qgWithLeafInfo = QgWithLeafInfo(qg, Set.empty, Set.empty, Some(rIdent), isTerminatingProjection = false)

    qgWithLeafInfo.patternRelationships should equal(Set(rIdent, r2Ident))
  }

  test("allKnownUnstableNodeLabelsFor should include all labels for unstable identifier") {
    val a = UnstableIdentifier(v"a")
    val r = PatternRelationship(v"r", (v"a", v"b"), OUTGOING, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(
      patternNodes = Set(v"a", v"b", v"c"),
      patternRelationships = Set(r),
      selections = Selections.from(Seq(hasLabels("a", "A"), or(hasLabels("a", "A2"), hasLabels("a", "A3"))))
    )
    val qgWithLeafInfo =
      QgWithLeafInfo(qg, Set.empty, Set.empty, Some(StableIdentifier(v"b")), isTerminatingProjection = false)

    qgWithLeafInfo.allKnownUnstableNodeLabelsFor(a) should equal(Set(labelName("A"), labelName("A2"), labelName("A3")))
  }

  test("allKnownUnstableNodeLabelsFor should include labels from HasLabelsOrTypes") {
    val a = UnstableIdentifier(v"a")
    val r = PatternRelationship(v"r", (v"a", v"b"), OUTGOING, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(
      patternNodes = Set(v"a", v"b", v"c"),
      patternRelationships = Set(r),
      selections = Selections.from(Seq(hasLabelsOrTypes("a", "A")))
    )
    val qgWithLeafInfo =
      QgWithLeafInfo(qg, Set.empty, Set.empty, Some(StableIdentifier(v"b")), isTerminatingProjection = false)

    qgWithLeafInfo.allKnownUnstableNodeLabelsFor(a) should equal(Set(labelName("A")))
  }

  test("allKnownUnstableNodeLabelsFor should include all labels from optional matches") {
    val a = UnstableIdentifier(v"a")
    val r = PatternRelationship(v"r", (v"a", v"b"), OUTGOING, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(
      patternNodes = Set(v"a", v"b", v"c"),
      patternRelationships = Set(r),
      optionalMatches = IndexedSeq(QueryGraph(
        patternNodes = Set(v"a"),
        selections = Selections.from(hasLabels("a", "A"))
      ))
    )
    val qgWithLeafInfo =
      QgWithLeafInfo(qg, Set.empty, Set.empty, Some(StableIdentifier(v"b")), isTerminatingProjection = false)

    qgWithLeafInfo.allKnownUnstableNodeLabelsFor(a) should equal(Set(labelName("A")))
  }

  test("allKnownUnstableNodeLabelsFor should not include any solved labels for stable identifier") {
    val b = StableIdentifier(v"b")
    val r = PatternRelationship(v"r", (v"a", v"b"), OUTGOING, Seq.empty, SimplePatternLength)
    val solvedExpression = hasLabels("b", "B2")
    val qg = QueryGraph(
      patternNodes = Set(v"a", v"b", v"c"),
      patternRelationships = Set(r),
      selections = Selections.from(Seq(hasLabels("b", "B"), solvedExpression))
    )
    val qgWithLeafInfo =
      QgWithLeafInfo(qg, solvedExpression.asPredicates, Set.empty, Some(b), isTerminatingProjection = false)

    qgWithLeafInfo.allKnownUnstableNodeLabelsFor(b) should equal(Set(labelName("B")))
  }

  test("allPossibleUnstableRelTypesFor should include all inlined rel types for unstable identifier") {
    val r =
      PatternRelationship(v"r", (v"a", v"b"), OUTGOING, Seq(relTypeName("R"), relTypeName("Q")), SimplePatternLength)
    val rIdent = UnstableIdentifier(v"r")

    val qg = QueryGraph(
      patternNodes = Set(v"a", v"b", v"c"),
      patternRelationships = Set(r)
    )
    val qgWithLeafInfo =
      QgWithLeafInfo(qg, Set.empty, Set.empty, Some(StableIdentifier(v"b")), isTerminatingProjection = false)

    qgWithLeafInfo.allPossibleUnstableRelTypesFor(rIdent) should equal(Set(relTypeName("R"), relTypeName("Q")))
  }

  test("allPossibleUnstableRelTypesFor should include all rel types from selections for unstable identifier") {
    val r = PatternRelationship(v"r", (v"a", v"b"), OUTGOING, Seq.empty, SimplePatternLength)
    val rIdent = UnstableIdentifier(v"r")

    val qg = QueryGraph(
      patternNodes = Set(v"a", v"b", v"c"),
      patternRelationships = Set(r),
      selections = Selections.from(Seq(hasTypes("r", "R"), or(hasTypes("r", "R2"), hasTypes("r", "R3"))))
    )
    val qgWithLeafInfo =
      QgWithLeafInfo(qg, Set.empty, Set.empty, Some(StableIdentifier(v"b")), isTerminatingProjection = false)

    qgWithLeafInfo.allPossibleUnstableRelTypesFor(rIdent) should equal(Set(
      relTypeName("R"),
      relTypeName("R2"),
      relTypeName("R3")
    ))
  }

  test(
    "allPossibleUnstableRelTypesFor should include all rel types from HasLabelsOrTypes selections for unstable identifier"
  ) {
    val r = PatternRelationship(v"r", (v"a", v"b"), OUTGOING, Seq.empty, SimplePatternLength)
    val rIdent = UnstableIdentifier(v"r")

    val qg = QueryGraph(
      patternNodes = Set(v"a", v"b", v"c"),
      patternRelationships = Set(r),
      selections =
        Selections.from(Seq(hasTypes("r", "R"), or(hasLabelsOrTypes("r", "R2"), hasLabelsOrTypes("r", "R3"))))
    )
    val qgWithLeafInfo =
      QgWithLeafInfo(qg, Set.empty, Set.empty, Some(StableIdentifier(v"b")), isTerminatingProjection = false)

    qgWithLeafInfo.allPossibleUnstableRelTypesFor(rIdent) should equal(Set(
      relTypeName("R"),
      relTypeName("R2"),
      relTypeName("R3")
    ))
  }

  test("allPossibleUnstableRelTypesFor should include all types from optional matches") {
    val r = PatternRelationship(v"r", (v"a", v"b"), OUTGOING, Seq.empty, SimplePatternLength)
    val rIdent = UnstableIdentifier(v"r")
    val qg = QueryGraph(
      patternNodes = Set(v"a", v"b", v"c"),
      patternRelationships = Set(r),
      optionalMatches = IndexedSeq(QueryGraph(
        patternNodes = Set(v"a"),
        selections = Selections.from(hasTypes("r", "R"))
      ))
    )
    val qgWithLeafInfo =
      QgWithLeafInfo(qg, Set.empty, Set.empty, Some(StableIdentifier(v"b")), isTerminatingProjection = false)

    qgWithLeafInfo.allPossibleUnstableRelTypesFor(rIdent) should equal(Set(relTypeName("R")))
  }

  test("allPossibleUnstableRelTypesFor should not include any solved types for stable identifier") {
    val r = PatternRelationship(v"r", (v"a", v"b"), OUTGOING, Seq(relTypeName("R3")), SimplePatternLength)
    val rIdent = StableIdentifier(v"r")
    val solvedExpression = hasTypes("r", "R")
    val qg = QueryGraph(
      patternNodes = Set(v"a", v"b", v"c"),
      patternRelationships = Set(r),
      selections = Selections.from(Seq(hasTypes("r", "R2"), solvedExpression))
    )
    val qgWithLeafInfo =
      QgWithLeafInfo(qg, solvedExpression.asPredicates, Set.empty, Some(rIdent), isTerminatingProjection = false)

    qgWithLeafInfo.allPossibleUnstableRelTypesFor(rIdent) should equal(Set(relTypeName("R2"), relTypeName("R3")))
  }

  test("allKnownUnstablePropertiesFor should include all properties for unstable identifier") {
    val a = UnstableIdentifier(v"a")
    val r = PatternRelationship(v"r", (v"a", v"b"), OUTGOING, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(
      patternNodes = Set(v"a", v"b", v"c"),
      patternRelationships = Set(r),
      selections = Selections.from(equals(prop("a", "prop"), literalInt(5)))
    )
    val qgWithLeafInfo =
      QgWithLeafInfo(qg, Set.empty, Set.empty, Some(StableIdentifier(v"b")), isTerminatingProjection = false)

    qgWithLeafInfo.allKnownUnstablePropertiesFor(a) should equal(Set(propName("prop")))
  }

  test("allKnownUnstablePropertiesFor should include all properties from optional matches") {
    val a = UnstableIdentifier(v"a")
    val r = PatternRelationship(v"r", (v"a", v"b"), OUTGOING, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(
      patternNodes = Set(v"a", v"b", v"c"),
      patternRelationships = Set(r),
      optionalMatches = IndexedSeq(QueryGraph(
        patternNodes = Set(v"a"),
        selections = Selections.from(equals(prop("a", "prop"), literalInt(5)))
      ))
    )
    val qgWithLeafInfo =
      QgWithLeafInfo(qg, Set.empty, Set.empty, Some(StableIdentifier(v"b")), isTerminatingProjection = false)

    qgWithLeafInfo.allKnownUnstablePropertiesFor(a) should equal(Set(propName("prop")))
  }

  test("allKnownUnstablePropertiesFor should not include any solved labels for stable identifier") {
    val b = StableIdentifier(v"b")
    val r = PatternRelationship(v"r", (v"a", v"b"), OUTGOING, Seq.empty, SimplePatternLength)
    val solvedExpression = equals(prop("b", "prop"), literalInt(5))
    val qg = QueryGraph(
      patternNodes = Set(v"a", v"b", v"c"),
      patternRelationships = Set(r),
      selections = Selections.from(Seq(solvedExpression, equals(prop("b", "prop2"), literalInt(5))))
    )
    val qgWithLeafInfo =
      QgWithLeafInfo(qg, solvedExpression.asPredicates, Set.empty, Some(b), isTerminatingProjection = false)

    qgWithLeafInfo.allKnownUnstablePropertiesFor(b) should equal(Set(propName("prop2")))
  }

  test("allKnownUnstableNodeLabels includes arguments not proven to be nodes") {
    val a = StableIdentifier(v"a")
    val qg = QueryGraph(
      patternNodes = Set(v"a"),
      argumentIds = Set(v"b"),
      selections = Selections.from(Seq(hasLabelsOrTypes("b", "B")))
    )
    val qgWithLeafInfo = QgWithLeafInfo(qg, Set.empty, Set.empty, Some(a), isTerminatingProjection = false)

    qgWithLeafInfo.allKnownUnstableNodeLabels(semanticTable) should equal(Set(labelName("B")))
  }

  test("allKnownUnstableNodeProperties includes arguments not proven to be nodes") {
    val a = StableIdentifier(v"a")
    val qg = QueryGraph(
      patternNodes = Set(v"a"),
      argumentIds = Set(v"b"),
      selections = Selections.from(Seq(propEquality("b", "prop", 5)))
    )
    val qgWithLeafInfo = QgWithLeafInfo(qg, Set.empty, Set.empty, Some(a), isTerminatingProjection = false)

    qgWithLeafInfo.allKnownUnstableNodeProperties(semanticTable) should equal(Set(propName("prop")))
  }

  test("allKnownUnstableNodeProperties includes ir expression property key names") {
    val a = StableIdentifier(v"a")
    val irExp = ExistsIRExpression(
      RegularSinglePlannerQuery(
        horizon = RegularQueryProjection(Map(
          v"x" -> MapExpression(Seq(propName("prop") -> literalInt(5)))(pos)
        ))
      ),
      varFor(""),
      ""
    )(pos, Some(Set.empty), Some(Set.empty))

    val qg = QueryGraph(
      patternNodes = Set(v"a"),
      argumentIds = Set(v"b"),
      selections = Selections.from(irExp)
    )
    val qgWithLeafInfo = QgWithLeafInfo(qg, Set.empty, Set.empty, Some(a), isTerminatingProjection = false)

    qgWithLeafInfo.allKnownUnstableNodeProperties(semanticTable) should equal(Set(propName("prop")))
  }

  test("allKnownUnstableRelProperties includes arguments not proven to be relationships") {
    val a = StableIdentifier(v"a")
    val qg = QueryGraph(
      patternNodes = Set(v"a"),
      argumentIds = Set(v"b"),
      selections = Selections.from(Seq(propEquality("b", "prop", 5)))
    )
    val qgWithLeafInfo = QgWithLeafInfo(qg, Set.empty, Set.empty, Some(a), isTerminatingProjection = false)

    qgWithLeafInfo.allKnownUnstableRelProperties(semanticTable) should equal(Set(propName("prop")))
  }

  test("allKnownUnstableRelProperties includes ir expression property key names") {
    val a = StableIdentifier(v"a")
    val irExp = ListIRExpression(
      RegularSinglePlannerQuery(
        horizon = RegularQueryProjection(Map(
          v"x" -> MapExpression(Seq(propName("prop") -> literalInt(5)))(pos)
        ))
      ),
      varFor(""),
      varFor(""),
      ""
    )(pos, Some(Set.empty), Some(Set.empty))

    val qg = QueryGraph(
      patternNodes = Set(v"a"),
      argumentIds = Set(v"b"),
      selections = Selections.from(irExp)
    )
    val qgWithLeafInfo = QgWithLeafInfo(qg, Set.empty, Set.empty, Some(a), isTerminatingProjection = false)

    qgWithLeafInfo.allKnownUnstableRelProperties(semanticTable) should equal(Set(propName("prop")))
  }
}
