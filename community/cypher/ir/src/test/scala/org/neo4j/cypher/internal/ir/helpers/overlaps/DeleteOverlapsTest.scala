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
package org.neo4j.cypher.internal.ir.helpers.overlaps

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.RELATIONSHIP_TYPE
import org.neo4j.cypher.internal.label_expressions.NodeLabels
import org.neo4j.cypher.internal.label_expressions.NodeLabels.KnownLabels
import org.neo4j.cypher.internal.label_expressions.NodeLabels.SomeUnknownLabels
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.Assertion

class DeleteOverlapsTest extends CypherFunSuite with AstConstructionTestSupport {

  test("MATCH () DELETE () overlaps") {
    expectOverlapOnDelete(Nil, Nil, KnownLabels(Set.empty))
  }

  test("MATCH (m:A) DELETE () overlaps") {
    expectOverlapOnDelete(List(hasLabels("m", "A")), Nil, knownLabels("A"))
  }

  test("MATCH (m:A) DELETE (n:A) overlaps") {
    expectOverlapOnDelete(List(hasLabels("m", "A")), List(hasLabels("n", "A")), knownLabels("A"))
  }

  test("MATCH (m:A) DELETE (n:B) overlaps") {
    expectOverlapOnDelete(List(hasLabels("m", "A")), List(hasLabels("n", "B")), knownLabels("A", "B"))
  }

  test("MATCH (m:A&B&C) DELETE (n:A&B) overlaps") {
    val read = List(hasLabels("m", "A"), hasLabels("m", "B"), hasLabels("m", "C"))
    val delete = List(hasLabels("n", "A"), hasLabels("n", "B"))
    val expected = knownLabels("A", "B", "C")
    expectOverlapOnDelete(read, delete, expected)
  }

  test("MATCH (m:A|B|C) DELETE (n:A|B) overlaps") {
    val read = List(ors(hasLabels("m", "A"), hasLabels("m", "B"), hasLabels("m", "C")))
    val delete = List(ors(hasLabels("n", "A"), hasLabels("n", "B")))
    val expected = knownLabels("A")
    expectOverlapOnDelete(read, delete, expected)
  }

  test("MATCH (m:A) DELETE (n:!A) does not overlap") {
    val read = List(hasLabels("m", "A"))
    val delete = List(not(hasLabels("n", "A")))
    expectNoOverlapOnDelete(read, delete)
  }

  test("MATCH (m) DELETE (n:!!A) overlaps") {
    val delete = List(not(not(hasLabels("n", "A"))))
    expectOverlapOnDelete(Nil, delete, knownLabels("A"))
  }

  test("MATCH (m:A&B&C) DELETE (n:!A) does not overlap") {
    val read = List(hasLabels("m", "A"), hasLabels("m", "B"), hasLabels("m", "C"))
    val delete = List(not(hasLabels("n", "A")))
    expectNoOverlapOnDelete(read, delete)
  }

  test("MATCH (m:A|B|C) DELETE (n:!A) overlaps") {
    val read = List(ors(hasLabels("m", "A"), hasLabels("m", "B"), hasLabels("m", "C")))
    val delete = List(not(hasLabels("n", "A")))
    val expected = knownLabels("B")
    expectOverlapOnDelete(read, delete, expected)
  }

  test("MATCH (m:A&(B|C)) DELETE (n:!(A|B)) does not overlap") {
    val read = List(hasLabels("m", "A"), ors(hasLabels("m", "B"), hasLabels("m", "C")))
    val delete = List(not(or(hasLabels("n", "A"), hasLabels("n", "B"))))
    expectNoOverlapOnDelete(read, delete)
  }

  test("MATCH (m:%) DELETE (n) overlaps") {
    val read = List(hasALabel("m"))
    expectOverlapOnDelete(read, Nil, SomeUnknownLabels)
  }

  test("MATCH (m:%) DELETE (n:%) overlaps") {
    val read = List(hasALabel("m"))
    val delete = List(hasALabel("n"))
    expectOverlapOnDelete(read, delete, SomeUnknownLabels)
  }

  test("MATCH (m) DELETE (n:!%) does overlap") {
    val delete = List(not(hasALabel("n")))
    expectOverlapOnDelete(Nil, delete, KnownLabels(Set.empty))
  }

  test("MATCH (m:A) DELETE (n:!%) does not overlap") {
    val read = List(hasLabels("m", "A"))
    val delete = List(not(hasALabel("n")))
    expectNoOverlapOnDelete(read, delete)
  }

  test("MATCH (m:(A&B)&!(B&C)) DELETE (n:!(A&%)&%)) does not overlap") {
    val read = List(
      and(hasLabels("m", "A"), hasLabels("m", "B")),
      not(and(hasLabels("m", "B"), hasLabels("m", "C")))
    )
    val delete = List(
      not(and(hasLabels("n", "A"), hasALabel("n"))),
      hasALabel("n")
    )
    expectNoOverlapOnDelete(read, delete)
  }

  test("overlapOnDelete ignores non-label predicates") {
    val read = List(InfinityLiteral)
    val delete = List(falseLiteral)
    expectOverlapOnDelete(read, delete, KnownLabels(Set.empty), read ++ delete)
  }

  test("overlapOnDelete ignores non-label predicates, including properties") {
    val read = List(in(prop("m", "property"), literalInt(10)))
    val delete = List(in(prop("n", "property"), literalInt(42)))
    // property predicates get ignored and returned as unprocessed
    val unprocessedExpressions = read ++ delete
    // there are no other predicates here, and so it is equivalent to calculating the overlap between () and ()
    expectOverlapOnDelete(read, delete, KnownLabels(Set.empty), unprocessedExpressions)
  }

  test("overlapOnDelete only processes label predicates – even when other predicates try to sneak in") {
    val read = List(not(and(
      or(hasALabel("m"), InfinityLiteral),
      or(
        hasLabels("m", "A"),
        // here we try to sneak a property predicate inside our label expression, and so the whole label expression must be ignored
        in(prop("m", "property"), literalInt(42))
      )
    )))
    val delete = List(hasLabels("n", "A"))

    expectOverlapOnDelete(read, delete, knownLabels("A"), read)
  }

  test("overlapOnDelete will process all label expressions in the top level conjunction, ignoring the rest") {
    val labelExpressionOnRead = ors(hasLabels("m", "A"), hasLabels("m", "B"), hasLabels("m", "C"))
    val otherPredicatesOnRead = List(InfinityLiteral, in(prop("m", "property"), literalInt(42)))
    val read = labelExpressionOnRead :: otherPredicatesOnRead

    val labelExpressionOnDelete = not(hasLabels("n", "A"))
    val propertyPredicateOnDelete = in(prop("n", "property"), literalInt(0))
    val otherPredicateOnDelete = falseLiteral
    // this internally gets flattened into: List(propertyPredicateOnDelete, labelExpressionOnDelete, otherPredicateOnDelete)
    val delete = List(ands(propertyPredicateOnDelete, and(labelExpressionOnDelete, otherPredicateOnDelete)))

    val unprocessedExpressions = otherPredicatesOnRead ++ List(propertyPredicateOnDelete, otherPredicateOnDelete)
    val expected = knownLabels("B")

    expectOverlapOnDelete(read, delete, expected, unprocessedExpressions)
  }

  def expectNoOverlapOnDelete(
    predicatesOnRead: Seq[Expression],
    predicatesOnDelete: Seq[Expression]
  ): Assertion = {
    val result = DeleteOverlaps.overlapNode(predicatesOnRead, predicatesOnDelete)
    val expected = DeleteOverlaps.NoLabelOverlap
    result shouldEqual expected
  }

  def expectOverlapOnDelete(
    predicatesOnRead: Seq[Expression],
    predicatesOnDelete: Seq[Expression],
    expectedLabelsOverlap: NodeLabels,
    expectedUnprocessedExpressions: Seq[Expression] = Nil
  ): Assertion = {
    val result = DeleteOverlaps.overlapNode(predicatesOnRead, predicatesOnDelete)
    val expected = DeleteOverlaps.Overlap(expectedUnprocessedExpressions, expectedLabelsOverlap)
    result shouldEqual expected
  }

  // ----------------------
  // Relationship type tests
  // ----------------------

  test("MATCH ()-[r]-() MATCH ()-[s]-() DELETE s overlaps (unknown relationship types)") {
    expectOverlapOnDeleteRel(Nil, Nil, SomeUnknownLabels)
  }

  test("MATCH ()-[r:R]-() MATCH ()-[s]-() DELETE s overlaps (delete has no type predicate)") {
    expectOverlapOnDeleteRel(List(hasTypes("r", "R")), Nil, knownLabels("R"))
  }

  test("MATCH ()-[r]-() MATCH ()-[s:R]-() DELETE s overlaps (read has no type predicate)") {
    expectOverlapOnDeleteRel(Nil, List(hasTypes("s", "R")), knownLabels("R"))
  }

  test("MATCH ()-[r:R]-() MATCH ()-[s:R]-() DELETE s overlaps (same type)") {
    expectOverlapOnDeleteRel(List(hasTypes("r", "R")), List(hasTypes("s", "R")), knownLabels("R"))
  }

  test("MATCH ()-[r:R]-() MATCH ()-[s:S]-() DELETE s does not overlap (different types)") {
    expectNoOverlapOnDeleteRel(List(hasTypes("r", "R")), List(hasTypes("s", "S")))
  }

  test("MATCH ()-[r:R|S]-() MATCH ()-[s:S]-() DELETE s overlaps (read contains delete type)") {
    val read = List(ors(hasTypes("r", "R"), hasTypes("r", "S")))
    val delete = List(hasTypes("s", "S"))
    expectOverlapOnDeleteRel(read, delete, knownLabels("S"))
  }

  test("MATCH ()-[r:R|S]-() MATCH ()-[s:R|S]-() DELETE s overlaps (disjunction on both sides)") {
    val read = List(ors(hasTypes("r", "R"), hasTypes("r", "S")))
    val delete = List(ors(hasTypes("s", "R"), hasTypes("s", "S")))
    // we only report the "lowest" of the overlapping, even though S would also be an overlapping type
    expectOverlapOnDeleteRel(read, delete, knownLabels("R"))
  }

  test("MATCH ()-[r:R]-() MATCH ()-[s:!R]-() DELETE s does not overlap (negation)") {
    val read = List(hasTypes("r", "R"))
    val delete = List(not(hasTypes("s", "R")))
    expectNoOverlapOnDeleteRel(read, delete)
  }

  test("MATCH ()-[r:!R&!S]-() MATCH ()-[s:R|S]-() DELETE s does not overlap (all delete types are negated)") {
    val read = List(not(hasTypes("r", "R")), not(hasTypes("r", "S")))
    val delete = List(ors(hasTypes("s", "R"), hasTypes("s", "S")))
    expectNoOverlapOnDeleteRel(read, delete)
  }

  test("MATCH ()-[r:!R]-() MATCH ()-[s:!R]-() DELETE s overlaps (symmetric negation, unknown type)") {
    // Both sides exclude type R. Any other type can satisfy both.
    val read = List(not(hasTypes("r", "R")))
    val delete = List(not(hasTypes("s", "R")))
    expectOverlapOnDeleteRel(read, delete, SomeUnknownLabels)
  }

  test("MATCH ()-[r:R|S]-() MATCH ()-[s:!R]-() DELETE s overlaps only with type S") {
    // Read matches type R or S. Delete excludes type R. Only type S can satisfy both.
    val read = List(ors(hasTypes("r", "R"), hasTypes("r", "S")))
    val delete = List(not(hasTypes("s", "R")))
    expectOverlapOnDeleteRel(read, delete, knownLabels("S"))
  }

  test("MATCH ()-[r:R|S]-() MATCH ()-[s:R&S]-() DELETE s does not overlap (contradictory delete predicates)") {
    // The delete side has the predicate s:R&S. For a relationship, this is impossible to fulfil.
    // That is why there cannot be a relationship that fulfils both read and write.
    val read = List(ors(hasTypes("r", "R"), hasTypes("r", "S")))
    val delete = List(hasTypes("s", "R"), hasTypes("s", "S"))
    expectNoOverlapOnDeleteRel(read, delete)
  }

  test("MATCH ()-[r:R&S]-() MATCH ()-[s:R|S]-() DELETE s does not overlap (contradictory read predicates)") {
    // The read side has the predicate r:R&S. For a relationship, this is impossible to fulfil.
    // That is why there cannot be a relationship that fulfils both read and write.
    val read = List(hasTypes("r", "R"), hasTypes("r", "S"))
    val delete = List(ors(hasTypes("s", "R"), hasTypes("s", "S")))
    expectNoOverlapOnDeleteRel(read, delete)
  }

  test("overlapOnDelete (relationships) ignores non-type predicates") {
    val read = List(InfinityLiteral)
    val delete = List(falseLiteral)
    expectOverlapOnDeleteRel(read, delete, SomeUnknownLabels, read ++ delete)
  }

  test("overlapOnDelete (relationships) processes type predicates in top-level conjunction only") {
    val typePredicateOnRead = hasTypes("s", "R")
    val otherPredicatesOnRead = List(InfinityLiteral)
    val read = otherPredicatesOnRead ++ List(typePredicateOnRead)

    val typePredicateOnDelete = hasTypes("s", "R")
    val propertyPredicateOnDelete = in(prop("s", "prop"), literalInt(0))
    val otherPredicateOnDelete = falseLiteral
    val delete =
      List(ands(
        propertyPredicateOnDelete,
        ors(
          not(typePredicateOnDelete),
          trueLiteral
        ),
        otherPredicateOnDelete
      ))

    val unprocessedExpressions = otherPredicatesOnRead ++ delete.head.exprs

    // Since the type predicate on the delete side is nested inside a disjunction, we do not inspect it for overlaps,
    // which is why we assume that there is an overlap
    expectOverlapOnDeleteRel(read, delete, knownLabels("R"), unprocessedExpressions)
  }

  def expectNoOverlapOnDeleteRel(
    predicatesOnRead: Seq[Expression],
    predicatesOnDelete: Seq[Expression]
  ): Assertion = {
    val result = DeleteOverlaps.overlap(predicatesOnRead, predicatesOnDelete, RELATIONSHIP_TYPE)
    val expected = DeleteOverlaps.NoLabelOverlap
    result shouldEqual expected
  }

  def expectOverlapOnDeleteRel(
    predicatesOnRead: Seq[Expression],
    predicatesOnDelete: Seq[Expression],
    expectedLabelsOverlap: NodeLabels,
    expectedUnprocessedExpressions: Seq[Expression] = Nil
  ): Assertion = {
    val result = DeleteOverlaps.overlap(predicatesOnRead, predicatesOnDelete, RELATIONSHIP_TYPE)
    val expected = DeleteOverlaps.Overlap(expectedUnprocessedExpressions, expectedLabelsOverlap)
    result shouldEqual expected
  }

  def knownLabels(labels: String*): NodeLabels =
    KnownLabels(labels.toSet)
}
