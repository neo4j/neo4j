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
    val result = DeleteOverlaps.overlap(predicatesOnRead, predicatesOnDelete)
    val expected = DeleteOverlaps.NoLabelOverlap
    result shouldEqual expected
  }

  def expectOverlapOnDelete(
    predicatesOnRead: Seq[Expression],
    predicatesOnDelete: Seq[Expression],
    expectedLabelsOverlap: NodeLabels,
    expectedUnprocessedExpressions: Seq[Expression] = Nil
  ): Assertion = {
    val result = DeleteOverlaps.overlap(predicatesOnRead, predicatesOnDelete)
    val expected = DeleteOverlaps.Overlap(expectedUnprocessedExpressions, expectedLabelsOverlap)
    result shouldEqual expected
  }

  def knownLabels(labels: String*): NodeLabels =
    KnownLabels(labels.toSet)
}
