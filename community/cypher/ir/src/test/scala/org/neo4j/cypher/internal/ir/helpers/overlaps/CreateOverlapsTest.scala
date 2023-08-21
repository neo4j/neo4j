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
import org.neo4j.cypher.internal.ir.CreatesKnownPropertyKeys
import org.neo4j.cypher.internal.ir.CreatesNoPropertyKeys
import org.neo4j.cypher.internal.ir.CreatesPropertyKeys
import org.neo4j.cypher.internal.ir.CreatesUnknownPropertyKeys
import org.neo4j.cypher.internal.ir.helpers.overlaps.CreateOverlaps.PropertiesOverlap
import org.neo4j.cypher.internal.label_expressions.NodeLabels
import org.neo4j.cypher.internal.label_expressions.NodeLabels.KnownLabels
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.Assertion

class CreateOverlapsTest extends CypherFunSuite with AstConstructionTestSupport {

  test("MATCH () CREATE () overlaps") {
    val predicatesOnRead = Nil
    val labelsToCreate = Set.empty[String]
    val propertiesToCreate = CreatesNoPropertyKeys
    val propertiesOverlap = PropertiesOverlap.Overlap(Set.empty)
    val expectedLabels = KnownLabels(Set.empty)

    expectOverlapOnCreate(predicatesOnRead, labelsToCreate, propertiesToCreate, propertiesOverlap, expectedLabels)
  }

  test("MATCH (n {property: 42}) CREATE () does not overlap") {
    val predicatesOnRead = List(in(prop("n", "property"), literalInt(42)))
    val labelsToCreate = Set.empty[String]
    val propertiesToCreate = CreatesNoPropertyKeys

    CreateOverlaps.overlap(
      predicatesOnRead,
      labelsToCreate,
      propertiesToCreate
    ) shouldEqual CreateOverlaps.NoPropertyOverlap
  }

  test("MATCH (n) WHERE n.property IS NOT NULL CREATE () does not overlap") {
    val predicatesOnRead = List(isNotNull(prop("n", "property")))
    val labelsToCreate = Set.empty[String]
    val propertiesToCreate = CreatesNoPropertyKeys

    CreateOverlaps.overlap(
      predicatesOnRead,
      labelsToCreate,
      propertiesToCreate
    ) shouldEqual CreateOverlaps.NoPropertyOverlap
  }

  test("MATCH (n {property: 42}) CREATE (m {property: *}) overlaps") {
    val predicatesOnRead = List(in(prop("n", "property"), literalInt(42)))
    val labelsToCreate = Set.empty[String]
    val propertiesToCreate = CreatesKnownPropertyKeys(Set(propName("property")))
    val propertiesOverlap = PropertiesOverlap.Overlap(Set(propName("property")))
    val expectedLabels = KnownLabels(Set.empty)

    expectOverlapOnCreate(predicatesOnRead, labelsToCreate, propertiesToCreate, propertiesOverlap, expectedLabels)
  }

  test("MATCH (n {property: 42}) CREATE (m {other: *}) does not overlap") {
    val predicatesOnRead = List(in(prop("n", "property"), literalInt(42)))
    val labelsToCreate = Set.empty[String]
    val propertiesToCreate = CreatesKnownPropertyKeys(Set(propName("other")))

    CreateOverlaps.overlap(
      predicatesOnRead,
      labelsToCreate,
      propertiesToCreate
    ) shouldEqual CreateOverlaps.NoPropertyOverlap
  }

  test("MATCH (n) WHERE n.property IS NOT NULL CREATE (m {property: *}) overlaps") {
    val predicatesOnRead = List(isNotNull(prop("n", "property")))
    val labelsToCreate = Set.empty[String]
    val propertiesToCreate = CreatesKnownPropertyKeys(Set(propName("property")))
    val propertiesOverlap = PropertiesOverlap.Overlap(Set(propName("property")))
    val expectedLabels = KnownLabels(Set.empty)

    expectOverlapOnCreate(predicatesOnRead, labelsToCreate, propertiesToCreate, propertiesOverlap, expectedLabels)
  }

  test("MATCH (n {property: 42}) CREATE ({properties}) overlaps") {
    val predicatesOnRead = List(in(prop("n", "property"), literalInt(42)))
    val labelsToCreate = Set.empty[String]
    val propertiesToCreate = CreatesUnknownPropertyKeys
    val propertiesOverlap = PropertiesOverlap.UnknownOverlap
    val expectedLabels = KnownLabels(Set.empty)

    expectOverlapOnCreate(predicatesOnRead, labelsToCreate, propertiesToCreate, propertiesOverlap, expectedLabels)
  }

  test("MATCH (m:A) CREATE () does not overlap") {
    val predicatesOnRead = List(hasLabels("m", "A"))
    val labelsToCreate = Set.empty[String]
    val propertiesToCreate = CreatesNoPropertyKeys

    CreateOverlaps.overlap(
      predicatesOnRead,
      labelsToCreate,
      propertiesToCreate
    ) shouldEqual CreateOverlaps.NoLabelOverlap
  }

  test("MATCH (m:A) CREATE (n:B) does not overlap") {
    val predicatesOnRead = List(hasLabels("m", "A"))
    val labelsToCreate = Set("B")
    val propertiesToCreate = CreatesNoPropertyKeys

    CreateOverlaps.overlap(
      predicatesOnRead,
      labelsToCreate,
      propertiesToCreate
    ) shouldEqual CreateOverlaps.NoLabelOverlap
  }

  test("MATCH (m:A {property: 42}) CREATE (n:B {property: *}) does not overlap") {
    val predicatesOnRead = List(hasLabels("m", "A"), in(prop("n", "property"), literalInt(42)))
    val labelsToCreate = Set("B")
    val propertiesToCreate = CreatesKnownPropertyKeys(Set(propName("property")))

    CreateOverlaps.overlap(
      predicatesOnRead,
      labelsToCreate,
      propertiesToCreate
    ) shouldEqual CreateOverlaps.NoLabelOverlap
  }

  test("MATCH (m:A {property: 42}) CREATE (n:B {properties}) does not overlap") {
    val predicatesOnRead = List(hasLabels("m", "A"), in(prop("n", "property"), literalInt(42)))
    val labelsToCreate = Set("B")
    val propertiesToCreate = CreatesUnknownPropertyKeys

    CreateOverlaps.overlap(
      predicatesOnRead,
      labelsToCreate,
      propertiesToCreate
    ) shouldEqual CreateOverlaps.NoLabelOverlap
  }

  test("MATCH (m:B) CREATE (n:A) does not overlap") {
    val predicatesOnRead = List(hasLabels("m", "B"))
    val labelsToCreate = Set("A")
    val propertiesToCreate = CreatesNoPropertyKeys

    CreateOverlaps.overlap(
      predicatesOnRead,
      labelsToCreate,
      propertiesToCreate
    ) shouldEqual CreateOverlaps.NoLabelOverlap
  }

  test("MATCH (m:A) CREATE (n:A) overlaps") {
    val predicatesOnRead = List(hasLabels("m", "A"))
    val labelsToCreate = Set("A")
    val propertiesToCreate = CreatesNoPropertyKeys
    val propertiesOverlap = PropertiesOverlap.Overlap(Set.empty)
    val expectedLabels = knownLabels("A")

    expectOverlapOnCreate(predicatesOnRead, labelsToCreate, propertiesToCreate, propertiesOverlap, expectedLabels)
  }

  test("MATCH (m:%) CREATE (n) does not overlap") {
    val predicatesOnRead = List(hasALabel("m"))
    val labelsToCreate = Set.empty[String]
    val propertiesToCreate = CreatesNoPropertyKeys

    CreateOverlaps.overlap(
      predicatesOnRead,
      labelsToCreate,
      propertiesToCreate
    ) shouldEqual CreateOverlaps.NoLabelOverlap
  }

  test("MATCH (m:%) CREATE (n:A) overlaps") {
    val predicatesOnRead = List(hasALabel("m"))
    val labelsToCreate = Set("A")
    val propertiesToCreate = CreatesNoPropertyKeys
    val propertiesOverlap = PropertiesOverlap.Overlap(Set.empty)
    val expectedLabels = knownLabels("A")

    expectOverlapOnCreate(predicatesOnRead, labelsToCreate, propertiesToCreate, propertiesOverlap, expectedLabels)
  }

  test("MATCH (m:!%) CREATE (n) overlaps") {
    val predicatesOnRead = List(not(hasALabel("m")))
    val labelsToCreate = Set.empty[String]
    val propertiesToCreate = CreatesNoPropertyKeys
    val propertiesOverlap = PropertiesOverlap.Overlap(Set.empty)
    val expectedLabels = KnownLabels(Set.empty)

    expectOverlapOnCreate(predicatesOnRead, labelsToCreate, propertiesToCreate, propertiesOverlap, expectedLabels)
  }

  test("MATCH (m:!A) CREATE (n:A) does not overlap") {
    val predicatesOnRead = List(not(hasLabels("m", "A")))
    val labelsToCreate = Set("A")
    val propertiesToCreate = CreatesNoPropertyKeys

    CreateOverlaps.overlap(
      predicatesOnRead,
      labelsToCreate,
      propertiesToCreate
    ) shouldEqual CreateOverlaps.NoLabelOverlap
  }

  test("MATCH (m:!A) CREATE (n) overlaps") {
    val predicatesOnRead = List(not(hasLabels("m", "A")))
    val labelsToCreate = Set.empty[String]
    val propertiesToCreate = CreatesNoPropertyKeys
    val propertiesOverlap = PropertiesOverlap.Overlap(Set.empty)
    val expectedLabels = KnownLabels(Set.empty)

    expectOverlapOnCreate(predicatesOnRead, labelsToCreate, propertiesToCreate, propertiesOverlap, expectedLabels)
  }

  test("MATCH (m:!!A) CREATE (n) does not overlap") {
    val predicatesOnRead = List(not(not(hasLabels("n", "A"))))
    val labelsToCreate = Set.empty[String]
    val propertiesToCreate = CreatesNoPropertyKeys

    CreateOverlaps.overlap(
      predicatesOnRead,
      labelsToCreate,
      propertiesToCreate
    ) shouldEqual CreateOverlaps.NoLabelOverlap
  }

  test("MATCH (m:A&B) CREATE (n:A:B) overlaps") {
    val predicatesOnRead = List(ands(hasLabels("m", "A"), hasLabels("m", "B")))
    val labelsToCreate = Set("A", "B")
    val propertiesToCreate = CreatesNoPropertyKeys
    val propertiesOverlap = PropertiesOverlap.Overlap(Set.empty)
    val expectedLabels = knownLabels("A", "B")

    expectOverlapOnCreate(predicatesOnRead, labelsToCreate, propertiesToCreate, propertiesOverlap, expectedLabels)
  }

  test("MATCH (m:A&B) CREATE (n:A) does not overlap") {
    val predicatesOnRead = List(ands(hasLabels("m", "A"), hasLabels("m", "B")))
    val labelsToCreate = Set("A")
    val propertiesToCreate = CreatesNoPropertyKeys

    CreateOverlaps.overlap(
      predicatesOnRead,
      labelsToCreate,
      propertiesToCreate
    ) shouldEqual CreateOverlaps.NoLabelOverlap
  }

  test("MATCH (m:!A) CREATE (n:A:B:C) does not overlap") {
    val predicatesOnRead = List(not(hasLabels("n", "A")))
    val labelsToCreate = Set("A", "B", "C")
    val propertiesToCreate = CreatesNoPropertyKeys

    CreateOverlaps.overlap(
      predicatesOnRead,
      labelsToCreate,
      propertiesToCreate
    ) shouldEqual CreateOverlaps.NoLabelOverlap
  }

  test("MATCH (m:A&B&C) CREATE (n:A:B:C) overlaps") {
    val predicatesOnRead = List(hasLabels("m", "A"), hasLabels("m", "B"), hasLabels("m", "C"))
    val labelsToCreate = Set("A", "B", "C")
    val propertiesToCreate = CreatesNoPropertyKeys
    val propertiesOverlap = PropertiesOverlap.Overlap(Set.empty)
    val expectedLabels = knownLabels("A", "B", "C")
    expectOverlapOnCreate(predicatesOnRead, labelsToCreate, propertiesToCreate, propertiesOverlap, expectedLabels)
  }

  test("MATCH (m:A|B|C) CREATE (n:A) overlaps") {
    val predicatesOnRead = List(ors(hasLabels("m", "A"), hasLabels("m", "B"), hasLabels("m", "C")))
    val labelsToCreate = Set("A")
    val propertiesToCreate = CreatesNoPropertyKeys
    val propertiesOverlap = PropertiesOverlap.Overlap(Set.empty)
    val expectedLabels = knownLabels("A")
    expectOverlapOnCreate(predicatesOnRead, labelsToCreate, propertiesToCreate, propertiesOverlap, expectedLabels)
  }

  test("MATCH (m:A&(B|C)) CREATE (n:A) does not overlap") {
    val predicatesOnRead = List(hasLabels("m", "A"), ors(hasLabels("m", "B"), hasLabels("m", "C")))
    val labelsToCreate = Set("A")
    val propertiesToCreate = CreatesNoPropertyKeys

    CreateOverlaps.overlap(
      predicatesOnRead,
      labelsToCreate,
      propertiesToCreate
    ) shouldEqual CreateOverlaps.NoLabelOverlap
  }

  test("MATCH (n:!(A|B)) CREATE (n:A) does not overlap") {
    val predicatesOnRead = List(not(or(hasLabels("n", "A"), hasLabels("n", "B"))))
    val labelsToCreate = Set("A")
    val propertiesToCreate = CreatesNoPropertyKeys

    CreateOverlaps.overlap(
      predicatesOnRead,
      labelsToCreate,
      propertiesToCreate
    ) shouldEqual CreateOverlaps.NoLabelOverlap
  }

  test("MATCH (m:A|B|C) WHERE m.prop2 = 42 CREATE (n:A {prop1: *}) does not overlap") {
    val predicatesOnRead = List(ands(
      in(prop("n", "prop2"), literalInt(42)),
      ors(hasLabels("m", "A"), hasLabels("m", "B"), hasLabels("m", "C"))
    ))
    val labelsToCreate = Set("A")
    val propertiesToCreate = CreatesKnownPropertyKeys(Set(propName("prop1")))

    CreateOverlaps.overlap(
      predicatesOnRead,
      labelsToCreate,
      propertiesToCreate
    ) shouldEqual CreateOverlaps.NoPropertyOverlap
  }

  test("MATCH (m) WHERE m:A XOR m:B CREATE (n:A:B) does not overlap") {
    val predicatesOnRead = List(xor(hasLabels("m", "A"), hasLabels("m", "B")))
    val labelsToCreate = Set("A", "B")

    CreateOverlaps.overlap(
      predicatesOnRead,
      labelsToCreate,
      CreatesNoPropertyKeys
    ) shouldEqual CreateOverlaps.NoLabelOverlap
  }

  // Normalised XOR
  test("MATCH (m:(A|B)&(!A|!B)) CREATE (n:A:B) does not overlap") {
    val predicatesOnRead = List(
      or(hasLabels("m", "A"), hasLabels("m", "B")),
      or(not(hasLabels("m", "A")), not(hasLabels("m", "B")))
    )
    val labelsToCreate = Set("A", "B")

    CreateOverlaps.overlap(
      predicatesOnRead,
      labelsToCreate,
      CreatesNoPropertyKeys
    ) shouldEqual CreateOverlaps.NoLabelOverlap
  }

  test("MATCH (m:!!A|B) CREATE (n:A:B) overlaps") {
    val predicatesOnRead = List(ors(
      not(not(hasLabels("m", "A"))),
      hasLabels("m", "B")
    ))
    val labelsToCreate = Set("A", "B")
    val propertiesToCreate = CreatesNoPropertyKeys
    val propertiesOverlap = PropertiesOverlap.Overlap(Set.empty)
    val expectedLabels = knownLabels("A", "B")
    expectOverlapOnCreate(predicatesOnRead, labelsToCreate, propertiesToCreate, propertiesOverlap, expectedLabels)
  }

  test("MATCH (m:(!!A|B)&C) CREATE (n:A:B) does not overlap") {
    val predicatesOnRead = List(
      ors(
        not(not(hasLabels("m", "A"))),
        hasLabels("m", "B")
      ),
      hasLabels("m", "C")
    )
    val labelsToCreate = Set("A", "B")
    CreateOverlaps.overlap(
      predicatesOnRead,
      labelsToCreate,
      CreatesNoPropertyKeys
    ) shouldEqual CreateOverlaps.NoLabelOverlap
  }

  test("overlapOnCreate ignores non-label predicates") {
    val read = List(InfinityLiteral)
    val labelsToCreate = Set("A")
    val propertiesToCreate = CreatesNoPropertyKeys
    val propertiesOverlap = PropertiesOverlap.Overlap(Set.empty)
    val expectedLabels = knownLabels("A")
    expectOverlapOnCreate(read, labelsToCreate, propertiesToCreate, propertiesOverlap, expectedLabels, read)
  }

  test("overlapOnCreate only processes label predicates – even when other predicates try to sneak in") {
    val read = List(not(and(
      or(hasALabel("m"), InfinityLiteral),
      or(
        hasLabels("m", "A"),
        // here we try to sneak a property predicate inside our label expression, and so the whole label expression must be ignored
        in(prop("m", "property"), literalInt(42))
      )
    )))
    val labelsToCreate = Set("A")
    val propertiesToCreate = CreatesNoPropertyKeys
    val propertiesOverlap = PropertiesOverlap.Overlap(Set.empty)
    val expectedLabels = knownLabels("A")
    expectOverlapOnCreate(read, labelsToCreate, propertiesToCreate, propertiesOverlap, expectedLabels, read)
  }

  test("overlapOnCreate will process all label expressions in the top level conjunction, ignoring the rest") {
    val labelExpressionOnRead = ors(hasLabels("m", "A"), hasLabels("m", "B"), hasLabels("m", "C"))
    val predicatesOnRead = List(ors(falseLiteral, in(prop("m", "property"), literalInt(42))))
    val read = labelExpressionOnRead :: predicatesOnRead

    val labelsToCreate = Set("A")
    val propertiesToCreate = CreatesKnownPropertyKeys(Set(propName("property")))
    val propertiesOverlap = PropertiesOverlap.Overlap(Set.empty)

    expectOverlapOnCreate(
      read,
      labelsToCreate,
      propertiesToCreate,
      propertiesOverlap,
      knownLabels("A"),
      predicatesOnRead
    )
  }

  def expectOverlapOnCreate(
    predicatesOnRead: Seq[Expression],
    labelsToCreate: Set[String],
    propertiesToCreate: CreatesPropertyKeys,
    expectedPropertiesOverlap: PropertiesOverlap,
    expectedNodeLabels: NodeLabels,
    expectedUnprocessedExpressions: Seq[Expression] = Nil
  ): Assertion = {
    val result = CreateOverlaps.overlap(predicatesOnRead, labelsToCreate, propertiesToCreate)
    val expected = CreateOverlaps.Overlap(expectedUnprocessedExpressions, expectedPropertiesOverlap, expectedNodeLabels)

    result shouldEqual expected
  }

  def knownLabels(labels: String*): NodeLabels = KnownLabels(labels.toSet)
}
