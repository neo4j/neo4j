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
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.ir.CreatesKnownPropertyKeys
import org.neo4j.cypher.internal.ir.CreatesNoPropertyKeys
import org.neo4j.cypher.internal.ir.CreatesNodeLabels
import org.neo4j.cypher.internal.ir.CreatesPropertyKeys
import org.neo4j.cypher.internal.ir.CreatesStaticNodeLabels
import org.neo4j.cypher.internal.ir.CreatesUnknownPropertyKeys
import org.neo4j.cypher.internal.ir.helpers.overlaps.CreateOverlaps.NodeLabelsOverlap
import org.neo4j.cypher.internal.ir.helpers.overlaps.CreateOverlaps.PropertiesOverlap
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.Assertion
import org.scalatest.OptionValues.convertOptionToValuable

class CreateOverlapsTest extends CypherFunSuite with AstConstructionTestSupport {

  test("MATCH () CREATE () overlaps") {
    val predicatesOnRead = Nil
    val labelsToCreate = staticLabels()
    val propertiesToCreate = CreatesNoPropertyKeys
    val propertiesOverlap = PropertiesOverlap.Overlap(Set.empty)
    val expectedLabels = staticLabelsOverlap()

    expectOverlapOnCreate(predicatesOnRead, labelsToCreate, propertiesToCreate, propertiesOverlap, expectedLabels)
  }

  test("MATCH (n {property: 42}) CREATE () does not overlap") {
    val predicatesOnRead = List(in(prop("n", "property"), literalInt(42)))
    val labelsToCreate = staticLabels()
    val propertiesToCreate = CreatesNoPropertyKeys

    CreateOverlaps.findNodeOverlap(
      predicatesOnRead,
      labelsToCreate,
      propertiesToCreate
    ) shouldBe empty
  }

  test("MATCH (n) WHERE n.property IS NOT NULL CREATE () does not overlap") {
    val predicatesOnRead = List(isNotNull(prop("n", "property")))
    val labelsToCreate = staticLabels()
    val propertiesToCreate = CreatesNoPropertyKeys

    CreateOverlaps.findNodeOverlap(
      predicatesOnRead,
      labelsToCreate,
      propertiesToCreate
    ) shouldBe empty
  }

  test("MATCH (n {property: 42}) CREATE (m {property: *}) overlaps") {
    val predicatesOnRead = List(in(prop("n", "property"), literalInt(42)))
    val labelsToCreate = staticLabels()
    val propertiesToCreate = CreatesKnownPropertyKeys(Set(propName("property")))
    val propertiesOverlap = PropertiesOverlap.Overlap(Set(propName("property")))
    val expectedLabels = staticLabelsOverlap()

    expectOverlapOnCreate(predicatesOnRead, labelsToCreate, propertiesToCreate, propertiesOverlap, expectedLabels)
  }

  test("MATCH (n {property: 42}) CREATE (m {other: *}) does not overlap") {
    val predicatesOnRead = List(in(prop("n", "property"), literalInt(42)))
    val labelsToCreate = staticLabels()
    val propertiesToCreate = CreatesKnownPropertyKeys(Set(propName("other")))

    CreateOverlaps.findNodeOverlap(
      predicatesOnRead,
      labelsToCreate,
      propertiesToCreate
    ) shouldBe empty
  }

  test("MATCH (n) WHERE n.property IS NOT NULL CREATE (m {property: *}) overlaps") {
    val predicatesOnRead = List(isNotNull(prop("n", "property")))
    val labelsToCreate = staticLabels()
    val propertiesToCreate = CreatesKnownPropertyKeys(Set(propName("property")))
    val propertiesOverlap = PropertiesOverlap.Overlap(Set(propName("property")))
    val expectedLabels = staticLabelsOverlap()

    expectOverlapOnCreate(predicatesOnRead, labelsToCreate, propertiesToCreate, propertiesOverlap, expectedLabels)
  }

  test("MATCH (n {property: 42}) CREATE ({properties}) overlaps") {
    val predicatesOnRead = List(in(prop("n", "property"), literalInt(42)))
    val labelsToCreate = staticLabels()
    val propertiesToCreate = CreatesUnknownPropertyKeys
    val propertiesOverlap = PropertiesOverlap.UnknownOverlap
    val expectedLabels = staticLabelsOverlap()

    expectOverlapOnCreate(predicatesOnRead, labelsToCreate, propertiesToCreate, propertiesOverlap, expectedLabels)
  }

  test("MATCH (m:A) CREATE () does not overlap") {
    val predicatesOnRead = List(hasLabels("m", "A"))
    val labelsToCreate = staticLabels()
    val propertiesToCreate = CreatesNoPropertyKeys

    CreateOverlaps.findNodeOverlap(
      predicatesOnRead,
      labelsToCreate,
      propertiesToCreate
    ) shouldBe empty
  }

  test("MATCH (m:A) CREATE (n:B) does not overlap") {
    val predicatesOnRead = List(hasLabels("m", "A"))
    val labelsToCreate = staticLabels("B")
    val propertiesToCreate = CreatesNoPropertyKeys

    CreateOverlaps.findNodeOverlap(
      predicatesOnRead,
      labelsToCreate,
      propertiesToCreate
    ) shouldBe empty
  }

  test("MATCH (m:A {property: 42}) CREATE (n:B {property: *}) does not overlap") {
    val predicatesOnRead = List(hasLabels("m", "A"), in(prop("n", "property"), literalInt(42)))
    val labelsToCreate = staticLabels("B")
    val propertiesToCreate = CreatesKnownPropertyKeys(Set(propName("property")))

    CreateOverlaps.findNodeOverlap(
      predicatesOnRead,
      labelsToCreate,
      propertiesToCreate
    ) shouldBe empty
  }

  test("MATCH (m:A {property: 42}) CREATE (n:B {properties}) does not overlap") {
    val predicatesOnRead = List(hasLabels("m", "A"), in(prop("n", "property"), literalInt(42)))
    val labelsToCreate = staticLabels("B")
    val propertiesToCreate = CreatesUnknownPropertyKeys

    CreateOverlaps.findNodeOverlap(
      predicatesOnRead,
      labelsToCreate,
      propertiesToCreate
    ) shouldBe empty
  }

  test("MATCH (m:B) CREATE (n:A) does not overlap") {
    val predicatesOnRead = List(hasLabels("m", "B"))
    val labelsToCreate = staticLabels("A")
    val propertiesToCreate = CreatesNoPropertyKeys

    CreateOverlaps.findNodeOverlap(
      predicatesOnRead,
      labelsToCreate,
      propertiesToCreate
    ) shouldBe empty
  }

  test("MATCH (m:A) CREATE (n:A) overlaps") {
    val predicatesOnRead = List(hasLabels("m", "A"))
    val labelsToCreate = staticLabels("A")
    val propertiesToCreate = CreatesNoPropertyKeys
    val propertiesOverlap = PropertiesOverlap.Overlap(Set.empty)
    val expectedLabels = staticLabelsOverlap("A")

    expectOverlapOnCreate(predicatesOnRead, labelsToCreate, propertiesToCreate, propertiesOverlap, expectedLabels)
  }

  test("MATCH (m:%) CREATE (n) does not overlap") {
    val predicatesOnRead = List(hasALabel("m"))
    val labelsToCreate = staticLabels()
    val propertiesToCreate = CreatesNoPropertyKeys

    CreateOverlaps.findNodeOverlap(
      predicatesOnRead,
      labelsToCreate,
      propertiesToCreate
    ) shouldBe empty
  }

  test("MATCH (m:%) CREATE (n:A) overlaps") {
    val predicatesOnRead = List(hasALabel("m"))
    val labelsToCreate = staticLabels("A")
    val propertiesToCreate = CreatesNoPropertyKeys
    val propertiesOverlap = PropertiesOverlap.Overlap(Set.empty)
    val expectedLabels = staticLabelsOverlap("A")

    expectOverlapOnCreate(predicatesOnRead, labelsToCreate, propertiesToCreate, propertiesOverlap, expectedLabels)
  }

  test("MATCH (m:!%) CREATE (n) overlaps") {
    val predicatesOnRead = List(not(hasALabel("m")))
    val labelsToCreate = staticLabels()
    val propertiesToCreate = CreatesNoPropertyKeys
    val propertiesOverlap = PropertiesOverlap.Overlap(Set.empty)
    val expectedLabels = staticLabelsOverlap()

    expectOverlapOnCreate(predicatesOnRead, labelsToCreate, propertiesToCreate, propertiesOverlap, expectedLabels)
  }

  test("MATCH (m:!A) CREATE (n:A) does not overlap") {
    val predicatesOnRead = List(not(hasLabels("m", "A")))
    val labelsToCreate = staticLabels("A")
    val propertiesToCreate = CreatesNoPropertyKeys

    CreateOverlaps.findNodeOverlap(
      predicatesOnRead,
      labelsToCreate,
      propertiesToCreate
    ) shouldBe empty
  }

  test("MATCH (m:!A) CREATE (n) overlaps") {
    val predicatesOnRead = List(not(hasLabels("m", "A")))
    val labelsToCreate = staticLabels()
    val propertiesToCreate = CreatesNoPropertyKeys
    val propertiesOverlap = PropertiesOverlap.Overlap(Set.empty)
    val expectedLabels = staticLabelsOverlap()

    expectOverlapOnCreate(predicatesOnRead, labelsToCreate, propertiesToCreate, propertiesOverlap, expectedLabels)
  }

  test("MATCH (m:!!A) CREATE (n) does not overlap") {
    val predicatesOnRead = List(not(not(hasLabels("n", "A"))))
    val labelsToCreate = staticLabels()
    val propertiesToCreate = CreatesNoPropertyKeys

    CreateOverlaps.findNodeOverlap(
      predicatesOnRead,
      labelsToCreate,
      propertiesToCreate
    ) shouldBe empty
  }

  test("MATCH (m:A&B) CREATE (n:A:B) overlaps") {
    val predicatesOnRead = List(ands(hasLabels("m", "A"), hasLabels("m", "B")))
    val labelsToCreate = staticLabels("A", "B")
    val propertiesToCreate = CreatesNoPropertyKeys
    val propertiesOverlap = PropertiesOverlap.Overlap(Set.empty)
    val expectedLabels = staticLabelsOverlap("A", "B")

    expectOverlapOnCreate(predicatesOnRead, labelsToCreate, propertiesToCreate, propertiesOverlap, expectedLabels)
  }

  test("MATCH (m:A&B) CREATE (n:A) does not overlap") {
    val predicatesOnRead = List(ands(hasLabels("m", "A"), hasLabels("m", "B")))
    val labelsToCreate = staticLabels("A")
    val propertiesToCreate = CreatesNoPropertyKeys

    CreateOverlaps.findNodeOverlap(
      predicatesOnRead,
      labelsToCreate,
      propertiesToCreate
    ) shouldBe empty
  }

  test("MATCH (m:!A) CREATE (n:A:B:C) does not overlap") {
    val predicatesOnRead = List(not(hasLabels("n", "A")))
    val labelsToCreate = staticLabels("A", "B", "C")
    val propertiesToCreate = CreatesNoPropertyKeys

    CreateOverlaps.findNodeOverlap(
      predicatesOnRead,
      labelsToCreate,
      propertiesToCreate
    ) shouldBe empty
  }

  test("MATCH (m:A&B&C) CREATE (n:A:B:C) overlaps") {
    val predicatesOnRead = List(hasLabels("m", "A"), hasLabels("m", "B"), hasLabels("m", "C"))
    val labelsToCreate = staticLabels("A", "B", "C")
    val propertiesToCreate = CreatesNoPropertyKeys
    val propertiesOverlap = PropertiesOverlap.Overlap(Set.empty)
    val expectedLabels = staticLabelsOverlap("A", "B", "C")
    expectOverlapOnCreate(predicatesOnRead, labelsToCreate, propertiesToCreate, propertiesOverlap, expectedLabels)
  }

  test("MATCH (m:A|B|C) CREATE (n:A) overlaps") {
    val predicatesOnRead = List(ors(hasLabels("m", "A"), hasLabels("m", "B"), hasLabels("m", "C")))
    val labelsToCreate = staticLabels("A")
    val propertiesToCreate = CreatesNoPropertyKeys
    val propertiesOverlap = PropertiesOverlap.Overlap(Set.empty)
    val expectedLabels = staticLabelsOverlap("A")
    expectOverlapOnCreate(predicatesOnRead, labelsToCreate, propertiesToCreate, propertiesOverlap, expectedLabels)
  }

  test("MATCH (m:A&(B|C)) CREATE (n:A) does not overlap") {
    val predicatesOnRead = List(hasLabels("m", "A"), ors(hasLabels("m", "B"), hasLabels("m", "C")))
    val labelsToCreate = staticLabels("A")
    val propertiesToCreate = CreatesNoPropertyKeys

    CreateOverlaps.findNodeOverlap(
      predicatesOnRead,
      labelsToCreate,
      propertiesToCreate
    ) shouldBe empty
  }

  test("MATCH (n:!(A|B)) CREATE (n:A) does not overlap") {
    val predicatesOnRead = List(not(or(hasLabels("n", "A"), hasLabels("n", "B"))))
    val labelsToCreate = staticLabels("A")
    val propertiesToCreate = CreatesNoPropertyKeys

    CreateOverlaps.findNodeOverlap(
      predicatesOnRead,
      labelsToCreate,
      propertiesToCreate
    ) shouldBe empty
  }

  test("MATCH (m:A|B|C) WHERE m.prop2 = 42 CREATE (n:A {prop1: *}) does not overlap") {
    val predicatesOnRead = List(ands(
      in(prop("n", "prop2"), literalInt(42)),
      ors(hasLabels("m", "A"), hasLabels("m", "B"), hasLabels("m", "C"))
    ))
    val labelsToCreate = staticLabels("A")
    val propertiesToCreate = CreatesKnownPropertyKeys(Set(propName("prop1")))

    CreateOverlaps.findNodeOverlap(
      predicatesOnRead,
      labelsToCreate,
      propertiesToCreate
    ) shouldBe empty
  }

  test("MATCH (m) WHERE m:A XOR m:B CREATE (n:A:B) does not overlap") {
    val predicatesOnRead = List(xor(hasLabels("m", "A"), hasLabels("m", "B")))
    val labelsToCreate = staticLabels("A", "B")

    CreateOverlaps.findNodeOverlap(
      predicatesOnRead,
      labelsToCreate,
      CreatesNoPropertyKeys
    ) shouldBe empty
  }

  // Normalised XOR
  test("MATCH (m:(A|B)&(!A|!B)) CREATE (n:A:B) does not overlap") {
    val predicatesOnRead = List(
      or(hasLabels("m", "A"), hasLabels("m", "B")),
      or(not(hasLabels("m", "A")), not(hasLabels("m", "B")))
    )
    val labelsToCreate = staticLabels("A", "B")

    CreateOverlaps.findNodeOverlap(
      predicatesOnRead,
      labelsToCreate,
      CreatesNoPropertyKeys
    ) shouldBe empty
  }

  test("MATCH (m:!!A|B) CREATE (n:A:B) overlaps") {
    val predicatesOnRead = List(ors(
      not(not(hasLabels("m", "A"))),
      hasLabels("m", "B")
    ))
    val labelsToCreate = staticLabels("A", "B")
    val propertiesToCreate = CreatesNoPropertyKeys
    val propertiesOverlap = PropertiesOverlap.Overlap(Set.empty)
    val expectedLabels = staticLabelsOverlap("A", "B")
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
    val labelsToCreate = staticLabels("A", "B")
    CreateOverlaps.findNodeOverlap(
      predicatesOnRead,
      labelsToCreate,
      CreatesNoPropertyKeys
    ) shouldBe empty
  }

  test("overlapOnCreate ignores non-label predicates") {
    val read = List(InfinityLiteral)
    val labelsToCreate = staticLabels("A")
    val propertiesToCreate = CreatesNoPropertyKeys
    val propertiesOverlap = PropertiesOverlap.Overlap(Set.empty)
    val expectedLabels = staticLabelsOverlap("A")
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
    val labelsToCreate = staticLabels("A")
    val propertiesToCreate = CreatesNoPropertyKeys
    val propertiesOverlap = PropertiesOverlap.Overlap(Set.empty)
    val expectedLabels = staticLabelsOverlap("A")
    expectOverlapOnCreate(read, labelsToCreate, propertiesToCreate, propertiesOverlap, expectedLabels, read)
  }

  test("overlapOnCreate will process all label expressions in the top level conjunction, ignoring the rest") {
    val labelExpressionOnRead = ors(hasLabels("m", "A"), hasLabels("m", "B"), hasLabels("m", "C"))
    val predicatesOnRead = List(ors(falseLiteral, in(prop("m", "property"), literalInt(42))))
    val read = labelExpressionOnRead :: predicatesOnRead

    val labelsToCreate = staticLabels("A")
    val propertiesToCreate = CreatesKnownPropertyKeys(Set(propName("property")))
    val propertiesOverlap = PropertiesOverlap.Overlap(Set.empty)

    expectOverlapOnCreate(
      read,
      labelsToCreate,
      propertiesToCreate,
      propertiesOverlap,
      staticLabelsOverlap("A"),
      predicatesOnRead
    )
  }

  def expectOverlapOnCreate(
    predicatesOnRead: Seq[Expression],
    nodeLabelsToCreate: CreatesNodeLabels,
    propertiesToCreate: CreatesPropertyKeys,
    expectedPropertiesOverlap: PropertiesOverlap,
    expectedNodeLabelsOverlap: NodeLabelsOverlap,
    expectedUnprocessedExpressions: Seq[Expression] = Nil
  ): Assertion = {
    val result = CreateOverlaps.findNodeOverlap(predicatesOnRead, nodeLabelsToCreate, propertiesToCreate)
    val expected =
      CreateOverlaps.NodeOverlap(expectedUnprocessedExpressions, expectedPropertiesOverlap, expectedNodeLabelsOverlap)

    result.value shouldEqual expected
  }

  def staticLabels(labels: String*): CreatesNodeLabels =
    CreatesStaticNodeLabels(labelNames(labels))

  def staticLabelsOverlap(labels: String*): NodeLabelsOverlap =
    NodeLabelsOverlap.Static(labelNames(labels))

  private def labelNames(labels: Seq[String]): Set[LabelName] =
    labels.map(label => LabelName(label)(InputPosition.NONE)).toSet
}
