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
package org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands

import org.mockito.Mockito.when
import org.neo4j.configuration.Config
import org.neo4j.cypher.internal.ast.AllConstraints
import org.neo4j.cypher.internal.ast.ExistsConstraints
import org.neo4j.cypher.internal.ast.KeyConstraints
import org.neo4j.cypher.internal.ast.NodeExistsConstraints
import org.neo4j.cypher.internal.ast.NodeKeyConstraints
import org.neo4j.cypher.internal.ast.NodePropTypeConstraints
import org.neo4j.cypher.internal.ast.NodeUniqueConstraints
import org.neo4j.cypher.internal.ast.PropTypeConstraints
import org.neo4j.cypher.internal.ast.RelExistsConstraints
import org.neo4j.cypher.internal.ast.RelKeyConstraints
import org.neo4j.cypher.internal.ast.RelPropTypeConstraints
import org.neo4j.cypher.internal.ast.RelUniqueConstraints
import org.neo4j.cypher.internal.ast.ShowConstraintsClause
import org.neo4j.cypher.internal.ast.UniqueConstraints
import org.neo4j.cypher.internal.ast.ValidSyntax
import org.neo4j.cypher.internal.runtime.ConstraintInfo
import org.neo4j.cypher.internal.runtime.IndexInfo
import org.neo4j.cypher.internal.runtime.IndexStatus
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.internal.schema.IndexPrototype
import org.neo4j.internal.schema.IndexType
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory
import org.neo4j.internal.schema.constraints.PropertyTypeSet
import org.neo4j.internal.schema.constraints.SchemaValueType
import org.neo4j.kernel.impl.index.schema.RangeIndexProvider
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualValues

class ShowConstraintsCommandTest extends ShowCommandTestBase {

  private val defaultColumns =
    ShowConstraintsClause(AllConstraints, brief = false, verbose = false, None, hasYield = false)(InputPosition.NONE)
      .unfilteredColumns
      .columns

  private val allColumns =
    ShowConstraintsClause(AllConstraints, brief = false, verbose = false, None, hasYield = true)(InputPosition.NONE)
      .unfilteredColumns
      .columns

  private val optionsMap = VirtualValues.map(
    Array("indexProvider", "indexConfig"),
    Array(Values.stringValue(RangeIndexProvider.DESCRIPTOR.name()), VirtualValues.EMPTY_MAP)
  )
  private val optionsString = s"OPTIONS {indexConfig: {}, indexProvider: '${RangeIndexProvider.DESCRIPTOR.name()}'}"

  private val nodeUniquenessIndexDescriptor =
    IndexPrototype.uniqueForSchema(labelDescriptor, RangeIndexProvider.DESCRIPTOR)
      .withName("constraint0")
      .materialise(0)
      .withOwningConstraintId(1)

  private val relUniquenessIndexDescriptor =
    IndexPrototype.uniqueForSchema(relTypeDescriptor, RangeIndexProvider.DESCRIPTOR)
      .withName("constraint2")
      .materialise(3)
      .withOwningConstraintId(4)

  private val nodeKeyIndexDescriptor =
    IndexPrototype.uniqueForSchema(labelDescriptor, RangeIndexProvider.DESCRIPTOR)
      .withName("constraint3")
      .materialise(5)
      .withOwningConstraintId(6)

  private val relKeyIndexDescriptor =
    IndexPrototype.uniqueForSchema(relTypeDescriptor, RangeIndexProvider.DESCRIPTOR)
      .withName("constraint4")
      .materialise(7)
      .withOwningConstraintId(8)

  private val nodeUniquenessConstraintDescriptor =
    ConstraintDescriptorFactory.uniqueForSchema(nodeUniquenessIndexDescriptor.schema(), IndexType.RANGE)
      .withName("constraint0")
      .withOwnedIndexId(0)
      .withId(1)

  private val relUniquenessConstraintDescriptor =
    ConstraintDescriptorFactory.uniqueForSchema(relUniquenessIndexDescriptor.schema(), IndexType.RANGE)
      .withName("constraint2")
      .withOwnedIndexId(3)
      .withId(4)

  private val nodeKeyConstraintDescriptor =
    ConstraintDescriptorFactory.keyForSchema(nodeKeyIndexDescriptor.schema(), IndexType.RANGE)
      .withName("constraint3")
      .withOwnedIndexId(5)
      .withId(6)

  private val relKeyConstraintDescriptor =
    ConstraintDescriptorFactory.keyForSchema(relKeyIndexDescriptor.schema(), IndexType.RANGE)
      .withName("constraint4")
      .withOwnedIndexId(7)
      .withId(8)

  private val nodeExistConstraintDescriptor =
    ConstraintDescriptorFactory.existsForSchema(labelDescriptor)
      .withName("constraint5")
      .withId(9)

  private val relExistConstraintDescriptor =
    ConstraintDescriptorFactory.existsForSchema(relTypeDescriptor)
      .withName("constraint1")
      .withId(2)

  private val nodePropTypeConstraintDescriptor =
    ConstraintDescriptorFactory.typeForSchema(labelDescriptor, PropertyTypeSet.of(SchemaValueType.BOOLEAN))
      .withName("constraint10")
      .withId(10)

  private val relPropTypeConstraintDescriptor =
    ConstraintDescriptorFactory.typeForSchema(relTypeDescriptor, PropertyTypeSet.of(SchemaValueType.STRING))
      .withName("constraint11")
      .withId(11)

  private val nodeUniquenessConstraintInfo =
    ConstraintInfo(List(label), List(prop), Some(nodeUniquenessIndexDescriptor))

  private val relUniquenessConstraintInfo =
    ConstraintInfo(List(relType), List(prop), Some(relUniquenessIndexDescriptor))

  private val nodeKeyConstraintInfo = ConstraintInfo(List(label), List(prop), Some(nodeKeyIndexDescriptor))
  private val relKeyConstraintInfo = ConstraintInfo(List(relType), List(prop), Some(relKeyIndexDescriptor))
  private val nodeExistConstraintInfo = ConstraintInfo(List(label), List(prop), None)
  private val relExistConstraintInfo = ConstraintInfo(List(relType), List(prop), None)
  private val nodePropTypeConstraintInfo = ConstraintInfo(List(label), List(prop), None)
  private val relPropTypeConstraintInfo = ConstraintInfo(List(relType), List(prop), None)

  override def beforeEach(): Unit = {
    super.beforeEach()

    // Defaults:
    when(ctx.getConfig).thenReturn(Config.defaults())
    when(ctx.getAllIndexes()).thenReturn(Map(
      nodeUniquenessIndexDescriptor -> IndexInfo(
        IndexStatus("ONLINE", "", 100.0, Some(nodeUniquenessConstraintDescriptor)),
        List(label),
        List(prop)
      )
    ))
  }

  // Only checks the given parameters
  private def checkResult(
    resultMap: Map[String, AnyValue],
    name: Option[String],
    id: Option[Long] = None,
    constraintType: Option[String] = None,
    entityType: Option[String] = None,
    labelsOrTypes: Option[List[String]] = None,
    properties: Option[List[String]] = None,
    index: Option[String] = None,
    options: Option[AnyValue] = None,
    createStatement: Option[String] = None
  ): Unit = {
    id.foreach(expected => resultMap("id") should be(Values.longValue(expected)))
    name.foreach(expected => resultMap("name") should be(Values.stringValue(expected)))
    constraintType.foreach(expected => resultMap("type") should be(Values.stringValue(expected)))
    entityType.foreach(expected => resultMap("entityType") should be(Values.stringValue(expected)))
    labelsOrTypes.foreach(expected =>
      resultMap("labelsOrTypes") should be(VirtualValues.list(expected.map(Values.stringValue): _*))
    )
    properties.foreach(expected =>
      resultMap("properties") should be(VirtualValues.list(expected.map(Values.stringValue): _*))
    )
    index.foreach(expected => resultMap("ownedIndex") should be(Values.stringOrNoValue(expected)))
    options.foreach(expected => resultMap("options") should be(expected))
    createStatement.foreach(expected => resultMap("createStatement") should be(Values.stringValue(expected)))
  }

  private def setupAllConstraints(): Unit = {
    // Override returned indexes:
    when(ctx.getAllIndexes()).thenReturn(Map(
      nodeUniquenessIndexDescriptor -> IndexInfo(
        IndexStatus("ONLINE", "", 100.0, Some(nodeUniquenessConstraintDescriptor)),
        List(label),
        List(prop)
      ),
      relUniquenessIndexDescriptor -> IndexInfo(
        IndexStatus("ONLINE", "", 100.0, Some(relUniquenessConstraintDescriptor)),
        List(relType),
        List(prop)
      ),
      nodeKeyIndexDescriptor -> IndexInfo(
        IndexStatus("ONLINE", "", 100.0, Some(nodeKeyConstraintDescriptor)),
        List(label),
        List(prop)
      ),
      relKeyIndexDescriptor -> IndexInfo(
        IndexStatus("ONLINE", "", 100.0, Some(relKeyConstraintDescriptor)),
        List(relType),
        List(prop)
      )
    ))

    // Set-up which constraints the context returns:
    when(ctx.getAllConstraints()).thenReturn(Map(
      nodeUniquenessConstraintDescriptor -> nodeUniquenessConstraintInfo,
      relUniquenessConstraintDescriptor -> relUniquenessConstraintInfo,
      nodeKeyConstraintDescriptor -> nodeKeyConstraintInfo,
      relKeyConstraintDescriptor -> relKeyConstraintInfo,
      nodeExistConstraintDescriptor -> nodeExistConstraintInfo,
      relExistConstraintDescriptor -> relExistConstraintInfo,
      nodePropTypeConstraintDescriptor -> nodePropTypeConstraintInfo,
      relPropTypeConstraintDescriptor -> relPropTypeConstraintInfo
    ))
  }

  // Tests

  test("show constraints should give back correct default values") {
    // Set-up which constraints to return:
    when(ctx.getAllConstraints()).thenReturn(Map(
      nodeUniquenessConstraintDescriptor -> nodeUniquenessConstraintInfo,
      relExistConstraintDescriptor -> relExistConstraintInfo
    ))

    // When
    val showConstraints = ShowConstraintsCommand(AllConstraints, verbose = false, defaultColumns)
    val result = showConstraints.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 2
    checkResult(
      result.head,
      name = "constraint0",
      id = 1,
      constraintType = "UNIQUENESS",
      entityType = "NODE",
      labelsOrTypes = List(label),
      properties = List(prop),
      index = "constraint0"
    )
    checkResult(
      result.last,
      name = "constraint1",
      id = 2,
      constraintType = "RELATIONSHIP_PROPERTY_EXISTENCE",
      entityType = "RELATIONSHIP",
      labelsOrTypes = List(relType),
      properties = List(prop),
      index = Some(null)
    )
    // confirm no verbose columns:
    result.foreach(res => {
      res.keys.toList should contain noElementsOf List("options", "createStatement")
    })
  }

  test("show constraints should give back correct full values") {
    // Set-up which constraints to return:
    when(ctx.getAllConstraints()).thenReturn(Map(
      nodeUniquenessConstraintDescriptor -> nodeUniquenessConstraintInfo,
      relExistConstraintDescriptor -> relExistConstraintInfo
    ))

    // When
    val showConstraints = ShowConstraintsCommand(AllConstraints, verbose = true, allColumns)
    val result = showConstraints.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 2
    checkResult(
      result.head,
      name = "constraint0",
      id = 1,
      constraintType = "UNIQUENESS",
      entityType = "NODE",
      labelsOrTypes = List(label),
      properties = List(prop),
      index = "constraint0",
      options = optionsMap,
      createStatement = s"CREATE CONSTRAINT `constraint0` FOR (n:`$label`) REQUIRE (n.`$prop`) IS UNIQUE $optionsString"
    )
    checkResult(
      result.last,
      name = "constraint1",
      id = 2,
      constraintType = "RELATIONSHIP_PROPERTY_EXISTENCE",
      entityType = "RELATIONSHIP",
      labelsOrTypes = List(relType),
      properties = List(prop),
      index = Some(null),
      options = Values.NO_VALUE,
      createStatement = s"CREATE CONSTRAINT `constraint1` FOR ()-[r:`$relType`]-() REQUIRE (r.`$prop`) IS NOT NULL"
    )
  }

  test("show constraints should return the constraints sorted on name") {
    // Set-up which constraints to return, ordered descending by name:
    when(ctx.getAllConstraints()).thenReturn(Map(
      relExistConstraintDescriptor -> relExistConstraintInfo,
      nodeUniquenessConstraintDescriptor -> nodeUniquenessConstraintInfo
    ))

    // When
    val showConstraints = ShowConstraintsCommand(AllConstraints, verbose = false, defaultColumns)
    val result = showConstraints.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 2
    checkResult(result.head, name = "constraint0")
    checkResult(result.last, name = "constraint1")
  }

  test("show constraints should show all constraint types") {
    // Given
    setupAllConstraints()

    // When
    val showConstraints = ShowConstraintsCommand(AllConstraints, verbose = true, allColumns)
    val result = showConstraints.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 8
    checkResult(
      result.head,
      name = "constraint0",
      constraintType = "UNIQUENESS",
      entityType = "NODE",
      index = "constraint0",
      options = optionsMap,
      createStatement =
        s"CREATE CONSTRAINT `constraint0` FOR (n:`$label`) REQUIRE (n.`$prop`) IS UNIQUE $optionsString"
    )
    checkResult(
      result(1),
      name = "constraint1",
      constraintType = "RELATIONSHIP_PROPERTY_EXISTENCE",
      entityType = "RELATIONSHIP",
      index = Some(null),
      options = Values.NO_VALUE,
      createStatement =
        s"CREATE CONSTRAINT `constraint1` FOR ()-[r:`$relType`]-() REQUIRE (r.`$prop`) IS NOT NULL"
    )
    checkResult(
      result(2),
      name = "constraint10",
      id = 10,
      constraintType = "NODE_PROPERTY_TYPE",
      entityType = "NODE",
      labelsOrTypes = List(label),
      properties = List(prop),
      index = Some(null),
      options = Values.NO_VALUE,
      createStatement = s"CREATE CONSTRAINT `constraint10` FOR (n:`$label`) REQUIRE (n.`$prop`) IS :: BOOLEAN"
    )
    checkResult(
      result(3),
      name = "constraint11",
      id = 11,
      constraintType = "RELATIONSHIP_PROPERTY_TYPE",
      entityType = "RELATIONSHIP",
      labelsOrTypes = List(relType),
      properties = List(prop),
      index = Some(null),
      options = Values.NO_VALUE,
      createStatement = s"CREATE CONSTRAINT `constraint11` FOR ()-[r:`$relType`]-() REQUIRE (r.`$prop`) IS :: STRING"
    )
    checkResult(
      result(4),
      name = "constraint2",
      constraintType = "RELATIONSHIP_UNIQUENESS",
      entityType = "RELATIONSHIP",
      index = "constraint2",
      options = optionsMap,
      createStatement =
        s"CREATE CONSTRAINT `constraint2` FOR ()-[r:`$relType`]-() REQUIRE (r.`$prop`) IS UNIQUE $optionsString"
    )
    checkResult(
      result(5),
      name = "constraint3",
      constraintType = "NODE_KEY",
      entityType = "NODE",
      index = "constraint3",
      options = optionsMap,
      createStatement =
        s"CREATE CONSTRAINT `constraint3` FOR (n:`$label`) REQUIRE (n.`$prop`) IS NODE KEY $optionsString"
    )
    checkResult(
      result(6),
      name = "constraint4",
      constraintType = "RELATIONSHIP_KEY",
      entityType = "RELATIONSHIP",
      index = "constraint4",
      options = optionsMap,
      createStatement =
        s"CREATE CONSTRAINT `constraint4` FOR ()-[r:`$relType`]-() REQUIRE (r.`$prop`) IS RELATIONSHIP KEY $optionsString"
    )
    checkResult(
      result(7),
      name = "constraint5",
      constraintType = "NODE_PROPERTY_EXISTENCE",
      entityType = "NODE",
      index = Some(null),
      options = Values.NO_VALUE,
      createStatement =
        s"CREATE CONSTRAINT `constraint5` FOR (n:`$label`) REQUIRE (n.`$prop`) IS NOT NULL"
    )
  }

  test("show property uniqueness constraints should show property uniqueness constraint types") {
    // Given
    setupAllConstraints()

    // When
    val showConstraints = ShowConstraintsCommand(UniqueConstraints, verbose = true, allColumns)
    val result = showConstraints.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 2
    checkResult(
      result.head,
      name = "constraint0",
      constraintType = "UNIQUENESS",
      entityType = "NODE",
      index = "constraint0",
      options = optionsMap,
      createStatement =
        s"CREATE CONSTRAINT `constraint0` FOR (n:`$label`) REQUIRE (n.`$prop`) IS UNIQUE $optionsString"
    )
    checkResult(
      result.last,
      name = "constraint2",
      constraintType = "RELATIONSHIP_UNIQUENESS",
      entityType = "RELATIONSHIP",
      index = "constraint2",
      options = optionsMap,
      createStatement =
        s"CREATE CONSTRAINT `constraint2` FOR ()-[r:`$relType`]-() REQUIRE (r.`$prop`) IS UNIQUE $optionsString"
    )
  }

  test("show node property uniqueness constraints should show node property uniqueness constraint types") {
    // Given
    setupAllConstraints()

    // When
    val showConstraints = ShowConstraintsCommand(NodeUniqueConstraints, verbose = true, allColumns)
    val result = showConstraints.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 1
    checkResult(
      result.head,
      name = "constraint0",
      constraintType = "UNIQUENESS",
      entityType = "NODE",
      index = "constraint0",
      options = optionsMap,
      createStatement =
        s"CREATE CONSTRAINT `constraint0` FOR (n:`$label`) REQUIRE (n.`$prop`) IS UNIQUE $optionsString"
    )
  }

  test(
    "show relationship property uniqueness constraints should show relationship property uniqueness constraint types"
  ) {
    // Given
    setupAllConstraints()

    // When
    val showConstraints = ShowConstraintsCommand(RelUniqueConstraints, verbose = true, allColumns)
    val result = showConstraints.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 1
    checkResult(
      result.head,
      name = "constraint2",
      constraintType = "RELATIONSHIP_UNIQUENESS",
      entityType = "RELATIONSHIP",
      index = "constraint2",
      options = optionsMap,
      createStatement =
        s"CREATE CONSTRAINT `constraint2` FOR ()-[r:`$relType`]-() REQUIRE (r.`$prop`) IS UNIQUE $optionsString"
    )
  }

  test("show key constraints should show key constraint types") {
    // Given
    setupAllConstraints()

    // When
    val showConstraints = ShowConstraintsCommand(KeyConstraints, verbose = true, allColumns)
    val result = showConstraints.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 2
    checkResult(
      result.head,
      name = "constraint3",
      constraintType = "NODE_KEY",
      entityType = "NODE",
      index = "constraint3",
      options = optionsMap,
      createStatement =
        s"CREATE CONSTRAINT `constraint3` FOR (n:`$label`) REQUIRE (n.`$prop`) IS NODE KEY $optionsString"
    )
    checkResult(
      result.last,
      name = "constraint4",
      constraintType = "RELATIONSHIP_KEY",
      entityType = "RELATIONSHIP",
      index = "constraint4",
      options = optionsMap,
      createStatement =
        s"CREATE CONSTRAINT `constraint4` FOR ()-[r:`$relType`]-() REQUIRE (r.`$prop`) IS RELATIONSHIP KEY $optionsString"
    )
  }

  test("show node key constraints should show node key constraint types") {
    // Given
    setupAllConstraints()

    // When
    val showConstraints = ShowConstraintsCommand(NodeKeyConstraints, verbose = true, allColumns)
    val result = showConstraints.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 1
    checkResult(
      result.head,
      name = "constraint3",
      constraintType = "NODE_KEY",
      entityType = "NODE",
      index = "constraint3",
      options = optionsMap,
      createStatement =
        s"CREATE CONSTRAINT `constraint3` FOR (n:`$label`) REQUIRE (n.`$prop`) IS NODE KEY $optionsString"
    )
  }

  test("show relationship key constraints should show relationship key constraint types") {
    // Given
    setupAllConstraints()

    // When
    val showConstraints = ShowConstraintsCommand(RelKeyConstraints, verbose = true, allColumns)
    val result = showConstraints.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 1
    checkResult(
      result.head,
      name = "constraint4",
      constraintType = "RELATIONSHIP_KEY",
      entityType = "RELATIONSHIP",
      index = "constraint4",
      options = optionsMap,
      createStatement =
        s"CREATE CONSTRAINT `constraint4` FOR ()-[r:`$relType`]-() REQUIRE (r.`$prop`) IS RELATIONSHIP KEY $optionsString"
    )
  }

  test("show property existence constraints should show property existence constraint types") {
    // Given
    setupAllConstraints()

    // When
    val showConstraints = ShowConstraintsCommand(ExistsConstraints(ValidSyntax), verbose = true, allColumns)
    val result = showConstraints.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 2
    checkResult(
      result.head,
      name = "constraint1",
      constraintType = "RELATIONSHIP_PROPERTY_EXISTENCE",
      entityType = "RELATIONSHIP",
      index = Some(null),
      options = Values.NO_VALUE,
      createStatement =
        s"CREATE CONSTRAINT `constraint1` FOR ()-[r:`$relType`]-() REQUIRE (r.`$prop`) IS NOT NULL"
    )
    checkResult(
      result.last,
      name = "constraint5",
      constraintType = "NODE_PROPERTY_EXISTENCE",
      entityType = "NODE",
      index = Some(null),
      options = Values.NO_VALUE,
      createStatement =
        s"CREATE CONSTRAINT `constraint5` FOR (n:`$label`) REQUIRE (n.`$prop`) IS NOT NULL"
    )
  }

  test("show node property existence constraints should show node property existence constraint types") {
    // Given
    setupAllConstraints()

    // When
    val showConstraints = ShowConstraintsCommand(NodeExistsConstraints(), verbose = true, allColumns)
    val result = showConstraints.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 1
    checkResult(
      result.head,
      name = "constraint5",
      constraintType = "NODE_PROPERTY_EXISTENCE",
      entityType = "NODE",
      index = Some(null),
      options = Values.NO_VALUE,
      createStatement =
        s"CREATE CONSTRAINT `constraint5` FOR (n:`$label`) REQUIRE (n.`$prop`) IS NOT NULL"
    )
  }

  test(
    "show relationship property existence constraints should show relationship property existence constraint types"
  ) {
    // Given
    setupAllConstraints()

    // When
    val showConstraints = ShowConstraintsCommand(RelExistsConstraints(), verbose = true, allColumns)
    val result = showConstraints.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 1
    checkResult(
      result.head,
      name = "constraint1",
      constraintType = "RELATIONSHIP_PROPERTY_EXISTENCE",
      entityType = "RELATIONSHIP",
      index = Some(null),
      options = Values.NO_VALUE,
      createStatement =
        s"CREATE CONSTRAINT `constraint1` FOR ()-[r:`$relType`]-() REQUIRE (r.`$prop`) IS NOT NULL"
    )
  }

  test("show property type constraints should show property type constraint types") {
    // Given
    setupAllConstraints()

    // When
    val showConstraints = ShowConstraintsCommand(PropTypeConstraints, verbose = true, allColumns)
    val result = showConstraints.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 2
    checkResult(
      result.head,
      name = "constraint10",
      constraintType = "NODE_PROPERTY_TYPE",
      entityType = "NODE",
      index = Some(null),
      options = Values.NO_VALUE,
      createStatement =
        s"CREATE CONSTRAINT `constraint10` FOR (n:`$label`) REQUIRE (n.`$prop`) IS :: BOOLEAN"
    )
    checkResult(
      result.last,
      name = "constraint11",
      constraintType = "RELATIONSHIP_PROPERTY_TYPE",
      entityType = "RELATIONSHIP",
      index = Some(null),
      options = Values.NO_VALUE,
      createStatement =
        s"CREATE CONSTRAINT `constraint11` FOR ()-[r:`$relType`]-() REQUIRE (r.`$prop`) IS :: STRING"
    )
  }

  test("show node property type constraints should show node property type constraint types") {
    // Given
    setupAllConstraints()

    // When
    val showConstraints = ShowConstraintsCommand(NodePropTypeConstraints, verbose = true, allColumns)
    val result = showConstraints.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 1
    checkResult(
      result.head,
      name = "constraint10",
      constraintType = "NODE_PROPERTY_TYPE",
      entityType = "NODE",
      index = Some(null),
      options = Values.NO_VALUE,
      createStatement =
        s"CREATE CONSTRAINT `constraint10` FOR (n:`$label`) REQUIRE (n.`$prop`) IS :: BOOLEAN"
    )
  }

  test(
    "show relationship property type constraints should show relationship property type constraint types"
  ) {
    // Given
    setupAllConstraints()

    // When
    val showConstraints = ShowConstraintsCommand(RelPropTypeConstraints, verbose = true, allColumns)
    val result = showConstraints.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 1
    checkResult(
      result.head,
      name = "constraint11",
      constraintType = "RELATIONSHIP_PROPERTY_TYPE",
      entityType = "RELATIONSHIP",
      index = Some(null),
      options = Values.NO_VALUE,
      createStatement =
        s"CREATE CONSTRAINT `constraint11` FOR ()-[r:`$relType`]-() REQUIRE (r.`$prop`) IS :: STRING"
    )
  }
}
