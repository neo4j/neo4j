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
package org.neo4j.cypher.internal.runtime.spec.tests.index

import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.graphdb.schema.IndexType
import org.neo4j.internal.schema.AllIndexProviderDescriptors
import org.neo4j.internal.schema.IndexCapability
import org.neo4j.internal.schema.IndexProviderDescriptor
import org.neo4j.internal.schema.IndexQuery.IndexQueryType
import org.neo4j.kernel.api.impl.schema.TextIndexProvider
import org.neo4j.kernel.impl.index.schema.PointIndexProvider
import org.neo4j.kernel.impl.index.schema.RangeIndexProvider
import org.neo4j.values.storable.ValueCategory
import org.neo4j.values.storable.ValueGroup
import org.neo4j.values.storable.ValueType
import org.scalactic.source.Position

trait PropertyIndexTestSupport[CONTEXT <: RuntimeContext] {
  self: RuntimeTestSuite[CONTEXT] =>

  private val defaultSupportedTypes: Seq[ValueType] = {
    val unsupportedTypes = Set(ValueType.CHAR, ValueType.CHAR_ARRAY, ValueType.BYTE, ValueType.BYTE_ARRAY)
    ValueType.values().toSeq.filterNot(unsupportedTypes.contains)
  }

  // Parallel has no support for functions so we are limited to values that have literals
  protected val parallelSupportedTypes: Seq[ValueType] = {
    val supportedGroups = Set(
      ValueGroup.NUMBER,
      ValueGroup.NUMBER_ARRAY,
      ValueGroup.BOOLEAN,
      ValueGroup.BOOLEAN_ARRAY,
      ValueGroup.TEXT,
      ValueGroup.TEXT_ARRAY
    )
    defaultSupportedTypes.filter(t => supportedGroups.contains(t.valueGroup))
  }

  private val indexToTest: Seq[IndexInTest] = Seq[IndexInTest](
    IndexInTest(
      AllIndexProviderDescriptors.RANGE_DESCRIPTOR,
      RangeIndexProvider.CAPABILITY,
      IndexType.RANGE,
      supportedPropertyTypes()
    ),
    IndexInTest(
      AllIndexProviderDescriptors.TEXT_V1_DESCRIPTOR,
      TextIndexProvider.CAPABILITY,
      IndexType.TEXT,
      supportedPropertyTypes()
    ),
    IndexInTest(
      AllIndexProviderDescriptors.POINT_DESCRIPTOR,
      PointIndexProvider.CAPABILITY,
      IndexType.POINT,
      supportedPropertyTypes()
    )
  )

  def supportedPropertyTypes(): Seq[ValueType] = defaultSupportedTypes

  def testWithIndex(name: String)(testFun: IndexInTest => Any)(implicit pos: Position): Unit =
    testWithIndex(_ => true, name)(testFun)

  def testWithIndex(indexFilter: IndexInTest => Boolean, name: String)(testFun: IndexInTest => Any)(implicit
  pos: Position): Unit = {
    val indexToUse = indexToTest.filter(indexFilter)
    if (indexToUse.isEmpty) {
      fail("Found no index to test with.")
    } else {
      indexToUse.foreach { index =>
        test(s"$name (${index.indexType})")(testFun(index))
      }
    }
  }
}

class IndexInTest(
  capability: IndexCapability,
  valueTypeSupportByQuery: Map[IndexQueryType, Seq[ValueType]],
  val indexType: IndexType
) {
  override def toString: String = indexType.toString

  def supportsUniqueness(queryType: IndexQueryType): Boolean = {
    supports(queryType) &&
    indexType != IndexType.TEXT && indexType != IndexType.POINT // Is there no better way?
  }

  def supportsValues(query: IndexQueryType): Boolean = provideValueSupport(query).nonEmpty

  def supports(query: IndexQueryType, valueTypes: ValueType*): Boolean = {
    val support = querySupport(query)
    support.nonEmpty && valueTypes.forall(support.contains)
  }

  def supportsComposite(query: IndexQueryType, a: ValueCategory, b: ValueCategory): Boolean = {
    querySupport(query).nonEmpty && capability.areValueCategoriesAccepted(a, b)
  }

  def supportsOrderAsc(query: IndexQueryType, valueTypes: ValueType*): Boolean = {
    val support = orderAscSupport(query)
    support.nonEmpty && valueTypes.forall(support.contains)
  }

  def supportsOrderDesc(query: IndexQueryType, valueTypes: ValueType*): Boolean = {
    val support = orderDescSupport(query)
    support.nonEmpty && valueTypes.forall(support.contains)
  }

  def querySupport(query: IndexQueryType): Seq[ValueType] = valueTypeSupportByQuery(query)

  def provideValueSupport(query: IndexQueryType): Seq[ValueType] =
    if (capability.supportsReturningValues) querySupport(query) else Seq.empty

  def orderAscSupport(query: IndexQueryType): Seq[ValueType] =
    if (capability.supportsOrdering) querySupport(query) else Seq.empty

  def orderDescSupport(query: IndexQueryType): Seq[ValueType] =
    if (capability.supportsOrdering) querySupport(query) else Seq.empty
}

object IndexInTest {

  def apply(
    descriptor: IndexProviderDescriptor,
    capability: IndexCapability,
    indexType: IndexType,
    propertyTypes: Seq[ValueType]
  ): IndexInTest = {
    new IndexInTest(capability, buildValueTypeSupport(capability, propertyTypes), indexType)
  }

  private def buildValueTypeSupport(
    capability: IndexCapability,
    propertyTypes: Seq[ValueType]
  ): Map[IndexQueryType, Seq[ValueType]] = {
    IndexQueryType.values()
      .map(query => query -> propertyTypes.filter(t => capability.isQuerySupported(query, t.valueGroup.category())))
      .toMap
  }
}
