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
package org.neo4j.cypher.internal.runtime.spec

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.values.storable.RandomValues
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.ValueCategory
import org.neo4j.values.storable.ValueGroup
import org.neo4j.values.storable.ValueType
import org.scalatest.Outcome
import org.scalatest.TestSuite
import org.scalatest.TestSuiteMixin

import java.lang.System.lineSeparator
import scala.util.Random

trait RandomValuesTestSupport extends TestSuiteMixin with TestSuite {
  self: CypherFunSuite =>

  private val initialSeed = Random.nextLong()

  val randomValues: RandomValues = {
    RandomValues.create(new java.util.Random(initialSeed))
  }

  private val _propertyValueTypes: Array[ValueType] = {
    val propertyTypeCategories = Set(ValueCategory.TEXT, ValueCategory.NUMBER, ValueCategory.BOOLEAN, ValueCategory.TEMPORAL)
    ValueType.values().filter(v => propertyTypeCategories.contains(v.valueGroup.category()) && v != ValueType.CHAR)
  }

  val stringValueTypes: Array[ValueType] = {
    ValueType.values().filter(v => v.valueClass == classOf[TextValue])
  }

  val numericValueTypes: Array[ValueType] = {
    ValueType.values().filter(v => v.valueGroup == ValueGroup.NUMBER)
  }

  def propertyValueTypes: Array[ValueType] = _propertyValueTypes
  def randomPropertyType(): ValueType = randomValues.among(propertyValueTypes)
  def randomValue(valueType: ValueType): Value = randomValues.nextValueOfType(valueType)
  def randomValues(size: Int, valueTypes: ValueType*): Array[Value] = randomValues.nextValuesOfTypes(size, valueTypes:_*)
  def randomAmong[T](values: Seq[T]): T = values(randomValues.nextInt(values.size))
  def randomPropertyValue(): Value = randomValues.nextValueOfTypes(propertyValueTypes:_*)

  abstract override def withFixture(test: NoArgTest): Outcome = {
    withClue(s"Initial Random Seed: $initialSeed${lineSeparator()}")(super.withFixture(test))
  }
}
