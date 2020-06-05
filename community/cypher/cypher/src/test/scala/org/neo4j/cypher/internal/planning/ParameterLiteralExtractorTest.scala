/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.planning

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.storable.Values.byteArray
import org.neo4j.values.storable.Values.doubleValue
import org.neo4j.values.storable.Values.longValue
import org.neo4j.values.storable.Values.stringValue
import org.neo4j.values.virtual.VirtualValues

class ParameterLiteralExtractorTest extends CypherFunSuite {

  test("extract simple values") {
    extract(w => w.writeBoolean(true)) should equal(Values.TRUE)
    extract(w => w.writeBoolean(false)) should equal(Values.FALSE)
    extract(w => w.writeNull()) should equal(NO_VALUE)
    extract(w => w.writeLong(42)) should equal(longValue(42))
    extract(w => w.writeDouble(2.7)) should equal(doubleValue(2.7))
    extract(w => w.writeString("hello")) should equal(stringValue("hello"))
    extract(w => w.writeByteArray(Array(3, 2, 1))) should equal(byteArray(Array(3, 2, 1)))
  }

  test("should extract list") {
    val extractor = new ParameterLiteralExtractor
    extractor.beginList(3)
    extractor.writeString("1")
    extractor.writeDouble(2.0)
    extractor.writeLong(3)
    extractor.endList()

    extractor.value should equal(VirtualValues.list(stringValue("1"), doubleValue(2.0), longValue(3)))
  }

  test("should extract nested list") {
    val extractor = new ParameterLiteralExtractor
    extractor.beginList(3)
    extractor.writeString("1")
    extractor.beginList(2)
    extractor.writeDouble(2.0)
    extractor.writeLong(3)
    extractor.endList()
    extractor.writeNull()
    extractor.endList()

    extractor.value should equal(VirtualValues.list(
      stringValue("1"),
      VirtualValues.list(doubleValue(2.0), longValue(3)),
      NO_VALUE))
  }

  private def extract(writer :ParameterLiteralExtractor => Unit) = {
    val extractor = new ParameterLiteralExtractor
    writer(extractor)
    extractor.value
  }

}
