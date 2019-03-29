/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.internal.collector

import java.time.ZonedDateTime
import java.util.{Locale, TimeZone}

import org.neo4j.cypher._

class DataCollectorMetaAcceptanceTest extends ExecutionEngineFunSuite {

  import DataCollectorMatchers._

  test("should get system data on retrieve('META')") {
    // when
    val res = execute("CALL db.stats.retrieve('META')")

    // then
    println(res.toList)
    res.toList.head should beMapContaining(
      "section" -> "META",
      "data" -> beMapContaining(
        "graphToken" -> null,
        "system" -> beSystemData
      )
    )
  }

  test("should get system data on retrieveAllAnonymized") {
    // when
    val res = execute("CALL db.stats.retrieveAllAnonymized('myGraphToken')")

    // then
    res.toList.head should beMapContaining(
      "section" -> "META",
      "data" -> beMapContaining(
        "graphToken" -> "myGraphToken",
        "system" -> beSystemData
      )
    )
  }

  test("should get internal data on retrieve('META')") {
    // when
    val res = execute("CALL db.stats.retrieve('META')")

    // then
    res.toList.head should beMapContaining(
      "section" -> "META",
      "data" -> beMapContaining(
        "graphToken" -> null,
        "internal" -> beInternalData
      )
    )
  }

  test("should get internal data on retrieveAllAnonymized") {
    // when
    val res = execute("CALL db.stats.retrieveAllAnonymized('myGraphToken')")

    // then
    res.toList.head should beMapContaining(
      "section" -> "META",
      "data" -> beMapContaining(
        "graphToken" -> "myGraphToken",
        "internal" -> beInternalData
      )
    )
  }

  private val beSystemData =
    beMapContaining(
      "jvmMemoryFree" -> (be > 0L),
      "jvmMemoryTotal" -> (be > 0L),
      "jvmMemoryMax" -> (be > 0L),
      "systemTotalPhysicalMemory" -> (be >= -1L),
      "systemFreePhysicalMemory" -> (be >= -1L),
      "systemCommittedVirtualMemory" -> (be >= -1L),
      "systemTotalSwapSpace" -> (be >= -1L),
      "systemFreeSwapSpace" -> (be >= -1L),
      "osArch" -> ofType[String],
      "osName" -> ofType[String],
      "osVersion" -> ofType[String],
      "availableProcessors" -> (be > 0),
      "byteOrder" -> ofType[String],
      "jvmName" -> ofType[String],
      "jvmVendor" -> ofType[String],
      "jvmVersion" -> ofType[String],
      "jvmJITCompiler" -> ofType[String],
      "userLanguage" -> Locale.getDefault.getLanguage,
      "userCountry" -> Locale.getDefault.getCountry,
      "userTimezone" -> TimeZone.getDefault.getID,
      "fileEncoding" -> System.getProperty( "file.encoding" )
    )

  private val beInternalData =
    beMapContaining(
      "numSilentQueryCollectionMisses" -> 0L
    )
}
