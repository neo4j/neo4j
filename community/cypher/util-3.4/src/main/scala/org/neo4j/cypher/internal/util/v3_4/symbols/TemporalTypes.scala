/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.util.v3_4.symbols

object TemporalTypes {
  val datetime = new DateTimeType {
    val parentType = CTAny
    override val toString = "DateTime"
    override val toNeoTypeString = "DATETIME?"
  }
  val localdatetime = new LocalDateTimeType {
    val parentType = CTAny
    override val toString = "LocalDateTime"
    override val toNeoTypeString = "LOCALDATETIME?"
  }
  val date = new DateType {
    val parentType = CTAny
    override val toString = "Date"
    override val toNeoTypeString = "DATE?"
  }
  val time = new TimeType {
    val parentType = CTAny
    override val toString = "Time"
    override val toNeoTypeString = "TIME?"
  }
  val localtime = new LocalTimeType {
    val parentType = CTAny
    override val toString = "LocalTime"
    override val toNeoTypeString = "LOCALTIME?"
  }
  val duration = new DurationType {
    val parentType = CTAny
    override val toString = "Duration"
    override val toNeoTypeString = "DURATION?"
  }
}

sealed abstract class DateTimeType extends CypherType
sealed abstract class LocalDateTimeType extends CypherType
sealed abstract class DateType extends CypherType
sealed abstract class TimeType extends CypherType
sealed abstract class LocalTimeType extends CypherType
sealed abstract class DurationType extends CypherType
