/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.v4_0.expressions.functions

import org.neo4j.cypher.internal.v4_0.util.symbols._

case object ToString extends Function with FunctionWithInfo {
  override def name = "toString"

  val validInputTypes = Seq(
    CTFloat,
    CTInteger,
    CTBoolean,
    CTString,
    CTDuration,
    CTDate,
    CTTime,
    CTDateTime,
    CTLocalTime,
    CTLocalDateTime,
    CTPoint
  )

  override def getSignatureAsString: String = name + "(input :: ANY?) :: (STRING?)"

  override def getDescription: String =
    "Converts an integer, float, boolean or temporal type (i.e. Date, Time, LocalTime, DateTime, LocalDateTime or Duration) value to a string."
}
