/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.util.symbols

import org.neo4j.cypher.internal.util.BucketSize
import org.neo4j.cypher.internal.util.SizeBucket
import org.neo4j.cypher.internal.util.UnknownSize

case class CypherTypeInfo(typ: CypherType, sizeHint: BucketSize)

object CypherTypeInfo {
  final val BOOL = CypherTypeInfo(CTBoolean, UnknownSize)
  final val INT = CypherTypeInfo(CTInteger, UnknownSize)
  // because Javascript sees everything as floats, even integer values, we need to do this until properly
  // fixing semantic checking
  final val FLOAT = CypherTypeInfo(CTAny, UnknownSize)
  final val POINT = CypherTypeInfo(CTPoint, UnknownSize)
  final val DATE_TIME = CypherTypeInfo(CTDateTime, UnknownSize)
  final val LOCAL_DATE_TIME = CypherTypeInfo(CTLocalDateTime, UnknownSize)
  final val TIME = CypherTypeInfo(CTTime, UnknownSize)
  final val LOCAL_TIME = CypherTypeInfo(CTLocalTime, UnknownSize)
  final val DATE = CypherTypeInfo(CTDate, UnknownSize)
  final val DURATION = CypherTypeInfo(CTDuration, UnknownSize)
  final val MAP = CypherTypeInfo(CTMap, UnknownSize)
  final val ANY = CypherTypeInfo(CTAny, UnknownSize)

  def info(typ: CypherType, size: Int): CypherTypeInfo = CypherTypeInfo(typ, SizeBucket.computeBucket(size))
}
