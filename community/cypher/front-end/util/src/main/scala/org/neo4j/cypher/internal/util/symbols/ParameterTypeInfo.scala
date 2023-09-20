/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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

/**
 * Compiles type information about a parameter.
 * @param typ the type of the parameter
 * @param sizeHint a sizeHint, will be [[UnknownSize]] for all but CTList and CTSTring
 */
case class ParameterTypeInfo private (typ: CypherType, sizeHint: BucketSize)

object ParameterTypeInfo {
  final val BOOL = ParameterTypeInfo(CTBoolean, UnknownSize)
  final val INT = ParameterTypeInfo(CTInteger, UnknownSize)
  // because Javascript sees everything as floats, even integer values, we need to do this until properly
  // fixing semantic checking
  final val FLOAT = ParameterTypeInfo(CTAny, UnknownSize)
  final val POINT = ParameterTypeInfo(CTPoint, UnknownSize)
  final val DATE_TIME = ParameterTypeInfo(CTDateTime, UnknownSize)
  final val LOCAL_DATE_TIME = ParameterTypeInfo(CTLocalDateTime, UnknownSize)
  final val TIME = ParameterTypeInfo(CTTime, UnknownSize)
  final val LOCAL_TIME = ParameterTypeInfo(CTLocalTime, UnknownSize)
  final val DATE = ParameterTypeInfo(CTDate, UnknownSize)
  final val DURATION = ParameterTypeInfo(CTDuration, UnknownSize)
  final val MAP = ParameterTypeInfo(CTMap, UnknownSize)
  final val ANY = ParameterTypeInfo(CTAny, UnknownSize)
  final val STRING = ParameterTypeInfo(CTString, UnknownSize)

  def info(typ: CypherType, size: Int): ParameterTypeInfo = typ match {
    case CTString | ListType(_, _) => ParameterTypeInfo(typ, SizeBucket.computeBucket(size))
    case _                         => throw new IllegalArgumentException(s"size is only supported for List and String")
  }
}
