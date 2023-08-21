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

object StorableType {

  /**
   * The type of a property stored in a Node or Relationship
   */
  val storableType: TypeSpec =
    CTBoolean | CTNumber.covariant | CTPoint | CTString | CTDuration | CTDate | CTTime | CTLocalTime | CTLocalDateTime | CTDateTime |
      CTList(CTBoolean) | CTList(CTNumber) | CTList(CTInteger) | CTList(CTFloat) | CTList(CTPoint) | CTList(
        CTString
      ) | CTList(CTDuration) | CTList(CTDate) |
      CTList(CTTime) | CTList(CTLocalTime) | CTList(CTLocalDateTime) | CTList(CTDateTime)
}
