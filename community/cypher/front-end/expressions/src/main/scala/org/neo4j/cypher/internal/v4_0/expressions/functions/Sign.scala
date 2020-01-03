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

import org.neo4j.cypher.internal.v4_0.expressions.{TypeSignature, TypeSignatures}
import org.neo4j.cypher.internal.v4_0.util.symbols._

case object Sign extends Function with TypeSignatures {
  def name = "sign"

  override val signatures = Vector(
    TypeSignature(name, CTInteger, CTInteger, "Returns the signum of an integer number: 0 if the number is 0, -1 for any negative number, and 1 for any positive number."),
    TypeSignature(name, CTFloat, CTInteger, "Returns the signum of a floating point number: 0 if the number is 0, -1 for any negative number, and 1 for any positive number.")
  )
}
