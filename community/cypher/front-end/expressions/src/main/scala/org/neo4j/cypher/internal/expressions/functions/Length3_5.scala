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
package org.neo4j.cypher.internal.expressions.functions

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.TypeSignature
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTPath
import org.neo4j.cypher.internal.util.symbols.CTString

/**
 * A version of `length()` that is backwards-compatible with CYPHER 3.5.
 * Not implemented as a `Function` in order to not expose it directly in Cypher.
 */
case class Length3_5(argument: Expression)(val position: InputPosition) extends Expression {
  def signatures: Vector[TypeSignature] = Length3_5.signatures
  override def asCanonicalStringVal = s"length(${argument.asCanonicalStringVal})"
}

object Length3_5 {
  val signatures: Vector[TypeSignature] = Vector(
    TypeSignature(outputType = CTInteger, argumentTypes = Vector(CTPath)),
    TypeSignature(outputType = CTInteger, argumentTypes = Vector(CTString)),
    TypeSignature(outputType = CTInteger, argumentTypes = Vector(CTList(CTAny))),
  )
}
