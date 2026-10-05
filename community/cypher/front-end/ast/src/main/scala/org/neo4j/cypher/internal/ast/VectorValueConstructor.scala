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
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.BFloat16Type
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.symbols.Float16Type
import org.neo4j.cypher.internal.util.symbols.Float32Type
import org.neo4j.cypher.internal.util.symbols.FloatType
import org.neo4j.cypher.internal.util.symbols.Integer16Type
import org.neo4j.cypher.internal.util.symbols.Integer32Type
import org.neo4j.cypher.internal.util.symbols.Integer8Type
import org.neo4j.cypher.internal.util.symbols.IntegerType

case class VectorValueConstructor(vectorCandidate: Expression, dimension: Expression, typeName: CypherType)(
  val position: InputPosition
) extends Expression {

  def validVectorInnerType: Boolean = typeName match {
    case _: IntegerType   => true
    case _: Integer32Type => true
    case _: Integer16Type => true
    case _: Integer8Type  => true
    case _: FloatType     => true
    case _: Float32Type   => true
    case _: BFloat16Type  => true
    case _: Float16Type   => true
    case _                => false
  }

  override def isConstantForQuery: Boolean = vectorCandidate.isConstantForQuery && dimension.isConstantForQuery
}
