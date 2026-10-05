/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.operations.CypherFunctions
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Value

abstract class VectorValueConstructorFunction(
  vector: Expression,
  dimension: Expression
) extends Expression {

  def vectorFunction(v: AnyValue, d: AnyValue): Value

  override def apply(row: ReadableRow, state: QueryState): AnyValue = {
    vectorFunction(vector(row, state), dimension(row, state))
  }

  override def arguments: Seq[Expression] = Seq(vector, dimension)
  override def children: Seq[AstNode[_]] = arguments
}

case class Int8VectorValueConstructorFunction(vector: Expression, dimension: Expression)
    extends VectorValueConstructorFunction(vector, dimension) {
  override def vectorFunction(v: AnyValue, d: AnyValue): Value = CypherFunctions.int8Vector(v, d)
  override def rewrite(f: Expression => Expression): Expression = f(copy(f(vector), f(dimension)))
}

case class Int16VectorValueConstructorFunction(vector: Expression, dimension: Expression)
    extends VectorValueConstructorFunction(vector, dimension) {
  override def vectorFunction(v: AnyValue, d: AnyValue): Value = CypherFunctions.int16Vector(v, d)
  override def rewrite(f: Expression => Expression): Expression = f(copy(f(vector), f(dimension)))
}

case class Int32VectorValueConstructorFunction(vector: Expression, dimension: Expression)
    extends VectorValueConstructorFunction(vector, dimension) {
  override def vectorFunction(v: AnyValue, d: AnyValue): Value = CypherFunctions.int32Vector(v, d)
  override def rewrite(f: Expression => Expression): Expression = f(copy(f(vector), f(dimension)))
}

case class Int64VectorValueConstructorFunction(vector: Expression, dimension: Expression)
    extends VectorValueConstructorFunction(vector, dimension) {
  override def vectorFunction(v: AnyValue, d: AnyValue): Value = CypherFunctions.int64Vector(v, d)
  override def rewrite(f: Expression => Expression): Expression = f(copy(f(vector), f(dimension)))
}

case class Float32VectorValueConstructorFunction(vector: Expression, dimension: Expression)
    extends VectorValueConstructorFunction(vector, dimension) {
  override def vectorFunction(v: AnyValue, d: AnyValue): Value = CypherFunctions.float32Vector(v, d)
  override def rewrite(f: Expression => Expression): Expression = f(copy(f(vector), f(dimension)))
}

case class Float16VectorValueConstructorFunction(vector: Expression, dimension: Expression)
    extends VectorValueConstructorFunction(vector, dimension) {
  override def vectorFunction(v: AnyValue, d: AnyValue): Value = CypherFunctions.float16Vector(v, d)
  override def rewrite(f: Expression => Expression): Expression = f(copy(f(vector), f(dimension)))
}

case class BFloat16VectorValueConstructorFunction(vector: Expression, dimension: Expression)
    extends VectorValueConstructorFunction(vector, dimension) {
  override def vectorFunction(v: AnyValue, d: AnyValue): Value = CypherFunctions.bfloat16Vector(v, d)
  override def rewrite(f: Expression => Expression): Expression = f(copy(f(vector), f(dimension)))
}

case class Float64VectorValueConstructorFunction(vector: Expression, dimension: Expression)
    extends VectorValueConstructorFunction(vector, dimension) {
  override def vectorFunction(v: AnyValue, d: AnyValue): Value = CypherFunctions.float64Vector(v, d)
  override def rewrite(f: Expression => Expression): Expression = f(copy(f(vector), f(dimension)))
}
