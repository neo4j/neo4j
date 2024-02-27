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
import org.neo4j.cypher.operations.CypherFunctions.vectorSimilarity
import org.neo4j.kernel.api.impl.schema.vector.VectorSimilarity
import org.neo4j.values.AnyValue

case class VectorSimilarityFunction(similarity: VectorSimilarity, v1: Expression, v2: Expression)
    extends Expression {

  override def apply(row: ReadableRow, state: QueryState): AnyValue =
    vectorSimilarity(similarity, v1(row, state), v2(row, state))

  override def rewrite(f: Expression => Expression): Expression =
    f(VectorSimilarityFunction(similarity, v1.rewrite(f), v2.rewrite(f)))

  override def arguments: collection.Seq[Expression] = v1.arguments ++ v2.arguments

  override def children: collection.Seq[AstNode[_]] = Seq(v1, v2)

  override def toString: String = s"VectorSimilarity${similarity.name.capitalize}(${v1}, ${v2})"
}
