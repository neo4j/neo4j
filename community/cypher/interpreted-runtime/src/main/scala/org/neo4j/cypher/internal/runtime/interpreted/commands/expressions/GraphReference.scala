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

import org.neo4j.cypher.internal.ast.CatalogName
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.operations.GraphFunctions
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.virtual.GraphReferenceValue

import java.util.UUID

abstract class GraphReference extends Expression {
  def rewrite(f: Expression => Expression): Expression = f(this)

  def arguments: collection.Seq[Expression] = Seq.empty

  def children: collection.Seq[AstNode[_]] = Seq.empty

}

abstract class NameGraphReference extends GraphReference {

  def apply(row: ReadableRow, state: QueryState): AnyValue =
    new GraphReferenceValue(GraphFunctions.graphByName(
      name(row, state),
      state.query.transactionalContext.constituentTransactionFactory.sessionDatabase(),
      state.query.transactionalContext.securityContext
    ))

  protected def name(row: ReadableRow, state: QueryState): String
}

case class ConstantGraphReference(name: CatalogName) extends NameGraphReference {
  protected def name(row: ReadableRow, state: QueryState): String = name.qualifiedNameString
}

case class NameExpressionGraphReference(name: Expression) extends NameGraphReference {
  override def rewrite(f: Expression => Expression): Expression = f(NameExpressionGraphReference(f(name)))

  override def children: collection.Seq[AstNode[_]] = Seq(name)

  protected def name(row: ReadableRow, state: QueryState): String =
    name.apply(row, state) match {
      case x: TextValue => x.stringValue()
      case x            => throw new CypherTypeException(s"graph.byName requires text value; found '$x''")
    }
}

case class IdExpressionGraphReference(id: Expression) extends GraphReference {
  override def rewrite(f: Expression => Expression): Expression = f(NameExpressionGraphReference(f(id)))

  override def children: collection.Seq[AstNode[_]] = Seq(id)

  def apply(row: ReadableRow, state: QueryState): AnyValue = {
    val idStr = id.apply(row, state).asInstanceOf[TextValue].stringValue()
    val uuid = UUID.fromString(idStr)

    new GraphReferenceValue(GraphFunctions.graphById(
      uuid,
      state.query.transactionalContext.constituentTransactionFactory.sessionDatabase(),
      state.query.transactionalContext.securityContext
    ))
  }
}
