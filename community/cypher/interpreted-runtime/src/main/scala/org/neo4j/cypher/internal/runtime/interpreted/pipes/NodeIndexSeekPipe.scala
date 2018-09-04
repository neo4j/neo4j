/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.v3_5.logical.plans._
import org.neo4j.internal.kernel.api.IndexReference
import org.opencypher.v9_0.expressions.LabelToken
import org.opencypher.v9_0.util.attribution.Id

case class NodeIndexSeekPipe(ident: String,
                             label: LabelToken,
                             properties: Array[IndexedProperty],
                             valueExpr: QueryExpression[Expression],
                             indexMode: IndexSeekMode = IndexSeek)
                            (val id: Id = Id.INVALID_ID) extends Pipe with NodeIndexSeeker with IndexPipeWithValues {

  override val propertyIds: Array[Int] = properties.map(_.propertyKeyToken.nameId.id)

  override val propertyIndicesWithValues: Array[Int] = properties.zipWithIndex.filter(_._1.shouldGetValue).map(_._2)
  override val propertyNamesWithValues: Array[String] = propertyIndicesWithValues.map(offset => ident + "." + properties(offset).propertyKeyToken.name)
  private val needsValues: Boolean = propertyIndicesWithValues.nonEmpty

  private var reference: IndexReference = IndexReference.NO_INDEX

  private def reference(context: QueryContext): IndexReference = {
    if (reference == IndexReference.NO_INDEX) {
      reference = context.indexReference(label.nameId.id, propertyIds:_*)
    }
    reference
  }

  valueExpr.expressions.foreach(_.registerOwningPipe(this))

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val indexReference = reference(state.query)
    val baseContext = state.createOrGetInitialContext(executionContextFactory)

    val resultCreator =
      if (needsValues) CtxResultCreatorWithValues(baseContext)
      else CtxResultCreator(baseContext)
    indexSeek(state, indexReference, needsValues, baseContext, resultCreator)
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[NodeIndexSeekPipe]

  override def equals(other: Any): Boolean = other match {
    case that: NodeIndexSeekPipe =>
      (that canEqual this) &&
        ident == that.ident &&
        label == that.label &&
        (properties sameElements that.properties) &&
        valueExpr == that.valueExpr &&
        indexMode == that.indexMode
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(ident, label, properties.toSeq, valueExpr, indexMode)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
