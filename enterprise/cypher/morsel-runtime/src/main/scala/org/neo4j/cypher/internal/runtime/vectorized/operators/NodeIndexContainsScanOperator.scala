/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.internal.kernel.api.{IndexOrder, IndexQuery, NodeValueIndexCursor}
import org.neo4j.values.storable.{TextValue, Values}
import org.opencypher.v9_0.util.CypherTypeException

class NodeIndexContainsScanOperator(longsPerRow: Int, refsPerRow: Int, offset: Int, label: Int, propertyKey: Int, valueExpr: Expression)
  extends NodeIndexOperator[NodeValueIndexCursor](longsPerRow, refsPerRow, offset) {

  override def operate(message: Message,
                       data: Morsel,
                       context: QueryContext,
                       state: QueryState): Continuation = {
    var valueIndexCursor: NodeValueIndexCursor  = null
    var iterationState: Iteration = null
    val read = context.transactionalContext.dataRead
    val index = context.transactionalContext.schemaRead.index(label, propertyKey)

    var nullExpression: Boolean = false

    message match {
      case StartLeafLoop(is) =>
        val currentRow = new MorselExecutionContext(data, longsPerRow, refsPerRow, currentRow = 0)
        val queryState = new OldQueryState(context, resources = null, params = state.params)
        val value = valueExpr(currentRow, queryState)

        valueIndexCursor = context.transactionalContext.cursors.allocateNodeValueIndexCursor()
        value match {
          case value: TextValue =>
            read.nodeIndexSeek(index, valueIndexCursor, IndexOrder.NONE, IndexQuery.stringContains(index.properties()(0), value.stringValue()))
          case Values.NO_VALUE =>
            // CONTAINS null does not produce any rows
            nullExpression = true
          case x => throw new CypherTypeException(s"Expected a string value, but got $x")
        }

        iterationState = is
      case ContinueLoopWith(ContinueWithSource(cursor, is)) =>
        valueIndexCursor = cursor.asInstanceOf[NodeValueIndexCursor]
        iterationState = is
      case _ => throw new IllegalStateException()
    }

    if(!nullExpression)
      iterate(data, valueIndexCursor, iterationState)
    else
      EndOfLoop(iterationState)
  }



  override def addDependency(pipeline: Pipeline): Dependency = NoDependencies
}
