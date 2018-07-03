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
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyTypes
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker.entityIsNull
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.internal.kernel.api.helpers.RelationshipSelectionCursor
import org.opencypher.v9_0.expressions.SemanticDirection

class ExpandAllOperator(fromOffset: Int,
                        relOffset: Int,
                        toOffset: Int,
                        dir: SemanticDirection,
                        types: LazyTypes) extends StreamingOperator {

  override def init(queryContext: QueryContext, state: QueryState, inputMorsel: MorselExecutionContext): ContinuableOperatorTask =
    new OTask(inputMorsel)

  class OTask(val inputRow: MorselExecutionContext) extends ContinuableOperatorTask {

    /*
    This might look wrong, but it's like this by design. This allows the loop to terminate early and still be
    picked up at any point again - all without impacting the tight loop.
    The mutable state is an unfortunate cost for this feature.
     */
    var readPos = 0
    var relationships: RelationshipSelectionCursor = _

    override def operate(outputRow: MorselExecutionContext,
                         context: QueryContext,
                         state: QueryState): Unit = {

      while (inputRow.hasMoreRows && outputRow.hasMoreRows) {

        val fromNode = inputRow.getLongAt(fromOffset)
        if (entityIsNull(fromNode)) inputRow.moveToNextRow()
        else {
          if (relationships == null) {
            relationships = context.getRelationshipsCursor(fromNode, dir, types.types(context))
          }

          while (outputRow.hasMoreRows && relationships.next()) {
            val relId = relationships.relationshipReference()
            val otherSide = relationships.otherNodeReference()

            // Now we have everything needed to create a row.
            outputRow.copyFrom(inputRow)
            outputRow.setLongAt(relOffset, relId)
            outputRow.setLongAt(toOffset, otherSide)
            outputRow.moveToNextRow()
          }

          //we haven't filled up the rows
          if (outputRow.hasMoreRows) {
            relationships.close()
            relationships = null
            inputRow.moveToNextRow()
          }
        }
      }
      outputRow.finishedWriting()
    }

    override def canContinue: Boolean = inputRow.hasMoreRows || relationships != null
  }
}
