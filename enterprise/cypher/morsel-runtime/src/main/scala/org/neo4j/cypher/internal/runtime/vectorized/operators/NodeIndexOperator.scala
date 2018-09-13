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

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.internal.kernel.api.{NodeIndexCursor, NodeValueIndexCursor}

abstract class NodeIndexOperator[CURSOR <: NodeIndexCursor](nodeOffset: Int) extends StreamingOperator {

  protected def iterate(currentRow: MorselExecutionContext, cursor: CURSOR, argumentSize: SlotConfiguration.Size): Boolean = {
    var cursorHasMore = true
    while (currentRow.hasMoreRows && cursorHasMore) {
      cursorHasMore = cursor.next()
      if (cursorHasMore) {
        currentRow.setLongAt(nodeOffset, cursor.nodeReference())
        extensionForEachRow(cursor, currentRow)
        currentRow.moveToNextRow()
      }
    }

    currentRow.finishedWriting()

    if (!cursorHasMore) {
      if (cursor != null) {
        cursor.close()
      }
    }
    cursorHasMore
  }

  /**
    * An extension point for subclasses to do more with each row.
    * This function is called in between `cursor.next()` and `currentRow.moveToNextRow()`
    */
  protected def extensionForEachRow(cursor: CURSOR, currentRow: MorselExecutionContext): Unit = {}
}

/**
  * Provides helper methods for index operators that get nodes together with actual property values.
  */
abstract class NodeIndexOperatorWithValues[CURSOR <: NodeValueIndexCursor](nodeOffset: Int, maybeValueFromIndexOffset: Option[Int])
  extends NodeIndexOperator[CURSOR](nodeOffset) {

  override protected def extensionForEachRow(cursor: CURSOR, currentRow: MorselExecutionContext): Unit = {
    maybeValueFromIndexOffset.foreach { valueOffset =>
      if (!cursor.hasValue) {
        // We were promised at plan time that we can get values everywhere, so this should never happen
        throw new IllegalStateException("NodeCursor unexpectedly had no values during index scan.")
      }
      val indexPropertyIndex = 0 // Because we only allow scan / contains with on single prop indexes
      val value = cursor.propertyValue(indexPropertyIndex)
      currentRow.setRefAt(valueOffset, value)
    }
  }
}
