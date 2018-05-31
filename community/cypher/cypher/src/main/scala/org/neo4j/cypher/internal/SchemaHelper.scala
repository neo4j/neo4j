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
package org.neo4j.cypher.internal

import java.util.concurrent.atomic.AtomicLong

import org.neo4j.cypher.CypherVersion
import org.neo4j.kernel.api.query.SchemaIndexUsage
import org.neo4j.kernel.impl.query.TransactionalContext

case class SchemaToken(x: Long) extends AnyVal

class SchemaHelper(val queryCache: QueryCache[_,_]) {

  private val schemaToken = new AtomicLong()

  def readSchemaToken(tc: TransactionalContext): SchemaToken = {
    val creator = new java.util.function.Function[SchemaHelper, SchemaToken]() {
      def apply(key: SchemaHelper): SchemaToken = {
        queryCache.clear()
        SchemaToken(schemaToken.incrementAndGet())
      }
    }
    tc.kernelTransaction().schemaRead().schemaStateGetOrCreate(this, creator)
  }

  def lockLabels(schemaTokenBefore: SchemaToken,
                 executionPlan: ExecutionPlan,
                 version: CypherVersion,
                 tc: TransactionalContext): Boolean = {
    val labelIds: Seq[Long] = extractPlanLabels(executionPlan, version, tc)
    if (labelIds.nonEmpty) {
      lockPlanLabels(tc, labelIds)
      val schemaTokenAfter = readSchemaToken(tc)

      // if the schema has changed while taking all locks we release locks and return false
      if (schemaTokenBefore != schemaTokenAfter) {
        releasePlanLabels(tc, labelIds)
        return false
      }
    }
    true
  }

  private def extractPlanLabels(plan: ExecutionPlan, version: CypherVersion, tc: TransactionalContext): Seq[Long] = {
    import scala.collection.JavaConverters._

    def planLabels = {
      plan.plannerInfo.indexes().asScala.collect { case item: SchemaIndexUsage => item.getLabelId.toLong }
    }

    def allLabels: Seq[Long] = {
      tc.kernelTransaction.tokenRead().labelsGetAllTokens().asScala.map(t => t.id().toLong).toSeq
    }

    version match {
      // old cypher versions plans do not contain information about indexes used in query
      // and since we do not know what labels are actually used by the query we assume that all of them are
      case CypherVersion.v2_3 => allLabels
      case CypherVersion.v3_1 => allLabels
      case _ => planLabels
    }
  }

  private def releasePlanLabels(tc: TransactionalContext, labelIds: Seq[Long]): Unit =
    tc.kernelTransaction.locks().releaseSharedLabelLock(labelIds.toArray[Long]:_*)

  private def lockPlanLabels(tc: TransactionalContext, labelIds: Seq[Long]): Unit =
    tc.kernelTransaction.locks().acquireSharedLabelLock(labelIds.toArray[Long]:_*)

}
