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
package org.neo4j.cypher.internal.frontend.phases

import java.util.concurrent.atomic.LongAdder

import scala.collection.concurrent.TrieMap

sealed trait InternalSyntaxUsageStats {
  def incrementSyntaxUsageCount(key: SyntaxUsageMetricKey): Unit
  def getSyntaxUsageCount(key: SyntaxUsageMetricKey): Long
}

object InternalSyntaxUsageStats {

  def newImpl(): InternalSyntaxUsageStats = new InternalSyntaxUsageStatsImpl()

  case object InternalSyntaxUsageStatsNoOp extends InternalSyntaxUsageStats {
    override def incrementSyntaxUsageCount(key: SyntaxUsageMetricKey): Unit = ()

    override def getSyntaxUsageCount(key: SyntaxUsageMetricKey): Long = 0
  }

  private class InternalSyntaxUsageStatsImpl() extends InternalSyntaxUsageStats {
    private val counts: TrieMap[SyntaxUsageMetricKey, LongAdder] = new TrieMap()

    override def incrementSyntaxUsageCount(key: SyntaxUsageMetricKey): Unit = {
      val longAdder = counts.getOrElseUpdate(key, new LongAdder)
      longAdder.increment()
    }

    override def getSyntaxUsageCount(key: SyntaxUsageMetricKey): Long = {
      counts.get(key) match {
        case Some(l) => l.longValue()
        case _       => 0L
      }
    }
  }
}
