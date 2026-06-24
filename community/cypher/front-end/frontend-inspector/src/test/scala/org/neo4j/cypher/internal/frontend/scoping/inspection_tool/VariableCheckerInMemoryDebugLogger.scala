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
package org.neo4j.cypher.internal.frontend.scoping.inspection_tool

import org.neo4j.cypher.internal.ast.semantics.scoping.WorkingScope
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.Acc
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.VariableCheckerDebugLogger
import org.neo4j.cypher.internal.frontend.scoping.inspection_tool.VariableCheckerInspectionPoint.AfterCheck
import org.neo4j.cypher.internal.frontend.scoping.inspection_tool.VariableCheckerInspectionPoint.BeforeCheck
import org.neo4j.cypher.internal.frontend.scoping.inspection_tool.VariableCheckerInspectionPoint.Visit
import org.neo4j.cypher.internal.util.Ref

import scala.collection.mutable.ArrayBuffer

class VariableCheckerInMemoryDebugLogger extends VariableCheckerDebugLogger {

  private val store: ArrayBuffer[(Ref[WorkingScope], Acc, VariableCheckerInspectionPoint)] = ArrayBuffer.empty

  override def logVisit(workingScope: WorkingScope, acc: Acc): Unit =
    store += ((Ref(workingScope), acc, Visit))

  override def logBeforeCheck(workingScope: WorkingScope, acc: Acc): Unit =
    store += ((Ref(workingScope), acc, BeforeCheck))

  override def logAfterCheck(workingScope: WorkingScope, acc: Acc): Unit =
    store += ((Ref(workingScope), acc, AfterCheck))

  def logs: Seq[(Ref[WorkingScope], Acc, VariableCheckerInspectionPoint)] = store.toSeq
}

sealed trait VariableCheckerInspectionPoint

object VariableCheckerInspectionPoint {
  case object Visit extends VariableCheckerInspectionPoint
  case object BeforeCheck extends VariableCheckerInspectionPoint
  case object AfterCheck extends VariableCheckerInspectionPoint
}
