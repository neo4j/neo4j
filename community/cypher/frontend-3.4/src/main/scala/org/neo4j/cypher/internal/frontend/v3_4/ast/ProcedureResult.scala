/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.frontend.v3_4.ast

import org.neo4j.cypher.internal.util.v3_4.{ASTNode, InputPosition}
import org.neo4j.cypher.internal.frontend.v3_4.SemanticCheck
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticCheckResult.success

object ProcedureResult {
  def from(items: ProcedureResultItem*)(position: InputPosition): ProcedureResult =
    ProcedureResult(items.toIndexedSeq, None)(position)
}

case class ProcedureResult(items: IndexedSeq[ProcedureResultItem], where: Option[Where] = None)
                          (val position: InputPosition)
  extends ASTNode {

  def semanticCheck: SemanticCheck =
    items.foldSemanticCheck(_.semanticCheck) chain where.map(_.semanticCheck).getOrElse(success)
}
