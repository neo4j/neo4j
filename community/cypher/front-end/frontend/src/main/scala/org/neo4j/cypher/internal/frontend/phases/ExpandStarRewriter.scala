/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.frontend.phases

import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.AST_REWRITE
import org.neo4j.cypher.internal.rewriting.conditions.containsNoReturnAll
import org.neo4j.cypher.internal.rewriting.rewriters.expandStar

object ExpandStarRewriter extends Phase[BaseContext, BaseState, BaseState] {

  def phase: CompilationPhaseTracer.CompilationPhase = AST_REWRITE

  def description: String = "expand *"

  def process(from: BaseState, context: BaseContext): BaseState =
    from.withStatement(from.statement().endoRewrite(expandStar(from.semantics())))

  override def postConditions: Set[Condition] =
    Set(StatementCondition(containsNoReturnAll))
}
