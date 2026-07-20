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
package org.neo4j.cypher.internal.frontend.phases.parserTransformers

import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.rewriting.rewriters.preparatoryRewriters.NormalizeWithAndReturnClauses
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer

case object NormalizeWithAndReturnClausesTransformer extends Transformer[BaseContext, BaseState, BaseState] {

  def rewriter(context: BaseContext): Rewriter =
    NormalizeWithAndReturnClauses(context.cypherExceptionFactory, Some(context.cypherVersion))

  override def transform(from: BaseState, context: BaseContext): BaseState = {
    from.withStatement(from.statement().endoRewrite(rewriter(context)))
  }

  override def name: String = "NormalizeWithAndReturnClausesTransformer"
  override def postConditions: Set[StepSequencer.Condition] = NormalizeWithAndReturnClauses.postConditions
}
