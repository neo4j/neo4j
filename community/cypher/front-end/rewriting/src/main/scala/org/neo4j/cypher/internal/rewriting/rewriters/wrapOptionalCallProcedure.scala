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
package org.neo4j.cypher.internal.rewriting.rewriters

import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.ScopeClauseSubqueryCall
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.rewriting.conditions.OptionalCallProcedureWrapped
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.rewriting.rewriters.factories.PreparatoryRewritingRewriterFactory
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.bottomUp

/**
 * Rewrites OPTIONAL CALL neo4j.proc(a,b) YIELD x, y to OPTIONAL CALL (*) { CALL neo4j.proc(a,b) YIELD x, y RETURN x, y }
 * Standalone procedure calls are not wrapped, e.g. OPTIONAL CALL neo4j.proc
 */
case object wrapOptionalCallProcedure extends StepSequencer.Step with PreparatoryRewritingRewriterFactory {

  override def preConditions: Set[StepSequencer.Condition] = Set()

  override def postConditions: Set[StepSequencer.Condition] = Set(OptionalCallProcedureWrapped)

  override def invalidatedConditions: Set[StepSequencer.Condition] = SemanticInfoAvailable

  private val rewriter: Rewriter = Rewriter.lift {
    case unresolved @ UnresolvedCall(_, _, _, Some(_), _, true) =>
      val pos = unresolved.position
      val copyResolved = unresolved.copy(optional = false)(pos)
      val returnItems = unresolved.returnVariables.explicitVariables.map(x => AliasedReturnItem(x))
      val returnClause =
        if (returnItems.nonEmpty) Seq(Return(ReturnItems(includeExisting = false, returnItems)(pos))(pos))
        else Seq.empty
      val innerQuery = SingleQuery(Seq(copyResolved) ++ returnClause)(pos)
      ScopeClauseSubqueryCall(innerQuery, isImportingAll = true, Seq.empty, None, optional = true)(pos)
  }

  private val instance = bottomUp(rewriter)

  override def getRewriter(
    cypherExceptionFactory: CypherExceptionFactory,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator
  ): Rewriter = instance
}
