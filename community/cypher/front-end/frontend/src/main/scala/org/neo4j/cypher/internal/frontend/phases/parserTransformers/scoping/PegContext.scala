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
package org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.scoping.RegularContext
import org.neo4j.cypher.internal.ast.semantics.scoping.ScopeState.RecordedScopes
import org.neo4j.cypher.internal.ast.semantics.scoping.WorkingContext
import org.neo4j.cypher.internal.ast.semantics.scoping.WorkingScope
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Ref

case class PegContext(
  anonVarGen: AnonymousVariableNameGenerator,
  language: CypherVersion,
  semanticFeatures: Set[SemanticFeature],
  recordedScopes: RecordedScopes
) {

  /**
   * Looks up the working scope for the given astNode in recordedScopes and returns it only
   * when the cached scope is observationally equivalent to a freshly computed one.
   *
   * `References` hold `Ref[LogicalVariable]` whose equality
   * is identity-based (`eq`). A cached scope's Refs therefore point at specific objects, and
   * the cache is only safe to reuse when:
   *
   *   1. The looked-up `astNode` is the same JVM instance as the node the cached scope was
   *      built for. The map is keyed by `Ref[ASTNode]`, so `recordedScopes.get(Ref(astNode))`
   *      enforces this — a retrieved entry is, by construction, for the same instance.
   *   2. Every LogicalVariable in `incoming` is the same instance as the one the cached
   *      scope's enclosing context flowed in (`identityEquivalent(ws.incoming, incoming)`).
   *      That guarantees Refs that resolved to variables from the outer scope (via
   *      `References.connect(v, incoming.allSymbolsAndKeys)`) still point at live variables.
   *
   * A miss here is never a correctness failure — it just means `peg(...)` recomputes fresh
   * scope identities for the current AST. The structural `incoming ==` check is required for
   * correctness, ensuring that the incmoing is of the same Context type, the localCallables
   * are the same, .
   */
  def getRecordScopeOrElse[T <: ASTNode](
    astNode: T,
    incoming: RegularContext,
    inImportingWith: Boolean,
    foreachIterVar: Option[LogicalVariable],
    peg: (T, RegularContext) => WorkingScope
  ): WorkingScope = {
    recordedScopes.get(Ref(astNode)).flatMap {
      case ws: WorkingScope
        if ws.inImportingWith == inImportingWith &&
          ws.foreachIterVar == foreachIterVar &&
          ws.incoming == incoming &&
          PegContext.identityEquivalent(ws.incoming, incoming) =>
        Some(ws)
      case _ => None
    }.getOrElse(peg(astNode, incoming))
  }
}

object PegContext {

  /**
   * True iff every LogicalVariable in `a.allSymbolsAndKeys` has a same-name counterpart in
   * `b.allSymbolsAndKeys` that is the same instance (`eq`), and vice versa.
   */
  private def identityEquivalent(a: WorkingContext, b: WorkingContext): Boolean =
    sameInstances(a.allSymbolsAndKeys, b.allSymbolsAndKeys)

  private def sameInstances(as: Set[LogicalVariable], bs: Set[LogicalVariable]): Boolean = {
    if (as.size != bs.size) false
    else {
      val bRefs = bs.iterator.map(Ref(_)).toSet
      as.iterator.forall(v => bRefs.contains(Ref(v)))
    }
  }
}
