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

import org.neo4j.cypher.internal.rewriting.AstRewritingMonitor
import org.neo4j.cypher.internal.rewriting.rewriters.deMorganRewriter
import org.neo4j.cypher.internal.rewriting.rewriters.distributeLawsRewriter
import org.neo4j.cypher.internal.rewriting.rewriters.flattenBooleanOperators
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeInequalities
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeSargablePredicates
import org.neo4j.cypher.internal.rewriting.rewriters.simplifyPredicates
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.inSequence

case object CNFNormalizer extends StatementRewriter {

  override def description: String = "normalize boolean predicates into conjunctive normal form"

  override def instance(context: BaseContext): Rewriter = {
    implicit val monitor = context.monitors.newMonitor[AstRewritingMonitor]()
    inSequence(
      deMorganRewriter(),
      distributeLawsRewriter(),
      normalizeInequalities,
      flattenBooleanOperators,
      simplifyPredicates,
      // Redone here since CNF normalization might introduce negated inequalities (which this removes)
      normalizeSargablePredicates
    )
  }

  override def postConditions: Set[Condition] = Set.empty
}
