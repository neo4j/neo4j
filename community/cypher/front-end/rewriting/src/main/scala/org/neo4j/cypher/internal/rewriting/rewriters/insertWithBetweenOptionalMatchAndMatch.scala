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

import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.rewriting.rewriters.factories.PreparatoryRewritingRewriterFactory
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.DefaultPostCondition
import org.neo4j.cypher.internal.util.StepSequencer.Step
import org.neo4j.cypher.internal.util.topDown

// Rewrites OPTIONAL MATCH (<n>) MATCH (<n>) RETURN <n> ==> OPTIONAL MATCH (<n>) WITH * MATCH (<n>) RETURN <n>
case object insertWithBetweenOptionalMatchAndMatch extends Step with DefaultPostCondition
    with PreparatoryRewritingRewriterFactory {

  override def preConditions: Set[StepSequencer.Condition] = Set.empty

  override def invalidatedConditions: Set[StepSequencer.Condition] = SemanticInfoAvailable

  val instance: Rewriter = topDown(Rewriter.lift {
    case sq @ SingleQuery(clauses) if clauses.nonEmpty =>
      val newClauses = clauses.sliding(2).collect {
        case Seq(match1: Match, match2: Match) if match1.optional && !match2.optional =>
          val withStar = With(
            distinct = false,
            ReturnItems(includeExisting = true, Seq.empty)(match1.position),
            None,
            None,
            None,
            None
          )(match1.position)
          Seq(match1, withStar)
        case Seq(firstClause, _) => Seq(firstClause)
      }.flatten.toSeq :+ clauses.last
      SingleQuery(newClauses)(sq.position)
  })

  override def getRewriter(cypherExceptionFactory: CypherExceptionFactory): Rewriter = instance
}
