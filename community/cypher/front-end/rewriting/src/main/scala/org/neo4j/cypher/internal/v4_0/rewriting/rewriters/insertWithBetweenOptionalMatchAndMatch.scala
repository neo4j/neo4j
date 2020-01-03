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
package org.neo4j.cypher.internal.v4_0.rewriting.rewriters

import org.neo4j.cypher.internal.v4_0.ast.{Match, ReturnItems, SingleQuery, With}
import org.neo4j.cypher.internal.v4_0.util.{Rewriter, topDown}

case object insertWithBetweenOptionalMatchAndMatch extends Rewriter {

  override def apply(that: AnyRef): AnyRef = instance(that)

  private val instance: Rewriter = topDown(Rewriter.lift {
    case sq@SingleQuery(clauses) if clauses.nonEmpty =>
      val newClauses = clauses.sliding(2).collect {
        case Seq(match1: Match, match2: Match) if match1.optional && !match2.optional =>
          val withStar = With(distinct = false, ReturnItems(includeExisting = true, Seq.empty)(match1.position), None, None, None, None)(match1.position)
          Seq(match1, withStar)
        case Seq(firstClause, _) => Seq(firstClause)
      }.flatten.toSeq :+ clauses.last
      SingleQuery(newClauses)(sq.position)
  })
}
