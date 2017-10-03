/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.frontend.v3_4.ast.rewriters

import org.neo4j.cypher.internal.aux.v3_4.{Rewriter, bottomUp}
import org.neo4j.cypher.internal.frontend.v3_4.ast._

case object createGraphIntroducesHorizon extends Rewriter {

  def apply(that: AnyRef): AnyRef = instance(that)

  private val instance = bottomUp(Rewriter.lift {
    case query@SingleQuery(clauses) =>
      val newClauses = clauses.flatMap {
        case clause@CreateNewSourceGraph(snapshot, graph, of, at) =>
          val createGraph = CreateRegularGraph(snapshot, graph, of, at)(clause.position)
          val p = clause.position
          val newWith = With(
            ReturnItems(includeExisting = true, Seq.empty)(p),
            GraphReturnItems(includeExisting = true, Seq(NewContextGraphs(GraphAs(graph, None)(p))(p)))(p)
          )(p)
          Seq(createGraph, newWith)

        case clause@CreateNewTargetGraph(snapshot, graph, of, at) =>
          val createGraph = CreateRegularGraph(snapshot, graph, of, at)(clause.position)
          val p = clause.position
          val newWith = With(
            ReturnItems(includeExisting = true, Seq.empty)(p),
            GraphReturnItems(includeExisting = true, Seq(NewTargetGraph(GraphAs(graph, None)(p))(p)))(p)
          )(p)
          Seq(createGraph, newWith)

        case clause =>
          Some(clause)
      }
      query.copy(clauses = newClauses)(query.position)
  })
}
