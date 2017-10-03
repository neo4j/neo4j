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
package org.neo4j.cypher.internal.frontend.v3_4.phases

import org.neo4j.cypher.internal.aux.v3_4.{Rewriter, inSequence}
import org.neo4j.cypher.internal.frontend.v3_4.ast.rewriters._

object LateAstRewriting extends StatementRewriter {
  override def instance(context: BaseContext): Rewriter = inSequence(
    collapseMultipleInPredicates,
    nameUpdatingClauses,
    projectNamedPaths,
//    enableCondition(containsNamedPathOnlyForShortestPath), // TODO Re-enable
    projectFreshSortExpressions
  )

  override def description: String = "normalize the AST"

  override def postConditions: Set[Condition] = Set.empty
}
