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
package org.neo4j.cypher.internal.frontend.phases

import org.neo4j.cypher.internal.ast.CatalogName
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.GraphDirectReference
import org.neo4j.cypher.internal.ast.GraphReference
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.UseGraph
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildrenNewAccForSiblings
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Ref
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.topDown

case object RemoveDuplicateUseClauses extends StatementRewriter with StepSequencer.Step {

  override def instance(from: BaseState, context: BaseContext): Rewriter =
    new UseClauseRewriter(context.targetsComposite, context.sessionDatabaseName, context.cancellationChecker)

  override def preConditions: Set[StepSequencer.Condition] = Set(BaseContains[Statement]())

  override def postConditions: Set[StepSequencer.Condition] = Set.empty

  override def invalidatedConditions: Set[StepSequencer.Condition] = SemanticInfoAvailable

  /**
   * Rewriter that keeps track of the working graph and removes use clauses with the same target
   * @param sessionDatabaseName the database a user is connected to
   */
  private class UseClauseRewriter(
    targetsComposite: Boolean,
    sessionDatabaseName: String,
    cancellationChecker: CancellationChecker
  ) extends Rewriter {

    override def apply(that: AnyRef): AnyRef = {
      if (targetsComposite) {
        val workGraph = Option.when(sessionDatabaseName != null)(
          GraphDirectReference(CatalogName(sessionDatabaseName))(InputPosition.NONE)
        )
        recursiveUseClauseRewriter(that, workGraph)
      } else {
        that
      }
    }

    private def recursiveUseClauseRewriter(ref: AnyRef, workGraph: Option[GraphReference]): AnyRef = {
      case class State(useClausesToRemove: Set[Ref[Clause]], currentWorkGraph: Option[GraphReference])

      // Accumulate USE clauses to delete
      val State(useClausesToRemove, _) =
        ref.folder(cancellationChecker).treeFold(State(Set.empty[Ref[Clause]], workGraph)) {
          case _: SingleQuery =>
            acc =>
              // Restore current work graph when done processing a single query
              TraverseChildrenNewAccForSiblings(acc, _.copy(currentWorkGraph = acc.currentWorkGraph))

          case useGraph @ UseGraph(reference) => acc =>
              val newState = if (acc.currentWorkGraph.isEmpty || !acc.currentWorkGraph.contains(reference)) {
                State(acc.useClausesToRemove, Some(reference))
              } else {
                State(acc.useClausesToRemove + Ref(useGraph), acc.currentWorkGraph)
              }
              SkipChildren(newState)
        }

      // Actually delete the USE clauses
      val rewriter = topDown(
        Rewriter.lift {
          case sq @ SingleQuery(clauses) =>
            sq.copy(clauses.filterNot(c => useClausesToRemove.contains(Ref(c))))(sq.position)
        },
        cancellation = cancellationChecker
      )
      rewriter(ref)
    }
  }
}
