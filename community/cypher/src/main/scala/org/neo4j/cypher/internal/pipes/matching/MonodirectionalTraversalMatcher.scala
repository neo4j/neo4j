/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.pipes.matching

import org.neo4j.graphdb._
import org.neo4j.cypher.internal.pipes.{ExecutionContext, QueryState}
import collection.JavaConverters._
import org.neo4j.kernel.{Uniqueness, Traversal}
import traversal._

class MonoDirectionalTraversalMatcher(steps: ExpanderStep, start: (ExecutionContext) => Iterable[Node])
  extends TraversalMatcher {

  val initialStartStep = new InitialStateFactory[Option[ExpanderStep]] {
    def initialState(path: Path): Option[ExpanderStep] = Some(steps)
  }

  def baseTraversal(params: ExecutionContext): TraversalDescription = Traversal.
    traversal(Uniqueness.RELATIONSHIP_PATH).
    evaluator(new MyEvaluator).
    expand(new TraversalPathExpander(params), initialStartStep)


  def findMatchingPaths(state: QueryState, context: ExecutionContext): Iterator[Path] = {
    val arr = start(context).toArray

    baseTraversal(context).traverse(arr: _*).iterator().asScala
  }


  class ExpanderEvaluator extends PathEvaluator[Option[ExpanderStep]] {
    def evaluate(path: Path, state: BranchState[Option[ExpanderStep]]) = Evaluation.ofIncludes(state.getState.isEmpty)

    def evaluate(path: Path) = Evaluation.INCLUDE_AND_CONTINUE
  }

}

class MyEvaluator extends PathEvaluator[Option[ExpanderStep]] {
  def evaluate(path: Path, state: BranchState[Option[ExpanderStep]]) = state.getState match {
      case Some(step: ExpanderStep) if step.shouldInclude() => Evaluation.INCLUDE_AND_CONTINUE
      case None                                             => Evaluation.INCLUDE_AND_PRUNE
      case _                                                => Evaluation.EXCLUDE_AND_CONTINUE
    }

  def evaluate(path: Path) = throw new RuntimeException
}
