/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
import traversal._
import java.lang.{Iterable => JIterable}
import collection.JavaConverters._
import org.neo4j.kernel.{Uniqueness, Traversal}
import collection.Map

class MonoDirectionalTraversalMatcher(steps: ExpanderStep, start: (ExecutionContext) => Iterable[Node])
  extends TraversalMatcher {

  val initialStartStep = new InitialStateFactory[Option[ExpanderStep]] {
    def initialState(path: Path): Option[ExpanderStep] = Some(steps)
  }

  def baseTraversal(params:ExecutionContext): TraversalDescription = Traversal.
    traversal(Uniqueness.RELATIONSHIP_PATH).
    evaluator(new MyEvaluator).
    expand(new TraversalPathExpander(params), initialStartStep)


  def findMatchingPaths(state: QueryState, context: ExecutionContext): Iterable[Path] = {
    val arr = start(context).toArray

    val traverse = baseTraversal(context).traverse(arr: _*).asScala.toList
    traverse
  }


  class ExpanderEvaluator extends PathEvaluator[Option[ExpanderStep]] {
    def evaluate(path: Path, state: BranchState[Option[ExpanderStep]]) = Evaluation.ofIncludes(state.getState.isEmpty)

    def evaluate(path: Path) = Evaluation.INCLUDE_AND_CONTINUE
  }

}

class MyEvaluator extends PathEvaluator[Option[ExpanderStep]] {
  def evaluate(path: Path, state: BranchState[Option[ExpanderStep]]) = {
    if (state.getState.isEmpty)
      Evaluation.INCLUDE_AND_PRUNE
    else
      Evaluation.EXCLUDE_AND_CONTINUE
  }

  def evaluate(path: Path) = throw new RuntimeException
}
