/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.spi.v2_3

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.pipes.matching.{TraversalMatcher, ExpanderStep, TraversalPathExpander}
import org.neo4j.cypher.internal.compiler.v2_3.pipes.{EntityProducer, QueryState}
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.Argument
import org.neo4j.graphdb.traversal._
import org.neo4j.graphdb.{Node, Path}
import org.neo4j.helpers.ThisShouldNotHappenError
import org.neo4j.kernel.{Traversal, Uniqueness}

import scala.collection.JavaConverters._

class MonoDirectionalTraversalMatcher(steps: ExpanderStep, start: EntityProducer[Node])
  extends TraversalMatcher {

  val initialStartStep = new InitialStateFactory[Option[ExpanderStep]] {
    def initialState(path: Path): Option[ExpanderStep] = Some(steps)
  }

  def baseTraversal(params: ExecutionContext, state:QueryState): TraversalDescription = Traversal.
    traversal(Uniqueness.RELATIONSHIP_PATH).
    evaluator(new MyEvaluator).
    expand(new TraversalPathExpander(params, state), initialStartStep)


  def findMatchingPaths(state: QueryState, context: ExecutionContext): Iterator[Path] = {
    // TODO memory waste
    val arr = start(context, state).toArray

    baseTraversal(context, state).traverse(arr: _*).iterator().asScala
  }

  class ExpanderEvaluator extends PathEvaluator[Option[ExpanderStep]] {
    def evaluate(path: Path, state: BranchState[Option[ExpanderStep]]) = Evaluation.ofIncludes(state.getState.isEmpty)

    def evaluate(path: Path) = Evaluation.INCLUDE_AND_CONTINUE
  }

  def arguments: Seq[Argument] = start.arguments
}

class MyEvaluator extends PathEvaluator[Option[ExpanderStep]] {
  def evaluate(path: Path, state: BranchState[Option[ExpanderStep]]) = state.getState match {
      case Some(step: ExpanderStep) if step.shouldInclude() => Evaluation.INCLUDE_AND_CONTINUE
      case None                                             => Evaluation.INCLUDE_AND_PRUNE
      case _                                                => Evaluation.EXCLUDE_AND_CONTINUE
    }

  def evaluate(path: Path) = throw new ThisShouldNotHappenError("Andres", "This method should never be used")
}
