/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.spi.v3_1

import java.util.function.Predicate

import org.neo4j.cypher.internal.compiler.v3_1._
import org.neo4j.cypher.internal.compiler.v3_1.pipes.matching.{ExpanderStep, TraversalMatcher, TraversalPathExpander}
import org.neo4j.cypher.internal.compiler.v3_1.pipes.{EntityProducer, QueryState}
import org.neo4j.cypher.internal.compiler.v3_1.planDescription.Argument
import org.neo4j.graphdb.impl.traversal.StandardBranchCollisionDetector
import org.neo4j.graphdb.traversal.{BranchCollisionPolicy, _}
import org.neo4j.graphdb.{Node, Path}
import org.neo4j.kernel.impl.traversal.{BidirectionalTraversalDescriptionImpl, MonoDirectionalTraversalDescription}

import scala.collection.JavaConverters._

class BidirectionalTraversalMatcher(steps: ExpanderStep,
                                    start: EntityProducer[Node],
                                    end: EntityProducer[Node]) extends TraversalMatcher {

  lazy val reversedSteps = steps.reverse()

  val initialStartStep = new InitialBranchState[Option[ExpanderStep]] {
    def initialState(path: Path): Option[ExpanderStep] = Some(steps)
    def reverse() = this
  }

  val initialEndStep = new InitialBranchState[Option[ExpanderStep]] {
    def initialState(path: Path): Option[ExpanderStep] = Some(reversedSteps)
    def reverse() = this
  }
  val baseTraversal: TraversalDescription = new MonoDirectionalTraversalDescription().uniqueness(Uniqueness.RELATIONSHIP_PATH)
  val collisionDetector = new StepCollisionDetector

  def findMatchingPaths(state: QueryState, context: ExecutionContext): Iterator[Path] = {
    // TODO memory waste
    val s = start(context, state).toList
    val e = end(context, state).toList

    def produceTraversalDescriptions() = {
      val startWithoutCutoff = baseTraversal.expand(new TraversalPathExpander(context, state), initialStartStep)
      val endWithoutCutOff = baseTraversal.expand(new TraversalPathExpander(context, state), initialEndStep)

      steps.size match {
        case None       => (startWithoutCutoff, endWithoutCutOff)
        case Some(size) => {
          val startDepth = atLeastOne(size / 2)
          val endDepth = atLeastOne(size - startDepth)
          (startWithoutCutoff.evaluator(Evaluators.toDepth(startDepth)),
            endWithoutCutOff.evaluator(Evaluators.toDepth(endDepth)))
        }
      }
    }

    val (startDescription, endDescription) = produceTraversalDescriptions()

    val result = new BidirectionalTraversalDescriptionImpl()
      .startSide(startDescription)
      .endSide(endDescription)
      .collisionPolicy(collisionDetector)
      .traverse(s.asJava, e.asJava).iterator()

    result.asScala
  }

  def atLeastOne(i: Int): Int = if (i < 1) {
    1
  } else {
    i
  }

  class StepCollisionDetector extends StandardBranchCollisionDetector(null) with BranchCollisionPolicy {
    override def includePath(path: Path, startPath: TraversalBranch, endPath: TraversalBranch): Boolean = {
      val s = startPath.state().asInstanceOf[Option[ExpanderStep]]
      val e = endPath.state().asInstanceOf[Option[ExpanderStep]]

      def doBranchesMatch(startStep:ExpanderStep, endStep:ExpanderStep):(Boolean,Boolean)={
        val foundEnd = endStep.id + 1 == startStep.id
        val includeButDoNotPrune = endStep.id == startStep.id && endStep.shouldInclude() || startStep.shouldInclude()
        (foundEnd || includeButDoNotPrune, foundEnd)
      }

      def atEndOf(branch: TraversalBranch): (Boolean, Boolean) = (branch.length() == 0, true)

      val (include, prune) = (s, e) match {
        case (Some(startStep), Some(endStep)) => doBranchesMatch(startStep, endStep)
        case (Some(_), None)                  => atEndOf(startPath)
        case (None, Some(_))                  => atEndOf(endPath)
        case _                                => (false, false)
      }

      if (prune) {
        if (e.isDefined)
          startPath.prune()
        if (s.isDefined)
          endPath.prune()
      }

      include
    }

    override def create(evaluator: Evaluator, pathPredicate: Predicate[Path]) = new StepCollisionDetector

    def create(evaluator: Evaluator) = new StepCollisionDetector

  }

  def arguments: Seq[Argument] = Seq.empty // TODO: Remove this class. This is wrong. But not worth fixing plans for.
                                           // This class will die when Ronja rules the world
}
