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
package org.neo4j.cypher.internal.util

import org.neo4j.cypher.internal.util.StepSequencer.AccumulatedSteps
import org.neo4j.cypher.internal.util.StepSequencer.ByInitialCondition
import org.neo4j.cypher.internal.util.StepSequencer.Condition
import org.neo4j.cypher.internal.util.StepSequencer.MutableDirectedGraph
import org.neo4j.cypher.internal.util.StepSequencer.Step
import org.neo4j.cypher.internal.util.StepSequencer.StepAccumulator

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
 * This class allows to order steps with dependencies and combine them given a [[StepAccumulator]]
 *
 */
case class StepSequencer[S <: Step, ACC](stepAccumulator: StepAccumulator[S, ACC]) {
  /**
   * Order a set of steps and combine them using the classes stepAccumulator.
   *
   * The order guarantees that each step is run at least once,
   * and that all post-conditions of any steps are given at the end of the step sequence.
   * This means that some steps can appear more than once in the step sequence.
   *
   * @param steps the steps to order.
   * @param initialConditions conditions that hold before any of the steps.
   *                          These can be invalidated by steps and can appear as pre-conditions.
   *                          They cannot appear as post-conditions (yet).
   *                          If there are steps that invalidate initial conditions, they will not be part of the post-conditions
   *                          of the returned sequence.
   */
  def orderSteps(steps: Set[S], initialConditions: Set[Condition] = Set.empty): AccumulatedSteps[ACC] = {
    // For each post-condition, find the step that introduces it
    val introducingSteps: Map[Condition, Either[ByInitialCondition.type, S]] = {
      val is = for {
        step <- steps.toSeq
        _ = { if (step.postConditions.isEmpty) throw new IllegalArgumentException(s"Step $step has no post-conditions. That is not allowed.") }
        postCondition <- step.postConditions
        _ = { if (initialConditions.contains(postCondition)) throw new IllegalArgumentException(s"Step $step introduces $postCondition, which is an initial condition. That is currently not allowed.") }
      } yield postCondition -> Right(step)

      if (is.map(_._1).distinct != is.map(_._1)) {
        throw new IllegalArgumentException("It is not allowed for multiple steps to have the same post-conditions.")
      }

      (is ++ initialConditions.map(_ -> Left(ByInitialCondition))).toMap
    }
    // For each condition, all steps that invalidate it
    val invalidingSteps: Map[Condition, Set[S]] = {
      val is = for {
        step <- steps.toSeq
        invalidatedCondition <- step.invalidatedConditions
      } yield invalidatedCondition -> step
      is.groupBy(_._1)
        .mapValues(_.map(_._2).toSet)
        .withDefaultValue(Set.empty)
    }

    // To implement a safety check for circular disabling steps, we create a graph
    // where each step points at the steps it invalidates.
    // We then try a topological sort of that graph, which will fail if there are circles.
    val graphWithDisabling = new MutableDirectedGraph[S]
    steps.foreach(graphWithDisabling.add)
    steps.foreach { step =>
      // (a)-->(b) means a invalidates b's work
      step.invalidatedConditions.foreach { condition =>
        introducingSteps.getOrElse(condition, throw new IllegalArgumentException(s"There is no step introducing $condition. That is not allowed.")).
          foreach(graphWithDisabling.connect(step, _))
      }
    }
    StepSequencer.topologicalSort(graphWithDisabling, None, (_: S, _: mutable.Set[S]) => ())


    // Assemble a dependency graph for the actual ordering.
    val graph = new MutableDirectedGraph[S]
    steps.foreach(graph.add)
    steps.foreach { step =>
      // (a)-->(b) means a needs to happen before b
      step.preConditions.foreach { condition =>
        introducingSteps
          .getOrElse(condition, throw new IllegalArgumentException(s"There is no step introducing $condition. That is not allowed.")) match {
          case Left(ByInitialCondition) =>
            // Initial conditions cannot be re-enabled by any step.
            // Therefore, there is hard requirement that a step that has an initial condition as a pre-condition runs before any steps that invalidate it.
            invalidingSteps(condition).foreach(graph.connect(step, _))
          case Right(introducingStep) =>
            // The introducing step needs to happen before the one that has it as a pre-condition.
            graph.connect(introducingStep, step)
        }
      }
    }

    // Sort steps topologically
    val AccumulatedSteps(sortedSteps, postConditions) = StepSequencer.sort(graph, introducingSteps, steps.toSeq, initialConditions)

    // Put steps together
    AccumulatedSteps(sortedSteps.foldLeft(stepAccumulator.empty)(stepAccumulator.addNext), postConditions)
  }
}

object StepSequencer {

  private case object ByInitialCondition

  trait Condition

  trait Step {
    /**
     * @return the conditions that needs to be met before this step can be allowed to run.
     */
    def preConditions: Set[Condition]

    /**
     * @return the conditions that are guaranteed to be met after this step has run.
     *         Must not be empty, and must not contain any elements that are postConditions of other steps.
     */
    def postConditions: Set[Condition]

    /**
     * @return the conditions that this step invalidates as a side-effect of its work.
     */
    def invalidatedConditions: Set[Condition]
  }

  /**
   * This is used to accumulate the final sequence of steps with a foldLeft.
   * @tparam S the Step type
   * @tparam ACC the accumulator type
   */
  trait StepAccumulator[S <: Step, ACC] {
    def empty: ACC
    def addNext(acc: ACC, step: S): ACC
  }

  case class AccumulatedSteps[ACC](steps: ACC, postConditions: Set[Condition])

  private case class AdjacencyList[S](outgoing: mutable.Set[S], incoming: mutable.Set[S])

  private object MutableDirectedGraph {
    def copyOf[S](other: MutableDirectedGraph[S]): MutableDirectedGraph[S] = {
      val res = new MutableDirectedGraph[S]
      other.allNodes.foreach(res.add)

      other.allNodes.foreach { node =>
        other.outgoing(node).foreach { neighbor => res.connect(node, neighbor) }
      }

      res
    }
  }

  /**
   * A mutable directed graph implemented with AdjacencyLists.
   */
  private class MutableDirectedGraph[S] {
    private val elems = mutable.Map.empty[S, AdjacencyList[S]]

    /**
     * Add a node to the graph.
     */
    def add(elem: S): Unit = elems += elem -> AdjacencyList(mutable.Set.empty[S], mutable.Set.empty[S])

    /**
     * Add an edge to the graph. Both `from` and `to` must have been added to the graph previously.
     */
    def connect(from: S, to: S): Unit = {
      elems(from).outgoing += to
      elems(to).incoming += from
    }

    /**
     * Remove an edge from the graph.
     */
    def disconnect(from: S, to: S): Unit = {
      elems(from).outgoing -= to
      elems(to).incoming -= from
    }

    /**
     * @return all nodes in the graph
     */
    def allNodes: Set[S] = elems.keySet.toSet

    /**
     * @param from the origin node
     * @return all neighbors of `from` going via outgoing edges.
     */
    def outgoing(from: S): Set[S] = elems(from).outgoing.toSet

    /**
     * @param to the target node
     * @return all neighbors of `to` going via incoming edges.
     */
    def incoming(to: S): Set[S] = elems(to).incoming.toSet
  }

  /**
   * Instead of picking a random Step when the topological Sort offers multiple options,
   * we define this heuristic that will lead to better results given that steps can undo work of other steps.
   *
   * The heuristic is to order steps that invalidate many conditions first and
   * steps that have their conditions invalidated often last.
   * These two criteria are weighted equally.
   */
  private def heuristicStepOrdering[S <: Step](numberOfTimesEachStepIsInvalidated: Map[S, Int], allSteps: Seq[S]): Ordering[S] = {
    val fixedProductionSeed = 42L
    // In production, let's use the same seed to generate the same sequences reproducibly
    val random = new Random(fixedProductionSeed)
    if (AssertionRunner.isAssertionsEnabled) {
      val seed = random.nextLong()
      // If tests start failing because of a wrong order, print the seed here and use to reproduce the same order.
      random.setSeed(seed)
      // Putting the steps in a random order in test setup will help us discover dependencies we didn't know about.
      // If a query is suddenly failing because the order of steps changed, it is likely that there is a dependency we didn't capture.
    }

    val allStepsInRandomOrder = random.shuffle(allSteps)
    (x, y) => {
      val diffInvalidating = y.invalidatedConditions.size - x.invalidatedConditions.size
      val diffInvalidated = numberOfTimesEachStepIsInvalidated(x) - numberOfTimesEachStepIsInvalidated(y)

      val diff = diffInvalidating + diffInvalidated

      if (diff != 0) {
        diff
      } else {
        // We need to define a total ordering for SortedSets
        allStepsInRandomOrder.indexOf(x) - allStepsInRandomOrder.indexOf(y)
      }
    }
  }

  /**
   * Implements topological sort using Kahn's algorithm.
   * In addition, whenever a step is added that undoes work of other steps,
   * these steps are re-inserted to be run again.
   *
   * @param graph             the dependency graph.
   * @param introducingSteps  Map from condition to the step that introduces it.
   * @param allSteps          all steps
   * @param initialConditions all initially holding conditions
   */
  private def sort[S <: Step](graph: MutableDirectedGraph[S],
                              introducingSteps: Map[Condition, Either[ByInitialCondition.type, S]],
                              allSteps: Seq[S],
                              initialConditions: Set[Condition]): AccumulatedSteps[Seq[S]] = {
    val allPostConditions: Set[Condition] = allSteps.flatMap(_.postConditions)(collection.breakOut)

    val numberOfTimesEachStepIsInvalidated = allSteps
      .flatMap(_.invalidatedConditions.collect { case s if introducingSteps(s).isRight => introducingSteps(s).right.get })
      .groupBy(identity)
      .mapValues(_.size)
      .withDefaultValue(0)
    val order = heuristicStepOrdering(numberOfTimesEachStepIsInvalidated, allSteps)

    // We need to be able to look at the original state, so we make a copy in the beginning
    val workingGraph = MutableDirectedGraph.copyOf(graph)
    // During the algorithm keep track of all conditions currently enabled
    val currentConditions = initialConditions.to[mutable.Set]

    def dealWithInvalidatedConditions(nextStep: S, startPoints: mutable.Set[S]): Unit = {
      currentConditions ++= nextStep.postConditions
      currentConditions --= nextStep.invalidatedConditions
      if (nextStep.invalidatedConditions.nonEmpty) {
        // We need to reinsert parts of the graph for the work that n is undoing.
        val cannotStartFrom = mutable.Set.empty[S]
        val stepsThatHaveTheirWorkUndone = nextStep.invalidatedConditions.collect {
          case s if introducingSteps(s).isRight => introducingSteps(s).right.get
        }
        for (r <- stepsThatHaveTheirWorkUndone) {
          // Go through the original outgoing edges of r and restore them
          graph.outgoing(r)
            .filterNot(step => step.postConditions.subsetOf(currentConditions)) // All things which either haven't run or need to re-run
            .foreach { rDep =>
              workingGraph.connect(r, rDep)
              // make sure to remove all dependencies of r from startPoints, since they now have incoming edges again
              cannotStartFrom += rDep
            }
        }
        // re-insert rs into the working graph for re-processing
        startPoints ++= stepsThatHaveTheirWorkUndone
        startPoints --= cannotStartFrom
      }
    }

    val result = topologicalSort(workingGraph, Some(order), dealWithInvalidatedConditions)

    if (AssertionRunner.isAssertionsEnabled) {
      if (allSteps.exists(!result.contains(_))) {
        throw new IllegalStateException(s"The step sequence $result did not include all steps from $allSteps.")
      }
      if (!allPostConditions.subsetOf(currentConditions)) {
        throw new IllegalStateException(s"The step sequence $result did not lead to a state where all conditions $allPostConditions are met. " +
          s"Only meeting $currentConditions.")
      }
    }

    AccumulatedSteps(result, currentConditions.toSet)
  }

  /**
   * Implements topological sort using Kahn's algorithm.
   *
   * @param graph          the graph
   * @param order          optionally define an order to choose between multiple starting points.
   * @param nextStepChosen a callback to extend the algorithm. Called every time a next step was chosen.
   *                       The arguments to the callback are the next step which was just chosen and the current startPoints.
   *                       The callback is allowed to modify the startPoints.
   * @return a topological sort of the graph.
   */
  private def topologicalSort[S](graph: MutableDirectedGraph[S],
                                 order: Option[Ordering[S]],
                                 nextStepChosen: (S, mutable.Set[S]) => Unit): Seq[S] = {
    // Empty list that will contain the sorted elements
    val result = new ArrayBuffer[S]()

    // In the beginning we can start with all nodes with no incoming edge.
    val startPoints = {
      val nonEmpty = graph.allNodes.filter(graph.incoming(_).isEmpty)
      order match {
        case None => nonEmpty.to[mutable.Set]
        case Some(order) =>
          implicit val implicitOrder: Ordering[S] = order
          nonEmpty.to[mutable.SortedSet]
      }
    }

    while (startPoints.nonEmpty) {
      val n = startPoints.head
      startPoints.remove(n)
      result += n

      for (m <- graph.outgoing(n)) {
        graph.disconnect(n, m)
        if (graph.incoming(m).isEmpty) {
          startPoints += m
        }
      }

      nextStepChosen(n, startPoints)
    }

    // If any edges remain, there was a cycle
    if (graph.allNodes.map(graph.outgoing).exists(_.nonEmpty)) {
      throw new IllegalArgumentException("There was a cycle in the graph.")
    }

    result
  }
}
