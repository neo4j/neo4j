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
package org.neo4j.cypher.internal.util

import org.neo4j.cypher.internal.util.StepSequencer.AccumulatedSteps
import org.neo4j.cypher.internal.util.StepSequencer.ByInitialCondition
import org.neo4j.cypher.internal.util.StepSequencer.Condition
import org.neo4j.cypher.internal.util.StepSequencer.MutableDirectedGraph
import org.neo4j.cypher.internal.util.StepSequencer.NegatedCondition
import org.neo4j.cypher.internal.util.StepSequencer.RepeatedSteps
import org.neo4j.cypher.internal.util.StepSequencer.Step

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
 * This class allows to order steps with dependencies and combine them into a list
 */
case class StepSequencer[S <: Step]() {

  /**
   * Order a set of steps.
   *
   * The order guarantees that each step is run at least once,
   * and that all post-conditions of any steps are given at the end of the step sequence.
   * This means that some steps can appear more than once in the step sequence.
   *
   * @param steps             the steps to order.
   * @param initialConditions conditions that hold before any of the steps.
   *                          These can be invalidated by steps and can appear as pre-conditions.
   *                          They cannot appear as post-conditions (yet).
   *                          If there are steps that invalidate initial conditions, they will not be part of the post-conditions
   *                          of the returned sequence.
   * @param fixedSeed         optionally a fixed seed for the random ordering
   */
  def orderSteps(
    steps: Set[S],
    initialConditions: Set[Condition] = Set.empty,
    printGraph: Boolean = false,
    fixedSeed: Option[Long] = None,
    repeatedSteps: RepeatedSteps = RepeatedSteps.Permitted
  ): AccumulatedSteps[S] = {
    // Abort if there is a negated initial condition
    initialConditions.foreach {
      case n: NegatedCondition => throw new IllegalArgumentException(s"Initial conditions cannot be negated: $n.")
      case _                   => // OK
    }

    // For each post-condition, find the step that introduces it
    val introducingSteps: Map[Condition, Either[StepSequencer.ByInitialCondition.type, S]] = {
      val is = for {
        step <- steps.toSeq
        _ = {
          if (step.postConditions.isEmpty)
            throw new IllegalArgumentException(s"Step $step has no post-conditions. That is not allowed.")
        }
        postCondition <- step.postConditions
        _ = {
          if (postCondition.isInstanceOf[NegatedCondition]) throw new IllegalArgumentException(
            s"Step $step has a negated post-condition: $postCondition. That is not allowed."
          )
        }
        _ = {
          if (initialConditions.contains(postCondition)) throw new IllegalArgumentException(
            s"Step $step introduces $postCondition, which is an initial condition. That is currently not allowed."
          )
        }
      } yield postCondition -> Right(step)

      is.groupBy(_._1)
        .filter(_._2.size > 1)
        .view
        .mapValues(_.map(_._2.toOption.get))
        .foreach {
          case (condition, steps) =>
            throw new IllegalArgumentException(s"Found same post-condition $condition in these steps: $steps.")
        }

      (is ++ initialConditions.map(_ -> Left(ByInitialCondition))).toMap
    }
    // For each condition, all steps that invalidate it
    val invalidatingSteps: Map[Condition, Set[S]] = {
      val is = for {
        step <- steps.toSeq
        invalidatedCondition <- step.invalidatedConditions
        _ = {
          if (invalidatedCondition.isInstanceOf[NegatedCondition]) throw new IllegalArgumentException(
            s"Step $step has an negated invalidated condition: $invalidatedCondition. That is not allowed."
          )
        }
      } yield invalidatedCondition -> step
      is.groupBy(_._1)
        .view
        .mapValues(_.map(_._2).toSet).toMap
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
        introducingSteps.get(condition)
          .foreach(_.foreach(graphWithDisabling.connect(step, _)))
      }
    }
    StepSequencer.topologicalSort(graphWithDisabling, None, (_: S, _: mutable.Set[S]) => ())

    // Assemble a dependency graph for the actual ordering.
    val graph = new MutableDirectedGraph[S]
    steps.foreach(graph.add)
    steps.foreach { step =>
      // A step might get scheduled twice if it is invalidated; to force unique steps we can encode invalidations
      // as inverted preconditions
      val preconditions = repeatedSteps match {
        case RepeatedSteps.Permitted => step.preConditions
        case RepeatedSteps.Forbidden => step.preConditions ++ step.invalidatedConditions.map(!_)
      }

      // (a)-->(b) means a needs to happen before b
      preconditions.foreach {
        case n @ NegatedCondition(inner) =>
          // For a negated precondition it is OK if there is no step that introduces it.
          introducingSteps.get(inner).foreach {
            case Left(ByInitialCondition) => throw new IllegalArgumentException(
                s"$step has $n as a pre-condition, but $inner is an initial condition. That is currently not allowed."
              )
            case Right(introducingStep) =>
              // The step with the negated pre-condition needs to happen before the introducing step.
              graph.connect(step, introducingStep)
          }
        case condition =>
          introducingSteps.getOrElse(
            condition,
            throw new IllegalArgumentException(s"There is no step introducing $condition. That is not allowed.")
          ) match {
            case Left(ByInitialCondition) =>
              // Initial conditions cannot be re-enabled by any step.
              // Therefore, there is hard requirement that a step that has an initial condition as a pre-condition runs before any steps that invalidate it.
              invalidatingSteps(condition)
                .filterNot(_ == step)
                .foreach(graph.connect(step, _))
            case Right(introducingStep) =>
              // The introducing step needs to happen before the one that has it as a pre-condition.
              graph.connect(introducingStep, step)
          }
      }
    }

    if (printGraph) {
      println(graph)
    }

    val introducingStepsNotByInitial = introducingSteps.collect {
      case (c, Right(step)) => c -> step
    }

    // Sort steps topologically
    val result =
      StepSequencer.sort(graph, introducingStepsNotByInitial, steps.toSeq, initialConditions, fixedSeed)

    if (printGraph) {
      println(result.steps.mkString("\n---\n", "\n", "\n---\n"))
    }

    result
  }
}

object StepSequencer {

  sealed trait RepeatedSteps

  object RepeatedSteps {
    object Permitted extends RepeatedSteps
    object Forbidden extends RepeatedSteps
  }

  private case object ByInitialCondition

  trait Condition {
    def unary_! : Condition = NegatedCondition(this)
  }

  private case class NegatedCondition(inner: Condition) extends Condition {
    override def toString: String = s"!$inner"
    override def unary_! : Condition = inner
  }

  trait Step {

    /**
     * @return the conditions that need to be met before this step can be allowed to run.
     */
    def preConditions: Set[Condition]

    /**
     * @return the conditions that are guaranteed to be met after this step has run.
     *         Must not be empty, and must not contain any elements that are postConditions of other steps.
     *         If there is a single post condition to a step, one can extend `DefaultPostCondition` to provide a post condition `completed`.
     */
    def postConditions: Set[Condition]

    /**
     * @return the conditions that this step invalidates as a side-effect of its work.
     */
    def invalidatedConditions: Set[Condition]
  }

  trait DefaultPostCondition {
    self: Step =>

    /**
     * Default condition for this step once completed.
     */
    object completed extends Condition {
      override def toString: String = self.toString + ".completed"
    }

    override def postConditions: Set[Condition] = Set(completed)
  }

  case class AccumulatedSteps[S](steps: Seq[S], postConditions: Set[Condition])

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

    override def toString: String = {
      val nodes = allNodes.toSeq.map(node => s"""  "$node";""").mkString("\n")
      val edges =
        allNodes.toSeq
          .map(node => outgoing(node).toSeq.map(other => s"""  "$node" -> "$other";""").mkString("\n"))
          .mkString("\n")
      s"digraph G {\n$nodes\n$edges\n}"
    }
  }

  /**
   * Instead of picking a random Step when the topological Sort offers multiple options,
   * we define this heuristic that will lead to better results given that steps can undo work of other steps.
   *
   * The heuristic is to order steps that invalidate many conditions first and
   * steps that have their conditions invalidated often last.
   * These two criteria are weighted equally.
   *
   * @param fixedSeed optionally a fixed seed for the random ordering
   */
  private def heuristicStepOrdering[S <: Step](
    numberOfTimesEachStepIsInvalidated: Map[S, Int],
    allSteps: Seq[S],
    fixedSeed: Option[Long]
  ): Ordering[S] = {
    val seed = fixedSeed getOrElse {
      if (AssertionRunner.isAssertionsEnabled) {
        // If tests start failing because of a wrong order, print the seed here and use to reproduce the same order.
        // Putting the steps in a random order in test setup will help us discover dependencies we didn't know about.
        // If a query is suddenly failing because the order of steps changed, it is likely that there is a dependency we didn't capture.
        new Random().nextLong()
      } else {
        // In production, let's use the same seed to generate the same sequences reproducibly
        42L
      }
    }

    val random = new Random(seed)

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
   * @param fixedSeed         optionally a fixed seed for the random ordering
   */
  private def sort[S <: Step](
    graph: MutableDirectedGraph[S],
    introducingSteps: Map[Condition, S],
    allSteps: Seq[S],
    initialConditions: Set[Condition],
    fixedSeed: Option[Long]
  ): AccumulatedSteps[S] = {
    val allPostConditions: Set[Condition] = allSteps.iterator.flatMap(_.postConditions).to(Set)

    val numberOfTimesEachStepIsInvalidated = allSteps
      .flatMap(_.invalidatedConditions.collect { case c if introducingSteps.contains(c) => introducingSteps(c) })
      .groupBy(identity)
      .view
      .mapValues(_.size)
      .toMap
      .withDefaultValue(0)
    val order = heuristicStepOrdering(numberOfTimesEachStepIsInvalidated, allSteps, fixedSeed)

    // We need to be able to look at the original state, so we make a copy in the beginning
    val workingGraph = MutableDirectedGraph.copyOf(graph)
    // During the algorithm keep track of all conditions currently enabled
    val currentConditions = initialConditions.to(mutable.Set)

    def dealWithInvalidatedConditions(nextStep: S, startPoints: mutable.Set[S]): Unit = {
      currentConditions ++= nextStep.postConditions
      if (nextStep.invalidatedConditions.nonEmpty) {
        // We need to reinsert parts of the graph for the work that n is undoing.
        val cannotStartFrom = mutable.Set.empty[S]

        // All steps which introduce a condition that gets invalidated
        val stepsThatHaveTheirWorkUndone: Set[S] =
          nextStep.invalidatedConditions
            .filter(currentConditions)
            .flatMap(introducingSteps.get)

        currentConditions --= nextStep.invalidatedConditions

        for (r <- stepsThatHaveTheirWorkUndone) {
          // All steps that depend on r ...
          graph.outgoing(r)
            // ... which either haven't run or need to re-run ...
            .filterNot(step =>
              step.postConditions.subsetOf(currentConditions)
            )
            // ... and which cannot run without having run r again first.
            .filterNot(step =>
              step.preConditions.subsetOf(currentConditions)
            )
            .foreach { stepAfterR =>
              // Restore the edges to those steps ...
              workingGraph.connect(r, stepAfterR)
              // ... and make sure to remove them from startPoints, since they now have incoming edges again.
              cannotStartFrom += stepAfterR
            }

          if (!r.preConditions.subsetOf(currentConditions)) {
            // r depends on some other steps that have had their work undone previously.
            // We need to restore incoming edges to those.
            val missingPreConditions = r.preConditions -- currentConditions
            graph.incoming(r)
              .filter(_.postConditions.intersect(missingPreConditions).nonEmpty)
              .foreach { stepBeforeR =>
                // Restore the edges to those steps ...
                workingGraph.connect(stepBeforeR, r)
                // ... and make sure to remove r from startPoints, since it now has incoming edges again.
                cannotStartFrom += r
              }
          }
        }
        // Re-insert rs into the working graph for re-processing ...
        startPoints ++= stepsThatHaveTheirWorkUndone
        // And remove all starting points which got incoming edges re-created.
        // This might overlap with stepsThatHaveTheirWorkUndone, so it's important this gets done 2nd.
        startPoints --= cannotStartFrom
      }
    }

    val result = topologicalSort(workingGraph, Some(order), dealWithInvalidatedConditions)

    if (AssertionRunner.isAssertionsEnabled) {
      if (allSteps.exists(!result.contains(_))) {
        throw new IllegalStateException(s"The step sequence $result did not include all steps from $allSteps.")
      }
      if (!allPostConditions.subsetOf(currentConditions)) {
        throw new IllegalStateException(
          s"The step sequence $result did not lead to a state where all conditions $allPostConditions are met. " +
            s"Only meeting $currentConditions."
        )
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
  private def topologicalSort[S](
    graph: MutableDirectedGraph[S],
    order: Option[Ordering[S]],
    nextStepChosen: (S, mutable.Set[S]) => Unit
  ): Seq[S] = {
    // Empty list that will contain the sorted elements
    val result = new ArrayBuffer[S]()

    // In the beginning we can start with all nodes with no incoming edge.
    val startPoints = {
      val nonEmpty = graph.allNodes.filter(graph.incoming(_).isEmpty)
      order match {
        case None => nonEmpty.to(mutable.Set)
        case Some(order) =>
          implicit val implicitOrder: Ordering[S] = order
          nonEmpty.to(mutable.SortedSet)
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
      throw new IllegalArgumentException(s"There was a cycle in the graph: $graph")
    }

    result
  }.toSeq
}
