/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.kernel.api.helpers.traversal.ppbfs

import org.neo4j.common.EntityType
import org.neo4j.cypher.internal.logical.plans.TraversalPathMode
import org.neo4j.cypher.internal.runtime.RuntimeUtilTestSuite
import org.neo4j.cypher.internal.util.test_helpers.InMemoryGraph
import org.neo4j.graphdb.Direction
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.InMemoryRelationshipCursor.TraversedRel
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.NfaDsl.DslPart
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.PGPathPropagatingBFSTestBase.Nfa
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.PGPathPropagatingBFSTestBase.nfa
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.TracedPath.PathEntity
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.EventPPBFSHooks
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.EventRecorder
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.LoggingPPBFSHooks
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.PPBFSHooks
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.VisualizingPPBSHooks
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.PGStateBuilder
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.State
import org.neo4j.kernel.api.AssertOpen
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.memory.MemoryTracker
import org.neo4j.storageengine.api.RelationshipDirection

import scala.annotation.tailrec
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.language.postfixOps

trait PGPathPropagatingBFSTestBase { self: RuntimeUtilTestSuite =>

  /** Recursively compares groups of paths until we reach an expected group that is smaller than the remaining k */
  @tailrec
  private def compareGroups(
    resultGroups: List[Set[TracedPath]],
    expectedGroups: List[Set[TracedPath]],
    k: Int
  ): Unit = {
    (resultGroups, expectedGroups) match {
      case (res :: resTail, exp :: expTail) if exp.size <= k =>
        res shouldBe exp
        compareGroups(resTail, expTail, k - exp.size)

      case (res :: _, exp :: _) =>
        res.size shouldBe k
        exp should contain allElementsOf res

      case (res, Nil) =>
        res shouldBe empty

      case (Nil, _) =>
        k shouldBe 0
    }
  }

  case class FixtureBuilder[A](
    graph: InMemoryGraph,
    nfa: PGStateBuilder,
    source: Long,
    intoTarget: Long,
    searchMode: SearchMode,
    isGroup: Boolean,
    maxDepth: Int,
    k: Int,
    projection: SignpostStack => A,
    predicate: A => Boolean,
    mt: MemoryTracker,
    hooks: PPBFSHooks,
    assertOpen: AssertOpen,
    pathMode: TraversalPathMode
  ) {

    def withGraph(graph: InMemoryGraph): FixtureBuilder[A] = copy(graph = graph)

    def withNfa(f: PGStateBuilder => Unit): FixtureBuilder[A] = copy(nfa = {
      val builder = new PGStateBuilder
      f(builder)
      builder
    })
    def from(source: Long): FixtureBuilder[A] = copy(source = source)

    def into(intoTarget: Long, searchMode: SearchMode): FixtureBuilder[A] =
      copy(intoTarget = intoTarget, searchMode = searchMode)

    def into(intoTarget: Long): FixtureBuilder[A] =
      into(intoTarget, if (intoTarget == -1L) SearchMode.Unidirectional else SearchMode.Bidirectional)

    def withMode(searchMode: SearchMode): FixtureBuilder[A] = copy(searchMode = searchMode)
    def withPathMode(mode: TraversalPathMode): FixtureBuilder[A] = copy(pathMode = mode)
    def grouped: FixtureBuilder[A] = copy(isGroup = true)
    def withMaxDepth(maxDepth: Int): FixtureBuilder[A] = copy(maxDepth = maxDepth)
    def withK(k: Int): FixtureBuilder[A] = copy(k = k)
    def withMemoryTracker(mt: MemoryTracker): FixtureBuilder[A] = copy(mt = mt)
    def onAssertOpen(assertOpen: => Unit): FixtureBuilder[A] = copy(assertOpen = () => assertOpen)

    /** NB: wipes any configured filter, since the iterated item type will change */
    def project[B](projection: SignpostStack => B): FixtureBuilder[B] =
      FixtureBuilder[B](
        graph,
        nfa,
        source,
        intoTarget,
        searchMode,
        isGroup,
        maxDepth,
        k,
        projection,
        _ => true,
        mt,
        hooks,
        assertOpen,
        pathMode
      )

    def filter(predicate: A => Boolean): FixtureBuilder[A] = copy(predicate = predicate)

    def tracker(memoryTracker: MemoryTracker, hooks: PPBFSHooks): TraversalPathModeFactory =
      pathMode match {
        case TraversalPathMode.Walk    => TraversalPathModeFactory.walkMode()
        case TraversalPathMode.Trail   => TraversalPathModeFactory.trailMode(memoryTracker, hooks)
        case TraversalPathMode.Acyclic => TraversalPathModeFactory.acyclicMode(memoryTracker, hooks)
      }

    def build(createPathTracer: (MemoryTracker, PPBFSHooks) => PathTracer[A] =
      (memoryTracker, hooks) => new PathTracer(memoryTracker, tracker(memoryTracker, hooks), hooks)) =
      new PGPathPropagatingBFS[A](
        source,
        nfa.getStart.state,
        intoTarget,
        nfa.getFinal.state,
        searchMode,
        new InMemoryRelationshipCursor(graph, hooks),
        createPathTracer(mt, hooks),
        projection(_),
        predicate(_),
        isGroup,
        maxDepth,
        k,
        nfa.stateCount,
        mt,
        hooks,
        assertOpen,
        tracker(EmptyMemoryTracker.INSTANCE, hooks)
      )

    def toList: Seq[A] = build().asScala.toList

    def logged(level: LoggingPPBFSHooks = LoggingPPBFSHooks.debug): FixtureBuilder[A] = copy(hooks = level)

    /** Render graphviz to stdout */
    def viz(compress: Boolean): FixtureBuilder[A] = copy(hooks = new VisualizingPPBSHooks(compress))

    /** Run the iterator with event hooks attached */
    def events(): Seq[EventRecorder.Event] = {
      val recorder = new EventRecorder
      // run the iterator
      copy(hooks = new EventPPBFSHooks(recorder)).toList
      recorder.getEvents
    }

    /** Run the iterator and extract the entity ids from the yielded paths.
     * Graph IDs are unique across nodes and relationships so there is no risk of inadvertent overlap when comparing. */
    def paths()(implicit ev: A =:= TracedPath): Seq[Seq[Long]] =
      build().asScala.map(_.entities.map(_.id).toSeq).toSeq

    /** Runs a naive/slow DFS equivalent of the algorithm and compares results with the real implementation */
    def assertExpected()(implicit ev: A =:= TracedPath): Unit = {
      val result = this.toList.map(ev)
      val expected = allPaths()

      if (k == Int.MaxValue) {
        // with no K value set, we can just compare all the paths without worrying about selectivity
        result should contain theSameElementsAs expected
      } else {
        // if we have a limit then we need to ensure that the paths beneath that limit have been returned
        val orderedResults = OrderedResults.fromSeq(result)
        val orderedExpected = OrderedResults.fromSeq(expected)
        if (isGroup) {
          // this is the simpler case: we can apply the K limit to each target list since they are already grouped
          orderedResults shouldBe orderedExpected.takeGroups(k)
        } else {
          // this is the most complex case since for each target we have to account for nondeterminism in chosen paths
          orderedResults.targets shouldBe orderedExpected.targets

          for ((target, resultPaths) <- orderedResults.byTargetThenLength) {
            val expectedPaths = orderedExpected.paths(target)

            compareGroups(resultPaths, expectedPaths, k)
          }
        }
      }
    }

    /** Runs a recursive exhaustive depth first search to provide a comparison.
     * Note that this does not truncate the result set by K, since that would be nondeterministic for non-grouped cases */
    def allPaths()(implicit ev: A =:= TracedPath): Seq[TracedPath] = {

      def recurse(stack: List[PathEntity], node: Long, state: State, totalDepth: Int): Seq[TracedPath] = {
        if (maxDepth != -1 && totalDepth > maxDepth) {
          Seq.empty
        } else {
          val nodeJuxtapositions = state.getNodeJuxtapositions
            .filter(nj => nj.targetState().test(node))
            .flatMap(nj =>
              recurse(PathEntity.fromNode(nj.targetState(), node) :: stack, node, nj.targetState(), totalDepth)
            )

          val relExpansions = for {
            re <- state.getRelationshipExpansions
            (rel, dir) <- graph.nodeRels(node)
            nextNode = dir match {
              case RelationshipDirection.OUTGOING => rel.target
              case RelationshipDirection.INCOMING => rel.source
              case RelationshipDirection.LOOP     => node
              case null                           => fail("inexhaustive match")
            }
            matchesDirection = dir.matches(re.direction)
            matchesRelFilter = re.testRelationship(TraversedRel(rel, node), TraversalDirection.FORWARD)
            matchesNodePredicate = re.targetState().test(nextNode)
            hasUniqueRelationship = pathMode == TraversalPathMode.Walk || !stack.exists(e => e.id == rel.id)
            hasUniqueNode = pathMode != TraversalPathMode.Acyclic || !stack.exists(e =>
              e.entityType == EntityType.NODE && e.id == nextNode
            )
            if matchesDirection && matchesRelFilter && matchesNodePredicate && hasUniqueRelationship && hasUniqueNode

            newStack = PathEntity.fromNode(re.targetState(), nextNode) ::
              PathEntity.fromRel(re, rel.id) ::
              stack

            path <- recurse(newStack, nextNode, re.targetState(), totalDepth + 1)
          } yield path

          val wholePath = Option.when(state.isFinalState && (intoTarget == -1L || intoTarget == node))(new TracedPath(
            stack.reverse
          )).toSeq

          wholePath ++ nodeJuxtapositions ++ relExpansions
        }
      }

      recurse(List(PathEntity.fromNode(nfa.getStart.state, source)), source, nfa.getStart.state, 0)
        .filter(path => this.predicate(ev.flip(path)))
    }
  }

  protected def fixture(): FixtureBuilder[TracedPath] = FixtureBuilder[TracedPath](
    graph = InMemoryGraph.empty,
    nfa = new PGStateBuilder,
    source = -1L,
    intoTarget = -1L,
    SearchMode.Unidirectional,
    isGroup = false,
    maxDepth = -1,
    k = Int.MaxValue,
    projection = TracedPath.fromSignpostStack,
    predicate = _ => true,
    mt = EmptyMemoryTracker.INSTANCE,
    hooks = PPBFSHooks.NULL,
    assertOpen = () => (),
    pathMode = TraversalPathMode.Trail
  )

  /** Divides up paths by their target, then chunks them into groups of ascending path length */
  private case class OrderedResults(byTargetThenLength: Map[Long, List[Set[TracedPath]]]) {
    def targets: Set[Long] = byTargetThenLength.keySet
    def paths(target: Long): List[Set[TracedPath]] = byTargetThenLength(target)

    /** Applies the K limit to each target group list */
    def takeGroups(k: Int): OrderedResults = copy(byTargetThenLength =
      byTargetThenLength
        .view
        .mapValues(_.take(k))
        .toMap
    )

    override def toString: String = {
      val sb = new StringBuilder()
      sb.append("\nOrdered results:\n")
      byTargetThenLength.toSeq.sortBy(_._1).foreach { case (target, pathGroups) =>
        sb.append(s"- Target $target:\n")
        pathGroups.foreach { group =>
          val len = group.head.length
          sb.append(s"  - Length $len (${group.size} paths)\n")
          group.foreach { path =>
            sb.append(s"    - $path\n")
          }
        }
      }
      sb.toString()
    }
  }

  private object OrderedResults {

    def fromSeq(seq: Seq[TracedPath]): OrderedResults = OrderedResults(
      seq
        .groupBy(_.target)
        .view
        .mapValues(_.groupBy(_.length).toList.sortBy(_._1).map(_._2.toSet))
        .toMap
    )

  }

  import NfaDsl.Implicits._
  protected val `(s) ((a)-->(b))* (t)`: Nfa = nfa("s" |> ("a" --> "b"*) |> "t")
  protected val `(s) ((a)-->(b))+ (t)`: Nfa = nfa("s" |> ("a" --> "b" +) |> "t")
  protected val `(s) ((a)--(b))+ (t)`: Nfa = nfa("s" |> ("a" -- "b" +) |> "t")

  protected val `(s) ((a)--(b)--(c))* (t)`: Nfa =
    nfa("(s) ((a)--(b)--(c))* (t)") { sb =>
      val s = sb.newState("s", isStartState = true)
      val a = sb.newState("a")
      val b = sb.newState("b")
      val c = sb.newState("c")
      val t = sb.newState("t", isFinalState = true)

      s.addNodeJuxtaposition(a)
      a.addRelationshipExpansion(b, direction = Direction.BOTH)
      b.addRelationshipExpansion(c, direction = Direction.BOTH)
      c.addNodeJuxtaposition(a)
      c.addNodeJuxtaposition(t)
      s.addNodeJuxtaposition(t)
    }

}

object PGPathPropagatingBFSTestBase {

  class Nfa(name: String, construct: PGStateBuilder => Unit) extends Function[PGStateBuilder, Unit] {
    override def toString: String = name
    def apply(v1: PGStateBuilder): Unit = construct(v1)
  }

  def nfa(name: String)(construct: PGStateBuilder => Unit): Nfa = new Nfa(name, construct)
  def nfa(defn: DslPart): Nfa = nfa(defn.toString())(defn.build)
}
