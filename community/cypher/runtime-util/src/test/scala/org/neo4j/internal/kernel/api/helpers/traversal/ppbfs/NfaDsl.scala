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

import org.neo4j.cypher.internal.runtime.RuntimeUtilTestSuite
import org.neo4j.cypher.internal.util.Repetition
import org.neo4j.cypher.internal.util.UpperBound.Limited
import org.neo4j.cypher.internal.util.UpperBound.Unlimited
import org.neo4j.function.Predicates
import org.neo4j.graphdb.Direction
import org.neo4j.internal.kernel.api.RelationshipTraversalEntities
import org.neo4j.internal.kernel.api.helpers.traversal.SlotOrName
import org.neo4j.internal.kernel.api.helpers.traversal.SlotOrName.VarName
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.NfaDsl.DslPart
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.NfaDsl.DslPart.Juxtaposition
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.NfaDsl.DslPart.RelExpansion
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.NfaDsl.DslPart.RepeatedRelExpansion
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.NfaDsl.DslPart.State
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.PGStateBuilder
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.PGStateBuilder.BuilderState

import java.util.function.LongPredicate
import java.util.function.Predicate

import scala.collection.mutable
import scala.language.implicitConversions
import scala.language.postfixOps
import scala.util.hashing.MurmurHash3

/**
 * Import for fluent cypher-like NFA builder for use in runtime unit tests
 */
object NfaDsl {

  private case class StateChain(first: BuilderState, last: BuilderState) {

    def connect(state: BuilderState) = {
      last.addNodeJuxtaposition(state)
      StateChain(first, state)
    }

    def connect(other: StateChain): StateChain = {
      last.addNodeJuxtaposition(other.first)
      StateChain(first, other.last)
    }

    def connectLoop() = {
      last.addNodeJuxtaposition(first)
    }
  }

  private object StateChain {

    def connectAll(chains: Seq[StateChain]): StateChain =
      chains.reduce(_ connect _)

    def repeat(times: Long)(f: => StateChain): StateChain =
      connectAll((0L until times).map(_ => f))
  }

  sealed trait DslPart {

    /** Create a node juxtaposition between this part and the other */
    def |>(other: DslPart): Juxtaposition = Juxtaposition(this, other)

    /** Construct the NFA */
    def build(sb: PGStateBuilder): Unit = {
      def recurse(
        defn: DslPart,
        isFirst: Boolean,
        isLast: Boolean,
        prevState: BuilderState
      ): StateChain =
        defn match {
          case RepeatedRelExpansion(defn, Repetition(0, Unlimited)) =>
            val chain = recurse(defn, isFirst, isLast, prevState)
            chain.connectLoop()

            val anonEndState = sb.newState("anon")
            prevState.addNodeJuxtaposition(anonEndState)
            chain.connect(anonEndState)

          case RepeatedRelExpansion(defn, Repetition(0, Limited(max))) =>
            val skippable = (0L until max).map { _ => recurse(defn, isFirst, isLast, prevState) }.toArray

            val anonEndState = sb.newState("anon")
            prevState.addNodeJuxtaposition(anonEndState)
            for (chain <- skippable.init) {
              chain.last.addNodeJuxtaposition(anonEndState)
            }

            StateChain.connectAll(skippable).connect(anonEndState)

          case RepeatedRelExpansion(defn, Repetition(min, Unlimited)) =>
            val prefix =
              (0L until min)
                .map { _ => recurse(defn, isFirst, isLast, prevState) }
                .toArray

            val last = prefix.last
            last.connectLoop()
            StateChain.connectAll(prefix)

          case RepeatedRelExpansion(defn, Repetition(min, Limited(max))) if min == max =>
            StateChain.repeat(min) { recurse(defn, isFirst, isLast, prevState) }

          case RepeatedRelExpansion(defn, Repetition(min, Limited(max))) =>
            val prefix = StateChain.repeat(min) { recurse(defn, isFirst, isLast, prevState) }

            val skippable = (min until max).map { _ => recurse(defn, isFirst, isLast, prevState) }.toArray

            val anonEndState = sb.newState("anon")
            prefix.last.addNodeJuxtaposition(anonEndState)
            for (chain <- skippable.init) {
              chain.last.addNodeJuxtaposition(anonEndState)
            }

            prefix.connect(StateChain.connectAll(skippable)).connect(anonEndState)

          case State(name, predicate) =>
            val state = sb.newState(
              name.orNull,
              isStartState = isFirst,
              isFinalState = isLast,
              predicate = predicate.getOrElse(Predicates.ALWAYS_TRUE_LONG)
            )
            StateChain(state, state)

          case Juxtaposition(first, second) =>
            val StateChain(s1, s2) = recurse(first, isFirst, isLast = false, prevState)
            val StateChain(s3, s4) = recurse(second, isFirst = false, isLast = isLast, s2)
            s2.addNodeJuxtaposition(s3)

            StateChain(s1, s4)

          case RelExpansion(from, direction, predicate, to) =>
            val StateChain(s1, s2) = recurse(from, isFirst, isLast = false, prevState)
            val StateChain(s3, s4) = recurse(to, isFirst = false, isLast = isLast, s2)
            val name = predicate.name match {
              case Some(value) => SlotOrName.VarName(value, isGroup = false)
              case None        => SlotOrName.None
            }
            s2.addRelationshipExpansion(
              s3,
              name = name,
              types = predicate.types,
              relPredicate = predicate.pred.getOrElse(Predicates.alwaysTrue()),
              direction = direction
            )
            StateChain(s1, s4)
        }

      recurse(this, isFirst = true, isLast = true, null)

      ()
    }

    /** Render the cypher text equivalent of the NFA */
    override def toString: String =
      this match {
        case RepeatedRelExpansion(defn, rept) =>
          val r = rept match {
            case Repetition(0, Unlimited) => "*"
            case Repetition(1, Unlimited) => "+"
            case _                        => rept.solvedString
          }
          s"($defn)$r"
        case Juxtaposition(first, second) => s"$first $second"
        case RelExpansion(from, dir, pred, to) =>
          val body = (pred.name, pred.types, pred.pred) match {
            case (None, null, None) => ""
            case (name, types, pred) =>
              val n = name.getOrElse("")
              val t = if (types == null) "" else types.mkString(":", "|", "")
              val p = pred.map(_ => " WHERE ...").getOrElse("")
              s"[$n$t$p]"
          }
          dir match {
            case Direction.OUTGOING => s"$from-$body->$to"
            case Direction.INCOMING => s"$from<-$body-$to"
            case Direction.BOTH     => s"$from-$body-$to"
          }
        case State(null, None) => "()"
        case State(name, pred) =>
          val n = name.getOrElse("")
          val p = pred.map(_ => " WHERE ...").getOrElse("")
          s"($n$p)"
      }
  }

  object DslPart {

    sealed trait RelExpansionOrState extends DslPart {

      /** Begin an OUTGOING or BOTH relationship expansion*/
      def -(predicate: RelPredicate): PartialRelOutgoingOrBoth =
        PartialRelOutgoingOrBoth(this, predicate)

      // we cannot use any operator with leading '<' because it has lower precedence than the corresponding -
      // see https://stackoverflow.com/questions/2922347/operator-precedence-in-scala
      /** Begin an INCOMING relationship expansion */
      def -<-(predicate: RelPredicate): PartialRelIncoming =
        PartialRelIncoming(this, predicate)

      /** Construct an entire anonymous OUTGOING relationship expansion */
      def -->(to: State): RelExpansion =
        RelExpansion(this, Direction.OUTGOING, RelPredicate(None, null, None), to)

      /** Construct an entire anonymous INCOMING relationship expansion */
      def <--(to: State): RelExpansion =
        RelExpansion(this, Direction.INCOMING, RelPredicate(None, null, None), to)

      /** Construct an entire anonymous BOTH relationship expansion */
      def --(to: State): RelExpansion =
        RelExpansion(this, Direction.BOTH, RelPredicate(None, null, None), to)
    }

    case class PartialRelOutgoingOrBoth(from: RelExpansionOrState, predicate: RelPredicate) {

      /** Complete a partial OUTGOING relationship expansion */
      def ->(to: State): RelExpansion =
        RelExpansion(from, Direction.OUTGOING, predicate, to)

      /** Complete a partial BOTH relationship expansion */
      def -(to: State): RelExpansion =
        RelExpansion(from, Direction.BOTH, predicate, to)
    }

    case class PartialRelIncoming(from: RelExpansionOrState, predicate: RelPredicate) {

      /** Complete a partial INCOMING relationship expansion */
      def -(to: State): RelExpansion =
        RelExpansion(from, Direction.INCOMING, predicate, to)
    }

    case class RelPredicate(
      name: Option[String],
      types: Array[Int],
      pred: Option[Predicate[RelationshipTraversalEntities]]
    ) {

      /** Set a predicate on the relationship expansion */
      def where(pred: Predicate[RelationshipTraversalEntities]): RelPredicate =
        copy(pred = Some(pred))

      /** Set a predicate on the relationship expansion */
      def where(pred: RelationshipTraversalEntities => Boolean): RelPredicate =
        where(r => pred(r))

      /** Add a relationship type specification (disjunction, naturally) */
      def :|(relType: Int): RelPredicate =
        copy(types = if (types == null) Array(relType) else types :+ relType)
    }

    case class RepeatedRelExpansion(defn: RelExpansion, rept: Repetition) extends DslPart

    case class State(name: Option[String], predicate: Option[LongPredicate]) extends RelExpansionOrState {

      /** Set a predicate on the node state */
      def where(predicate: LongPredicate): State =
        copy(predicate = Some(predicate))
    }

    case class Juxtaposition(first: DslPart, second: DslPart) extends DslPart

    case class RelExpansion(
      from: RelExpansionOrState,
      direction: Direction,
      predicate: RelPredicate,
      to: State
    ) extends RelExpansionOrState {

      /** Repeat the relationship expansion at least once */
      def + : RepeatedRelExpansion = RepeatedRelExpansion(this, Repetition(1, Unlimited))

      /** Repeat the relationship expansion any number of times */
      def * : RepeatedRelExpansion = RepeatedRelExpansion(this, Repetition(0, Unlimited))

      /** Repeat the relationship expansion a bounded number of times */
      def rep(min: Int, max: Int): RepeatedRelExpansion = RepeatedRelExpansion(this, Repetition(min, Limited(max)))
    }
  }

  object Implicits {
    import DslPart._

    /** Allows a name string to be used in a relationship pattern like {{{()-"name"->()}}} */
    implicit def stringToRelPredicate(name: String): RelPredicate =
      RelPredicate(Some(name), null, None)

    /** Allows unit to be used for anonymous relationship pattern like {{{()-(() where ...)->()}}} */
    implicit def unitToRelPredicate(_unit: Unit): RelPredicate = RelPredicate(None, null, None)

    /** Allows a rel type predicate to be used for anonymous relationship pattern like
     *
     * {{{
     *   val REL = 1
     *   val pattern = ()-REL->()
     * }}}
     *  */
    implicit def intToRelPredicate(relType: Int): RelPredicate =
      RelPredicate(None, Array(relType), None)

    /** Allows a name string to be used as a state like {{{"s"-->"t"}}} */
    implicit def stringToState(str: String): State = State(Some(str), None)

    /** Allows unit to be used for anonymous states like {{{()-->()}}} */
    implicit def unitToState(_unit: Unit): State = State(None, None)
  }

}

class NfaDslTests extends RuntimeUtilTestSuite {
  import NfaDsl.Implicits._

  private val nodePredicate: LongPredicate = Predicates.ALWAYS_TRUE_LONG
  private val relPredicate: Predicate[RelationshipTraversalEntities] = _ => true

  Seq(
    // format: off
    (() |> ())                       -> "() ()",
    ("s" |> ("a"--"b").* |> "t")     -> "(s) ((a)--(b))* (t)",
    ("s" where nodePredicate)        -> "(s WHERE ...)",
    (()-("r":|1)-())                 -> "()-[r:1]-()",
    (()-1->())                       -> "()-[:1]->()",
    (()-(1:|2)->())                  -> "()-[:1|2]->()",
    (()-("r" where relPredicate)-()) -> "()-[r WHERE ...]-()",
    (()<--())                        -> "()<--()",
    (()--())                         -> "()--()",
    (()-->())                        -> "()-->()",
    ("a"-->"b"<--"c")                -> "(a)-->(b)<--(c)",
    (("a"-->"b"<--"c").*)            -> "((a)-->(b)<--(c))*",
    ("a"-->"b"<--"c"+)               -> "((a)-->(b)<--(c))+",
    ("a"-->"b" rep (1, 5))           -> "((a)-->(b)){1, 5}",
    ("a"--"b"--"c" rep (1, 5))       -> "((a)--(b)--(c)){1, 5}",
    // format: on
  ).foreach { case (nfa, expected) =>
    test(s"$expected toString") {
      nfa.toString shouldBe expected
    }
  }

  testEqual(() |> ()) { sb =>
    val s1 = sb.newState(isStartState = true)
    val s2 = sb.newState(isFinalState = true)
    s1.addNodeJuxtaposition(s2)
  }

  testEqual("s") { sb =>
    sb.newState("s", isStartState = true, isFinalState = true)
  }

  testEqual("s" where nodePredicate) { sb =>
    sb.newState("s", isStartState = true, isFinalState = true, predicate = nodePredicate)
  }

  testEqual("a" --> "b") { sb =>
    val a = sb.newState("a", isStartState = true)
    val b = sb.newState("b", isFinalState = true)
    a.addRelationshipExpansion(b, direction = Direction.OUTGOING)
  }

  testEqual("a" <-- "b") { sb =>
    val a = sb.newState("a", isStartState = true)
    val b = sb.newState("b", isFinalState = true)
    a.addRelationshipExpansion(b, direction = Direction.INCOMING)
  }

  testEqual("a" -- "b") { sb =>
    val a = sb.newState("a", isStartState = true)
    val b = sb.newState("b", isFinalState = true)
    a.addRelationshipExpansion(b, direction = Direction.BOTH)
  }

  testEqual(() - 1 - ()) { sb =>
    val a = sb.newState(isStartState = true)
    val b = sb.newState(isFinalState = true)
    a.addRelationshipExpansion(b, types = Array(1))
  }

  testEqual(() - (1 :| 2) - ()) { sb =>
    val a = sb.newState(isStartState = true)
    val b = sb.newState(isFinalState = true)
    a.addRelationshipExpansion(b, types = Array(1, 2))
  }

  testEqual(() - ("" where relPredicate) - ()) { sb =>
    val a = sb.newState(isStartState = true)
    val b = sb.newState(isFinalState = true)
    a.addRelationshipExpansion(b, relPredicate = relPredicate, name = VarName("", isGroup = false))
  }

  testEqual("s" |> ("a" -- "b").* |> "t") { sb =>
    val s = sb.newState("s", isStartState = true)
    val a = sb.newState("a")
    val b = sb.newState("b")
    val anon = sb.newState("anon")
    val t = sb.newState("t", isFinalState = true)

    s.addNodeJuxtaposition(a)
    a.addRelationshipExpansion(b)
    b.addNodeJuxtaposition(anon)
    b.addNodeJuxtaposition(a)
    s.addNodeJuxtaposition(anon)
    anon.addNodeJuxtaposition(t)
  }

  testEqual("s" |> ("a" -- "b" +) |> "t") { sb =>
    val s = sb.newState("s", isStartState = true)
    val a = sb.newState("a")
    val b = sb.newState("b")
    val t = sb.newState("t", isFinalState = true)

    s.addNodeJuxtaposition(a)
    a.addRelationshipExpansion(b)
    b.addNodeJuxtaposition(a)
    b.addNodeJuxtaposition(t)
  }

  testEqual("s" |> ("a" -- "b" rep (1, 2)) |> "t") { sb =>
    val s = sb.newState("s", isStartState = true)
    val a1 = sb.newState("a")
    val b1 = sb.newState("b")
    val a2 = sb.newState("a")
    val b2 = sb.newState("b")
    val anon = sb.newState("anon")
    val t = sb.newState("t", isFinalState = true)

    s.addNodeJuxtaposition(a1)
    a1.addRelationshipExpansion(b1)
    b1.addNodeJuxtaposition(a2)
    a2.addRelationshipExpansion(b2)
    b2.addNodeJuxtaposition(anon)
    anon.addNodeJuxtaposition(t)
    b1.addNodeJuxtaposition(anon)
  }

  testEqual("s" |> ("a" -- "b" rep (2, 2)) |> "t") { sb =>
    val s = sb.newState("s", isStartState = true)
    val a1 = sb.newState("a")
    val b1 = sb.newState("b")
    val a2 = sb.newState("a")
    val b2 = sb.newState("b")
    val t = sb.newState("t", isFinalState = true)

    s.addNodeJuxtaposition(a1)
    a1.addRelationshipExpansion(b1)
    b1.addNodeJuxtaposition(a2)
    a2.addRelationshipExpansion(b2)
    b2.addNodeJuxtaposition(t)
  }

  testEqual("s" |> ("a" -- "b" -- "c" rep (2, 2)) |> "t") { sb =>
    val s = sb.newState("s", isStartState = true)
    val a1 = sb.newState("a")
    val b1 = sb.newState("b")
    val c1 = sb.newState("c")
    val a2 = sb.newState("a")
    val b2 = sb.newState("b")
    val c2 = sb.newState("c")
    val t = sb.newState("t", isFinalState = true)

    s.addNodeJuxtaposition(a1)
    a1.addRelationshipExpansion(b1)
    b1.addRelationshipExpansion(c1)
    c1.addNodeJuxtaposition(a2)
    a2.addRelationshipExpansion(b2)
    b2.addRelationshipExpansion(c2)
    c2.addNodeJuxtaposition(t)
  }

  private def testEqual(dsl: DslPart)(f: PGStateBuilder => Unit) = {
    test(dsl.toString + " builds the expected nfa") {
      val actual = new MockStateBuilder()
      dsl.build(actual)

      val expected = new MockStateBuilder()
      f(expected)

      actual shouldEqual expected
    }
  }

  sealed trait MockTransition {
    def from: MockBuilderState
    def to: MockBuilderState
  }

  case class MockJuxtaposition(from: MockBuilderState, to: MockBuilderState) extends MockTransition {
    override def toString: String = s"(${from.id} ${from.name}) (${to.id} ${to.name})"
  }

  case class MockExpansion(
    from: MockBuilderState,
    to: MockBuilderState,
    relPredicate: Predicate[RelationshipTraversalEntities],
    types: Option[Set[Int]],
    direction: Direction,
    name: SlotOrName
  ) extends MockTransition {

    override def toString: String = {
      val strPred = if (relPredicate == Predicates.alwaysTrue()) "" else " WHERE ..."
      val strTypes = types.getOrElse(Set.empty).mkString(":", "|", "")
      val strName = name.toString
      val strRel = s"[$strName$strTypes$strPred]"
      val strDir = direction match {
        case Direction.OUTGOING => s"-$strRel->"
        case Direction.INCOMING => s"<-$strRel-"
        case Direction.BOTH     => s"-$strRel-"
      }
      s"(${from.id} ${from.name})$strDir(${to.id} ${to.name})"
    }
  }

  class MockBuilderState(
    parent: MockStateBuilder,
    val id: Int,
    val name: String,
    val isStartState: Boolean,
    val isFinalState: Boolean,
    val predicate: LongPredicate
  ) extends PGStateBuilder.BuilderState(null) {

    override def addNodeJuxtaposition(target: BuilderState): Unit =
      parent.addTransition(MockJuxtaposition(this, target.asInstanceOf[MockBuilderState]))

    override def addRelationshipExpansion(
      target: BuilderState,
      relPredicate: Predicate[RelationshipTraversalEntities],
      types: Array[Int],
      direction: Direction,
      name: SlotOrName
    ): Unit =
      parent.addTransition(MockExpansion(
        this,
        target.asInstanceOf[MockBuilderState],
        relPredicate,
        Option(types).map(_.toSet),
        direction,
        name
      ))

    override def equals(other: Any): Boolean = other match {
      case that: MockBuilderState =>
        id == that.id &&
        name == that.name &&
        isStartState == that.isStartState &&
        isFinalState == that.isFinalState &&
        predicate == that.predicate
      case _ => false
    }

    override def hashCode(): Int = {
      MurmurHash3.productHash((name, isStartState, isFinalState, predicate))
    }

    override def toString: String =
      s"($id, $name, $isStartState, $isFinalState, $predicate)"
  }

  class MockStateBuilder extends PGStateBuilder {
    private val transitions = mutable.Set.empty[MockTransition]
    private val states = mutable.Set.empty[MockBuilderState]

    override def newState(
      name: String,
      isStartState: Boolean,
      isFinalState: Boolean,
      predicate: LongPredicate
    ): BuilderState = {
      val state = new MockBuilderState(this, states.size, name, isStartState, isFinalState, predicate)
      states += state
      state
    }

    def addTransition(transition: MockTransition): Unit =
      transitions += transition

    override def equals(other: Any): Boolean = other match {
      case that: MockStateBuilder =>
        transitions == that.transitions &&
        states == that.states
      case _ => false
    }

    override def hashCode(): Int = {
      MurmurHash3.productHash((transitions, states))
    }

    override def toString: String = {
      s"""
         |States:
         |${states.toSeq.sortBy(_.id).mkString("- ", "\n- ", "")}
         |
         |Transitions:
         |${transitions.toSeq.sortBy(x => (x.from.id, x.to.id)).mkString("- ", "\n- ", "")}
         |""".stripMargin
    }
  }
}
