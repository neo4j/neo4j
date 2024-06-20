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
package org.neo4j.cypher.internal.plandescription

import org.neo4j.cypher.internal.macros.AssertMacros.checkOnlyWhenAssertionsAreEnabled
import org.neo4j.cypher.internal.plandescription.Arguments.BatchSize
import org.neo4j.cypher.internal.plandescription.Arguments.ByteCode
import org.neo4j.cypher.internal.plandescription.Arguments.DbHits
import org.neo4j.cypher.internal.plandescription.Arguments.Details
import org.neo4j.cypher.internal.plandescription.Arguments.IdArg
import org.neo4j.cypher.internal.plandescription.Arguments.PageCacheHits
import org.neo4j.cypher.internal.plandescription.Arguments.PageCacheMisses
import org.neo4j.cypher.internal.plandescription.Arguments.Planner
import org.neo4j.cypher.internal.plandescription.Arguments.Rows
import org.neo4j.cypher.internal.plandescription.Arguments.Runtime
import org.neo4j.cypher.internal.plandescription.Arguments.RuntimeVersion
import org.neo4j.cypher.internal.plandescription.Arguments.SourceCode
import org.neo4j.cypher.internal.plandescription.Arguments.Time
import org.neo4j.cypher.internal.plandescription.Arguments.Version
import org.neo4j.cypher.internal.plandescription.InternalPlanDescription.TotalHits
import org.neo4j.cypher.internal.util.Foldable
import org.neo4j.cypher.internal.util.Rewritable
import org.neo4j.cypher.internal.util.Rewritable.IteratorEq
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.InternalException
import org.neo4j.graphdb.ExecutionPlanDescription
import org.neo4j.graphdb.ExecutionPlanDescription.ProfilerStatistics

import java.util
import java.util.Locale

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.jdk.CollectionConverters.SetHasAsJava

/**
 * Abstract description of an execution plan
 */
sealed trait InternalPlanDescription extends org.neo4j.graphdb.ExecutionPlanDescription with Rewritable with Foldable {
  self =>

  def arguments: Seq[Argument]

  def id: Id

  def name: String

  def children: Children

  def variables: Set[PrettyString]

  def cd(name: String): InternalPlanDescription = children.find(name).head

  def map(f: InternalPlanDescription => InternalPlanDescription): InternalPlanDescription

  def find(name: String): Seq[InternalPlanDescription]

  def addArgument(arg: Argument): InternalPlanDescription

  def flatten: Seq[InternalPlanDescription] = {
    val flatten = new ArrayBuffer[InternalPlanDescription]
    val stack = new mutable.Stack[InternalPlanDescription]()
    stack.push(self)
    while (stack.nonEmpty) {
      val plan = stack.pop()
      flatten += plan
      plan.children match {
        case NoChildren =>
        case SingleChild(child) =>
          stack.push(child)
        case TwoChildren(l, r) =>
          stack.push(r)
          stack.push(l)
      }
    }
    flatten
  }.toSeq

  def totalDbHits: TotalHits = {
    val allMaybeDbHits: Seq[TotalHits] = flatten.map {
      (plan: InternalPlanDescription) =>
        plan.arguments.collectFirst { case DbHits(x) => TotalHits(x, uncertain = false) }.getOrElse(TotalHits(
          0,
          uncertain = true
        ))
    }
    allMaybeDbHits.reduce(_ + _)
  }

  def findPlanByPredicate(predicate: InternalPlanDescription => Boolean): Option[InternalPlanDescription] =
    folder.treeFind[InternalPlanDescription] {
      case plan: InternalPlanDescription if predicate(plan) => true
    }

  def containsDetail(infoString: String): Boolean =
    arguments.exists {
      case Details(info) => info.contains(asPrettyString.raw(infoString))
      case _             => false
    }

  // Implement public Java API here=
  override def getName: String = name

  override def getChildren: util.List[ExecutionPlanDescription] = {
    val childPlans: Seq[org.neo4j.graphdb.ExecutionPlanDescription] = children.toIndexedSeq
    childPlans.asJava
  }

  override def getArguments: util.Map[String, AnyRef] = {
    // The ID has its own column and is not included in `arguments`, but since we want to sent it to cients with their own rendering logic,
    // we include it here in `getArguments`.
    val argsWithId = arguments :+ IdArg(id)
    val map = argsWithId.map { arg => arg.name -> PlanDescriptionArgumentSerializer.serialize(arg) }.toMap
    map.asJava
  }

  override def getIdentifiers: util.Set[String] = variables.map(_.prettifiedString).asJava

  override def hasProfilerStatistics: Boolean = arguments.exists(_.isInstanceOf[DbHits])

  override def getProfilerStatistics: ExecutionPlanDescription.ProfilerStatistics = new ProfilerStatistics {
    override def hasRows: Boolean = arguments.exists(_.isInstanceOf[Rows])

    override def hasDbHits: Boolean = arguments.exists(_.isInstanceOf[DbHits])

    override def hasPageCacheStats: Boolean =
      arguments.exists(_.isInstanceOf[PageCacheHits]) && arguments.exists(_.isInstanceOf[PageCacheMisses])

    override def hasTime: Boolean = arguments.exists(_.isInstanceOf[Time])

    override def getDbHits: Long =
      extract { case DbHits(count) => count }.getOrElse(throw new InternalException("Db hits were not recorded."))

    override def getRows: Long =
      extract { case Rows(count) => count }.getOrElse(throw new InternalException("Rows were not recorded."))

    override def getPageCacheHits: Long = extract { case PageCacheHits(count) => count }.getOrElse(
      throw new InternalException("Page cache stats were not recorded.")
    )

    override def getPageCacheMisses: Long = extract { case PageCacheMisses(count) => count }.getOrElse(
      throw new InternalException("Page cache stats were not recorded.")
    )

    override def getTime: Long =
      extract { case Time(value) => value }.getOrElse(throw new InternalException("Time was not recorded."))

    private def extract(f: PartialFunction[Argument, Long]): Option[Long] = arguments.collectFirst(f)
  }

}

object InternalPlanDescription {

  case class TotalHits(hits: Long, uncertain: Boolean) {
    def +(other: TotalHits) = TotalHits(this.hits + other.hits, this.uncertain || other.uncertain)
  }

  def error(msg: String): InternalPlanDescription =
    PlanDescriptionImpl(Id.INVALID_ID, msg, NoChildren, Nil, Set.empty)
}

sealed trait Children {

  def isEmpty: Boolean = toIndexedSeq.isEmpty

  def tail: Seq[InternalPlanDescription] = toIndexedSeq.tail

  def head: InternalPlanDescription = toIndexedSeq.head

  def toIndexedSeq: Seq[InternalPlanDescription]

  def find(name: String): Seq[InternalPlanDescription] = toIndexedSeq.flatMap(_.find(name))

  def map(f: InternalPlanDescription => InternalPlanDescription): Children

  def foreach[U](f: InternalPlanDescription => U): Unit = {
    toIndexedSeq.foreach(f)
  }
}

case object NoChildren extends Children {

  def toIndexedSeq = Seq.empty

  def map(f: InternalPlanDescription => InternalPlanDescription) = NoChildren
}

final case class SingleChild(child: InternalPlanDescription) extends Children {

  val toIndexedSeq = Seq(child)

  def map(f: InternalPlanDescription => InternalPlanDescription) = SingleChild(child = child.map(f))
}

final case class TwoChildren(lhs: InternalPlanDescription, rhs: InternalPlanDescription) extends Children {

  val toIndexedSeq = Seq(lhs, rhs)

  def map(f: InternalPlanDescription => InternalPlanDescription) = TwoChildren(lhs = lhs.map(f), rhs = rhs.map(f))
}

final case class PlanDescriptionImpl(
  id: Id,
  name: String,
  children: Children,
  arguments: Seq[Argument],
  variables: Set[PrettyString],
  withRawCardinalities: Boolean = false,
  withDistinctness: Boolean = false
) extends InternalPlanDescription {

  checkOnlyWhenAssertionsAreEnabled(arguments.count(_.isInstanceOf[Details]) < 2)

  def find(name: String): Seq[InternalPlanDescription] =
    children.find(name) ++ (if (this.name == name)
                              Some(this)
                            else {
                              None
                            })

  def addArgument(argument: Argument): InternalPlanDescription = copy(arguments = arguments :+ argument)

  def map(f: InternalPlanDescription => InternalPlanDescription): InternalPlanDescription = f(
    copy(children = children.map(f))
  )

  def toIndexedSeq: Seq[InternalPlanDescription] = this +: children.toIndexedSeq

  val NL: String = System.lineSeparator()

  override def toString: String = {
    val version = arguments.collectFirst {
      case Version(v) => s"Cypher $v$NL"
    }
    val planner = arguments.collectFirst {
      case Planner(n) => s"Planner ${n.toUpperCase(Locale.ROOT)}$NL"
    }

    val runtime = arguments.collectFirst {
      case Runtime(n) => s"Runtime ${n.toUpperCase(Locale.ROOT)}$NL"
    }

    val runtimeVersion = arguments.collectFirst {
      case RuntimeVersion(n) => s"Runtime version ${n.toUpperCase(Locale.ROOT)}$NL"
    }

    val batchSize = arguments.collectFirst {
      case BatchSize(n) => s"Batch size $n$NL"
    }

    val prefix = version ++ planner ++ runtime ++ runtimeVersion ++ batchSize
    s"${prefix.mkString("", NL, NL)}${renderAsTreeTable(this, withRawCardinalities, withDistinctness)}$NL${renderSummary(this)}$renderSources"
  }

  private def renderSources = {
    arguments.flatMap {
      case SourceCode(className, sourceCode) => Some(s"=== Java Source: $className ===$NL$sourceCode")
      case ByteCode(className, byteCode)     => Some(s"=== Bytecode: $className ===$NL$byteCode")
      case _                                 => None
    }.mkString(NL, NL, "")
  }

  override def dup(children: Seq[AnyRef]): PlanDescriptionImpl.this.type = {
    if (children.iterator eqElements this.treeChildren) {
      this
    } else {
      val id = children.head
      copy(
        // TODO Better way to achieve this!
        if (id.isInstanceOf[java.lang.Integer]) Id(id.asInstanceOf[java.lang.Integer]) else id.asInstanceOf[Id],
        children(1).asInstanceOf[String],
        children(2).asInstanceOf[Children],
        children(3).asInstanceOf[Seq[Argument]],
        children(4).asInstanceOf[Set[PrettyString]]
      ).asInstanceOf[this.type]
    }
  }
}

object CompactedPlanDescription {

  def create(similar: Seq[InternalPlanDescription]): InternalPlanDescription =
    if (similar.size == 1) similar.head else CompactedPlanDescription(similar)
}

final case class CompactedPlanDescription(similar: Seq[InternalPlanDescription]) extends InternalPlanDescription {

  override def name: String = s"${similar.head.name}(${similar.size})"

  override lazy val variables: Set[PrettyString] = similar.foldLeft(Set.empty[PrettyString]) { (acc, plan) =>
    acc ++ plan.variables
  }

  override def children: Children = similar.last.children

  override val arguments: Seq[Argument] = {
    var dbHits: Option[Long] = None
    var pageCacheHits: Option[Long] = None
    var pageCacheMisses: Option[Long] = None
    var time: Option[Long] = None
    var rows: Option[Long] = None

    similar.foldLeft(Set.empty[Argument]) {
      (acc, plan) =>
        val args = plan.arguments.filter {
          case DbHits(v)          => dbHits = Some(dbHits.map(_ + v).getOrElse(v)); false
          case PageCacheHits(v)   => pageCacheHits = Some(pageCacheHits.map(_ + v).getOrElse(v)); false
          case PageCacheMisses(v) => pageCacheMisses = Some(pageCacheMisses.map(_ + v).getOrElse(v)); false
          case Time(v)            => time = Some(time.map(_ + v).getOrElse(v)); false
          case Rows(v)            => rows = Some(rows.map(o => Math.max(o, v)).getOrElse(v)); false
          case _                  => true
        }
        acc ++ args
    }.toIndexedSeq ++ dbHits.map(DbHits.apply) ++
      pageCacheHits.map(PageCacheHits.apply) ++
      pageCacheMisses.map(PageCacheMisses.apply) ++
      time.map(Time.apply) ++ rows.map(Rows.apply)
  }

  override def find(name: String): Seq[InternalPlanDescription] = similar.last.find(name)

  override def id: Id = similar.last.id

  override def addArgument(argument: Argument): InternalPlanDescription = ???

  override def map(f: InternalPlanDescription => InternalPlanDescription): InternalPlanDescription =
    f(copy(similar =
      similar
        .map(f)
    ))

  override def dup(children: Seq[AnyRef]): CompactedPlanDescription.this.type = {
    if (children.iterator eqElements this.treeChildren) {
      this
    } else {
      copy(children.head.asInstanceOf[Seq[InternalPlanDescription]]).asInstanceOf[this.type]
    }
  }
}

final case class ArgumentPlanDescription(id: Id, arguments: Seq[Argument] = Seq.empty, variables: Set[PrettyString])
    extends InternalPlanDescription {

  def children = NoChildren

  def find(searchedName: String) = if (searchedName == name) Seq(this) else Seq.empty

  def name = "EmptyRow"

  def render(builder: StringBuilder): Unit = {}

  def render(builder: StringBuilder, separator: String, levelSuffix: String): Unit = {}

  def addArgument(arg: Argument): InternalPlanDescription = copy(arguments = arguments :+ arg)

  def map(f: InternalPlanDescription => InternalPlanDescription): InternalPlanDescription = f(this)

  def toIndexedSeq: Seq[InternalPlanDescription] = Seq(this)

  override def dup(children: Seq[AnyRef]): ArgumentPlanDescription.this.type = {
    if (children.iterator eqElements this.treeChildren) {
      this
    } else {
      copy(
        children(0).asInstanceOf[Id],
        children(1).asInstanceOf[Seq[Argument]],
        children(2).asInstanceOf[Set[PrettyString]]
      ).asInstanceOf[this.type]
    }
  }
}
