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

import org.neo4j.cypher.internal.macros.AssertMacros3.checkOnlyWhenAssertionsAreEnabled
import org.neo4j.cypher.internal.plandescription.Arguments.BatchSize
import org.neo4j.cypher.internal.plandescription.Arguments.ByteCode
import org.neo4j.cypher.internal.plandescription.Arguments.CypherPlannerVersion
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
import org.neo4j.kernel.impl.query.statistic.PlanDetailsToBeLogged
import org.neo4j.kernel.impl.query.statistic.PlanOperatorDetailsToBeLogged

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

  def children: Seq[InternalPlanDescription]

  def variables: Set[PrettyString]

  def cd(name: String): InternalPlanDescription = children.flatMap(_.find(name)).head

  def map(f: InternalPlanDescription => InternalPlanDescription): InternalPlanDescription

  def find(name: String): Seq[InternalPlanDescription]

  def addArgument(arg: Argument): InternalPlanDescription

  def flatten: Seq[InternalPlanDescription] = flatten(leftPrecedence = true)

  private def flatten(leftPrecedence: Boolean): Seq[InternalPlanDescription] = {
    val flatten = new ArrayBuffer[InternalPlanDescription]
    val stack = new mutable.Stack[InternalPlanDescription]()
    stack.push(self)
    while (stack.nonEmpty) {
      val plan = stack.pop()
      flatten += plan
      val childrenByPrecedence = if (leftPrecedence) plan.children.reverse else plan.children
      childrenByPrecedence.foreach(stack.push)
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

  private def logInfoSingleOperator(): PlanOperatorDetailsToBeLogged = {
    val result = new PlanOperatorDetailsToBeLogged()

    result.setOperatorName(name)
    result.setOperatorId(id.x)

    if (children.nonEmpty) result.setLeftOperatorId(children.head.id.x)
    if (children.size > 1) result.setRightOperatorId(children(1).id.x)

    arguments.foreach {
      case Arguments.Details(detailsString) if detailsString.nonEmpty =>
        result.setDetails(detailsString.mkString(", "))
      case Arguments.EstimatedRows(effectiveCardinality, cardinality) =>
        val m = new java.util.LinkedHashMap[String, java.lang.Double]()
        m.put("effectiveCardinality", effectiveCardinality)
        cardinality.foreach(c => m.put("rawCardinality", c))
        result.setEstimatedRows(m)
      case Arguments.Order(orderString) =>
        result.setOrder(orderString.prettifiedString)
      case Arguments.Distinctness(distinctnessString) =>
        val distinctnessPrettified = distinctnessString.prettifiedString
        if (distinctnessPrettified.nonEmpty) result.setDistinctness(distinctnessPrettified)
      case Arguments.PipelineInfo(pipelineId, _, markAsSerial) =>
        val m = new java.util.LinkedHashMap[String, Object]()
        m.put("pipelineId", pipelineId.toString)
        m.put("requiresSerialExecution", Boolean.box(markAsSerial))
        result.setPipelineInfo(m)
      case _ =>
    }
    result
  }

  def logInfo(): PlanDetailsToBeLogged = {
    val operatorDetails = flatten(leftPrecedence = false)
      .map(_.logInfoSingleOperator()).toArray
    new PlanDetailsToBeLogged(operatorDetails)
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
      extract { case DbHits(count) => count }.getOrElse(throw InternalException.internalError(
        this.getClass.getSimpleName,
        "Db hits were not recorded."
      ))

    override def getRows: Long =
      extract { case Rows(count) => count }.getOrElse(throw InternalException.internalError(
        this.getClass.getSimpleName,
        "Rows were not recorded."
      ))

    override def getPageCacheHits: Long = extract { case PageCacheHits(count) => count }.getOrElse(
      throw InternalException.internalError(this.getClass.getSimpleName, "Page cache stats were not recorded.")
    )

    override def getPageCacheMisses: Long = extract { case PageCacheMisses(count) => count }.getOrElse(
      throw InternalException.internalError(this.getClass.getSimpleName, "Page cache stats were not recorded.")
    )

    override def getTime: Long =
      extract { case Time(value) => value }.getOrElse(throw InternalException.internalError(
        this.getClass.getSimpleName,
        "Time was not recorded."
      ))

    private def extract(f: PartialFunction[Argument, Long]): Option[Long] = arguments.collectFirst(f)
  }

}

object InternalPlanDescription {

  case class TotalHits(hits: Long, uncertain: Boolean) {
    def +(other: TotalHits): TotalHits = TotalHits(this.hits + other.hits, this.uncertain || other.uncertain)
  }

  def error(msg: String): InternalPlanDescription =
    PlanDescriptionImpl(Id.INVALID_ID, msg, Seq.empty, Nil, Set.empty)
}

final case class PlanDescriptionImpl(
  id: Id,
  name: String,
  children: Seq[InternalPlanDescription],
  arguments: Seq[Argument],
  variables: Set[PrettyString],
  withRawCardinalities: Boolean = false,
  withDistinctness: Boolean = false
) extends InternalPlanDescription {

  checkOnlyWhenAssertionsAreEnabled(arguments.count(_.isInstanceOf[Details]) < 2)

  def find(name: String): Seq[InternalPlanDescription] =
    children.flatMap(_.find(name)) ++
      (if (this.name == name)
         Some(this)
       else {
         None
       })

  def addArgument(argument: Argument): InternalPlanDescription = copy(arguments = arguments :+ argument)

  def map(f: InternalPlanDescription => InternalPlanDescription): InternalPlanDescription = f(
    copy(children = children.map(_.map(f)))
  )

  def toIndexedSeq: Seq[InternalPlanDescription] = this +: children.toIndexedSeq

  val NL: String = System.lineSeparator()

  override def toString: String = {
    val version = arguments.collectFirst {
      case Version(v) => s"Cypher $v$NL"
    }

    val plannerVersion = arguments.collectFirst {
      // TODO: when releasing the display_planner_version feature this check should look at PlannerVersion instead.
      case v: CypherPlannerVersion => s"Planner version ${v.value.toUpperCase(Locale.ROOT)}$NL"
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

    val prefix = version ++ planner ++ plannerVersion ++ runtime ++ runtimeVersion ++ batchSize
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
        children(2).asInstanceOf[Seq[InternalPlanDescription]],
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

  override def children: Seq[InternalPlanDescription] = similar.last.children

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

  def children: Seq[InternalPlanDescription] = Seq.empty

  def find(searchedName: String): Seq[InternalPlanDescription] = if (searchedName == name) Seq(this) else Seq.empty

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

case class WorkingScopePlanDescription(
  id: Id,
  name: String,
  variables: Set[PrettyString],
  arguments: Seq[Argument],
  children: Seq[InternalPlanDescription]
) extends InternalPlanDescription {
  override def map(f: InternalPlanDescription => InternalPlanDescription): InternalPlanDescription = f(this)

  override def find(searchedName: String): Seq[InternalPlanDescription] =
    if (searchedName == name) Seq(this) else Seq.empty

  override def addArgument(arg: Argument): InternalPlanDescription = copy(arguments = arguments :+ arg)

  override def dup(children: Seq[AnyRef]): WorkingScopePlanDescription.this.type = {
    if (children.iterator eqElements this.treeChildren) {
      this
    } else {
      copy(
        children(0).asInstanceOf[Id],
        children(1).asInstanceOf[String],
        children(2).asInstanceOf[Set[PrettyString]],
        children(3).asInstanceOf[Seq[Argument]],
        children(4).asInstanceOf[Seq[InternalPlanDescription]]
      ).asInstanceOf[this.type]
    }
  }
}
