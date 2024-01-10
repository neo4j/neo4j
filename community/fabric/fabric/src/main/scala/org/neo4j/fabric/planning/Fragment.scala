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
package org.neo4j.fabric.planning

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.GraphSelection
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.SubqueryCall
import org.neo4j.cypher.internal.expressions.AutoExtractedParameter
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.util.Foldable
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewritable
import org.neo4j.cypher.internal.util.Rewritable.IteratorEq
import org.neo4j.cypher.rendering.QueryRenderer
import org.neo4j.fabric.planning.Fragment.Apply
import org.neo4j.fabric.planning.Fragment.Segment
import org.neo4j.fabric.planning.Fragment.Union
import org.neo4j.fabric.util.Folded
import org.neo4j.fabric.util.Folded.FoldableOps
import org.neo4j.fabric.util.PrettyPrinting
import org.neo4j.graphdb.ExecutionPlanDescription

import java.util

import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.SetHasAsJava

sealed trait Fragment extends Fragment.RewritingSupport {

  /** Columns available to this fragment from an applied argument */
  def argumentColumns: Seq[String]

  /** Columns imported from the argument */
  def importColumns: Seq[String]

  /** Produced columns */
  def outputColumns: Seq[String]

  /** Whether this fragment produces final query output */
  def producesResults: Boolean

  /** ExecutionPlanDescription */
  def description: Fragment.Description
  /* Original input position */
  def pos: InputPosition

  def flatten: Seq[Fragment] = {
    this match {
      case apply: Apply       => apply.input.flatten ++ Seq(apply) ++ apply.inner.flatten
      case union: Union       => union.lhs.flatten ++ Seq(union) ++ union.rhs.flatten
      case segment: Segment   => segment.input.flatten ++ Seq(segment)
      case fragment: Fragment => Seq(fragment)
    }
  }
}

object Fragment {

  sealed trait Chain extends Fragment {

    /** Graph selection for this fragment */
    def use: Use
  }

  sealed trait Segment extends Fragment.Chain {
    def input: Fragment.Chain
    val use: Use = input.use
    val argumentColumns: Seq[String] = input.argumentColumns
    val importColumns: Seq[String] = input.importColumns
  }

  sealed trait Command extends Fragment {

    /** Graph selection for this fragment */
    def use: Use
    def command: ast.Statement
    def queryType: QueryType

    override val argumentColumns: Seq[String] = Seq.empty
    override val importColumns: Seq[String] = Seq.empty
    override val outputColumns: Seq[String] = command.returnColumns.map(_.name)
    override val producesResults: Boolean = true
  }

  final case class Init(
    use: Use,
    argumentColumns: Seq[String] = Seq.empty,
    importColumns: Seq[String] = Seq.empty
  ) extends Fragment.Chain {
    override val outputColumns: Seq[String] = Seq.empty
    override val description: Fragment.Description = Description.InitDesc(this)
    override val producesResults: Boolean = false
    override val pos: InputPosition = InputPosition.NONE
  }

  final case class Apply(
    input: Fragment.Chain,
    inner: Fragment,
    inTransactionsParameters: Option[SubqueryCall.InTransactionsParameters]
  )(
    val pos: InputPosition
  ) extends Fragment.Segment {

    override val outputColumns: Seq[String] = {
      val columns = Columns.combine(input.outputColumns, inner.outputColumns)
      inTransactionsParameters.flatMap(_.reportParams)
        .map(reportParams => reportParams.reportAs.name)
        .map(reportVariable => columns :+ reportVariable)
        .getOrElse(columns)
    }
    override val producesResults: Boolean = false
    override val description: Fragment.Description = Description.ApplyDesc(this)
  }

  final object Apply {
    final val CALL_IN_TX_ROWS = "call_in_tx_rows"
    final val CALL_IN_TX_ROW = "call_in_tx_row"
    final val CALL_IN_TX_ROW_ID = "call_in_tx_row_id"
  }

  final case class Union(
    input: Fragment.Init,
    distinct: Boolean,
    lhs: Fragment,
    rhs: Fragment.Chain
  )(
    val pos: InputPosition
  ) extends Fragment {
    override val outputColumns: Seq[String] = lhs.outputColumns
    override val producesResults: Boolean = rhs.producesResults
    override val argumentColumns: Seq[String] = input.argumentColumns
    override val importColumns: Seq[String] = Columns.combine(lhs.importColumns, rhs.importColumns)
    override val description: Fragment.Description = Description.UnionDesc(this)
  }

  final case class Leaf(
    input: Fragment.Chain,
    clauses: Seq[ast.Clause],
    outputColumns: Seq[String]
  )(
    val pos: InputPosition
  ) extends Fragment.Segment {
    override val producesResults: Boolean = false
    override val description: Fragment.Description = Description.LeafDesc(this)
    val parameters: Map[String, String] = Columns.asParamMappings(importColumns)
    val queryType: QueryType = QueryType.of(clauses)
    val executable: Boolean = hasExecutableClauses(clauses)
  }

  final case class Exec(
    input: Fragment.Chain,
    query: Statement,
    localQuery: BaseState,
    remoteQuery: RemoteQuery,
    sensitive: Boolean,
    outputColumns: Seq[String]
  ) extends Fragment.Segment {

    override val producesResults: Boolean = query match {
      case query: Query => query.isReturning
      case _            => true
    }
    val parameters: Map[String, String] = Columns.asParamMappings(importColumns)
    val executable: Boolean = hasExecutableClauses(query)
    val description: Fragment.Description = Description.ExecDesc(this)
    val queryType: QueryType = QueryType.of(query)
    val statementType: StatementType = StatementType.of(query)
    def pos: InputPosition = query.position
  }

  final case class RemoteQuery(
    query: String,
    extractedLiterals: Map[AutoExtractedParameter, Expression]
  )

  final case class SchemaCommand(
    use: Use,
    command: ast.SchemaCommand
  ) extends Command {
    val description: Description = Description.CommandDesc(this, "Command")
    val queryType: QueryType = QueryType.of(command)
    def pos: InputPosition = command.position
  }

  final case class AdminCommand(
    use: Use,
    command: ast.AdministrationCommand
  ) extends Command {
    val description: Description = Description.CommandDesc(this, "AdminCommand")
    val queryType: QueryType = QueryType.of(command)
    def pos: InputPosition = command.position
  }

  trait RewritingSupport extends Product with Foldable with Rewritable {
    protected def pos: InputPosition

    def dup(children: Seq[AnyRef]): this.type =
      if (children.iterator eqElements this.treeChildren)
        this
      else {
        val args = children
        val hasExtraParam = Rewritable.numParameters(this) == children.length + 1
        val lastParamIsPos = Rewritable.includesPosition(this)
        val ctorArgs = if (hasExtraParam && lastParamIsPos) args :+ this.pos else args
        val duped = Rewritable.copyProduct(this, ctorArgs.toArray)
        duped.asInstanceOf[this.type]
      }
  }

  private def hasExecutableClauses(clauses: Seq[ast.Clause]) =
    clauses.exists(isExecutable)

  private def hasExecutableClauses(query: Statement) =
    query.folded(false)(_ || _) {
      case clause: Clause               => Folded.Stop(isExecutable(clause))
      case _: ast.SchemaCommand         => Folded.Stop(true)
      case _: ast.AdministrationCommand => Folded.Stop(true)
    }

  private def isExecutable(clause: ast.Clause) =
    clause match {
      case _: GraphSelection => false
      case _                 => true
    }

  sealed abstract class Description(name: String, fragment: Fragment) extends ExecutionPlanDescription {
    override def getName: String = name
    override def getIdentifiers: util.Set[String] = fragment.outputColumns.toSet.asJava
    override def hasProfilerStatistics: Boolean = false
    override def getProfilerStatistics: ExecutionPlanDescription.ProfilerStatistics = null
  }

  final object Description {

    final case class InitDesc(fragment: Fragment.Init) extends Description("Init", fragment) {
      override def getChildren: util.List[ExecutionPlanDescription] = list()

      override def getArguments: util.Map[String, AnyRef] = map(
        "argumentColumns" -> fragment.argumentColumns.mkString(","),
        "importColumns" -> fragment.importColumns.mkString(",")
      )
    }

    final case class ApplyDesc(fragment: Fragment.Apply) extends Description("Apply", fragment) {

      override def getChildren: util.List[ExecutionPlanDescription] =
        list(fragment.input.description, fragment.inner.description)
      override def getArguments: util.Map[String, AnyRef] = map()
    }

    final case class LeafDesc(fragment: Fragment.Leaf) extends Description("Leaf", fragment) {
      override def getChildren: util.List[ExecutionPlanDescription] = list(fragment.input.description)

      override def getArguments: util.Map[String, AnyRef] = map(
        "query" -> QueryRenderer.render(fragment.clauses)
      )
    }

    final case class ExecDesc(fragment: Fragment.Exec) extends Description("Exec", fragment) {
      override def getChildren: util.List[ExecutionPlanDescription] = list(fragment.input.description)

      override def getArguments: util.Map[String, AnyRef] = map(
        "query" -> QueryRenderer.render(fragment.query)
      )
    }

    final case class UnionDesc(fragment: Fragment.Union) extends Description("Union", fragment) {

      override def getChildren: util.List[ExecutionPlanDescription] =
        list(fragment.lhs.description, fragment.rhs.description)
      override def getArguments: util.Map[String, AnyRef] = map()
    }

    final case class CommandDesc(fragment: Fragment.Command, name: String) extends Description(name, fragment) {
      override def getChildren: util.List[ExecutionPlanDescription] = list()
      override def getArguments: util.Map[String, AnyRef] = map("query" -> QueryRenderer.render(fragment.command))
    }

    private def list[E](es: E*): util.List[E] =
      util.List.of(es: _*)

    private def map[K, V](es: (K, V)*): util.Map[K, V] =
      Map(es: _*).asJava
  }

  val pretty: PrettyPrinting[Fragment] = new PrettyPrinting[Fragment] {

    def pretty: Fragment => Stream[String] = {
      case f: Init => node(
          name = "init",
          fields = Seq(
            "use" -> use(f.use),
            "arg" -> list(f.argumentColumns),
            "imp" -> list(f.importColumns)
          )
        )

      case f: Apply => node(
          name = "apply",
          fields = Seq(
            "out" -> list(f.outputColumns),
            "tx" -> f.inTransactionsParameters
          ),
          children = Seq(f.inner, f.input)
        )

      case f: Fragment.Union => node(
          name = "union",
          fields = Seq(
            "out" -> list(f.outputColumns),
            "dist" -> f.distinct
          ),
          children = Seq(f.lhs, f.rhs)
        )

      case f: Fragment.Leaf => node(
          name = "leaf",
          fields = Seq(
            "use" -> use(f.use),
            "arg" -> list(f.argumentColumns),
            "imp" -> list(f.importColumns),
            "out" -> list(f.outputColumns),
            "qry" -> query(f.clauses)
          ),
          children = Seq(f.input)
        )

      case f: Fragment.Exec => node(
          name = "exec",
          fields = Seq(
            "use" -> use(f.use),
            "arg" -> list(f.argumentColumns),
            "imp" -> list(f.importColumns),
            "out" -> list(f.outputColumns),
            "qry" -> query(f.query)
          ),
          children = Seq(f.input)
        )
    }

    private def use(u: Use) = u match {
      case d: Use.Declared  => "declared " + d.graphSelection.graphReference
      case i: Use.Inherited => "inherited " + i.graphSelection.graphReference
    }
  }
}
