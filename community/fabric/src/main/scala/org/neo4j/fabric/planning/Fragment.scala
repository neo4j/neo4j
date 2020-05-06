/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.fabric.planning

import java.util

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.GraphSelection
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.rendering.QueryRenderer
import org.neo4j.fabric.util.Folded
import org.neo4j.fabric.util.Folded.FoldableOps
import org.neo4j.fabric.util.PrettyPrinting
import org.neo4j.graphdb.ExecutionPlanDescription

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.JavaConverters.setAsJavaSet

sealed trait Fragment {
  /** Columns available to this fragment from an applied argument */
  def argumentColumns: Seq[String]
  /** Columns imported from the argument */
  def importColumns: Seq[String]
  /** Produced columns */
  def outputColumns: Seq[String]
  /** ExecutionPlanDescription */
  def description: Fragment.Description
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

    val argumentColumns: Seq[String] = Seq.empty
    val importColumns: Seq[String] = Seq.empty
    val outputColumns: Seq[String] = command.returnColumns.map(_.name)
  }

  final case class Init(
    use: Use,
    argumentColumns: Seq[String] = Seq.empty,
    importColumns: Seq[String] = Seq.empty,
  ) extends Fragment.Chain {
    val outputColumns: Seq[String] = Seq.empty
    val description: Fragment.Description = Description.InitDesc(this)
  }

  final case class Apply(
    input: Fragment.Chain,
    inner: Fragment,
  ) extends Fragment.Segment {
    val outputColumns: Seq[String] = Columns.combine(input.outputColumns, inner.outputColumns)
    val description: Fragment.Description = Description.ApplyDesc(this)
  }

  final case class Union(
    input: Fragment.Init,
    distinct: Boolean,
    lhs: Fragment,
    rhs: Fragment.Chain,
  ) extends Fragment {
    val outputColumns: Seq[String] = rhs.outputColumns
    val argumentColumns: Seq[String] = input.argumentColumns
    val importColumns: Seq[String] = Columns.combine(lhs.importColumns, rhs.importColumns)
    val description: Fragment.Description = Description.UnionDesc(this)
  }

  final case class Leaf(
    input: Fragment.Chain,
    clauses: Seq[ast.Clause],
    outputColumns: Seq[String],
  ) extends Fragment.Segment {
    val parameters: Map[String, String] = Columns.asParamMappings(importColumns)
    val description: Fragment.Description = Description.LeafDesc(this)
    val queryType: QueryType = QueryType.of(clauses)
    val executable: Boolean = hasExecutableClauses(clauses)
  }

  final case class Exec(
    input: Fragment.Chain,
    query: Statement,
    localQuery: BaseState,
    remoteQuery: RemoteQuery,
    sensitive: Boolean,
    outputColumns: Seq[String],
  ) extends Fragment.Segment {
    val parameters: Map[String, String] = Columns.asParamMappings(importColumns)
    val executable: Boolean = hasExecutableClauses(query)
    val description: Fragment.Description = Description.ExecDesc(this)
    val queryType: QueryType = QueryType.of(query)
    val statementType: StatementType = StatementType.of(query)
  }

  final case class RemoteQuery(
    query: String,
    extractedLiterals: Map[String, Any]
  )

  final case class SchemaCommand(
    use: Use,
    command: ast.SchemaCommand,
  ) extends Command {
    val description: Description = Description.CommandDesc(this, "Command")
    val queryType: QueryType = QueryType.of(command)
  }

  final case class AdminCommand(
    use: Use,
    command: ast.AdministrationCommand,
  ) extends Command {
    val description: Description = Description.CommandDesc(this, "AdminCommand")
    val queryType: QueryType = QueryType.of(command)
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
    override def getIdentifiers: util.Set[String] = setAsJavaSet(fragment.outputColumns.toSet)
    override def hasProfilerStatistics: Boolean = false
    override def getProfilerStatistics: ExecutionPlanDescription.ProfilerStatistics = null
  }

  final object Description {
    final case class InitDesc(fragment: Fragment.Init) extends Description("Init", fragment) {
      override def getChildren: util.List[ExecutionPlanDescription] = list()
      override def getArguments: util.Map[String, AnyRef] = map(
        "argumentColumns" -> fragment.argumentColumns.mkString(","),
        "importColumns" -> fragment.importColumns.mkString(","),
      )
    }

    final case class ApplyDesc(fragment: Fragment.Apply) extends Description("Apply", fragment) {
      override def getChildren: util.List[ExecutionPlanDescription] = list(fragment.input.description, fragment.inner.description)
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
      override def getChildren: util.List[ExecutionPlanDescription] = list(fragment.lhs.description, fragment.rhs.description)
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
          "imp" -> list(f.importColumns),
        )
      )

      case f: Apply => node(
        name = "apply",
        fields = Seq(
          "out" -> list(f.outputColumns),
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
          "qry" -> query(f.clauses),
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
          "qry" -> query(f.query),
        ),
        children = Seq(f.input)
      )
    }

    private def use(u: Use) = u match {
      case d: Use.Declared  => "declared " + expr(d.graphSelection.expression)
      case i: Use.Inherited => "inherited " + expr(i.graphSelection.expression)
    }
  }
}





