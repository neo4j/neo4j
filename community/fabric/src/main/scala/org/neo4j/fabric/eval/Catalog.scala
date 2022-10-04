/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.fabric.eval

import org.neo4j.configuration.helpers.NormalizedGraphName
import org.neo4j.configuration.helpers.RemoteUri
import org.neo4j.cypher.internal.ast.CatalogName
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.exceptions.InternalException
import org.neo4j.fabric.eval.Catalog.normalize
import org.neo4j.fabric.util.Errors
import org.neo4j.fabric.util.Errors.show
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.IntegralValue
import org.neo4j.values.storable.StringValue

import java.util.UUID

object Catalog {

  sealed trait Entry

  sealed trait Graph extends Entry {
    def id: Long
    def uuid: UUID
    def name: Option[String]
  }

  sealed trait ConcreteGraph extends Graph

  sealed trait Alias extends Graph {
    def graphName: NormalizedGraphName
    def graphNamespace: Option[NormalizedGraphName]
  }

  case class InternalGraph(
    id: Long,
    uuid: UUID,
    graphName: NormalizedGraphName,
    databaseName: NormalizedDatabaseName
  ) extends ConcreteGraph {
    override def name: Option[String] = Some(graphName.name())
    override def toString: String = s"internal graph $id" + name.map(n => s" ($n)").getOrElse("")
  }

  // TODO: remove
  case class ExternalGraph(
    id: Long,
    name: Option[String],
    uuid: UUID
  ) extends ConcreteGraph {
    override def toString: String = s"external graph $id" + name.map(n => s" ($n)").getOrElse("")
  }

  case class InternalAlias(
    id: Long,
    uuid: UUID,
    graphName: NormalizedGraphName,
    graphNamespace: Option[NormalizedGraphName],
    databaseName: NormalizedDatabaseName
  ) extends Alias {
    override def name: Option[String] = Some(graphName.name())
    override def toString: String = s"graph alias $id" + name.map(n => s" ($n)").getOrElse("")
  }

  case class ExternalAlias(
    id: Long,
    uuid: UUID,
    graphName: NormalizedGraphName,
    graphNamespace: Option[NormalizedGraphName],
    localDatabaseName: NormalizedDatabaseName,
    remoteDatabaseName: NormalizedDatabaseName,
    uri: RemoteUri
  ) extends Alias {
    override def name: Option[String] = Some(graphName.name())

    override def toString: String = s"graph alias $id" + name.map(n => s" ($n)").getOrElse("")
  }

  case class Composite(
    id: Long,
    uuid: UUID,
    databaseName: NormalizedDatabaseName
  ) extends ConcreteGraph {
    override def name: Option[String] = Some(databaseName.name())

    override def toString: String = s"composite $id" + name.map(n => s" ($n)").getOrElse("")
  }

  case class NamespacedGraph(namespace: String, graph: Graph) extends Graph {
    override def id: Long = graph.id
    override def uuid: UUID = graph.uuid
    override def name: Option[String] = graph.name
    override def toString: String = s"${graph.toString} in namespace $namespace"
  }

  trait View extends Entry {
    val arity: Int
    val signature: Seq[Arg[_]]

    def eval(args: Seq[AnyValue], catalog: Catalog): Graph

    def checkArity(args: Seq[AnyValue]): Unit =
      if (args.size != arity) Errors.wrongArity(arity, args.size, InputPosition.NONE)

    def cast[T <: AnyValue](a: Arg[T], v: AnyValue, args: Seq[AnyValue]): T =
      try a.tpe.cast(v)
      catch {
        case _: ClassCastException => Errors.wrongType(show(signature), show(args))
      }
  }

  abstract class View1[A1 <: AnyValue](a1: Arg[A1]) extends View {
    val arity: Int = 1
    val signature: Seq[Arg[A1]] = Seq(a1)

    def eval(args: Seq[AnyValue], catalog: Catalog): Graph = {
      checkArity(args)
      eval(cast(a1, args(0), args), catalog)
    }

    def eval(a1Value: A1, catalog: Catalog): Graph
  }

  case class Arg[T <: AnyValue](name: String, tpe: Class[T])

  def create(
    internalGraphs: Seq[Graph],
    externalGraphs: Seq[Graph],
    graphAliases: Seq[Graph],
    fabricNamespace: Option[String]
  ): Catalog =
    create(internalGraphs, externalGraphs, graphAliases, Seq.empty, fabricNamespace)

  def create(
    internalGraphs: Seq[Graph],
    externalGraphs: Seq[Graph],
    graphAliases: Seq[Graph],
    composites: Seq[(Composite, Seq[Graph])],
    fabricNamespace: Option[String]
  ): Catalog = {
    if (fabricNamespace.isEmpty) {

      val databasesAndAliases = byQualifiedName(internalGraphs ++ graphAliases)
      val compositesAndAliases = composites.foldLeft(Catalog.empty) { case (catalog, (composite, aliases)) =>
        val byName = byQualifiedName(composite +: aliases)
        catalog ++ byName
      }

      databasesAndAliases ++ compositesAndAliases ++ graphByNameView

    } else {

      val allGraphs = externalGraphs ++ internalGraphs
      val byId = byIdView(allGraphs, fabricNamespace.get)
      val externalByName = byName(externalGraphs, fabricNamespace.get)
      val internalByName = byName(internalGraphs)
      val aliasesByName = byName(graphAliases)

      byId ++ externalByName ++ internalByName ++ aliasesByName
    }
  }

  def empty: Catalog = Catalog(Map())

  def byQualifiedName(graphs: Seq[Catalog.Graph]): Catalog =
    Catalog(graphs = (for {
      graph <- graphs
      name <- graph.name
      catalogName = graph match {
        case n: NamespacedGraph => normalizedName(n.namespace, name)
        case _                  => normalizedName(name)
      }
    } yield catalogName -> graph).toMap)

  def catalogName(graph: Graph): CatalogName =
    graph match {
      case n: NamespacedGraph => normalizedName(n.namespace, graphName(n.graph))
      case _                  => normalizedName(graphName(graph))
    }

  private def graphName(graph: Graph): String =
    graph match {
      case g: InternalGraph => g.graphName.name()
      case g: InternalAlias => g.graphName.name()
      case g: ExternalAlias => g.graphName.name()
      case g: Composite     => g.databaseName.name()
      case _                => throw new InternalException(s"Unexpected graph type: ${graph.getClass.getSimpleName}")
    }

  private def byName(graphs: Seq[Catalog.Graph], namespace: String*): Catalog =
    Catalog(graphs = (for {
      graph <- graphs
      name <- graph.name
      fqn = namespace :+ name
    } yield CatalogName(fqn.toList) -> graph).toMap)

  private def byIdView(graphs: Seq[Catalog.Graph], namespace: String): Catalog = {
    Catalog(
      views = Map(normalizedName(namespace, "graph") -> new ByIdView(graphs))
    )
  }

  class ByIdView(graphs: Seq[Catalog.Graph]) extends View1(Arg("gid", classOf[IntegralValue])) {

    override def eval(gid: IntegralValue, catalog: Catalog): Graph = {
      val gidValue = gid.longValue();
      graphs
        .collectFirst { case g: ConcreteGraph if g.id == gidValue => g }
        .getOrElse(Errors.entityNotFound("Graph", show(gid)))
    }
  }

  private val graphByNameView: Catalog = {
    Catalog(
      views = Map(normalizedName("graph", "byName") -> new ByNameView())
    )
  }

  class ByNameView() extends View1(Arg("name", classOf[StringValue])) {

    override def eval(arg: StringValue, catalog: Catalog): Graph =
      catalog.resolveGraphByNameString(arg.stringValue())
  }

  private def normalize(graphName: String): String =
    new NormalizedGraphName(graphName).name()

  private def normalize(name: CatalogName): CatalogName =
    CatalogName(name.parts.map(normalize))

  private def normalizedName(parts: String*): CatalogName =
    normalize(CatalogName(parts: _*))
}

case class Catalog(
  graphs: Map[CatalogName, Catalog.Graph] = Map(),
  views: Map[CatalogName, Catalog.View] = Map()
) {

  def resolveGraph(name: CatalogName): Catalog.Graph =
    resolveGraphOption(name)
      .getOrElse(Errors.entityNotFound("Graph", show(name)))

  def resolveGraphOption(name: CatalogName): Option[Catalog.Graph] =
    graphs.get(normalize(name))

  def resolveGraphByNameString(name: String): Catalog.Graph =
    resolveGraphOptionByNameString(name)
      .getOrElse(Errors.entityNotFound("Graph", name))

  // TODO: Parse the argument with quoting rules instead, to allow more cases
  def resolveGraphOptionByNameString(name: String): Option[Catalog.Graph] = {
    val normalizedName = Catalog.normalize(name)
    graphs.collectFirst { case (cn, graph) if cn.qualifiedNameString == normalizedName => graph }
  }

  def resolveView(name: CatalogName, args: Seq[AnyValue]): Catalog.Graph =
    resolveViewOption(name, args).getOrElse(Errors.entityNotFound("View", show(name)))

  def resolveViewOption(name: CatalogName, args: Seq[AnyValue]): Option[Catalog.Graph] =
    views.get(normalize(name)).map(v => v.eval(args, this))

  def graphNamesIn(namespace: String): Array[String] =
    graphs.collect {
      case (cn @ CatalogName(List(`namespace`, name)), _: Catalog.Graph) => cn.qualifiedNameString
    }.toArray

  def ++(that: Catalog): Catalog = Catalog(this.graphs ++ that.graphs, this.views ++ that.views)
}
