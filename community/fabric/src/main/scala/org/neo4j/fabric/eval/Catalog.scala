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
import org.neo4j.fabric.eval.Catalog.byIdView
import org.neo4j.fabric.eval.Catalog.byQualifiedName
import org.neo4j.fabric.eval.Catalog.normalize
import org.neo4j.fabric.util.Errors
import org.neo4j.fabric.util.Errors.show
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.IntegralValue
import org.neo4j.values.storable.StringValue

import java.nio.charset.Charset
import java.nio.charset.StandardCharsets
import java.util.Base64
import java.util.UUID

object Catalog {

  sealed trait Entry

  sealed trait Graph extends Entry {
    def id: Long
    def uuid: UUID
    def name: Option[String]
  }

  sealed trait ConcreteGraph extends Graph
  sealed trait Alias extends Graph

  case class InternalGraph(
    id: Long,
    uuid: UUID,
    graphName: NormalizedGraphName,
    databaseName: NormalizedDatabaseName
  ) extends ConcreteGraph {
    override def name: Option[String] = Some(graphName.name())
    override def toString: String = s"internal graph $id" + name.map(n => s" ($n)").getOrElse("")
  }

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
    databaseName: NormalizedDatabaseName
  ) extends Alias {
    override def name: Option[String] = Some(graphName.name())
    override def toString: String = s"graph alias $id" + name.map(n => s" ($n)").getOrElse("")
  }

  case class ExternalAlias(
    id: Long,
    uuid: UUID,
    graphName: NormalizedGraphName,
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

    def eval(args: Seq[AnyValue]): Graph

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

    def eval(args: Seq[AnyValue]): Graph = {
      checkArity(args)
      eval(cast(a1, args(0), args))
    }

    def eval(a1Value: A1): Graph
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
        val byNameView = graphByNameView(aliases, composite.databaseName.name())
        catalog ++ byName ++ byNameView
      }

      databasesAndAliases ++ compositesAndAliases

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
    Catalog((for {
      graph <- graphs
      name <- graph.name
      catalogName = graph match {
        case n: NamespacedGraph => normalizedName(n.namespace, name)
        case _                  => normalizedName(name)
      }
    } yield catalogName -> graph).toMap)

  private def byName(graphs: Seq[Catalog.Graph], namespace: String*): Catalog =
    Catalog((for {
      graph <- graphs
      name <- graph.name
      fqn = namespace :+ name
    } yield CatalogName(fqn.toList) -> graph).toMap)

  private def byIdView(graphs: Seq[Catalog.Graph], namespace: String): Catalog = {
    Catalog(Map(
      normalizedName(namespace, "graph") -> new ByIdView(graphs)
    ))
  }

  class ByIdView(graphs: Seq[Catalog.Graph]) extends View1(Arg("gid", classOf[IntegralValue])) {

    override def eval(gid: IntegralValue): Graph = {
      val gidValue = gid.longValue();
      graphs
        .collectFirst { case g: ConcreteGraph if g.id == gidValue => g }
        .getOrElse(Errors.entityNotFound("Graph", show(gid)))
    }
  }

  private def graphByNameView(graphs: Seq[Catalog.Graph], namespace: String): Catalog = {
    Catalog(Map(
      normalizedName(namespace, "graph") -> new ByNameView(namespace, graphs)
    ))
  }

  class ByNameView(namespace: String, graphs: Seq[Catalog.Graph]) extends View1(Arg("gid", classOf[StringValue])) {

    override def eval(arg: StringValue): Graph = {
      val name = normalize(arg.stringValue())
      graphs
        .collectFirst { case g if g.name.contains(name) => g }
        .getOrElse(Errors.entityNotFound("Graph", s"${show(arg)} in $namespace"))
    }
  }

  private def normalize(graphName: String): String =
    new NormalizedGraphName(graphName).name()

  private def normalize(name: CatalogName): CatalogName =
    CatalogName(name.parts.map(normalize))

  private def normalizedName(parts: String*): CatalogName =
    normalize(CatalogName(parts: _*))
}

case class Catalog(entries: Map[CatalogName, Catalog.Entry]) {

  def resolve(name: CatalogName): Catalog.Graph =
    resolve(name, Seq())

  def resolve(name: CatalogName, args: Seq[AnyValue]): Catalog.Graph =
    resolveOption(name, args).getOrElse(Errors.entityNotFound("Catalog entry", show(name)))

  def resolveOption(name: CatalogName): Option[Catalog.Graph] =
    resolveOption(name, Seq())

  def resolveOption(name: CatalogName, args: Seq[AnyValue]): Option[Catalog.Graph] = {
    val normalizedName = normalize(name)
    entries.get(normalizedName) match {
      case None =>
        None

      case Some(g: Catalog.Graph) =>
        if (args.nonEmpty) Errors.wrongArity(0, args.size, InputPosition.NONE)
        else Some(g)

      case Some(v: Catalog.View) =>
        Some(v.eval(args))
    }
  }

  def graphNamesIn(namespace: String): Array[String] =
    entries.collect {
      case (CatalogName(List(`namespace`, name)), _: Catalog.Graph) => name
    }.toArray

  def ++(that: Catalog): Catalog = Catalog(this.entries ++ that.entries)
}
