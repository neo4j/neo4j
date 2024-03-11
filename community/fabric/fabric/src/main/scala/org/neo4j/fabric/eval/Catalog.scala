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
package org.neo4j.fabric.eval

import org.neo4j.configuration.helpers.NormalizedGraphName
import org.neo4j.cypher.internal.ast.CatalogName
import org.neo4j.fabric.eval.Catalog.normalize
import org.neo4j.fabric.util.Errors
import org.neo4j.fabric.util.Errors.show
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.kernel.database.DatabaseReference
import org.neo4j.kernel.database.DatabaseReferenceImpl
import org.neo4j.kernel.database.DatabaseReferenceImpl.External
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.neo4j.values.AnyValue
import org.neo4j.values.ElementIdDecoder
import org.neo4j.values.storable.StringValue

import java.util.UUID

import scala.jdk.OptionConverters.RichOptional

object Catalog {

  sealed trait Entry

  sealed trait Graph extends Entry {
    def id: Long
    def reference: DatabaseReference
    def uuid: UUID = reference.id()
    def name: NormalizedGraphName = toGraphName(reference.alias())
    def namespace: Option[NormalizedGraphName] = reference.namespace().toScala.map(toGraphName)
  }

  sealed trait Alias extends Graph

  case class InternalAlias(
    id: Long,
    reference: DatabaseReferenceImpl.Internal
  ) extends Alias

  case class ExternalAlias(
    id: Long,
    reference: DatabaseReferenceImpl.External
  ) extends Alias

  case class Composite(
    id: Long,
    reference: DatabaseReferenceImpl.Composite
  ) extends Graph {
    override def namespace: Option[NormalizedGraphName] = None
  }

  private def toGraphName(name: NormalizedDatabaseName) =
    new NormalizedGraphName(name.name())

  trait View extends Entry {
    val arity: Int
    val signature: Seq[Arg[_]]

    def eval(args: Seq[AnyValue], catalog: Catalog, sessionDb: DatabaseReference): Graph

    def checkArity(args: Seq[AnyValue]): Unit =
      if (args.size != arity) Errors.wrongArity(arity, args.size)

    def cast[T <: AnyValue](a: Arg[T], v: AnyValue, args: Seq[AnyValue]): T =
      try a.tpe.cast(v)
      catch {
        case _: ClassCastException => Errors.wrongType(show(signature), show(args))
      }
  }

  abstract class View1[A1 <: AnyValue](a1: Arg[A1]) extends View {
    val arity: Int = 1
    val signature: Seq[Arg[A1]] = Seq(a1)

    def eval(args: Seq[AnyValue], catalog: Catalog, sessionDb: DatabaseReference): Graph = {
      checkArity(args)
      eval(cast(a1, args(0), args), catalog, sessionDb)
    }

    def eval(a1Value: A1, catalog: Catalog, sessionDb: DatabaseReference): Graph
  }

  case class Arg[T <: AnyValue](name: String, tpe: Class[T])

  def create(
    graphAliases: Seq[Alias],
    composites: Seq[(Composite, Seq[Alias])]
  ): Catalog = {
    val databasesAndAliases = byQualifiedName(graphAliases)
    val compositesAndAliases = composites.foldLeft(Catalog.empty) { case (catalog, (composite, aliases)) =>
      val byName = byQualifiedName(composite +: aliases)
      catalog ++ byName
    }

    databasesAndAliases ++ compositesAndAliases ++ graphByNameView ++ graphByElementIdView
  }

  def empty: Catalog = Catalog(Map())

  def byQualifiedName(graphs: Seq[Graph]): Catalog =
    Catalog(graphs = graphs.map(graph => catalogName(graph) -> graph).toMap)

  def catalogName(graph: Graph): CatalogName =
    graph.namespace match {
      case Some(ns) => CatalogName(ns.name(), graph.name.name())
      case None     => CatalogName(graph.name.name())
    }

  private val graphByNameView: Catalog = {
    Catalog(
      views = Map(normalizedName("graph", "byName") -> new ByNameView())
    )
  }

  private class ByNameView() extends View1(Arg("name", classOf[StringValue])) {

    override def eval(arg: StringValue, catalog: Catalog, sessionDb: DatabaseReference): Graph =
      catalog.resolveGraphByNameString(arg.stringValue())
  }

  private val graphByElementIdView: Catalog = {
    Catalog(
      views = Map(normalizedName("graph", "byElementId") -> new ByElementIdView())
    )
  }

  private class ByElementIdView() extends View1(Arg("elementId", classOf[StringValue])) {

    override def eval(arg: StringValue, catalog: Catalog, sessionDb: DatabaseReference): Graph = {
      val elementIdText = arg.stringValue()
      val aliases = catalog.resolveNamespacedGraph(
        sessionDb.alias().name(),
        ElementIdDecoder.database(elementIdText),
        SecurityContext.AUTH_DISABLED
      ) // TODO: fix!
      if (aliases.isEmpty) {
        Errors.entityNotFound("Database corresponding to element id", elementIdText)
      }

      catalog.resolveGraphByNameString(aliases.head)
    }
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

  // TODO: Parse the argument with quoting rules instead, to allow more cases
  def resolveGraphByNameString(name: String): Catalog.Graph =
    resolveGraphOptionByNameString(name)
      .getOrElse(Errors.entityNotFound("Graph", name))

  def resolveGraphByNameString(name: String, securityContext: SecurityContext): Catalog.Graph =
    resolveGraphOptionByNameString(name, securityContext)
      .getOrElse(Errors.entityNotFound("Graph", name))

  private def resolveGraphOptionByNameString(name: String, securityContext: SecurityContext): Option[Catalog.Graph] = {
    val normalizedName = Catalog.normalize(name)
    graphs.collectFirst {
      case (cn, graph) if cn.qualifiedNameString == normalizedName && canAccessDatabase(graph, securityContext) => graph
    }
  }

  private def resolveGraphOptionByNameString(name: String): Option[Catalog.Graph] = {
    val normalizedName = Catalog.normalize(name)
    graphs.collectFirst { case (cn, graph) if cn.qualifiedNameString == normalizedName => graph }
  }

  def resolveView(name: CatalogName, args: Seq[AnyValue], sessionDb: DatabaseReference): Catalog.Graph =
    resolveViewOption(name, args, sessionDb).getOrElse(Errors.entityNotFound("View", show(name)))

  private def resolveViewOption(
    name: CatalogName,
    args: Seq[AnyValue],
    sessionDb: DatabaseReference
  ): Option[Catalog.Graph] =
    views.get(normalize(name)).map(v => v.eval(args, this, sessionDb))

  def graphNamesIn(namespace: String, securityContext: SecurityContext): Array[String] = {
    graphs.collect {
      case (cn @ CatalogName(List(`namespace`, _)), graph: Catalog.Graph)
        if canAccessDatabase(graph, securityContext) =>
        cn.qualifiedNameString
    }.toArray
  }

  def resolveNamespacedGraph(namespace: String, database: UUID, securityContext: SecurityContext): Array[String] = {
    graphs.collect {
      case (cn @ CatalogName(List(`namespace`, _)), graph: Catalog.Graph)
        if canAccessDatabase(graph, securityContext) && graph.uuid.equals(database) =>
        cn.qualifiedNameString
    }.toArray
  }

  def nameById(databaseId: UUID, securityContext: SecurityContext): Option[String] = {
    val alias = findPrimaryAlias(databaseId)
    val canSeeDb = ref => securityContext.databaseAccessMode.canSeeDatabase(ref)

    alias.filter(canSeeDb).map(_.alias().name())
  }

  private def canAccessDatabase(graph: Catalog.Graph, securityContext: SecurityContext): Boolean = {
    val hasAccessToDb = ref => securityContext.databaseAccessMode.canAccessDatabase(ref)

    graph.reference.isInstanceOf[External] && hasAccessToDb(graph.reference) || findPrimaryAlias(
      graph.reference.id
    ).exists(hasAccessToDb)
  }

  private def findPrimaryAlias(uuid: UUID): Option[DatabaseReference] = {
    graphs
      .map { case (_, graph) => graph.reference }
      .find { ref => ref.id().equals(uuid) && ref.isPrimary }
  }

  def ++(that: Catalog): Catalog = Catalog(this.graphs ++ that.graphs, this.views ++ that.views)

}
