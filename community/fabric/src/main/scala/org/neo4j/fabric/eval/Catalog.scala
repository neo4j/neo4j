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
package org.neo4j.fabric.eval

import java.util.UUID

import org.neo4j.configuration.helpers.NormalizedDatabaseName
import org.neo4j.configuration.helpers.NormalizedGraphName
import org.neo4j.cypher.internal.ast.CatalogName
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.fabric.util.Errors
import org.neo4j.fabric.util.Errors.show
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.IntegralValue

object Catalog {

  sealed trait Entry

  sealed trait Graph extends Entry {
    def id: Long
    def uuid: UUID
    def name: Option[String]
  }

  case class InternalGraph(
    id: Long,
    uuid: UUID,
    graphName: NormalizedGraphName,
    databaseName: NormalizedDatabaseName
  ) extends Graph {
    def name: Option[String] = Some(graphName.name())
    override def toString: String = s"internal graph $id" + name.map(n => s" ($n)").getOrElse("")
  }

  case class ExternalGraph(
    id: Long,
    name: Option[String],
    uuid: UUID
  ) extends Graph {
    override def toString: String = s"external graph $id" + name.map(n => s" ($n)").getOrElse("")
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

  case class View1[A1 <: AnyValue](a1: Arg[A1])(f: A1 => Graph) extends View {
    val arity = 1
    val signature = Seq(a1)

    def eval(args: Seq[AnyValue]): Graph = {
      checkArity(args)
      f(cast(a1, args(0), args))
    }
  }

  case class Arg[T <: AnyValue](name: String, tpe: Class[T])

  def create(internalGraphs: Seq[Graph], externalGraphs: Seq[Graph], fabricNamespace: Option[String]): Catalog = {
    if (fabricNamespace.isEmpty) {
      byName(internalGraphs)
    } else {
      val allGraphs = externalGraphs ++ internalGraphs
      val byId = byIdView(allGraphs, fabricNamespace.get)
      val externalByName = byName(externalGraphs, fabricNamespace.get)
      val internalByName = byName(internalGraphs)

      byId ++ externalByName ++ internalByName
    }
  }

  private def byName(graphs: Seq[Catalog.Graph], namespace: String*): Catalog =
    Catalog((for {
      graph <- graphs
      name <- graph.name
      fqn = namespace :+ name
    } yield CatalogName(fqn.toList) -> graph).toMap)

  private def byIdView(lookupIn: Seq[Catalog.Graph], namespace: String): Catalog =
    Catalog(Map(
      CatalogName(namespace, "graph") -> View1(Arg("gid", classOf[IntegralValue]))(gid =>
        lookupIn
          .collectFirst { case g if g.id == gid.longValue() => g }
          .getOrElse(Errors.entityNotFound("Graph", show(gid)))
      )
    ))
}

case class Catalog(entries: Map[CatalogName, Catalog.Entry]) {

  def resolve(name: CatalogName): Catalog.Graph =
    resolve(name, Seq())

  def resolve(name: CatalogName, args: Seq[AnyValue]): Catalog.Graph = {
    val normalizedName = CatalogName(name.parts.map(normalize))
    entries.get(normalizedName) match {
      case None => Errors.entityNotFound("Catalog entry", show(name))

      case Some(g: Catalog.Graph) =>
        if (args.nonEmpty) Errors.wrongArity(0, args.size, InputPosition.NONE)
        else g

      case Some(v: Catalog.View) => v.eval(args)
    }
  }

  private def normalize(name: String): String =
    new NormalizedGraphName(name).name

  def ++(that: Catalog): Catalog = Catalog(this.entries ++ that.entries)
}
