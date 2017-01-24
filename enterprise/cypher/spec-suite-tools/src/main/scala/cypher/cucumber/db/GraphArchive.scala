/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cypher.cucumber.db

import cypher.cucumber.db.GraphArchive.Use.{Updating, ReadOnly}
import cypher.cucumber.db.GraphRecipe.CypherScript
import org.json4s.JsonAST.JString
import org.json4s.{CustomSerializer, Formats}
import org.neo4j.kernel.internal.Version

import scala.reflect.io.File

object GraphArchive {
  def apply(graph: GraphRecipe.Descriptor[CypherScript], dbConfig: Map[String, String] = Map.empty) =
    Descriptor(graph, dbConfig = dbConfig)

  final case class Descriptor(recipe: GraphRecipe.Descriptor[CypherScript],
                              os: OperatingSystem.Descriptor = OperatingSystem.local,
                              runtime: JavaRuntime.Descriptor = JavaRuntime.local,
                              kernel: GraphKernel.Descriptor = GraphKernel.local,
                              dbConfig: Map[String, String] = Map.empty
                             ) {

    self =>
    override val toString = {
      val code = hashCode()
      s"${recipe.name}-archive${if (code >= 0) "+" else ""}$code"
    }

    def scripts = recipe.scripts

    def readOnlyUse = ReadOnly(self)
    def updatingUse = Updating(self)
  }

  sealed trait Use {
    def archive: Descriptor
    def dbConfig: Map[String, String]
  }

  object Use {
    final case class ReadOnly(archive: Descriptor) extends Use {
      def dbConfig = archive.dbConfig + ("dbms.read_only" -> "true")
    }

    final case class Updating(archive: Descriptor) extends Use {
      def dbConfig = archive.dbConfig
    }
  }
}


object GraphRecipe {

  final case class CypherScript(file: File, hash: String)

  final case class Descriptor[T](name: String,
                                 scripts: Seq[T],
                                 nodes: Set[NodePropertyInfo],
                                 relationships: Set[RelationshipPropertyInfo],
                                 labels: Set[LabelInfo]
                                ) {
    def mapScripts[S](f: T => S) = copy(scripts = scripts.map(f))

    val labelImplications: Map[String, Set[String]] = labels
      .map(l => l.label -> l.sublabels.filter(_.advices(ImpliedAdvice)).map(_.label))
      .filter { case (l, implied) => implied.nonEmpty }
      .toMap

    val allUniqueNodeProperties = nodes.flatMap(_.collectIf(UniqueAdvice)(n => n.label -> n.key))
    val allIndexedNodeProperties = nodes.flatMap(_.collectIf(IndexAdvice)(n => n.label -> n.key))

    val uniqueNodeProperties = allUniqueNodeProperties.filter(!allUniqueNodeProperties.coveredByImpliedLabel(_)).properties
    val indexedNodeProperties = allIndexedNodeProperties.filter(entry => !allIndexedNodeProperties.coveredByImpliedLabel(entry) && !uniqueNodeProperties(entry)).properties

    // Neo4j disk size estimation
    val nodeSize = nodes.find(i => i.label == "" && i.key == "").map(_.count * 14).get
    val nodePropertySize = nodes.filter(i => i.label == "" && i.key != "").map(_.count * 41).sum
    val relationshipSize = relationships.find(i => i.`type` == "" && i.key == "").map(_.count * 33).get
    val relationshipPropertySize = relationships.filter(i => i.`type` == "" && i.key != "").map(_.count * 41).sum

    val estimatedDiskSize =
      nodeSize + nodePropertySize + relationshipSize + relationshipPropertySize

    val recommendedPageCacheSize = (estimatedDiskSize + (estimatedDiskSize / 5)) * 2

    implicit class NodeProperties(val properties: Set[(String, String)]) {
      self =>

      def coveredByImpliedLabel(entry: (String, String), seen: Set[String] = Set.empty): Boolean = {
        val (label, key) = entry
        labelImplications.get(label).exists {
          impliedLabels =>
            impliedLabels.exists(impliedLabel => !seen(impliedLabel) && (properties.contains(impliedLabel -> key) || coveredByImpliedLabel(impliedLabel -> key, seen + impliedLabel)))
        }
      }

      def filter(f: ((String, String)) => Boolean): NodeProperties =
        new NodeProperties(properties.filter(f))
    }
  }

  final case class NodePropertyInfo(label: String, key: String, count: Int, distinct: Int, advices: Set[Advice]) {
    def collectIf[T](advice: Advice)(f: this.type => T): Option[T] = if (advices(advice)) Some(f(this)) else None
  }

  final case class RelationshipPropertyInfo(`type`: String, key: String, count: Int, distinct: Int, advices: Set[Advice])

  final case class LabelInfo(label: String, count: Int, sublabels: Set[LabelInfo], advices: Set[Advice])

  sealed class Advice(val name: String)

  case object ExistsAdvice extends Advice("exists")
  case object UniqueAdvice extends Advice("unique")
  case object IndexAdvice extends Advice("index")
  case object ImpliedAdvice extends Advice("implied")

  class AdviceSerializer extends CustomSerializer[Advice]((formats: Formats) => (
      {
        case JString(ExistsAdvice.name) => ExistsAdvice
        case JString(UniqueAdvice.name) => UniqueAdvice
        case JString(IndexAdvice.name) => IndexAdvice
        case JString(ImpliedAdvice.name) => ImpliedAdvice
      },
      {
        case x: Advice => JString(x.name)
      }
    ))
}

object OperatingSystem {
  val local = Descriptor(
    System.getProperty("os.name"),
    System.getProperty("os.version"),
    System.getProperty("os.arch")
  )

  final case class Descriptor(name: String, version: String, arch: String)
}

object JavaRuntime {
  val local = Descriptor(
    System.getProperty("java.vendor"),
    System.getProperty("java.version")
  )

  final case class Descriptor(vendor: String, version: String)
}

object GraphKernel {
  val local = Descriptor(Version.getKernelVersion)

  final case class Descriptor(version: String)
}


