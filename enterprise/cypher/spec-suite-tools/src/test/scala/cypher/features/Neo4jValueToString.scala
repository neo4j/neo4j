/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package cypher.features

import java.time._
import java.time.temporal.TemporalAmount

import org.neo4j.graphdb.{Node, Path, Relationship}
import org.neo4j.values.storable.DurationValue

import scala.collection.JavaConverters._

object Neo4jValueToString extends (Any => String) {

  def apply(value: Any): String = {
    def convertList(elements: Traversable[_]): String = {
      val convertedElements = elements.map(Neo4jValueToString)
      s"[${convertedElements.mkString(", ")}]"
    }

    value match {
      case null => "null"

      case n: Node =>
        val labels = n.getLabels.asScala.map(_.name()).toList
        val labelString = if (labels.isEmpty) "" else labels.mkString(":", ":", " ")
        val properties = Neo4jValueToString(n.getAllProperties)
        s"($labelString$properties)"

      case r: Relationship =>
        val relType = r.getType.name()
        val properties = Neo4jValueToString(r.getAllProperties)
        s"[:$relType$properties]"

      case a: Array[_] => convertList(a)

      case l: java.util.List[_] => convertList(l.asScala)

      case m: java.util.Map[_, _] =>
        val properties = m.asScala.map {
          case (k, v) => (k.toString, Neo4jValueToString(v))
        }
        s"{${
          properties.map {
            case (k, v) => s"$k: $v"
          }.mkString(", ")
        }}"

      case path: Path =>
        val (string, _) = path.relationships().asScala.foldLeft((Neo4jValueToString(path.startNode()), path.startNode().getId)) {
          case ((currentString, currentNodeId), nextRel) =>
            if (currentNodeId == nextRel.getStartNodeId) {
              val updatedString = s"$currentString-${Neo4jValueToString(nextRel)}->${Neo4jValueToString(nextRel.getEndNode)}"
              updatedString -> nextRel.getEndNodeId
            } else {
              val updatedString = s"$currentString<-${Neo4jValueToString(nextRel)}-${Neo4jValueToString(nextRel.getStartNode)}"
              updatedString -> nextRel.getStartNodeId
            }
        }
        s"<$string>"

      case s: String => s"'$s'"
      case l: Long => l.toString
      case i: Integer => i.toString
      case d: Double => d.toString
      case f: Float => f.toString
      case b: Boolean => b.toString
      // TODO workaround to escape date time strings until TCK error
      // with colons in unescaped strings is fixed.
      case x: LocalTime => s"'${x.toString}'"
      case x: LocalDate => s"'${x.toString}'"
      case x: LocalDateTime => s"'${x.toString}'"
      case x: OffsetTime => s"'${x.toString}'"
      case x: ZonedDateTime => s"'${x.toString}'"
      case x: DurationValue => s"'${x.prettyPrint}'"
      case x: TemporalAmount => s"'${x.toString}'"

      case other =>
        println(s"could not convert $other of type ${other.getClass}")
        other.toString
    }
  }

}
