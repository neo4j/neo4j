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
package cypher.features

import org.neo4j.cypher.testing.api.Incoming
import org.neo4j.cypher.testing.api.Node
import org.neo4j.cypher.testing.api.Outgoing
import org.neo4j.cypher.testing.api.Path
import org.neo4j.cypher.testing.api.Relationship
import org.neo4j.values.storable.DurationValue

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetTime
import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit
import java.time.temporal.TemporalAmount

object CypherTestValueToString extends (Any => String) {

  def apply(value: Any): String = {
    def convertList(elements: Iterable[_]): String = {
      val convertedElements = elements.map(CypherTestValueToString)
      s"[${convertedElements.mkString(", ")}]"
    }

    value match {
      case null => "null"

      case n: Node =>
        val labels = n.labels
        val labelString = if (labels.isEmpty) "" else labels.mkString(":", ":", " ")
        val properties = CypherTestValueToString(n.properties)
        s"($labelString$properties)"

      case r: Relationship =>
        val relType = r.relType
        val properties = CypherTestValueToString(r.properties)
        s"[:$relType$properties]"

      case a: Array[_] => convertList(a)

      case s: Seq[_] => convertList(s)

      case m: Map[_, _] =>
        val properties = m.map {
          case (k, v) => (k, CypherTestValueToString(v))
        }
        s"{${properties.map {
            case (k, v) => s"$k: $v"
          }.mkString(", ")}}"

      case path: Path =>
        val string = path.connections.foldLeft(CypherTestValueToString(path.startNode)) {
          case (currentString, nextConnection: Outgoing) =>
            s"$currentString-${CypherTestValueToString(nextConnection.relationship)}->${CypherTestValueToString(nextConnection.node)}"
          case (currentString, nextConnection: Incoming) =>
            s"$currentString<-${CypherTestValueToString(nextConnection.relationship)}-${CypherTestValueToString(nextConnection.node)}"
        }
        s"<$string>"

      //  TCK values parser expects escaped backslashes or single quotes so we have to mirror that here
      case s: String  => s"'${s.replace("\\", "\\\\").replace("'", "\\'")}'"
      case l: Long    => l.toString
      case i: Integer => i.toString
      case d: Double  => d.toString
      case f: Float   => f.toString
      case b: Boolean => b.toString
      // TODO workaround to escape date time strings until TCK error
      // with colons in unescaped strings is fixed.
      case x: LocalTime      => s"'${x.toString}'"
      case x: LocalDate      => s"'${x.toString}'"
      case x: LocalDateTime  => s"'${x.toString}'"
      case x: OffsetTime     => s"'${x.toString}'"
      case x: ZonedDateTime  => s"'${x.toString}'"
      case x: TemporalAmount =>
        // Cypher Duration type is represented as TemporalAmount and should always have these 4 fields:
        val duration = DurationValue.duration(
          x.get(ChronoUnit.MONTHS),
          x.get(ChronoUnit.DAYS),
          x.get(ChronoUnit.SECONDS),
          x.get(ChronoUnit.NANOS)
        )
        // The tests expect to string of the duration to be in a specific format, which DurationValue can produce
        s"'${duration.toString}'"

      case other =>
        println(s"could not convert $other of type ${other.getClass}")
        other.toString
    }
  }
}
