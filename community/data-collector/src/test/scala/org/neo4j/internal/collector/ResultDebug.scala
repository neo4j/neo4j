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
package org.neo4j.internal.collector

object ResultDebug {
  def asJson(result: Any, indent: String = ""): String =
    result match {
      case seq: Seq[_] =>
        "\n" + indent + "[\n" +
          seq.map(indent + asJson(_, indent = indent + "  ")).mkString(",\n") + "\n" +
          indent + "]"
      case map: Map[String, _] =>
        "\n" + indent + "{\n" +
          map.map{
            case (key, value) => asJson(key, value, indent + "  ")
          }.mkString(",\n") + "\n" +
          indent + "}"
      case x => x.toString
    }

  def asJson(key: String, value: Any, indent: String): String =
    indent + key + ": " + asJson(value, indent = indent + "  ")
}
