/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v2_3.birk.il

import org.neo4j.cypher.internal.compiler.v2_3.birk.CodeGenerator.n

case class ProduceResults(nodes: Map[String, String],
                          relationships: Map[String, String],
                          other: Map[String, String]) extends Instruction {

  val columns = nodes.keySet ++ relationships.keySet ++ other.keySet

  def generateCode() =
    s"""${nodes.toSeq.map { case (k, v) => s"""row.setNode("$k", $v);"""}.mkString(n)}
       |${relationships.toSeq.map { case (k, v) => s"""row.setRelationship("$k", $v);"""}.mkString(n)}
       |${other.toSeq.map { case (k, v) => s"""row.set("$k", $v);"""}.mkString(n)}
       |if ( !visitor.visit(row) )
       |{
       |return;
       |}""".stripMargin

  def generateInit() = ""


  def fields() = {
    val columnsList = columns.toList match {
      case Nil => "Collections.emptyList()"
      case lst => s"Arrays.asList( ${lst.mkString("\"", "\", \"", "\"")} )"
    }

    s"""@Override
       |public List<String> javaColumns( )
       |{
       |return $columnsList;
       |}""".
      stripMargin
    }


  override def _importedClasses() = Set(
    "java.util.List", "java.util.Arrays", "java.util.Collections"
  )
}
