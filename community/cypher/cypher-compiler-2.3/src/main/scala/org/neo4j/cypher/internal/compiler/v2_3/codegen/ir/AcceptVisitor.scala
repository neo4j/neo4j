/*
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
package org.neo4j.cypher.internal.compiler.v2_3.codegen.ir

import org.neo4j.cypher.internal.compiler.v2_3.codegen.CodeGenerator.n
import org.neo4j.cypher.internal.compiler.v2_3.codegen.JavaUtils.{JavaString, JavaSymbol}
import org.neo4j.cypher.internal.compiler.v2_3.codegen.MethodStructure

case class AcceptVisitor(id: String, columns: Map[String, JavaSymbol]) extends Instruction {


  override protected def columnNames = columns.keys

  override def body[E](generator: MethodStructure[E]) = generator.trace(id) { body =>
    columns.foreach { case (k, v) =>
      body.setInRow(k, v.generate(body))
    }
    body.visitRow()
    body.incrementRows()
  }

  def generateCode() = {
    val eventVar = "event_" + id
    s"""${columns.toSeq.map { case (k, v) => s"""row.set( "${k.toJava}", ${v.materialize.name} );""" }.mkString(n)}
       |try ( QueryExecutionEvent $eventVar = tracer.executeOperator( $id ) )
       |{
       |if ( !visitor.visit(row) )
       |{
       |success();
       |return;
       |}
       |$eventVar.row();
       |}""".stripMargin
  }

  def generateInit() = ""

  def members() = {
    val columnsList = columns.keys.map(_.toJava) match {
      case Nil => "Collections.emptyList()"
      case lst => s"Arrays.asList( ${lst.mkString("\"", "\", \"", "\"")} )"
    }

    s"""private final List<String> javaColumns = $columnsList;
       |
       |@Override
       |public List<String> javaColumns( )
       |{
       |return this.javaColumns;
       |}""".
      stripMargin
  }

  override protected def operatorId = Some(id)

  override protected def importedClasses = Set(
    "java.util.List", "java.util.Arrays", "java.util.Collections"
  )

  override protected def children = Seq.empty
}
