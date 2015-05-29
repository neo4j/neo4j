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
import org.neo4j.cypher.internal.compiler.v2_3.codegen.JavaUtils.JavaSymbol
import org.neo4j.cypher.internal.compiler.v2_3.codegen._

case class MethodInvocation(override val operatorId: Option[String],
                            symbol:JavaSymbol,
                            methodName: String,
                            statements: Seq[Instruction]) extends Instruction {

  override def init[E](generator: MethodStructure[E]) = {}

  override def body[E](generator: MethodStructure[E]) = {
    generator.method(symbol.tableType.get, symbol.name, methodName) { body =>
      statements.foreach(_.init(body))
      statements.foreach(_.body(body))
    }
  }

  def generateCode() = operatorId match {
    case Some(id) =>
      s"""final ${symbol.javaType} ${symbol.name};
         |try ( QueryExecutionEvent event_$id = tracer.executeOperator( $id ) )
         |{
         |${symbol.name} = $methodName();
         |}
       """.stripMargin
    case None => s"final ${symbol.javaType} ${symbol.name} = $methodName();"
  }

  override def children = statements

  def generateInit() = ""

  def members() = statements.map(_.members()).reduce(_ + n + _)

  override protected def method = Some(new Method {
    def generateCode = {
      val init = statements.map(_.generateInit()).reduce(_ + n + _)
      val methodBody = statements.map(_.generateCode()).reduce(_ + n + _)
      val exceptions = statements.flatMap(_.allExceptions).map(_.throwClause)
      val throwClause = if (exceptions.isEmpty) "" else exceptions.mkString("throws ", ",", "")

      s"""private ${symbol.javaType} $methodName() throws KernelException
         |{
         |$init
         |$methodBody
         |return ${symbol.name};
         |}""".
        stripMargin
    }

    def name = methodName
  })

  override protected def exceptions = Set(KernelExceptionCodeGen)

  override protected def importedClasses = Set("org.neo4j.kernel.api.exceptions.KernelException")
}
