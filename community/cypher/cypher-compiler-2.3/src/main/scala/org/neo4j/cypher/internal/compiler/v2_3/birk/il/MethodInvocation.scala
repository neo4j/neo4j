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
package org.neo4j.cypher.internal.compiler.v2_3.birk.il

import org.neo4j.cypher.internal.compiler.v2_3.birk.CodeGenerator.n

case class MethodInvocation(override val operatorId: Option[String],
                            resultVariable: String,
                            resultType: String,
                            methodName: String,
                            statements: Seq[Instruction]) extends Instruction {

  def generateCode() = operatorId match {
    case Some(id) =>
      s"""final $resultType $resultVariable;
         |try ( QueryExecutionEvent event_$id = tracer.executeOperator( $id ) )
         |{
         |$resultVariable = $methodName();
         |}
       """.stripMargin
    case None => s"final $resultType $resultVariable = $methodName();"
  }

  override def methods = super.methods

  override def children = statements

  def generateInit() = ""

  override protected def _method = Some(new Method {
    def generateCode = {
      val init = statements.map(_.generateInit()).reduce(_ + n + _)
      val methodBody = statements.map(_.generateCode()).reduce(_ + n + _)

      s"""private $resultType $methodName() throws KernelException
         |{
         |$init
         |$methodBody
         |return $resultVariable;
         |}""".
        stripMargin
    }

    def name = methodName
  })

  def members() = statements.map(_.members()).reduce(_ + n + _)

  override def _importedClasses() = Set("org.neo4j.kernel.api.exceptions.KernelException")
}
