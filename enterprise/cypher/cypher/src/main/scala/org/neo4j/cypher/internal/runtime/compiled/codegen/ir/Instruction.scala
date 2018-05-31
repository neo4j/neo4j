/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.ir

import org.neo4j.cypher.internal.runtime.compiled.codegen.CodeGenContext
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.MethodStructure

trait Instruction {
  def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit = children.foreach(_.init(generator))
  def body[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit

  protected def children: Seq[Instruction]

  private def treeView: Seq[Instruction] = {
    children.foldLeft(Seq(this)) { (acc, child) => acc ++ child.treeView }
  }

  // Aggregating methods -- final to prevent overriding
  final def allOperatorIds: Set[String] = treeView.flatMap(_.operatorId).toSet

  protected def operatorId: Set[String] = Set.empty
}

object Instruction {

  val empty = new Instruction {
    override def body[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {}

    override protected def children = Seq.empty

    override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {}
  }
}
