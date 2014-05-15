/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.pipes

import org.neo4j.cypher.internal.compiler.v2_1._
import org.neo4j.cypher.internal.compiler.v2_1.symbols.CTNode
import org.neo4j.cypher.internal.compiler.v2_1.commands.expressions.Expression
import org.neo4j.kernel.api.index.IndexDescriptor
import org.neo4j.cypher.internal.compiler.v2_1.symbols.SymbolTable
import org.neo4j.cypher.internal.compiler.v2_1.PlanDescription.Arguments.{IntroducedIdentifier, Index}

case class NodeUniqueIndexSeekPipe(ident: String,
                                   label: Either[String, LabelId],
                                   propertyKey: Either[String, PropertyKeyId],
                                   valueExpr: Expression)(implicit pipeMonitor: PipeMonitor) extends Pipe {

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val optLabelId = label match {
      case Left(str)      => state.query.getOptLabelId(str).map(LabelId)
      case Right(labelId) => Some(labelId)
    }

    val optPropertyKeyId = propertyKey match {
      case Left(str)      => state.query.getOptPropertyKeyId(str).map(PropertyKeyId)
      case Right(propertyKeyId) => Some(propertyKeyId)
    }

    (optLabelId, optPropertyKeyId) match {
      case (Some(labelId), Some(propertyKeyId)) => {
        val descriptor = new IndexDescriptor(labelId.id, propertyKeyId.id)
        val value = valueExpr(ExecutionContext.empty)(state)
        state.query.exactUniqueIndexSearch(descriptor, value) match {
          case Some(node) => Iterator(ExecutionContext.from(ident -> node))
          case _          => Iterator.empty
        }
      }
      case _ => Iterator.empty
    }
  }

  def exists(predicate: Pipe => Boolean): Boolean = predicate(this)

  private def labelName = label match {
    case Left(name) => name
    case Right(id) => id.toString
  }

  private def propertyName = propertyKey match {
    case Left(name) => name
    case Right(id) => id.toString
  }

  def planDescription = new PlanDescriptionImpl(this, "NodeUniqueIndexSeek", NoChildren, Seq(
    IntroducedIdentifier(ident), Index(labelName, propertyName)))

  def symbols: SymbolTable = new SymbolTable(Map(ident -> CTNode))

  override def monitor = pipeMonitor
}
