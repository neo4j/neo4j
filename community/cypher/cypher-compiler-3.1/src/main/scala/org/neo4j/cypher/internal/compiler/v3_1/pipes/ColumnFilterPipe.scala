/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_1.pipes

import org.neo4j.cypher.internal.compiler.v3_1._
import org.neo4j.cypher.internal.compiler.v3_1.commands._
import org.neo4j.cypher.internal.compiler.v3_1.commands.expressions._
import org.neo4j.cypher.internal.compiler.v3_1.helpers.UnNamedNameGenerator.isNamed
import org.neo4j.cypher.internal.compiler.v3_1.planDescription.InternalPlanDescription.Arguments
import org.neo4j.cypher.internal.compiler.v3_1.planDescription.{PlanDescriptionImpl, SingleChild}
import org.neo4j.cypher.internal.compiler.v3_1.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v3_1.symbols._
import org.neo4j.cypher.internal.compiler.v3_1.executionplan.Effects._


case class ColumnFilterPipe(source: Pipe, returnItems: Seq[ReturnItem])
                           (implicit pipeMonitor: PipeMonitor) extends PipeWithSource(source, pipeMonitor) {
  val returnItemNames: Seq[String] = returnItems.map(_.name)
  val symbols = SymbolTable(variables2.toMap)

  private lazy val variables2: Seq[(String, CypherType)] = returnItems.
    map( ri => ri.name->ri.expression.getType(source.symbols))

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState) = {
    input.map(ctx => {
      val newMap = MutableMaps.create(ctx.size)

      returnItems.foreach {
        case ReturnItem(Variable(oldName), newName) if isNamed(newName) => newMap.put(newName, ctx(oldName))
        case ReturnItem(CachedExpression(oldName, _), newName)            => newMap.put(newName, ctx(oldName))
        case ReturnItem(_, name)                                          => newMap.put(name, ctx(name))
      }

      ctx.newFromMutableMap( newMap )
    })
  }

  def planDescription =
    new PlanDescriptionImpl(this.id, "ColumnFilter", SingleChild(source.planDescription),
      Seq(Arguments.ColumnsLeft(returnItemNames.toList)), variables)

  def dependencies = Seq()

  def dup(sources: List[Pipe]): Pipe = {
    val (head :: Nil) = sources
    copy(source = head)
  }

  override def localEffects = returnItems.effects(symbols)
}
