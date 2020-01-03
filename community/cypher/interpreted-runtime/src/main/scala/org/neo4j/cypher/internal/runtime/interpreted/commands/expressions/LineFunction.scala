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
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import java.net.URLDecoder

import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.{ExecutionContext, ResourceLinenumber}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values

case class Linenumber() extends Expression {
  override def apply(ctx: ExecutionContext, state: QueryState): AnyValue = ctx.getLinenumber match {
    case Some(ResourceLinenumber(_, line, _)) => Values.longValue(line)
    case _ => Values.NO_VALUE
  }

  override def rewrite(f: Expression => Expression): Expression = f(Linenumber())

  override def arguments: Seq[Expression] = Seq.empty

  override def children: Seq[AstNode[_]] = Seq.empty
}


case class File() extends Expression {
  override def apply(ctx: ExecutionContext, state: QueryState): AnyValue = ctx.getLinenumber match {
    case Some(ResourceLinenumber(name, _, _)) => Values.stringValue(URLDecoder.decode(name, "UTF-8")) // decode to make %20 from urls into spaces etc
    case _ => Values.NO_VALUE
  }

  override def rewrite(f: Expression => Expression): Expression = f(File())

  override def arguments: Seq[Expression] = Seq.empty

  override def children: Seq[AstNode[_]] = Seq.empty
}
