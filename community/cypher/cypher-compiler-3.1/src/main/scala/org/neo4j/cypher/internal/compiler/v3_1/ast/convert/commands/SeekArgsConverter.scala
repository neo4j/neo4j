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
package org.neo4j.cypher.internal.compiler.v3_1.ast.convert.commands

import org.neo4j.cypher.internal.compiler.v3_1.ast.convert.commands.ExpressionConverters.toCommandExpression
import org.neo4j.cypher.internal.compiler.v3_1.pipes.{ManySeekArgs, SeekArgs, SingleSeekArg}
import org.neo4j.cypher.internal.frontend.v3_1.ast.{Expression, ListLiteral}
import org.neo4j.cypher.internal.ir.v3_1.helpers.{Many, One, Zero, ZeroOneOrMany}
import org.neo4j.cypher.internal.ir.v3_1.logical.plans.{ManySeekableArgs, SeekableArgs, SingleSeekableArg}

case object SeekArgsConverter {
  def toCommandSeekArgs(args: SeekableArgs): SeekArgs = args match {
    case ManySeekableArgs(coll: ListLiteral) =>
      ZeroOneOrMany(coll.expressions) match {
        case Zero => SeekArgs.empty
        case One(value) => SingleSeekArg(toCommandExpression(value))
        case Many(values) => ManySeekArgs(toCommandExpression(coll))
      }

    case ManySeekableArgs(exp: Expression) =>
      ManySeekArgs(toCommandExpression(exp))

    case SingleSeekableArg(exp: Expression) =>
      SingleSeekArg(toCommandExpression(exp))
  }
}
