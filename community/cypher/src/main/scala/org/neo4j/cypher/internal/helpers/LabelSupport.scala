/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.helpers

import org.neo4j.cypher.internal.ExecutionContext
import org.neo4j.cypher.internal.commands.values.{LabelName, LabelValue}
import org.neo4j.cypher.CypherTypeException
import org.neo4j.cypher.internal.commands.expressions.{Literal, Expression}
import org.neo4j.cypher.internal.spi.QueryContext
import org.neo4j.cypher.internal.pipes.QueryState

object LabelSupport extends CollectionSupport {

  def getLabelsAsLongs(context: ExecutionContext, labels: Expression)(implicit state: QueryState) =
    makeTraversable(labels(context)).map {
      case x: LabelValue => x.resolveForId(state.query).id
      case _             => throw new CypherTypeException("Label expressions must return labels")
    }

  def labelCollection(elems: String*): Expression = Literal(Seq(elems.map(LabelName(_)): _*))
}