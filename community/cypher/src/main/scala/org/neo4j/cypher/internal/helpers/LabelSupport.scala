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
import org.neo4j.cypher.internal.commands.values.LabelValue
import org.neo4j.cypher.CypherTypeException
import org.neo4j.cypher.internal.commands.expressions.Expression
import org.neo4j.cypher.internal.spi.QueryContext

trait LabelSupport extends CollectionSupport {

  def labelSeq(context: ExecutionContext, labelSeqExpr: Expression): Iterable[LabelValue] =
    liftAsCollection[LabelValue]({ case (l: LabelValue) => l})(labelSeqExpr(context)).getOrElse {
          throw new CypherTypeException("Encountered label collection with non-label values")
     }


  def getLabelsAsLongs(executionContext: ExecutionContext, labels: Expression) =
    labelSeq(executionContext, labels).map(_.resolveForId(executionContext.state.queryContext).id)
}