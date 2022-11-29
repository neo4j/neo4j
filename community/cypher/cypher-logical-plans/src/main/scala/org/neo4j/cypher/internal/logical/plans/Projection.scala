/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.macros.AssertMacros
import org.neo4j.cypher.internal.util.attribution.IdGen

/**
 * For each source row produce:
 * - the projected expressions (`projectExpressions`)
 * - columns from the source that are not discarded (`discardSymbols`)
 * 
 * For each entry in 'expressions', the produced row get an extra variable 
 * name as the key, with the value of the expression.
 * 
 * Implementations are allowed to ignore the `discardSymbols` 
 * (for performance reasons for example). 
 */
case class Projection(
  override val source: LogicalPlan,
  discardSymbols: Set[String],
  projectExpressions: Map[String, Expression]
)(implicit idGen: IdGen) extends LogicalUnaryPlan(idGen) with ProjectingPlan {

  AssertMacros.checkOnlyWhenAssertionsAreEnabled(
    discardSymbols.diff(source.availableSymbols).isEmpty,
    s"Unknown discard symbols: ${discardSymbols.diff(source.availableSymbols)}"
  )

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)

  val availableSymbols: Set[String] = (source.availableSymbols -- discardSymbols) ++ projectExpressions.keySet

  /**
   * Custom copy method to make rewriting work.
   */
  def copy(
    source: LogicalPlan = this.source,
    discardSymbols: Set[String] = this.discardSymbols,
    projectExpressions: Map[String, Expression] = this.projectExpressions
  )(implicit idGen: IdGen = this.idGen): Projection = {
    val newDiscardSymbols = discardSymbols.intersect(source.availableSymbols)
    Projection(source, newDiscardSymbols, projectExpressions)(idGen)
  }
}
