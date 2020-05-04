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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.ir.StrictnessMode
import org.neo4j.cypher.internal.util.attribution.IdGen

/**
 * For each input row, create a new node with the provided labels and properties,
 * and assign it to the variable 'idName'.
 *
 * This is a special version of CreateNode, which is used in a merge plan after checking that no node with the same
 * labels and properties exist.
 */
case class MergeCreateNode(source: LogicalPlan, idName: String, labels: Seq[LabelName], properties: Option[Expression])(implicit idGen: IdGen)
  extends LogicalPlan(idGen) with UpdatingPlan {

  override def lhs: Option[LogicalPlan] = Some(source)

  override val availableSymbols: Set[String] = {
    source.availableSymbols + idName
  }

  override def rhs: Option[LogicalPlan] = None

  override def strictness: StrictnessMode = source.strictness
}
