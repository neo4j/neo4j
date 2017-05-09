/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans

import org.neo4j.cypher.internal.ir.v3_3.{CardinalityEstimation, IdName, PlannerQuery}

/*
RollUp is the inverse of the Unwind operator. For each row passed in from the LHS, the whole RHS is executed.
For each row produced by the RHS, a single column value is extracted and inserted into a collection.

It is used for sub queries that return collections, such as pattern expressions (returns a collection of paths) and
pattern comprehension.

Note about nullableIdentifiers: when any of these identifiers is null, the collection should be null.
 */
case class RollUpApply(source: LogicalPlan, inner: LogicalPlan, collectionName: IdName, variableToCollect: IdName,
                       nullableVariables: Set[IdName])(val solved: PlannerQuery with CardinalityEstimation)
  extends LogicalPlan with LazyLogicalPlan {

  override def lhs = Some(source)

  override def availableSymbols = source.availableSymbols + collectionName

  override def rhs = Some(inner)
}
