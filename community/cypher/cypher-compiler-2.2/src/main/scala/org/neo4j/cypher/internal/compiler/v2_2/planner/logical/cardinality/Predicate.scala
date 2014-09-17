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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality

import org.neo4j.cypher.internal.compiler.v2_2.ast
import org.neo4j.cypher.internal.compiler.v2_2.ast.{Ors, LabelName, PropertyKeyName}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.PatternRelationship

sealed trait Predicate
case class ExpressionPredicate(e: ast.Expression) extends Predicate
case class PatternPredicate(p: PatternRelationship) extends Predicate

trait PredicateCombination {
  def containedPredicates: Set[Predicate]
}

case class OrCombination(or: Ors, selectedPredicate: PredicateCombination) extends PredicateCombination {
  def containedPredicates: Set[Predicate] = Set(ExpressionPredicate(or))
}

case class RelationshipWithLabels(lhs: Option[LabelName],
                                  pattern: PatternRelationship,
                                  rhs: Option[LabelName],
                                  containedPredicates: Set[Predicate]) extends PredicateCombination

case class SingleExpression(inner: ast.Expression) extends PredicateCombination {
  def containedPredicates = Set(ExpressionPredicate(inner))
}

trait PropertyAndLabelPredicate {
  def propertyKey: PropertyKeyName
  def valueCount: Int
  def label: LabelName
  def containedPredicates: Set[Predicate]
}

case class PropertyEqualsAndLabelPredicate(propertyKey: PropertyKeyName,
                                     valueCount: Int,
                                     label: LabelName,
                                     containedPredicates: Set[Predicate]) extends PredicateCombination with PropertyAndLabelPredicate

case class PropertyNotEqualsAndLabelPredicate(propertyKey: PropertyKeyName,
                                           valueCount: Int,
                                           label: LabelName,
                                           containedPredicates: Set[Predicate]) extends PredicateCombination with PropertyAndLabelPredicate
