/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.executionplan.builders

import org.mockito.Mockito._
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.commands.expressions.{Collection, Identifier, Property}
import org.neo4j.cypher.internal.compiler.v2_2.commands.values.{UnresolvedLabel, UnresolvedProperty}
import org.neo4j.cypher.internal.compiler.v2_2.commands.{AnyInCollection, Equals, HasLabel}
import org.neo4j.cypher.internal.compiler.v2_2.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v2_2.symbols._
import org.neo4j.kernel.api.index.IndexDescriptor

class NodeFetchStrategyTest extends CypherFunSuite {
  val propertyName = "prop"
  val labelName = "Label"

  test("should_not_select_schema_index_when_expression_is_missing_dependencies") {
    //Given
    val noSymbols = new SymbolTable()
    val equalityPredicate = Equals(Property(Identifier("a"), UnresolvedProperty(propertyName)), Identifier("b"))
    val labelPredicate = HasLabel(Identifier("a"), UnresolvedLabel(labelName))
    val planCtx = mock[PlanContext]
    val indexDescriptor = new IndexDescriptor(0, 0)

    when(planCtx.getIndexRule(labelName, propertyName)).thenReturn(Some(indexDescriptor))

    // When
    val foundStartItem = NodeFetchStrategy.findStartStrategy("a", Seq(equalityPredicate, labelPredicate), planCtx, noSymbols)

    // Then
    foundStartItem.rating should equal(NodeFetchStrategy.LabelScan)
  }

  test("should_select_schema_index_when_expression_valid") {
    //Given
    val noSymbols = new SymbolTable(Map("b"->CTNode))
    val equalityPredicate = Equals(Property(Identifier("a"), UnresolvedProperty(propertyName)), Identifier("b"))
    val labelPredicate = HasLabel(Identifier("a"), UnresolvedLabel(labelName))
    val planCtx = mock[PlanContext]
    val indexDescriptor = new IndexDescriptor(0, 0)

    when(planCtx.getIndexRule(labelName, propertyName)).thenReturn(Some(indexDescriptor))
    when(planCtx.getUniquenessConstraint(labelName, propertyName)).thenReturn(None)

    // When
    val foundStartItem = NodeFetchStrategy.findStartStrategy("a", Seq(equalityPredicate, labelPredicate), planCtx, noSymbols)

    // Then
    foundStartItem.rating should equal(NodeFetchStrategy.IndexEquality)
  }

  test("should_select_schema_index_when_expression_property_check_with_in") {
    //Given
    val noSymbols = new SymbolTable(Map("b"->CTNode))
    val inPredicate = AnyInCollection(Collection(Identifier("b")),"_inner_",Equals(Property(Identifier("a"), UnresolvedProperty(propertyName)), Identifier("_inner_")))
    val labelPredicate = HasLabel(Identifier("a"), UnresolvedLabel(labelName))
    val planCtx = mock[PlanContext]
    val indexDescriptor = new IndexDescriptor(0, 0)

    when(planCtx.getIndexRule(labelName, propertyName)).thenReturn(Some(indexDescriptor))
    when(planCtx.getUniquenessConstraint(labelName, propertyName)).thenReturn(None)

    // When
    val foundStartItem = NodeFetchStrategy.findStartStrategy("a", Seq(inPredicate, labelPredicate), planCtx, noSymbols)

    // Then
    foundStartItem.rating should equal(NodeFetchStrategy.IndexEquality)
  }
}
