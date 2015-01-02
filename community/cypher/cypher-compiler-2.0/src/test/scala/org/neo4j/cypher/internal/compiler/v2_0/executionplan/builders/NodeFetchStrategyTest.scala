/**
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
package org.neo4j.cypher.internal.compiler.v2_0.executionplan.builders

import org.neo4j.cypher.internal.compiler.v2_0._
import commands.{HasLabel, Equals}
import commands.values.{UnresolvedLabel, UnresolvedProperty}
import commands.expressions.{Property, Identifier}
import spi.PlanContext
import symbols._
import org.neo4j.kernel.api.index.IndexDescriptor
import org.junit.Test
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import org.scalatest.Assertions

class NodeFetchStrategyTest extends MockitoSugar with Assertions {
  val propertyName = "prop"
  val labelName = "Label"

  @Test def should_not_select_schema_index_when_expression_is_missing_dependencies() {
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
    assert(foundStartItem.rating === NodeFetchStrategy.LabelScan)
  }

  @Test def should_select_schema_index_when_expression_valid() {
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
    assert(foundStartItem.rating === NodeFetchStrategy.IndexEquality)
  }
}
