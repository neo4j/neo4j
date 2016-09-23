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
package org.neo4j.cypher.internal.compiler.v3_1.executionplan.builders

import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v3_1.ast.convert.commands.ExpressionConverters._
import org.neo4j.cypher.internal.compiler.v3_1.commands.AnyInList
import org.neo4j.cypher.internal.compiler.v3_1.commands.expressions.{ListLiteral, Property, Variable}
import org.neo4j.cypher.internal.compiler.v3_1.commands.predicates.{Equals, HasLabel}
import org.neo4j.cypher.internal.compiler.v3_1.commands.values.{UnresolvedLabel, UnresolvedProperty}
import org.neo4j.cypher.internal.compiler.v3_1.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v3_1.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v3_1.ast
import org.neo4j.cypher.internal.frontend.v3_1.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.frontend.v3_1.helpers.NonEmptyList
import org.neo4j.cypher.internal.frontend.v3_1.symbols._
import org.neo4j.cypher.internal.frontend.v3_1.test_helpers.CypherFunSuite
import org.neo4j.kernel.api.index.IndexDescriptor

class NodeFetchStrategyTest extends CypherFunSuite {
  val propertyName = "prop"
  val labelName = "Label"

  test("should not select schema index when expression is missing dependencies") {
    //Given
    val noSymbols = new SymbolTable()
    val equalityPredicate = Equals(Property(Variable("a"), UnresolvedProperty(propertyName)), Variable("b"))
    val labelPredicate = HasLabel(Variable("a"), UnresolvedLabel(labelName))
    val planCtx = mock[PlanContext]
    val indexDescriptor = new IndexDescriptor(0, 0)

    when(planCtx.getIndexRule(labelName, propertyName)).thenReturn(Some(indexDescriptor))

    // When
    val foundStartItem = NodeFetchStrategy.findStartStrategy("a", Seq(equalityPredicate, labelPredicate), planCtx, noSymbols)

    // Then
    foundStartItem.rating should equal(NodeFetchStrategy.LabelScan)
  }

  test("should select schema index when expression valid") {
    //Given
    val noSymbols = new SymbolTable(Map("b"->CTNode))
    val equalityPredicate = Equals(Property(Variable("a"), UnresolvedProperty(propertyName)), Variable("b"))
    val labelPredicate = HasLabel(Variable("a"), UnresolvedLabel(labelName))
    val planCtx = mock[PlanContext]
    val indexDescriptor = new IndexDescriptor(0, 0)

    when(planCtx.getIndexRule(labelName, propertyName)).thenReturn(Some(indexDescriptor))
    when(planCtx.getUniquenessConstraint(labelName, propertyName)).thenReturn(None)

    // When
    val foundStartItem = NodeFetchStrategy.findStartStrategy("a", Seq(equalityPredicate, labelPredicate), planCtx, noSymbols)

    // Then
    foundStartItem.rating should equal(NodeFetchStrategy.IndexEquality)
  }

  test("should select schema index when expression property check with in") {
    //Given
    val noSymbols = new SymbolTable(Map("b"->CTNode))
    val inPredicate = AnyInList(ListLiteral(Variable("b")), "_inner_", Equals(Property(Variable("a"), UnresolvedProperty(propertyName)), Variable("_inner_")))
    val labelPredicate = HasLabel(Variable("a"), UnresolvedLabel(labelName))
    val planCtx = mock[PlanContext]
    val indexDescriptor = new IndexDescriptor(0, 0)

    when(planCtx.getIndexRule(labelName, propertyName)).thenReturn(Some(indexDescriptor))
    when(planCtx.getUniquenessConstraint(labelName, propertyName)).thenReturn(None)

    // When
    val foundStartItem = NodeFetchStrategy.findStartStrategy("a", Seq(inPredicate, labelPredicate), planCtx, noSymbols)

    // Then
    foundStartItem.rating should equal(NodeFetchStrategy.IndexEquality)
  }

  test("should select schema index for prefix search") {
    object inner extends AstConstructionTestSupport {

      def run(): Unit = {
        //Given
        val nodeName = "n"
        val symbols = new SymbolTable(Map(nodeName -> CTNode))
        val labelPredicate = HasLabel(Variable(nodeName), UnresolvedLabel(labelName))
        val startsWith: ast.StartsWith = ast.StartsWith(ast.Property(varFor(nodeName), ast.PropertyKeyName(propertyName)_)_, ast.StringLiteral("prefix%")_)_
        val startsWithPredicate = toCommandPredicate(startsWith)

        val planCtx = mock[PlanContext]
        val indexDescriptor = new IndexDescriptor(0, 0)

        when(planCtx.getIndexRule(labelName, propertyName)).thenReturn(Some(indexDescriptor))
        when(planCtx.getUniquenessConstraint(labelName, propertyName)).thenReturn(None)

        // When
        val foundStartItem = NodeFetchStrategy.findStartStrategy(nodeName, Seq(startsWithPredicate, labelPredicate), planCtx, symbols)

        // Then
        foundStartItem.rating should equal(NodeFetchStrategy.IndexRange)
      }
    }

    inner.run()
  }

  test("should select schema index for range queries") {
    object inner extends AstConstructionTestSupport {

      def run(): Unit = {
        //Given
        val nodeName = "n"
        val symbols = new SymbolTable(Map(nodeName -> CTNode))
        val labelPredicate = HasLabel(Variable(nodeName), UnresolvedLabel(labelName))
        val prop: ast.Property = ast.Property(varFor("n"), ast.PropertyKeyName("prop") _) _
        val inequality = ast.AndedPropertyInequalities(varFor("n"), prop, NonEmptyList(ast.GreaterThan(prop, ast.SignedDecimalIntegerLiteral("42") _) _))

        val inequalityPredicate = toCommandPredicate(inequality)

        val planCtx = mock[PlanContext]
        val indexDescriptor = new IndexDescriptor(0, 0)

        when(planCtx.getIndexRule(labelName, propertyName)).thenReturn(Some(indexDescriptor))
        when(planCtx.getUniquenessConstraint(labelName, propertyName)).thenReturn(None)

        // When
        val foundStartItem = NodeFetchStrategy.findStartStrategy(nodeName, Seq(inequalityPredicate, labelPredicate), planCtx, symbols)

        // Then
        foundStartItem.rating should equal(NodeFetchStrategy.IndexRange)
      }
    }

    inner.run()
  }
}
