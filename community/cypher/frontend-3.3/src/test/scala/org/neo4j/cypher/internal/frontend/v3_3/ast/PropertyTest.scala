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
package org.neo4j.cypher.internal.frontend.v3_3.ast

import org.neo4j.cypher.internal.frontend.v3_3.symbols._
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_3.{SemanticError, SemanticState}

class PropertyTest extends CypherFunSuite with AstConstructionTestSupport {

  test("accepts property access on a map") {
    val mapExpr: Variable = varFor("map")
    val propertyKey: PropertyKeyName = PropertyKeyName("prop") _

    val beforeState = SemanticState.clean.newChildScope.declareVariable(mapExpr, CTMap).right.get

    val result = (Property(mapExpr, propertyKey) _).semanticCheck(Expression.SemanticContext.Simple)(beforeState)

    result.errors shouldBe empty
  }

  test("accepts property access on a node") {
    val mapExpr: Variable = varFor("map")
    val propertyKey: PropertyKeyName = PropertyKeyName("prop") _

    val beforeState = SemanticState.clean.newChildScope.declareVariable(mapExpr, CTNode).right.get

    val result = (Property(mapExpr, propertyKey) _).semanticCheck(Expression.SemanticContext.Simple)(beforeState)

    result.errors shouldBe empty
  }

  test("accepts property access on a relationship") {
    val mapExpr: Variable = varFor("map")
    val propertyKey: PropertyKeyName = PropertyKeyName("prop") _

    val beforeState = SemanticState.clean.newChildScope.declareVariable(mapExpr, CTRelationship).right.get

    val result = (Property(mapExpr, propertyKey) _).semanticCheck(Expression.SemanticContext.Simple)(beforeState)

    result.errors shouldBe empty
  }

  test("accepts property access on an Any") {
    val mapExpr: Variable = varFor("map")
    val propertyKey: PropertyKeyName = PropertyKeyName("prop") _

    val beforeState = SemanticState.clean.newChildScope.declareVariable(mapExpr, CTAny).right.get

    val result = (Property(mapExpr, propertyKey) _).semanticCheck(Expression.SemanticContext.Simple)(beforeState)

    result.errors shouldBe empty
  }

  test("refuses property access on an Integer") {
    val mapExpr: Variable = varFor("map")
    val propertyKey: PropertyKeyName = PropertyKeyName("prop") _

    val beforeState = SemanticState.clean.newChildScope.declareVariable(mapExpr, CTInteger).right.get

    val result = (Property(mapExpr, propertyKey) _).semanticCheck(Expression.SemanticContext.Simple)(beforeState)

    result.errors should equal(List(SemanticError("Type mismatch: expected Any, Map, Node or Relationship but was Integer", pos)))
  }
}
