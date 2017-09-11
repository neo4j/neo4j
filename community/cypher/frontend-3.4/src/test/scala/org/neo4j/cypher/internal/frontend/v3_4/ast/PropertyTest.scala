/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.v3_4.ast

import org.neo4j.cypher.internal.frontend.v3_4.symbols._
import org.neo4j.cypher.internal.apa.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_4.{SemanticError, SemanticState}

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
