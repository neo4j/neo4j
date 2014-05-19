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
package org.neo4j.cypher.internal.compiler.v2_1.pprint.docgen

import org.neo4j.cypher.internal.compiler.v2_1.pprint.SingleDocBuilder
import org.neo4j.cypher.internal.compiler.v2_1.ast.{Expression, PropertyKeyName, Property, Equals}

class AstExpressionDocBuilderTest extends DocBuilderTestSuite[Any] {

  object docBuilder extends SingleDocBuilder[Any] {
    val nested = astExpressionDocBuilder.nested orElse simpleDocBuilder.nested
  }

  test("Identifier(\"a\") => a") {
    format(ident("a")) should equal("a")
  }

  test("Equals(left, right) => left = right") {
    val expr: Expression = Equals(ident("a"), ident("b"))_
    format(expr) should equal("a = b")
  }

  test("Property(map, name) => map.name") {
    val expr: Expression = Property(ident("a"), PropertyKeyName("name")_)_
    format(expr) should equal("a.name")
  }
}
