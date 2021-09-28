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
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.ast.factory.ASTExceptionFactory
import org.neo4j.cypher.internal.ast.factory.ConstraintType
import org.neo4j.cypher.internal.ast.factory.CreateIndexTypes
import org.neo4j.cypher.internal.ast.factory.ShowCommandFilterTypes
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class Neo4jASTFactoryTest extends CypherFunSuite {

  test("invalidDropCommand") {
    ASTExceptionFactory.invalidDropCommand shouldBe "Unsupported drop constraint command: Please delete the constraint by name instead"
  }

  test("invalidCatalogStatement") {
    ASTExceptionFactory.invalidCatalogStatement shouldBe "CATALOG is not allowed for this statement"
  }

  test("relationShipPattternNotAllowed") {
    ASTExceptionFactory.relationshipPattternNotAllowed(ConstraintType.UNIQUE) shouldBe "'IS UNIQUE' does not allow relationship patterns"
  }

  test("onlySinglePropertyAllowed") {
    ASTExceptionFactory.onlySinglePropertyAllowed(ConstraintType.NODE_EXISTS) shouldBe "'EXISTS' does not allow multiple properties"
  }

  test("invalidShowFilterType") {
    ASTExceptionFactory.invalidShowFilterType("indexes", ShowCommandFilterTypes.INVALID) shouldBe "Filter type INVALID is not defined for show indexes command."
  }

  test("invalidCreateIndexType") {
    ASTExceptionFactory.invalidCreateIndexType(CreateIndexTypes.INVALID) shouldBe "Index type INVALID is not defined for create index command."
  }
}
