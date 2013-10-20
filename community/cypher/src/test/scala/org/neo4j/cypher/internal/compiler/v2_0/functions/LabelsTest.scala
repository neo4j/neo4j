/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0.functions

import org.neo4j.cypher.internal.compiler.v2_0._
import symbols._
import org.junit.Test
import org.scalatest.Assertions

class LabelsTest extends Assertions {

  @Test
  def shouldHaveCollectionOfStringsType() {
    val nodeIdentifier = ast.Identifier("n", DummyToken(11, 12))
    val labelsInvocation = ast.FunctionInvocation(
      ast.Identifier("labels", DummyToken(6, 9)),
      false,
      Seq(nodeIdentifier),
      DummyToken(5,14)
    )

    val state = SemanticState.clean.declareIdentifier(nodeIdentifier, NodeType()).right.get
    val result = labelsInvocation.semanticCheck(ast.Expression.SemanticContext.Simple)(state)
    assert(result.errors === Seq())
    assert(labelsInvocation.types(result.state) === Set(CollectionType(StringType())))
  }

  @Test
  def shouldReturnErrorIfNotANodeArgument() {
    val nonNodeIdentifier = ast.Identifier("n", DummyToken(11, 12))
    val labelsInvocation = ast.FunctionInvocation(
      ast.Identifier("labels", DummyToken(6, 9)),
      false,
      Seq(nonNodeIdentifier),
      DummyToken(5,14)
    )

    val state = SemanticState.clean.declareIdentifier(nonNodeIdentifier, RelationshipType()).right.get
    val result = labelsInvocation.semanticCheck(ast.Expression.SemanticContext.Simple)(state)
    assert(result.errors.size === 1)
    assert(result.errors.head.msg === "Type mismatch: n already defined with conflicting type Relationship (expected Node)")
    assert(result.errors.head.token === nonNodeIdentifier.token)
  }

}
