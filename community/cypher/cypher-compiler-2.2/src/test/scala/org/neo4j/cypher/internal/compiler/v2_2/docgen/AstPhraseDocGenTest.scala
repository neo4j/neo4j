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
package org.neo4j.cypher.internal.compiler.v2_2.docgen

import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.perty.gen.DocHandlerTestSuite

class AstPhraseDocGenTest extends DocHandlerTestSuite[ASTNode] with AstConstructionTestSupport {

  val docGen = astPhraseDocGen ++ astExpressionDocGen ++ astParticleDocGen

  test("ORDER BY n") {
    val astNode: ASTNode = OrderBy(Seq(AscSortItem(ident("n"))_))_
    pprintToString(astNode) should equal("ORDER BY n")
  }

  test("USING INDEX n:Person(name)") {
    val astNode: ASTNode = UsingIndexHint(ident("n"), LabelName("Person")_, ident("name"))_
    pprintToString(astNode) should equal("USING INDEX n:Person(name)")
  }

  test("USING SCAN n:Person") {
    val astNode: ASTNode = UsingScanHint(ident("n"), LabelName("Person")_)_
    pprintToString(astNode) should equal("USING SCAN n:Person")
  }
}
