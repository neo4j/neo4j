/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.docgen

import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.frontend.v2_3.perty.gen.DocHandlerTestSuite

class AstPhraseDocGenTest extends DocHandlerTestSuite[ASTNode] with AstConstructionTestSupport {

  val docGen = astPhraseDocGen orElse astExpressionDocGen orElse astParticleDocGen

  test("RETURN *") {
    val astNode: ASTNode = Return(distinct = false, ReturnItems(includeExisting = true, Seq())_, None, None, None)_
    pprintToString(astNode) should equal("RETURN *")
  }

  test("RETURN *, n AS m") {
    val astNode: ASTNode = Return(distinct = false, ReturnItems(includeExisting = true, Seq(AliasedReturnItem(ident("n"), ident("m"))_))_, None, None, None)_
    pprintToString(astNode) should equal("RETURN *, n AS m")
  }

  test("RETURN DISTINCT *") {
    val astNode: ASTNode = Return(distinct = true, ReturnItems(includeExisting = true, Seq())_, None, None, None)_
    pprintToString(astNode) should equal("RETURN DISTINCT *")
  }

  test("RETURN * ORDER BY n") {
    val astNode: ASTNode = Return(distinct = false, ReturnItems(includeExisting = true, Seq())_, Some(OrderBy(Seq(AscSortItem(ident("n"))_))_), None, None)_
    pprintToString(astNode) should equal("RETURN * ORDER BY n")
  }

  test("RETURN * SKIP 6") {
    val astNode: ASTNode = Return(distinct = false, ReturnItems(includeExisting = true, Seq())_, None, Some(Skip(UnsignedDecimalIntegerLiteral("6")_)_), None)_
    pprintToString(astNode) should equal("RETURN * SKIP 6")
  }

  test("RETURN * LIMIT 6") {
    val astNode: ASTNode = Return(distinct = false, ReturnItems(includeExisting = true, Seq())_, None, None, Some(Limit(UnsignedDecimalIntegerLiteral("6")_)_))_
    pprintToString(astNode) should equal("RETURN * LIMIT 6")
  }

  test("RETURN n") {
    val astNode: ASTNode = Return(distinct = false, ReturnItems(includeExisting = false, Seq(UnaliasedReturnItem(ident("n"), "n")_))_, None, None, None) _
    pprintToString(astNode) should equal("RETURN n")
  }

  test("RETURN n AS m") {
    val astNode: ASTNode = Return(distinct = false, ReturnItems(includeExisting = false, Seq(AliasedReturnItem(ident("n"), ident("m"))_))_, None, None, None) _
    pprintToString(astNode) should equal("RETURN n AS m")
  }

  test("RETURN `x`, n AS m") {
    val astNode: ASTNode = Return(distinct = false, ReturnItems(includeExisting = false, Seq(UnaliasedReturnItem(ident("x"), "`x`")_, AliasedReturnItem(ident("n"), ident("m"))_))_, None, None, None) _
    pprintToString(astNode) should equal("RETURN `x`, n AS m")
  }

  test("WITH * WHERE true") {
    val astNode: ASTNode = With(distinct = false, ReturnItems(includeExisting = true, Seq())_, None, None, None, Some(Where(True()_)_))_
    pprintToString(astNode) should equal("WITH * WHERE true")
  }

  test("WITH *, n AS m WHERE true") {
    val astNode: ASTNode = With(distinct = false, ReturnItems(includeExisting = true, Seq(AliasedReturnItem(ident("n"), ident("m"))_))_, None, None, None, Some(Where(True()_)_))_
    pprintToString(astNode) should equal("WITH *, n AS m WHERE true")
  }

  test("ORDER BY n") {
    val astNode: ASTNode = OrderBy(Seq(AscSortItem(ident("n"))_))_
    pprintToString(astNode) should equal("ORDER BY n")
  }

  test("USING INDEX n:Person(name)") {
    val astNode: ASTNode = UsingIndexHint(ident("n"), LabelName("Person")_, PropertyKeyName("name")(pos))_
    pprintToString(astNode) should equal("USING INDEX n:Person(name)")
  }

  test("USING SCAN n:Person") {
    val astNode: ASTNode = UsingScanHint(ident("n"), LabelName("Person")_)_
    pprintToString(astNode) should equal("USING SCAN n:Person")
  }

  ignore("USING JOIN ON n") {
    val astNode: ASTNode = UsingJoinHint(Seq(ident("n")))_
    pprintToString(astNode) should equal("USING JOIN ON n")
  }

  test("UNWIND x AS y") {
    val astNode: ASTNode = Unwind(ident("x"), ident("y"))_
    pprintToString(astNode) should equal("UNWIND x AS y")
  }
}
