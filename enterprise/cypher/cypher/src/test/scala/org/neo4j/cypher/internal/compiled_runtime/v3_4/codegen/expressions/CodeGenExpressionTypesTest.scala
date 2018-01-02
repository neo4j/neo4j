/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiled_runtime.v3_4.codegen.expressions

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.expressions._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.{CodeGenContext, Variable}
import org.neo4j.cypher.internal.util.v3_4.symbols._
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite

class CodeGenExpressionTypesTest extends CypherFunSuite {

  val int = Literal(1: java.lang.Integer)
  val double = Literal(1.1: java.lang.Double)
  val string = Literal("apa")
  val node = NodeProjection(Variable("a", CypherCodeGenType(CTNode, ReferenceType)))
  val rel = RelationshipProjection(Variable("a", CypherCodeGenType(CTRelationship, ReferenceType)))
  val intCollection = ListLiteral(Seq(int))
  val doubleCollection = ListLiteral(Seq(double))
  val stringCollection = ListLiteral(Seq(string))
  val nodeCollection = ListLiteral(Seq(node))
  val relCollection = ListLiteral(Seq(rel))

  test("collection") {
    implicit val context: CodeGenContext = null

                       ListLiteral(Seq(int)).codeGenType.ct should equal(CTList(CTInteger))
                       ListLiteral(Seq(double)).codeGenType.ct should equal(CTList(CTFloat))
                       ListLiteral(Seq(int, double)).codeGenType.ct should equal(CTList(CTNumber))
                       ListLiteral(Seq(string, int)).codeGenType.ct should equal(CTList(CTAny))
                       ListLiteral(Seq(node, rel)).codeGenType.ct should equal(CTList(CTMap))
  }

  test("add") {
    implicit val context: CodeGenContext = null

    Addition(int, double).codeGenType should equal(CypherCodeGenType(CTFloat, ReferenceType))
    Addition(string, int).codeGenType should equal(CypherCodeGenType(CTString, ReferenceType))
    Addition(string, string).codeGenType should equal(CypherCodeGenType(CTString, ReferenceType))
    Addition(intCollection, int).codeGenType should equal(CypherCodeGenType(CTList(CTInteger), ReferenceType))
    Addition(int, intCollection).codeGenType should equal(CypherCodeGenType(CTList(CTInteger), ReferenceType))
    Addition(double, intCollection).codeGenType should equal(CypherCodeGenType(CTList(CTNumber), ReferenceType))
    Addition(doubleCollection, intCollection).codeGenType should equal(CypherCodeGenType(CTList(CTNumber), ReferenceType))
    Addition(stringCollection, string).codeGenType should equal(CypherCodeGenType(CTList(CTString), ReferenceType))
    Addition(string, stringCollection).codeGenType should equal(CypherCodeGenType(CTList(CTString), ReferenceType))
  }
}
