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
package org.neo4j.cypher.internal.compiler.v3_1.codegen.ir.expressions

import org.neo4j.cypher.internal.compiler.v3_1.codegen.{CodeGenContext, Variable}
import org.neo4j.cypher.internal.frontend.v3_1.symbols._
import org.neo4j.cypher.internal.frontend.v3_1.test_helpers.CypherFunSuite

class CodeGenExpressionTypesTest extends CypherFunSuite {

  val int = Literal(1: java.lang.Integer)
  val double = Literal(1.1: java.lang.Double)
  val string = Literal("apa")
  val node = NodeProjection(Variable("a", CTNode))
  val rel = RelationshipProjection(Variable("a", CTRelationship))
  val intCollection = Collection(Seq(int))
  val doubleCollection = Collection(Seq(double))
  val stringCollection = Collection(Seq(string))
  val nodeCollection = Collection(Seq(node))
  val relCollection = Collection(Seq(rel))

  test("collection") {
    implicit val context: CodeGenContext = null

    Collection(Seq(int)).codeGenType.ct should equal(CTList(CTInteger))
    Collection(Seq(double)).codeGenType.ct should equal(CTList(CTFloat))
    Collection(Seq(int, double)).codeGenType.ct should equal(CTList(CTNumber))
    Collection(Seq(string, int)).codeGenType.ct should equal(CTList(CTAny))
    Collection(Seq(node, rel)).codeGenType.ct should equal(CTList(CTMap))
  }

  test("add") {
    implicit val context: CodeGenContext = null
//
//    Addition(int, double).codeGenType should equal(CodeGenType(CTFloat, ObjectType))
//    Addition(string, int).codeGenType should equal(CodeGenType(CTString, ObjectType))
//    Addition(string, string).codeGenType should equal(CodeGenType(CTString, ObjectType))
//    Addition(intCollection, int).codeGenType should equal(CodeGenType(CTList(CTInteger), ObjectType))
//    Addition(int, intCollection).codeGenType should equal(CodeGenType(CTList(CTInteger), ObjectType))
//    Addition(double, intCollection).codeGenType should equal(CodeGenType(CTList(CTNumber), ObjectType))
//    Addition(doubleCollection, intCollection).codeGenType should equal(CodeGenType(CTList(CTNumber), ObjectType))
    Addition(stringCollection, string).codeGenType should equal(CodeGenType(CTList(CTString), ReferenceType))
    Addition(string, stringCollection).codeGenType should equal(CodeGenType(CTList(CTString), ReferenceType))
  }
}
