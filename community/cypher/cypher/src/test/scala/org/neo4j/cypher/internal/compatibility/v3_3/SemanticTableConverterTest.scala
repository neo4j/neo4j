/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compatibility.v3_3

import org.neo4j.cypher.internal.compatibility.v3_3.SemanticTableConverter.ExpressionMapping3To4
import org.neo4j.cypher.internal.frontend.v3_3.symbols.{TypeSpec => TypeSpecV3_3}
import org.neo4j.cypher.internal.frontend.v3_3.{ExpressionTypeInfo => ExpressionTypeInfoV3_3, InputPosition => InputPositionV3_3, SemanticTable => SemanticTableV3_3, ast => astV3_3}
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.{v3_4 => utilV3_4}
import org.neo4j.cypher.internal.v3_4.{expressions => expressionsV3_4}

class SemanticTableConverterTest extends CypherFunSuite {

  test("should handle converting variable with same names but different position") {
    // given
    val typeInfo = ExpressionTypeInfoV3_3(TypeSpecV3_3.all)
    val variable1 = astV3_3.Variable("A")(InputPositionV3_3(1,1,1))
    val variable2 = astV3_3.Variable("A")(InputPositionV3_3(2,2,2))
    val table = SemanticTableV3_3(types = astV3_3.ASTAnnotationMap((variable1, typeInfo), (variable2, typeInfo)))
    val mapping: ExpressionMapping3To4 = Map(
      (variable1, variable1.position) -> expressionsV3_4.Variable("A")(utilV3_4.InputPosition(1, 1, 1)),
      (variable2, variable2.position) -> expressionsV3_4.Variable("A")(utilV3_4.InputPosition(2, 2, 2))
    )

    // when
    val converted = SemanticTableConverter.convertSemanticTable(table, mapping)

    //then
    converted.types should have size 2
  }
}
