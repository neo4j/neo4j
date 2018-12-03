/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.compatibility.v3_5

import org.neo4j.cypher.internal.compatibility.v3_5.SemanticTableConverter.ExpressionMapping4To5
import org.opencypher.v9_0.ast.semantics.{ExpressionTypeInfo => ExpressionTypeInfoV3_5, SemanticTable => SemanticTableV3_5}
import org.opencypher.v9_0.{ast => astV3_5}
import org.opencypher.v9_0.util.symbols.{TypeSpec => TypeSpecV3_5}
import org.opencypher.v9_0.util.{InputPosition => InputPositionV3_5}
import org.opencypher.v9_0.{expressions => expressionsV3_5}
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v4_0.{expressions => expressionsv4_0, util => utilv4_0}

class SemanticTableConverterTest extends CypherFunSuite {

  test("should handle converting variable with same names but different position") {
    // given
    val typeInfo = ExpressionTypeInfoV3_5(TypeSpecV3_5.all)
    val variable1 = expressionsV3_5.Variable("A")(InputPositionV3_5(1,1,1))
    val variable2 = expressionsV3_5.Variable("A")(InputPositionV3_5(2,2,2))
    val table = SemanticTableV3_5(types = astV3_5.ASTAnnotationMap((variable1, typeInfo), (variable2, typeInfo)))
    val mapping: ExpressionMapping4To5 = Map(
      (variable1, variable1.position) -> expressionsv4_0.Variable("A")(utilv4_0.InputPosition(1, 1, 1)),
      (variable2, variable2.position) -> expressionsv4_0.Variable("A")(utilv4_0.InputPosition(2, 2, 2))
    )

    // when
    val converted = SemanticTableConverter.convertSemanticTable(table, mapping)

    //then
    converted.types should have size 2
  }
}
