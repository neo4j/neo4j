/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.ir.ordering

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.expressions.DesugaredMapProjection
import org.neo4j.cypher.internal.expressions.LiteralEntry
import org.neo4j.cypher.internal.ir.ordering.ColumnOrder.Asc
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ColumnOrderTest extends CypherFunSuite with AstConstructionTestSupport {

  test("Column Order should return correct dependencies when no projections") {
    val columnOrder = Asc(varFor("a"), projections = Map.empty)

    columnOrder.dependencies shouldBe Set(varFor("a"))
  }

  test("Column Order on variable, with non empty projection list, should return correct dependencies") {
    val projections = Map(
      "a3" -> varFor("a2"),
      "a2" -> prop("a1", "prop1"),
      "b" -> varFor("b1")
    )
    val columnOrder = Asc(varFor("a3"), projections)

    columnOrder.dependencies shouldBe Set(varFor("a1"))
  }

  test("Column Order on property, with non empty projection list, should return correct dependencies") {
    val projections = Map(
      "a3" -> varFor("a2"),
      "a2" -> varFor("a1"),
      "b" -> varFor("b1")
    )
    val columnOrder = Asc(prop("a3", "prop1"), projections)

    columnOrder.dependencies shouldBe Set(varFor("a1"))
  }

  test("Column Order on complex expression, with non empty projection list, should return correct dependencies") {
    val projections = Map(
      "a3" -> varFor("a2"),
      "a2" -> varFor("a1"),
      "b" -> varFor("b1")
    )
    val columnOrder = Asc(add(literalInt(1), prop("a3", "prop1")), projections)

    columnOrder.dependencies shouldBe Set(varFor("a1"))
  }

  test(
    "Column Order on relationships uniqueness predicate, with non-empty projection list containing non-var expressions, should return correct dependencies"
  ) {
    val pathVar = varFor("path")
    val projections = Map(
      "r" -> nullLiteral,
      "rr" -> containerIndex(function("relationships", pathVar), 0)
    )
    val columnOrder = Asc(differentRelationships("r", "rr"), projections)
    columnOrder.dependencies shouldBe Set(pathVar)
  }

  // `RETURN actor AS keanu ORDER BY keanu{.name}` depends on `actor`
  test(
    "Column Order on map projection of a projected variable should return the original variable as a dependency"
  ) {
    val projections = Map(
      "keanu" -> varFor("actor")
    )
    val desugaredMapProjection = DesugaredMapProjection(
      variable = varFor("keanu"),
      items = List(
        LiteralEntry(propName("name"), prop("keanu", "name"))(pos)
      ),
      includeAllProps = false
    )(pos)
    val columnOrder = Asc(desugaredMapProjection, projections)
    columnOrder.dependencies shouldBe Set(varFor("actor"))
  }

  // `RETURN actor AS keanu, collect(movie) AS movies ORDER BY keanu{.name, movies: movies}` depends on `actor` and `movie`
  test(
    "Column Order on map projection of a projected variable with a literal entry should return the original variable and the one in the literal entry as dependencies"
  ) {
    val projections = Map(
      "keanu" -> varFor("actor"),
      "movies" -> collect(varFor("movie"))
    )
    val desugaredMapProjection = DesugaredMapProjection(
      variable = varFor("keanu"),
      items = List(
        LiteralEntry(propName("name"), prop("keanu", "name"))(pos),
        LiteralEntry(propName("movies"), varFor("movies"))(pos)
      ),
      includeAllProps = false
    )(pos)
    val columnOrder = Asc(desugaredMapProjection, projections)
    columnOrder.dependencies shouldBe Set(varFor("actor"), varFor("movie"))
  }

  // `RETURN {'name': 'Keanu Reeves'} AS keanu, collect(movie) AS movies ORDER BY keanu{.name, movies: movies}` depends on `keanu` and `movie`
  test(
    "Column Order on map projection of a projected non-variable expression with a literal entry should return the projected expression and the variable in the literal entry as dependencies"
  ) {
    val projections = Map(
      "keanu" -> mapOf(
        "name" -> literalString("Keanu Reeves")
      ),
      "movies" -> collect(varFor("movie"))
    )
    val desugaredMapProjection = DesugaredMapProjection(
      variable = varFor("keanu"),
      items = List(
        LiteralEntry(propName("name"), prop("keanu", "name"))(pos),
        LiteralEntry(propName("movies"), varFor("movies"))(pos)
      ),
      includeAllProps = false
    )(pos)
    val columnOrder = Asc(desugaredMapProjection, projections)
    columnOrder.dependencies shouldBe Set(varFor("keanu"), varFor("movie"))
  }

}
