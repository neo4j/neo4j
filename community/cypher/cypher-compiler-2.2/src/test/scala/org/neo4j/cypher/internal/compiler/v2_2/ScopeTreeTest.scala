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
package org.neo4j.cypher.internal.compiler.v2_2

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.ast.Statement
import org.neo4j.cypher.internal.compiler.v2_2.symbols._

class ScopeTreeTest extends CypherFunSuite {

  import org.neo4j.cypher.internal.compiler.v2_2.parser.ParserFixture.parser

  //////00000000001111111111222222222233333333334444444444
  //////01234567890123456789012345678901234567890123456789
  test("match n return n as m => { { match n return n } { return n as m } }") {
    val ast = parser.parse("match n return n as m")
    val scopeTree = scopesOf(ast)

    scopeTree should equal(scope()(
      scope(nodeSymbol("n", 6, 15))(),
      scope(nodeSymbol("m", 20))()
    ))
  }

  //////00000000001111111111222222222233333333334444444444
  //////01234567890123456789012345678901234567890123456789
  test("match a with a as b return b as b => { { match a with a } { as b return b } { return b as b } }") {
    val ast = parser.parse("match a with a as b return b as b")
    val scopeTree = scopesOf(ast)

    scopeTree should equal(scope()(
      scope(nodeSymbol("a", 6, 13))(),
      scope(nodeSymbol("b", 18, 27))(),
      scope(nodeSymbol("b", 18, 27, 32))()
    ))
  }

  //////00000000001111111111222222222233333333334444444444
  //////01234567890123456789012345678901234567890123456789
  test("match a with a order by a.name limit 1 match a-->b return a as a => { { match a with a } { as a order by a.name limit 1 match a-->b return a } { return a as a } }") {
    val ast = parser.parse("match a with a order by a.name limit 1 match a-->b return a as a")
    val scopeTree = scopesOf(ast)

    // TODO This looks suspicious; since we only use aliased items for identifierNamespacing, it should be ok though

    // Would rewrite to match a6 with a13 order by a13.name limit 1 match a13-->b49 return a13 as a63, which is wrong

    scopeTree should equal(scope()(
      scope(nodeSymbol("a", 6, 13))(),
      scope(
        nodeSymbol("a", 6, 13, 24, 45, 58),
        nodeSymbol("b", 49)
      )(),
      scope(nodeSymbol("a", 6, 13, 24, 45, 58, 63))()
    ))
  }

  //////00000000001111111111222222222233333333334444444444
  //////01234567890123456789012345678901234567890123456789
  test("match (a:Party) return a union match (a:Animal) return a => { { match (a:Party) return a } { } union { match (a:Animal) return a } { } }") {
    val ast = parser.parse("match (a:Party) return a union match (a:Animal) return a")
    val scopeTree = scopesOf(ast)

    scopeTree should equal(scope()(
      scope()(
        scope(nodeSymbol("a", 7, 23))(),
        scope(nodeSymbol("a", 7, 23))()
      ),
      scope()(
        scope(nodeSymbol("a", 38, 55))(),
        scope(nodeSymbol("a", 38, 55))()
      )
    ))
  }

  //////00000000001111111111222222222233333333334444444444
  //////01234567890123456789012345678901234567890123456789
  test("match a with a where a:Foo with a return a as a => { { match a with a } { as a where a:Foo with a } { as a return a } { return a as a } }") {
    val ast = parser.parse("match a with a where a:Foo with a return a as a")
    val scopeTree = scopesOf(ast)

    scopeTree should equal(scope()(
      scope(nodeSymbol("a", 6, 13))(),
      scope(nodeSymbol("a", 6, 13, 21, 32))(),
      scope(nodeSymbol("a", 6, 13, 21, 32, 41))(),
      scope(nodeSymbol("a", 6, 13, 21, 32, 41, 46))()
    ))
  }

  //////000000000011111111112222222222333333333344444444445555555555
  //////012345678901234567890123456789012345678901234567890123456789
  test("match a with a as a where a:Foo with a return a as a => { { match a with a } { as a where a:Foo with a } { as a return a } { return a as a } }") {
    val ast = parser.parse("match a with a as a where a:Foo with a return a as a")
    val scopeTree = scopesOf(ast)

    scopeTree should equal(scope()(
      scope(nodeSymbol("a", 6, 13))(),
      scope(nodeSymbol("a", 6, 13, 18, 26, 37))(),
      scope(nodeSymbol("a", 6, 13, 18, 26, 37, 46))(),
      scope(nodeSymbol("a", 6, 13, 18, 26, 37, 46, 51))()
    ))
  }

  //////000000000011111111112222222222333333333344444444445555555555
  //////012345678901234567890123456789012345678901234567890123456789
  test("match a with a optional match b with b return b => { { match a with a } { as a optional match b with b } { as b return b } { return b as b } }") {
    val ast = parser.parse("match a with a optional match b with b return b")
    val scopeTree = scopesOf(ast)

    scopeTree should equal(scope()(
      scope(nodeSymbol("a", 6, 13))(),
      scope(nodeSymbol("a", 6, 13), nodeSymbol("b", 30, 37))(),
      scope(nodeSymbol("b", 30, 37, 46))(),
      scope(nodeSymbol("b", 30, 37, 46))()
    ))
  }

  //////000000000011111111112222222222333333333344444444445555555555
  //////012345678901234567890123456789012345678901234567890123456789
  test("return [ a in [1, 2, 3] | a ] as r => { { return { [ a in [1, 2, 3] | a ] } } { } }") {
    val ast = parser.parse("return [ a in [1, 2, 3] | a ] as r")
    val scopeTree = scopesOf(ast)

    scopeTree should equal(scope()(
      scope()(
        scope(intSymbol("a", 9, 26))()
      ),
      scope(intCollectionSymbol("r", 33))()
    ))
  }

  //////000000000011111111112222222222333333333344444444445555555555
  //////012345678901234567890123456789012345678901234567890123456789
  test("with 1 as c return [ a in [1, 2, 3] | a + c ] as r => { { with 1 } { as c return { [ a in [1, 2, 3] | a + c ] } } { } }") {
    val ast = parser.parse("with 1 as c return [ a in [1, 2, 3] | a + c ] as r")
    val scopeTree = scopesOf(ast)

    scopeTree should equal(scope()(
      scope()(),
      scope(
        intSymbol("c", 10))(
        scope(
          intSymbol("a", 21, 38),
          intSymbol("c", 10, 42)
        )()
      ),
      scope(intCollectionSymbol("r", 49))()
    ))
  }


  //////000000000011111111112222222222333333333344444444445555555555
  //////012345678901234567890123456789012345678901234567890123456789
  test("return [ a in [1, 2, 3] | [ b in [4, 5, 6] | a + b ] ] as r => { { return { [ a in [1, 2, 3] | { [ b in [4, 5, 6] | a + b ] } ] } } { }") {
    val ast = parser.parse("return [ a in [1, 2, 3] | [ b in [4, 5, 6] | a + b ] ] as r")
    val scopeTree = scopesOf(ast)

    scopeTree should equal(scope()(
      scope()(
        scope(intSymbol("a", 9))(
          scope(
            intSymbol("a", 9, 45),
            intSymbol("b", 28, 49)
          )()
        )
      ),
      scope(intCollectionCollectionSymbol("r", 58))()
    ))
  }


  //////000000000011111111112222222222333333333344444444445555555555
  //////012345678901234567890123456789012345678901234567890123456789
  test("match a where not a-->() return a as a => { { match a where not a-->() return a } { return a as a } }") {
    val ast = parser.parse("match a where not a-->() return a as a")
    val scopeTree = scopesOf(ast)

    scopeTree should equal(scope()(
      scope(nodeSymbol("a", 6, 18, 32))(),
      scope(nodeSymbol("a", 6, 18, 32, 37))()
    ))
  }

  //////0000000000111111111122222222223333333333444444444455555555556666666666777777777788888888889999999999000000000011111111112222222222
  //////0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
  test("MATCH root CREATE book FOREACH(name in ['a','b','c'] | CREATE UNIQUE root-[:tag]->(tag {name:name})<-[:tagged]-book) RETURN book") {
    val ast = parser.parse("MATCH root CREATE book FOREACH(name in ['a','b','c'] | CREATE UNIQUE root-[:tag]->(tag {name:name})<-[:tagged]-book) RETURN book")
    val scopeTree = scopesOf(ast)

    scopeTree should equal(scope()(
      scope(nodeSymbol("root", 6), nodeSymbol("book", 18, 124))(
        scope(stringSymbol("name", 31, 93), nodeSymbol("root", 6, 69), nodeSymbol("tag", 83), nodeSymbol("book", 18, 111))()
      ),
      scope(nodeSymbol("book", 18, 124))()
    ))
  }

  def scopesOf(ast: Statement) = ast.semanticCheck(SemanticState.clean) match {
    case SemanticCheckResult(state, errors) =>
      if (errors.isEmpty) {
        val result = state.scopeTree
        // println(pprintToString(result, DocFormatters.pageFormatter(180)))
        result
      } else
        fail(s"Failure during semantic checking of $ast with errors $errors")
  }

  def scope(entries: Symbol*)(children: Scope*): Scope =
    Scope(entries.map { symbol => symbol.name -> symbol }.toMap, asSeq(children))

  def nodeSymbol(name: String, offsets: Int*): Symbol =
    typedSymbol(name, TypeSpec.exact(CTNode), offsets: _*)

  def intSymbol(name: String, offsets: Int*): Symbol =
    typedSymbol(name, TypeSpec.exact(CTInteger), offsets: _*)

  def stringSymbol(name: String, offsets: Int*): Symbol =
    typedSymbol(name, TypeSpec.exact(CTString), offsets: _*)

  def intCollectionSymbol(name: String, offsets: Int*): Symbol =
    typedSymbol(name, TypeSpec.exact(CTCollection(CTInteger)), offsets: _*)

  def intCollectionCollectionSymbol(name: String, offsets: Int*): Symbol =
    typedSymbol(name, TypeSpec.exact(CTCollection(CTCollection(CTInteger))), offsets: _*)

  def typedSymbol(name: String, typeSpec: TypeSpec, offsets: Int*) =
    Symbol(name, offsets.map(offset => new InputPosition(offset, 1, offset + 1)).toSet, typeSpec)

  def asSeq[T](input: TraversableOnce[T]) = if (input.isEmpty) Vector() else input.toList
}
