package org.neo4j.cypher.internal.commands.expressions

import org.scalatest.Assertions
import org.junit.Test
import org.neo4j.cypher.internal.symbols.{StringType, CollectionType, SymbolTable}

class ExtractFunctionTest extends Assertions {
  @Test def apa() {
    //GIVEN
    val collection = Literal(List(1, 2, 3))
    val func = new ExtractFunction(collection, "x", StrFunction(Identifier("x")))
    val symbols = new SymbolTable()

    //WHEN
    val typ = func.evaluateType(new CollectionType(StringType()), symbols)

    //THEN
    assert(typ === new CollectionType(StringType()))
  }
}