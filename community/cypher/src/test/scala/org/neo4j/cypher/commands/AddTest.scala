package org.neo4j.cypher.commands

import org.junit.Test
import org.scalatest.Assertions
import org.neo4j.cypher.CypherTypeException

class AddTest extends Assertions {
  
  val m = Map[String, Any]()
  
  @Test def numbers() {
    val expr = Add(Literal(1), Literal(1))
    assert(expr(m) === 2)
  }

  @Test def strings() {
    val expr = Add(Literal("hello"), Literal("world"))
    assert(expr(m) === "helloworld")
  }

  @Test def stringPlusNumber() {
    val expr = Add(Literal("hello"), Literal(1))
    intercept[CypherTypeException](expr(m))
  }

  @Test def numberPlusString() {
    val expr = Add(Literal(1), Literal("world"))
    intercept[CypherTypeException](expr(m))
  }

  @Test def numberPlusBool() {
    val expr = Add(Literal("1"), Literal(true))
    intercept[CypherTypeException](expr(m))
  }
}