package org.neo4j.cypher.internal.compiler.v2_1.pprint

import org.neo4j.cypher.internal.commons.CypherFunSuite

class CondenseTest extends CypherFunSuite {

  test("Keeps single PrintText (using Seq)") {
    condense(Seq(PrintText("a"))) should equal(Seq(PrintText("a")))
  }

  test("Combines two PrintTexts (using Seq)") {
    condense(Seq(PrintText("a"), PrintText("b"))) should equal(Seq(PrintText("ab")))
  }

  test("Keeps new lines (using Seq)") {
    condense(Seq(PrintText("a"), PrintNewLine(2), PrintText("b"))) should equal(Seq(PrintText("a"), PrintNewLine(2), PrintText("b")))
  }

  test("Keeps single PrintText (using Vector)") {
    condense(Vector(PrintText("a"))) should equal(Vector(PrintText("a")))
  }

  test("Combines two PrintTexts (using Vector)") {
    condense(Vector(PrintText("a"), PrintText("b"))) should equal(Vector(PrintText("ab")))
  }

  test("Keeps new lines (using Vector)") {
    condense(Vector(PrintText("a"), PrintNewLine(2), PrintText("b"))) should equal(Vector(PrintText("a"), PrintNewLine(2), PrintText("b")))
  }
}
