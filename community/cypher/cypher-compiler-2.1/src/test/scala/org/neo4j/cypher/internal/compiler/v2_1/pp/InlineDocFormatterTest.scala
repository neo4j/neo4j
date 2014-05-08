package org.neo4j.cypher.internal.compiler.v2_1.pp

import org.neo4j.cypher.internal.commons.CypherFunSuite

class InlineDocFormatterTest extends CypherFunSuite {

  test("format NilDoc") {
    InlineDocFormatter(NilDoc) should equal(Seq(PrintText("")))
  }

  test("format BreakDoc") {
    InlineDocFormatter(BreakDoc) should equal(Seq(PrintText(" ")))
  }

  test("format NestDoc") {
    InlineDocFormatter(NestDoc(10, NilDoc)) should equal(Seq(PrintText("")))
  }

  test("format GroupDoc") {
    InlineDocFormatter(GroupDoc(ConsDoc(TextDoc("a"), TextDoc("b")))) should equal(Seq(PrintText("a"), PrintText("b")))
  }

  test("format ConsDoc") {
    InlineDocFormatter(ConsDoc(TextDoc("a"), ConsDoc(TextDoc("b")))) should equal(Seq(PrintText("a"), PrintText("b")))
  }
}
