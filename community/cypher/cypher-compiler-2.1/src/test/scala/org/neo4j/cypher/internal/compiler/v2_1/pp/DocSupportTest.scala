package org.neo4j.cypher.internal.compiler.v2_1.pp

import org.neo4j.cypher.internal.commons.CypherFunSuite

class DocSupportTest extends CypherFunSuite {

  test("empty == NilDoc") {
    DocSupport.end should equal(NilDoc)
  }

  test("break == BreakDoc") {
    DocSupport.break should equal(BreakDoc)
  }

  test("breakWith(v) == BreakWith(v)") {
    DocSupport.breakWith("a") should equal(BreakWith("a"))
  }

  test("text(v) = TextDoc(v)") {
    DocSupport.text("text") should equal(TextDoc("text"))
  }

  test("group(...) = GroupDoc(...)") {
    DocSupport.group(ConsDoc(BreakDoc, NilDoc)) should equal(GroupDoc(ConsDoc(BreakDoc, NilDoc)))
  }

  test("nest(i, ...) = NestDoc(i, ...)") {
    DocSupport.nest(3, BreakDoc) should equal(NestDoc(3, BreakDoc))
  }


  test("cons(hd, tl) = ConsDocs(hd, tl)") {
    DocSupport.cons(BreakDoc, NilDoc) should equal(ConsDoc(BreakDoc, NilDoc))
  }
}
