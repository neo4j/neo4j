package org.neo4j.cypher.internal.compiler.v2_1.pp

import org.neo4j.cypher.internal.commons.CypherFunSuite

class DocStructureDocGeneratorRenderTest extends CypherFunSuite {

  import DocSupport._

  test("end => \"ø\"") {
    render(end) should equal("ø")
  }

  test("break => \"_\"") {
    render(break) should equal("_")
  }

  test("breakWith(...) => \"_..._\"") {
    render(breakWith("...")) should equal("_..._")
  }

  test("text(...) => \"...\"") {
    render(text("...")) should equal("\"...\"")
  }

  test("text(\"a\")·text(\"b\") => \"a\"·\"b\"·ø") {
    render(cons(text("a"), cons(text("b")))) should equal("\"a\"·\"b\"·ø")
  }

  test("group(text(\"a\")) => [\"a\"]") {
    render(group(text("a"))) should equal("[\"a\"]")
  }

  test("nest(3, text(\"a\")) => (3)<\"a\">") {
    render(nest(3, text("a"))) should equal("(3)<\"a\">")
  }

  private def render(doc: Doc) = printString(InlineDocFormatter(DocStructureDocGenerator(doc)))
}
