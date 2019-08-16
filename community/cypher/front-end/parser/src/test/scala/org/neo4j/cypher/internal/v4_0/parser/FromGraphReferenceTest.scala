package org.neo4j.cypher.internal.v4_0.parser

import org.neo4j.cypher.internal.v4_0.ast
import org.neo4j.cypher.internal.v4_0.{expressions => exp}
import org.neo4j.cypher.internal.v4_0.ast.AstConstructionTestSupport
import  org.neo4j.cypher.internal.v4_0.util.symbols.CTAny
import org.parboiled.scala.Rule1

class FromGraphReferenceTest
  extends ParserAstTest[ast.FromGraph]
    with Query
    with Expressions
    with AstConstructionTestSupport {

  implicit val parser: Rule1[ast.FromGraph] = FromGraph

  test("Graph reference is available in valid cases") {
    parsing("FROM x")
      .shouldVerify(c => c.graphReference shouldEqual Some(ast.GraphRef(ast.CatalogName("x"))(pos)))
    parsing("FROM x.y.z")
      .shouldVerify(c => c.graphReference shouldEqual Some(ast.GraphRef(ast.CatalogName("x", "y", "z"))(pos)))
    parsing("FROM v(x, $p)")
      .shouldVerify(c => c.graphReference shouldEqual Some(ast.ViewRef(ast.CatalogName("v"), Seq(exp.Variable("x")(pos), exp.Parameter("p", CTAny)(pos)))(pos)))
    parsing("FROM a.b.v(x, $p)")
      .shouldVerify(c => c.graphReference shouldEqual Some(ast.ViewRef(ast.CatalogName("a", "b", "v"), Seq(exp.Variable("x")(pos), exp.Parameter("p", CTAny)(pos)))(pos)))
  }

  test("Graph reference is not available in invalid cases") {
    parsing("FROM 1")
      .shouldVerify(c => c.graphReference shouldEqual None)
    parsing("FROM 'a'")
      .shouldVerify(c => c.graphReference shouldEqual None)
    parsing("FROM [x]")
      .shouldVerify(c => c.graphReference shouldEqual None)
    parsing("FROM 1 + 2")
      .shouldVerify(c => c.graphReference shouldEqual None)
  }
}
