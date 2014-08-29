package org.neo4j.cypher.internal.compiler.v2_2.docbuilders

import org.neo4j.cypher.internal.compiler.v2_2.ast.ASTNode
import org.neo4j.cypher.internal.compiler.v2_2.perty.docbuilders.{simpleDocBuilder, DocBuilderTestSuite}

class AstDocBuilderTest extends DocBuilderTestSuite[Any] {

  val docBuilder = astDocBuilder orElse simpleDocBuilder

  test("should work inside non-ast nodes") {
    case class Container(astNode: ASTNode)
    format(Container(ident("a"))) should equal("Container(a)")
  }

  test("should work inside non-ast-nodes inside unknown ast nodes") {
    case class UnExpected(v: Any) extends ASTNode { def position = null }
    case class Container(astNode: ASTNode)
    format(UnExpected(Container(ident("a")))) should equal("UnExpected(Container(a))")
  }
}
