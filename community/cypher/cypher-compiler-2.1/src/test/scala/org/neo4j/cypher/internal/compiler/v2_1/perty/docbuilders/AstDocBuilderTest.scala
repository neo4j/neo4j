package org.neo4j.cypher.internal.compiler.v2_1.perty.docbuilders

import org.neo4j.cypher.internal.compiler.v2_1.docbuilders.{astDocBuilder, astExpressionDocBuilder}
import org.neo4j.cypher.internal.compiler.v2_1.ast.{ASTNode, LabelName, AstConstructionTestSupport, UsingIndexHint}

class AstDocBuilderTest extends DocBuilderTestSuite[Any] with AstConstructionTestSupport {

  val docBuilder = astDocBuilder orElse astExpressionDocBuilder orElse simpleDocBuilder

  test("USING INDEX n:Person(name)") {
    val astNode: ASTNode = UsingIndexHint(ident("n"), LabelName("Person")_, ident("name"))_
    format(astNode) should equal("USING INDEX n:Person(name)")
  }
}
