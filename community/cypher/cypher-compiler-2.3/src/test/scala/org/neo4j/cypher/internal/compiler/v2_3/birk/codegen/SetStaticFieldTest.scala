package org.neo4j.cypher.internal.compiler.v2_3.birk.codegen

import org.neo4j.cypher.internal.compiler.v2_3.test_helpers.CypherFunSuite

class SetStaticFieldTest extends CypherFunSuite {
  test("do it") {
    setStaticField(classOf[APA], "X", "HELLO WORLD!")

    APA.X should equal("HELLO WORLD!")
  }
}

object APA {
  var X : String = ""
  val Xval : String = ""
  def Xdef : String = ""
}

class APA