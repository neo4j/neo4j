package org.neo4j.cypher.internal.frontend.v3_0.test_helpers

import org.scalatest.Tag

trait IgnoreAllTests {

  self: CypherFunSuite =>

  def ignoranceRationale = ""

  override protected def test(testName: String, testTags: Tag*)(testFun: => Unit) {
    val finalTestName = if (ignoranceRationale.isEmpty) testName else s"testName [$ignoranceRationale]"
    ignore(finalTestName, testTags: _*)(testFun)
  }
}
