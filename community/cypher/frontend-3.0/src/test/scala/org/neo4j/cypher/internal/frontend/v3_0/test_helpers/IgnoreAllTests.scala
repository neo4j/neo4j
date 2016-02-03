package org.neo4j.cypher.internal.frontend.v3_0.test_helpers

import org.scalatest.Tag

trait IgnoreAllTests extends CypherFunSuite {

  def ignoranceRationale = ""

  abstract override protected def test(testName: String, testTags: Tag*)(testFun: => Unit) {
    val ignoredTestName = if (ignoranceRationale.isEmpty) testName else s"testName [$ignoranceRationale]"
    ignore(ignoredTestName, testTags: _*)(testFun)
  }

  protected def testIgnored(testName: String, testTags: Tag*)(testFun: => Unit): Unit = {
    super.test(testName, testTags: _*)(testFun)
  }
}
