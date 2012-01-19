package org.neo4j.cypher

import org.junit.Test

class ColumnNameTest extends ExecutionEngineHelper {
  @Test def shouldKeepUsedExpression1() {
    val result = parseAndExecute("start n=node(0) return cOuNt( * )")
    assert(result.columns === List("cOuNt( * )"))
  }

  @Test def shouldKeepUsedExpression2() {
    val result = parseAndExecute("start n=node(0) match p=n-->b return nOdEs( p )")
    assert(result.columns === List("nOdEs( p )"))
  }

  @Test def shouldKeepUsedExpression3() {
    val result = parseAndExecute("start n=node(0) match p=n-->b return coUnt( dIstInct p )")
    assert(result.columns === List("coUnt( dIstInct p )"))
  }

  @Test def shouldKeepUsedExpression4() {
    val result = parseAndExecute("start n=node(0) match p=n-->b return aVg(    n.age     )")
    assert(result.columns === List("aVg(    n.age     )"))
  }
}