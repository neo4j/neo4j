package org.neo4j.cypher.internal.compatibility.v3_4

import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite

class LFUCacheTest extends CypherFunSuite {

  test("testClear") {
    val cache = new LFUCache[String, String](5)

    cache.put("A","A")
    cache.put("B","B")
    cache.put("C","C")
    cache.put("D","D")
    cache.put("E","E")

    cache.get("A").isEmpty should be (false)
    cache.get("B").isEmpty should be (false)
    cache.get("C").isEmpty should be (false)
    cache.get("D").isEmpty should be (false)
    cache.get("E").isEmpty should be (false)

    cache.clear()

    cache.get("A").isEmpty should be (true)
    cache.get("B").isEmpty should be (true)
    cache.get("C").isEmpty should be (true)
    cache.get("D").isEmpty should be (true)
    cache.get("E").isEmpty should be (true)
  }
}
