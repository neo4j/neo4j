package org.neo4j.cypher.internal.compatibility.v3_4

import java.util.concurrent.atomic.AtomicInteger

import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite

class QueryCacheTest extends CypherFunSuite {

  test("should be able to add and get keys") {
    val monitor = new TestMonitor
    val cache = new QueryCache(cache = new LFUCache[String,String](5),cacheAccessor = new MonitoringCacheAccessor[String, String](monitor))
    def f: (String) => Boolean = {s => false}

    cache.getOrElseUpdate("A","B",f,"C")
    monitor.toString() should equal ("TestMonitor{hits=0, misses=1, discards=0}")
    cache.getOrElseUpdate("A","B",f,"C")
    monitor.toString() should equal ("TestMonitor{hits=1, misses=1, discards=0}")
  }

  test("should be able to clear the cache") {
    val monitor = new TestMonitor
    val cache = new QueryCache(cache = new LFUCache[String,String](5),cacheAccessor = new MonitoringCacheAccessor[String, String](monitor))
    def f: (String) => Boolean = {s => false}

    cache.getOrElseUpdate("A","B",f,"C")
    cache.getOrElseUpdate("B","B",f,"X")
    cache.getOrElseUpdate("C","B",f,"Y")

    cache.clear()

    monitor.toString() should equal ("TestMonitor{hits=0, misses=3, discards=3}")
  }
}

private class TestMonitor extends CypherCacheHitMonitor[String] {
  final private val hits = new AtomicInteger
  final private val misses = new AtomicInteger
  final private val discards = new AtomicInteger

  override def cacheHit(key: String): Unit = {
    hits.incrementAndGet
  }

  override def cacheMiss(key: String): Unit = {
    misses.incrementAndGet
  }

  override def cacheDiscard(key: String, ignored: String): Unit = {
    discards.incrementAndGet
  }

  override def toString: String = "TestMonitor{hits=" + hits + ", misses=" + misses + ", discards=" + discards + "}"
}
