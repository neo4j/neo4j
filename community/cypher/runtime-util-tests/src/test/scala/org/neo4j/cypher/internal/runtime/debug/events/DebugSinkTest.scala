/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.debug.events

import org.neo4j.cypher.internal.runtime.RuntimeUtilTestSuite
import org.neo4j.cypher.internal.runtime.debug.events.Debug.LogContext

/**
 * Tests the module-local debug-logging infrastructure: the [[DebugLog]]
 * sinks and their serialisation. Concrete event families and their
 * renderings live in the pipelined-runtime module and are exercised there;
 * here we use minimal local events and a local [[DebugEventRenderer]] so
 * the sink behaviour can be pinned without depending on any particular
 * family.
 */
class DebugSinkTest extends RuntimeUtilTestSuite {

  private val TestSite = CallSite("org.neo4j.Test#method", "Test.scala", 7)
  private val TestContext = LogContext(TestSite, "main-test", None)

  test("you forgot to reset") {
    Debug.isEnabled[DebugCategory.Asm] shouldBe false
    Debug.isEnabled[DebugCategory.BatchFormation] shouldBe false
    Debug.isEnabled[DebugCategory.Buffers] shouldBe false
    Debug.isEnabled[DebugCategory.Cleanup] shouldBe false
    Debug.isEnabled[DebugCategory.ConcurrentTransactions] shouldBe false
    Debug.isEnabled[DebugCategory.ConcurrentTransactionsWorker] shouldBe false
    Debug.isEnabled[DebugCategory.Cursors] shouldBe false
    Debug.isEnabled[DebugCategory.ErrorHandling] shouldBe false
    Debug.isEnabled[DebugCategory.GeneratedIrCode] shouldBe false
    Debug.isEnabled[DebugCategory.Locks] shouldBe false
    Debug.isEnabled[DebugCategory.MemoryTracking] shouldBe false
    Debug.isEnabled[DebugCategory.MorselReuse] shouldBe false
    Debug.isEnabled[DebugCategory.PhysicalPlanning] shouldBe false
    Debug.isEnabled[DebugCategory.Pipelines] shouldBe false
    Debug.isEnabled[DebugCategory.Progress] shouldBe false
    Debug.isEnabled[DebugCategory.Queries] shouldBe false
    Debug.isEnabled[DebugCategory.Scheduling] shouldBe false
    Debug.isEnabled[DebugCategory.Tracker] shouldBe false
    Debug.isEnabled[DebugCategory.TransactionalContext] shouldBe false
    Debug.isEnabled[DebugCategory.Workers] shouldBe false
  }

  test("DebugEventRenderer.loaded falls back to toString rendering when no renderer is registered") {
    // runtime-util's classpath carries no service registration — the
    // production renderer lives in pipelined-runtime.
    DebugEventRenderer.loaded shouldBe DebugEventRenderer.ToString

    val event = Plain("hello")
    DebugEventRenderer.ToString.render(event, TestContext) shouldBe event.toString
    DebugEventRenderer.ToString.fields(event, TestContext) shouldBe Seq("message" -> LogValue.Str(event.toString))
  }

  test("CallSite.here captures the enclosing definition, file and line of the call") {
    val site = CallSite.here
    site.fileName shouldBe "DebugSinkTest.scala"
    site.enclosing should include("DebugSinkTest")
    site.line should be > 0
  }

  test("CallSite.here sees through nested inline defs to the outermost call site, like Debug.log does") {
    val site = indirectSite
    site.fileName shouldBe "DebugSinkTest.scala"
    site.enclosing should include("DebugSinkTest")
  }

  test("CallSite renders stack-frame style, dropping the package path in the short form") {
    TestSite.shortString shouldBe "Test#method(Test.scala:7)"
    TestSite.toString shouldBe "org.neo4j.Test#method(Test.scala:7)"
  }

  test("StructuredDebugLog emits one NDJSON object per event with the fields the renderer advised") {
    val log = newStructuredLog()
    log.sink.log(
      WithFields(
        "r",
        Seq(
          "task" -> LogValue.opaque("task-1"),
          "workerId" -> LogValue.num(7),
          "threadName" -> LogValue.str("worker-3")
        )
      ),
      TestContext
    )

    log.lines shouldBe Seq(
      """{"ts":42,"thread":"main-test","event":"DebugSinkTest.WithFields",""" +
        """"site":{"enclosing":"org.neo4j.Test#method","file":"Test.scala","line":7},""" +
        """"fields":{"task":"task-1","workerId":7,"threadName":"worker-3"}}"""
    )
  }

  test("StructuredDebugLog unfolds Throwables into a nested class+message object") {
    val log = newStructuredLog()
    log.sink.log(WithFields("r", Seq("error" -> LogValue.err(new RuntimeException("boom")))), TestContext)

    val line = log.lines.head
    line should include("\"error\":{\"class\":\"java.lang.RuntimeException\",\"message\":\"boom\"}")
  }

  test("StructuredDebugLog renders primitive int arrays as JSON arrays of numbers") {
    val log = newStructuredLog()
    log.sink.log(WithFields("r", Seq("order" -> LogValue.ints(Seq(3, 1, 2)))), TestContext)

    log.lines.head should include("\"order\":[3,1,2]")
  }

  test("StructuredDebugLog renders Seq fields as JSON arrays of strings") {
    val log = newStructuredLog()
    log.sink.log(WithFields("r", Seq("lines" -> LogValue.strs(Seq("line-a", "line-b")))), TestContext)

    log.lines.head should include("\"lines\":[\"line-a\",\"line-b\"]")
  }

  test("StructuredDebugLog renders num/real/bool/null scalars in their JSON forms") {
    val log = newStructuredLog(properties = Seq(StructuredProperty.Fields))
    log.sink.log(
      WithFields(
        "r",
        Seq(
          "n" -> LogValue.num(7L),
          "r" -> LogValue.real(1.5),
          "b" -> LogValue.bool(true),
          "nul" -> LogValue.str(null)
        )
      ),
      TestContext
    )

    log.lines shouldBe Seq("""{"fields":{"n":7,"r":1.5,"b":true,"nul":null}}""")
  }

  test("StructuredDebugLog falls back to a string for non-finite reals, keeping the line parseable") {
    val log = newStructuredLog(properties = Seq(StructuredProperty.Fields))
    log.sink.log(WithFields("r", Seq("x" -> LogValue.real(Double.NaN))), TestContext)

    log.lines.head should include("\"x\":\"NaN\"")
  }

  test("StructuredDebugLog escapes JSON special characters in string fields") {
    val log = newStructuredLog()
    log.sink.log(WithFields("r", Seq("task" -> LogValue.opaque("a\"b\\c\nd"))), TestContext)

    log.lines.head should include("\"task\":\"a\\\"b\\\\c\\nd\"")
  }

  test("StructuredDebugLog escapes control characters via the JSON uXXXX form") {
    val log = newStructuredLog()
    log.sink.log(WithFields("r", Seq("task" -> LogValue.opaque("ab"))), TestContext)

    log.lines.head should include("\"task\":\"a\\u0001b\"")
  }

  test("StructuredDebugLog defaults to a message-only fields object for events without a dedicated encoding") {
    val log = newStructuredLog()
    log.sink.log(Plain("waitForWorkersToIdle SUCCESS"), TestContext)

    log.lines shouldBe Seq(
      """{"ts":42,"thread":"main-test","event":"DebugSinkTest.Plain",""" +
        """"site":{"enclosing":"org.neo4j.Test#method","file":"Test.scala","line":7},""" +
        """"fields":{"message":"waitForWorkersToIdle SUCCESS"}}"""
    )
  }

  test("StructuredDebugLog only emits the properties it was configured with, in the configured order") {
    val log = newStructuredLog(properties =
      Seq(StructuredProperty.Event, StructuredProperty.Message)
    )
    log.sink.log(Plain("the message"), TestContext)

    val line = log.lines.head
    line should startWith("""{"event":"DebugSinkTest.Plain",""")
    line should include(""""message":"the message"""")
    line should not include "\"ts\""
    line should not include "\"thread\""
    line should not include "\"site\""
    line should not include "\"fields\""
  }

  test("StructuredDebugLog with an empty property list emits an empty object per event") {
    val log = newStructuredLog(properties = Seq.empty)
    log.sink.log(Plain("anything"), TestContext)

    log.lines shouldBe Seq("{}")
  }

  test("StructuredDebugLog tags enum events with family and case name, for parameterless cases too") {
    val log = newStructuredLog(properties = Seq(StructuredProperty.Event))
    log.sink.log(EnumEvent.Parameterized(1), TestContext)
    log.sink.log(EnumEvent.Singleton, TestContext)

    log.lines shouldBe Seq(
      """{"event":"DebugSinkTest.EnumEvent.Parameterized"}""",
      """{"event":"DebugSinkTest.EnumEvent.Singleton"}"""
    )
  }

  private inline def indirectSite: CallSite = CallSite.here

  private def newStructuredLog(
    properties: Seq[StructuredProperty] = StructuredDebugLog.DefaultProperties
  ): StructuredHarness =
    new StructuredHarness(properties)

  private class StructuredHarness(properties: Seq[StructuredProperty]) {
    private val out = new java.io.ByteArrayOutputStream()

    val sink: StructuredDebugLog = new StructuredDebugLog(
      new java.io.PrintStream(out, true, java.nio.charset.StandardCharsets.UTF_8),
      properties = properties,
      clock = () => 42L,
      renderer = TestRenderer
    )

    def lines: Seq[String] =
      out.toString(java.nio.charset.StandardCharsets.UTF_8).stripSuffix("\n").split("\n").toSeq
  }

  private case class Plain(text: String)
      extends DebugEvent {}

  private case class WithFields(
    text: String,
    fieldSeq: Seq[(String, LogValue)]
  ) extends DebugEvent {}

  /** Mirrors the enum shape of the production event families. */
  private enum EnumEvent extends DebugEvent {
    case Singleton
    case Parameterized(x: Int)
  }

  /** Local stand-in for the production renderer in pipelined-runtime. */
  private object TestRenderer extends DebugEventRenderer {

    override def render(event: DebugEvent, context: LogContext): String = event match {
      case Plain(text)         => text
      case WithFields(text, _) => text
      case other               => other.toString
    }

    override def fields(event: DebugEvent, context: LogContext): Seq[(String, LogValue)] = event match {
      case WithFields(_, fieldSeq) => fieldSeq
      case other                   => Seq("message" -> LogValue.str(render(other, context)))
    }
  }
}
