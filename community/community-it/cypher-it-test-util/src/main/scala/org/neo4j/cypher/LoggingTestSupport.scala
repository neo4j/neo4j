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
package org.neo4j.cypher

import org.mockito.ArgumentMatcher
import org.mockito.ArgumentMatchers.argThat
import org.neo4j.collection.Dependencies
import org.neo4j.gqlstatus.ErrorGqlStatusObject
import org.neo4j.logging.AssertableLogProvider
import org.neo4j.logging.internal.LogService
import org.neo4j.logging.internal.SimpleLogService
import org.neo4j.server.security.auth.CommunitySecurityModule
import org.scalatest.Assertion

import scala.jdk.CollectionConverters.IteratorHasAsScala

trait LoggingTestSupport extends GraphDatabaseTestSupport {
  self: CypherITTestSuite =>

  protected val securityLogProvider: AssertableLogProvider = new AssertableLogProvider()
  protected val userLogProvider: AssertableLogProvider = new AssertableLogProvider()
  protected val logService: LogService = new SimpleLogService(userLogProvider, securityLogProvider)

  override def dependencies(): Option[Dependencies] = {
    val dependencies = new Dependencies()
    dependencies.satisfyDependency(logService)
    Some(dependencies)
  }

  // Omitting this may use large amounts of memory,
  // avoid overriding beforeEach without including this!
  protected def resetLogs(): Unit = {
    securityLogProvider.clear()
    userLogProvider.clear()
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    resetLogs()
  }

  protected def exceptionWithMessage(message: String): ErrorGqlStatusObject & Exception = argThat(
    new ArgumentMatcher[ErrorGqlStatusObject & Exception] {
      override def matches(e: ErrorGqlStatusObject & Exception): Boolean = e.legacyMessage().equals(message)

      override def toString: String = s"<GQL status with message: \"$message\">"
    }
  )

  protected def assert42NFFLogWithMessage(
    message: String,
    loginContext: Option[String] = None,
    community: Boolean = false
  ): Assertion = {
    val logger = if (community) classOf[CommunitySecurityModule].getCanonicalName else "SecurityLogger"
    val context = loginContext.map(c => s"[$c]: ").getOrElse("")
    securityLogProvider.logLines().asScala.toSeq should contain(
      s"ERROR @ $logger: ${context}Exception thrown, 42NFF: $message"
    )
  }

  protected def assert42NFFLogWithMessageContains(
    message: String,
    loginContext: Option[String] = None,
    community: Boolean = false
  ): Assertion = {
    val logger = if (community) classOf[CommunitySecurityModule].getCanonicalName else "SecurityLogger"
    val context = loginContext.map(c => s"[$c]: ").getOrElse("")
    val logLines = securityLogProvider.logLines().asScala.toSeq
    val start = s"ERROR @ $logger: ${context}Exception thrown, 42NFF: $message"
    withClue(
      s"""Did not find line containing
         |$start
         in
         |${logLines.mkString("\n")}""".stripMargin
    ) {
      logLines.exists(line => line.startsWith(start)) shouldBe true
    }
  }
}
