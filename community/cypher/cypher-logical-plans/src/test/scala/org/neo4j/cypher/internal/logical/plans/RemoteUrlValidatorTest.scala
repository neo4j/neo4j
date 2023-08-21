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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.messages.MessageUtil

import scala.jdk.CollectionConverters.SeqHasAsJava

class RemoteUrlValidatorTest extends CypherFunSuite {

  val url = "://localhost"
  val unsecureSchemes = Seq("neo4j")
  val secureSchemes = Seq("neo4j+s", "neo4j+ssc")
  val validSchemes = Seq("neo4j", "neo4j+s", "neo4j+ssc")
  val secureUrls: Seq[String] = secureSchemes.map(_ + url)
  val insecureUrls: Seq[String] = unsecureSchemes.map(_ + url)
  val noSchemeUrl = "localhost"

  insecureUrls.foreach(url =>
    test(s"validator should throw exception for url '$url' with insecure schemes") {
      val exception = RemoteUrlValidator.assertValidRemoteDatabaseUrl(url, secure = true).getOrElse(fail)
      exception shouldBe a[InvalidArgumentException]
      exception should have message MessageUtil.insecureScheme(url, secureSchemes.asJava)
    }
  )

  secureUrls.foreach(url =>
    test(s"validator should return no exception for url '$url' with secure schemes") {
      RemoteUrlValidator.assertValidRemoteDatabaseUrl(url, secure = true) shouldBe None
    }
  )

  (insecureUrls ++ secureUrls).foreach(url =>
    test(s"validator should return no exception for url '$url' with no security requirement") {
      RemoteUrlValidator.assertValidRemoteDatabaseUrl(url, secure = false) shouldBe None
    }
  )

  Seq("http" + url, "https" + url, noSchemeUrl).foreach {
    url =>
      test(s"validator should throw exception for invalid url '$url' with insecure schemes") {
        val exception = RemoteUrlValidator.assertValidRemoteDatabaseUrl(url, secure = true).getOrElse(fail)
        exception shouldBe a[InvalidArgumentException]
        exception should have message MessageUtil.invalidScheme(url, validSchemes.asJava)
      }

      test(
        s"validator should throw exception for invalid url '$url' with insecure schemes and no security requirement"
      ) {
        val exception = RemoteUrlValidator.assertValidRemoteDatabaseUrl(url, secure = false).getOrElse(fail)
        exception shouldBe a[InvalidArgumentException]
        exception should have message MessageUtil.invalidScheme(url, validSchemes.asJava)
      }
  }
}
