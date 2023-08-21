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

import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.messages.MessageUtil

import java.net.URI

import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.util.Failure
import scala.util.Success
import scala.util.Try

object RemoteUrlValidator {

  def assertValidRemoteDatabaseUrl(url: String, secure: Boolean): Option[Throwable] = Try {
    if (url == null || url.isEmpty) {
      throw new InvalidArgumentException("The provided url is empty.")
    }

    val uriScheme = URI.create(url).getScheme
    val validSchemes = Seq("neo4j", "neo4j+s", "neo4j+ssc")
    if (uriScheme == null || !validSchemes.contains(uriScheme)) {
      throw new InvalidArgumentException(MessageUtil.invalidScheme(url, validSchemes.asJava))
    }

    if (secure && !(uriScheme.endsWith("+s") || uriScheme.endsWith("+ssc"))) {
      val secureSchemes = Seq("neo4j+s", "neo4j+ssc")
      throw new InvalidArgumentException(MessageUtil.insecureScheme(url, secureSchemes.asJava))
    }
  } match {
    case Success(_) => None
    case Failure(e) => Some(e)
  }

}
