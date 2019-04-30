/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.v4_0.ast

import java.util.regex.Pattern

import org.neo4j.cypher.internal.v4_0.util.InvalidArgumentException

object UserNameValidator {
  // Allow all ascii from '!' to '~', apart from ',' and ':' which are used as separators in flat file
  private val usernamePattern = Pattern.compile("^[\\x21-\\x2B\\x2D-\\x39\\x3B-\\x7E]+$")

  def assertValidUsername(username: String): Unit = {
    if (username == null || username.isEmpty)
      throw new InvalidArgumentException("The provided username is empty.")
    if (!usernamePattern.matcher(username).matches)
      throw new InvalidArgumentException(
        s"""Username '$username' contains illegal characters.
           |Use ascii characters that are not ',', ':' or whitespaces.""".stripMargin)
  }

}
