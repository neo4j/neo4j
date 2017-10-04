/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.util.v3_4.test_helpers

import org.scalactic.Equality

// Makes it easy to compare strings without having to worry about new lines
object WindowsStringSafe extends Equality[String] {
  override def areEqual(a: String, b: Any) = b match {
    case b: String =>
      a.replaceAll("\r\n", "\n") equals b.replaceAll("\r\n", "\n")
    case _ => false
  }
}
