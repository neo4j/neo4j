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
package org.neo4j.cypher.internal.runtime.spec.matcher

import org.neo4j.io.fs.FileSystemAbstraction
import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8

import scala.util.Using

trait FileSystemAbstractionMatchers {

  class ContainsFileMatcher(pathString: String, contains: Option[String]) extends Matcher[FileSystemAbstraction] {
    private val path = java.nio.file.Path.of(pathString)

    def containing(expected: String): ContainsFileMatcher = {
      require(contains.isEmpty)
      new ContainsFileMatcher(pathString, Some(expected))
    }

    def apply(left: FileSystemAbstraction): MatchResult = {
      val exists = fileExist(left)
      if (!exists.matches) exists
      else fileContains(left).getOrElse(exists)
    }

    private def fileExist(left: FileSystemAbstraction): MatchResult = {
      MatchResult(
        left.fileExists(path),
        s"File system did not contain '$pathString'",
        s"File system did contain '$pathString'"
      )
    }

    private def fileContains(left: FileSystemAbstraction): Option[MatchResult] = contains.map(fileContains(left, _))

    private def fileContains(left: FileSystemAbstraction, expected: String): MatchResult = {
      def result(matches: Boolean) = MatchResult(
        matches,
        s"File '$pathString' did not contain '$expected'",
        s"File '$pathString' did contain '$expected'"
      )
      Using.resource(left.read(path)) { read =>
        val bufferSize = math.max(expected.length * 2, 16_000)
        val buffer = ByteBuffer.wrap(new Array[Byte](bufferSize))
        while (read.read(buffer) > 0) {
          val lastNewLinePos = buffer.array().lastIndexOf('\n', buffer.position())
          require(lastNewLinePos > 0)
          val readString = new String(buffer.array(), 0, lastNewLinePos, UTF_8)
          // println(readString)
          if (readString.contains(expected)) {
            return result(matches = true)
          }
          buffer.position(lastNewLinePos)
          buffer.clear()
        }
      }
      result(matches = false)
    }
  }

  def haveFile(path: String) = new ContainsFileMatcher(path, None)
}
