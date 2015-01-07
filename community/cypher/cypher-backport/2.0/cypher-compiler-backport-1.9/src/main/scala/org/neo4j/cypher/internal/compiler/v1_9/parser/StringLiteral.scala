/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.v1_9.parser

import org.neo4j.cypher.internal.compiler.v1_9.commands.expressions.Literal


trait StringLiteral extends Base {
  def stringLit: Parser[Literal] = Parser {
    case in if in.atEnd => Failure("out of string", in)
    case in =>
      val start = handleWhiteSpace(in.source, in.offset)
      val string = in.source.subSequence(start, in.source.length()).toString
      val startChar = string.charAt(0)
      if (startChar != '\"' && startChar != '\'')
        Failure("expected string", in)
      else {

        var ls = string.toList.tail
        val sb = new StringBuilder(ls.length)
        var idx = start
        var result: Option[ParseResult[Literal]] = None

        while (!ls.isEmpty && result.isEmpty) {
          val (pref, suf) = ls span {
            c => c != '\\' && c != startChar
          }
          idx += pref.length
          sb ++= pref

          if (suf.isEmpty) {
            result = Some(Failure("end of string missing", in))
          } else {

            val first: Char = suf(0)
            if (first == startChar) {
              result = Some(Success(Literal(sb.result()), in.drop(idx - in.offset + 2)))
            } else {
              val (escChars, afterEscape) = suf.splitAt(2)

              if (escChars.size == 1) {
                result = Some(Failure("invalid escape sequence", in.drop(1)))
              } else {

                ls = afterEscape
                idx += 2

                parseEscapeChars(escChars.tail, in) match {
                  case Left(c)        => sb.append(c)
                  case Right(failure) => result = Some(failure)
                }
              }
            }
          }
        }

        result match {
          case Some(x) => x
          case None    => Failure("end of string missing", in)
        }
      }
  }

  case class EscapeProduct(result: Option[ParseResult[Literal]])

  private def parseEscapeChars(suf: List[Char], in:Input): Either[Char, Failure] = suf match {
    case '\\' :: tail => Left('\\')
    case '\'' :: tail => Left('\'')
    case '"' :: tail  => Left('"')
    case 'b' :: tail  => Left('\b')
    case 'f' :: tail  => Left('\f')
    case 'n' :: tail  => Left('\n')
    case 'r' :: tail  => Left('\r')
    case 't' :: tail  => Left('\t')
    case _            => Right(Failure("invalid escape sequence", in))
  }
}
