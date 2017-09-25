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
package org.neo4j.cypher.internal.frontend.v3_4.parser

import org.parboiled.Context
import org.parboiled.scala._

trait Strings extends Base {

  protected def StringCharacters(c: Char): Rule1[String] = {
    push(new java.lang.StringBuilder) ~ zeroOrMore(EscapedChar | NormalChar(c)) ~~> (_.toString())
  }

  protected def NormalChar(c: Char) = {
    !(ch('\\') | ch(c)) ~ ANY ~:% withContext(appendToStringBuilder(_)(_))
  }

  protected def EscapedChar = {
    "\\" ~ (
        ch('\\') ~:% withContext(appendToStringBuilder(_)(_))
      | ch('\'') ~:% withContext(appendToStringBuilder(_)(_))
      | ch('"') ~:% withContext(appendToStringBuilder(_)(_))
      | ch('b') ~ appendToStringBuilder('\b')
      | ch('f') ~ appendToStringBuilder('\f')
      | ch('n') ~ appendToStringBuilder('\n')
      | ch('r') ~ appendToStringBuilder('\r')
      | ch('t') ~ appendToStringBuilder('\t')
      | UTF16 ~~% withContext((code, ctx) => appendCodePointToStringBuilder(code)(ctx))
      | UTF32 ~~% withContext((code, ctx) => appendCodePointToStringBuilder(code)(ctx))
    )
  }

  protected def UTF16 = rule { ch('u') ~ group(HexDigit ~ HexDigit ~ HexDigit ~ HexDigit) ~> (java.lang.Integer.parseInt(_, 16)) }
  protected def UTF32 = rule { ch('U') ~ group(HexDigit ~ HexDigit ~ HexDigit ~ HexDigit ~ HexDigit ~ HexDigit ~ HexDigit ~ HexDigit) ~> (java.lang.Integer.parseInt(_, 16)) }
  private def HexDigit = rule ("four hexadecimal digits specifying a unicode character") { "0" - "9" | "a" - "f" | "A" - "F" }

  protected def appendToStringBuilder(c: Any): Context[Any] => Unit = ctx =>
    ctx.getValueStack.peek.asInstanceOf[java.lang.StringBuilder].append(c)
    ()

  protected def appendCodePointToStringBuilder(codePoint: java.lang.Integer): Context[Any] => Unit = ctx =>
    ctx.getValueStack.peek.asInstanceOf[java.lang.StringBuilder].appendCodePoint(codePoint)
    ()
}
