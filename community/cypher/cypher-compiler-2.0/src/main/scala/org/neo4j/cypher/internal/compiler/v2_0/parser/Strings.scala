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
package org.neo4j.cypher.internal.compiler.v2_0.parser

import org.parboiled.scala._
import org.parboiled.Context

trait Strings extends Base {

  protected def StringCharacters(c: Char): Rule1[String] = {
    push(new StringBuilder) ~ zeroOrMore(EscapedChar | NormalChar(c)) ~~> (_.toString())
  }

  protected def NormalChar(c: Char) = {
    !(ch('\\') | ch(c)) ~ ANY ~:% withContext(appendToStringBuffer(_)(_))
  }

  protected def EscapedChar = {
    "\\" ~ (
      ch('\\') ~:% withContext(appendToStringBuffer(_)(_))
        | ch('\'') ~:% withContext(appendToStringBuffer(_)(_))
        | ch('"') ~:% withContext(appendToStringBuffer(_)(_))
        | ch('b') ~ appendToStringBuffer('\b')
        | ch('f') ~ appendToStringBuffer('\f')
        | ch('n') ~ appendToStringBuffer('\n')
        | ch('r') ~ appendToStringBuffer('\r')
        | ch('t') ~ appendToStringBuffer('\t')
        | Unicode ~~% withContext((code, ctx) => appendToStringBuffer(code.asInstanceOf[Char])(ctx))
      )
  }

  protected def Unicode = rule { ch('u') ~ group(HexDigit ~ HexDigit ~ HexDigit ~ HexDigit) ~> (java.lang.Integer.parseInt(_, 16)) }
  private def HexDigit = rule { "0" - "9" | "a" - "f" | "A" - "F" }

  protected def appendToStringBuffer(c: Any): Context[Any] => Unit = { ctx =>
    ctx.getValueStack.peek.asInstanceOf[StringBuilder].append(c)
    ()
  }
}
