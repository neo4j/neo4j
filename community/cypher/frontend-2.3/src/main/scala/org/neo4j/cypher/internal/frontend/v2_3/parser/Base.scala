/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v2_3.parser

import org.neo4j.cypher.internal.frontend.v2_3.{InputPosition, InternalException, SyntaxException}
import org.parboiled.Context
import org.parboiled.errors.{InvalidInputError, ParseError}
import org.parboiled.scala._
import org.parboiled.support.IndexRange

trait Base extends Parser {

  def OpChar = rule("an operator char") { anyOf("|^&<>=?!:+-*/%~") }
  def OpCharTail = rule("an operator char") { anyOf("|^&<>=?!:*/%~") }

  def DecimalInteger = rule { (optional("-") ~ UnsignedDecimalInteger).memoMismatches }
  def UnsignedDecimalInteger = rule { (group(("1" - "9") ~ optional(DigitString)) | "0").memoMismatches }
  def RegularDecimalReal = rule { (optional("-") ~ zeroOrMore("0" - "9") ~ "." ~ DigitString).memoMismatches }
  def ExponentDecimalReal = rule { (optional("-") ~ oneOrMore("0" - "9" | ".") ~ (ch('e') | ch('E')) ~ optional("-") ~ DigitString).memoMismatches }
  def DigitString = rule("'0'-'9'") { oneOrMore(IdentifierPart) ~ !IdentifierPart }
  def OctalInteger = rule { (optional("-") ~ UnsignedOctalInteger).memoMismatches }
  def UnsignedOctalInteger = rule { ("0" ~ OctalString).memoMismatches }
  def OctalString = rule("'0'-'7'") { oneOrMore(IdentifierPart) ~ !IdentifierPart }
  def HexInteger = rule { (optional("-") ~ UnsignedHexInteger).memoMismatches }
  def UnsignedHexInteger = rule { ("0x" ~ HexString).memoMismatches }
  def HexString = rule("'0'-'9', 'a'-'f'") { oneOrMore(IdentifierPart) ~ !IdentifierPart }

  def Dash = rule("'-'") {
    // U+002D ‑ hyphen-minus
    // U+00AD - soft hyphen
    // U+2010 - hyphen
    // U+2011 ‐ non-breaking hyphen
    // U+2012 ‒ figure dash
    // U+2013 – en dash
    // U+2014 — em dash
    // U+2015 ― horizontal bar
    // U+2212 − minus sign
    // U+FE58 ﹘ small em dash
    // U+FE63 ﹣ small hyphen-minus
    // U+FF0D － full-width hyphen-minus
    anyOf(Array('\u002d', '\u00ad', '\u2010', '\u2011', '\u2012', '\u2013', '\u2014', '\u2015', '\u2212', '\ufe58', '\ufe63', '\uff0d'))
  }
  def LeftArrowHead = rule("'<'") {
    // U+003c < less-than sign
    // U+27e8 ⟨ mathematical left angle bracket
    // U+3008 〈 left angle bracket
    // U+fe64 ﹤ small less-than sign
    // U+ff1c ＜ full-width less-than sign
    anyOf(Array('\u003c', '\u27e8', '\u3008', '\ufe64', '\uff1c'))
  }
  def RightArrowHead = rule("'>'") {
    // U+003e > greater-than sign
    // U+27e9 ⟩ mathematical left angle bracket
    // U+3009 〉 right angle bracket
    // U+fe65 ﹥ small greater-than sign
    // U+ff1e ＞ full-width greater-than sign
    anyOf(Array('\u003e', '\u27e9', '\u3009', '\ufe65', '\uff1e'))
  }

  def CommaSep = rule("','") { WS ~ ch(',') ~ WS }

  def WS = rule("whitespace") {
    zeroOrMore(
        (oneOrMore(WSChar) memoMismatches)
      | (ch('/').label("comment") ~ (
          ch('*') ~ zeroOrMore(!"*/" ~ ANY) ~ "*/"
        | ch('/') ~ zeroOrMore(!anyOf("\n\r") ~ ANY) ~ (optional(ch('\r')) ~ ch('\n') | EOI)
      ) memoMismatches)
    )
  }.suppressNode

  def keyword(string: String): Rule0 = {
    def word(string: String): Rule0 = group(ignoreCase(string).label(string.toUpperCase) ~ !IdentifierPart)
    val strings = string.trim.split(' ')
    group(strings.tail.foldLeft(word(strings.head)) {
      (acc, s) => acc ~ WS ~ word(s)
    })
  }
  def operator(string: String) = group(string ~ !OpCharTail)

  def push[R](f: InputPosition => R): Rule1[R] = pushFromContext(ctx => f(ContextPosition(ctx)))

  def position = withContext((_: IndexRange, ctx: Context[Any]) => ContextPosition(ctx))

  def SymbolicNameString: Rule1[String] = UnescapedSymbolicNameString | EscapedSymbolicNameString

  def UnescapedSymbolicNameString: Rule1[String] = rule("an identifier") {
    group(IdentifierStart ~ zeroOrMore(IdentifierPart)) ~> (_.toString) ~ !IdentifierPart
  }

  def EscapedSymbolicNameString: Rule1[String] = rule("an identifier") {
    (oneOrMore(
      ch('`') ~ zeroOrMore(!ch('`') ~ ANY) ~> (_.toString) ~ ch('`')
    ) memoMismatches) ~~> (_.reduce(_ + '`' + _))
  }

  def parseOrThrow[T](input: String, initialOffset: Option[InputPosition], rule: Rule1[Seq[T]]): T = {
    val parsingResults = ReportingParseRunner(rule).run(input)
    parsingResults.result match {
      case Some(statements) =>
        if (statements.size == 1) {
          val statement = statements.head
          statement
        } else {
          throw new SyntaxException(s"Expected exactly one statement per query but got: ${statements.size}")
        }
      case _ => {
        val parseErrors: List[ParseError] = parsingResults.parseErrors
        parseErrors.map { error =>
          val message = if (error.getErrorMessage != null) {
            error.getErrorMessage
          } else {
            error match {
              case invalidInput: InvalidInputError => new InvalidInputErrorFormatter().format(invalidInput)
              case _ => error.getClass.getSimpleName
            }
          }

          val bufferPosition = BufferPosition(error.getInputBuffer, error.getStartIndex)
          val position = bufferPosition.withOffset(initialOffset)
          throw new SyntaxException(s"$message ($position)", input, position.offset)
        }

        throw new InternalException("Parsing failed but no parse errors were provided")
      }
    }
  }

  implicit class RichRule0(r: Rule0) {
    def ~~(other: Rule0): Rule0 = r ~ WS ~ other
    def ~~[A](other: Rule1[A]): Rule1[A] = r ~ WS ~ other
    def ~~[A, B](other: Rule2[A, B]): Rule2[A, B] = r ~ WS ~ other
    def ~~[A, B, C](other: Rule3[A, B, C]): Rule3[A, B, C] = r ~ WS ~ other
    def ~~[A, B, C, D](other: Rule4[A, B, C, D]): Rule4[A, B, C, D] = r ~ WS ~ other
    def ~~[A, B, C, D, E](other: Rule5[A, B, C, D, E]): Rule5[A, B, C, D, E] = r ~ WS ~ other
    def ~~[A, B, C, D, E, F](other: Rule6[A, B, C, D, E, F]): Rule6[A, B, C, D, E, F] = r ~ WS ~ other
    def ~~[A, B, C, D, E, F, G](other: Rule7[A, B, C, D, E, F, G]): Rule7[A, B, C, D, E, F, G] = r ~ WS ~ other

    def ~~[A, B](other: ReductionRule1[A, B]): ReductionRule1[A, B] = r ~ WS ~ other
    def ~~[A, B, C](other: ReductionRule2[A, B, C]): ReductionRule2[A, B, C] = r ~ WS ~ other

    def ~>>>[R](f: String => InputPosition => R): Rule1[R] = r ~> withContext((s: String, ctx) => f(s)(ContextPosition(ctx)))
    def ~~>>[Z, R](f: (Z) => (InputPosition => R)): ReductionRule1[Z, R] =
      r ~~> withContext((z: Z, ctx) => f(z)(ContextPosition(ctx)))
    def ~~>>[Y, Z, R](f: (Y, Z) => (InputPosition => R)): ReductionRule2[Y, Z, R] =
      r ~~> withContext((y: Y, z: Z, ctx) => f(y, z)(ContextPosition(ctx)))
  }

  implicit class RichString(s: String) {
    def ~~(other: Rule0): Rule0 = s ~ WS ~ other
    def ~~[A](other: String): Rule0 = s ~ WS ~ other
    def ~~[A](other: Rule1[A]): Rule1[A] = s ~ WS ~ other
    def ~~[A, B](other: Rule2[A, B]): Rule2[A, B] = s ~ WS ~ other
    def ~~[A, B, C](other: Rule3[A, B, C]): Rule3[A, B, C] = s ~ WS ~ other
    def ~~[A, B, C, D](other: Rule4[A, B, C, D]): Rule4[A, B, C, D] = s ~ WS ~ other
    def ~~[A, B, C, D, E](other: Rule5[A, B, C, D, E]): Rule5[A, B, C, D, E] = s ~ WS ~ other
    def ~~[A, B, C, D, E, F](other: Rule6[A, B, C, D, E, F]): Rule6[A, B, C, D, E, F] = s ~ WS ~ other
    def ~~[A, B, C, D, E, F, G](other: Rule7[A, B, C, D, E, F, G]): Rule7[A, B, C, D, E, F, G] = s ~ WS ~ other

    def ~~[A, B](other: ReductionRule1[A, B]): ReductionRule1[A, B] = s ~ WS ~ other
    def ~~[A, B, C](other: ReductionRule2[A, B, C]): ReductionRule2[A, B, C] = s ~ WS ~ other
  }

  implicit class RichRule1[+A](r: Rule1[A]) {
    def ~~(other: Rule0): Rule1[A] = r ~ WS ~ other
    def ~~[B](other: Rule1[B]): Rule2[A, B] = r ~ WS ~ other
    def ~~[B, C](other: Rule2[B, C]): Rule3[A, B, C] = r ~ WS ~ other
    def ~~[B, C, D](other: Rule3[B, C, D]): Rule4[A, B, C, D] = r ~ WS ~ other
    def ~~[B, C, D, E](other: Rule4[B, C, D, E]): Rule5[A, B, C, D, E] = r ~ WS ~ other
    def ~~[B, C, D, E, F](other: Rule5[B, C, D, E, F]): Rule6[A, B, C, D, E, F] = r ~ WS ~ other
    def ~~[B, C, D, E, F, G](other: Rule6[B, C, D, E, F, G]): Rule7[A, B, C, D, E, F, G] = r ~ WS ~ other

    def ~~[B](other: ReductionRule1[A, B]): Rule1[B] = r ~ WS ~ other

    def ~~>>[R](f: (A) => (InputPosition => R)): Rule1[R] =
      r ~~> withContext((a: A, ctx) => f(a)(ContextPosition(ctx)))
    def ~~>>[Z, R](f: (Z, A) => (InputPosition => R)): ReductionRule1[Z, R] =
      r ~~> withContext((z: Z, a: A, ctx) => f(z, a)(ContextPosition(ctx)))
    def ~~>>[Y, Z, R](f: (Y, Z, A) => (InputPosition => R)): ReductionRule2[Y, Z, R] =
      r ~~> withContext((y: Y, z: Z, a: A, ctx) => f(y, z, a)(ContextPosition(ctx)))
  }

  implicit class RichRule2[+A, +B](r: Rule2[A, B]) {
    def ~~(other: Rule0): Rule2[A, B] = r ~ WS ~ other
    def ~~[C](other: Rule1[C]): Rule3[A, B, C] = r ~ WS ~ other
    def ~~[C, D](other: Rule2[C, D]): Rule4[A, B, C, D] = r ~ WS ~ other
    def ~~[C, D, E](other: Rule3[C, D, E]): Rule5[A, B, C, D, E] = r ~ WS ~ other
    def ~~[C, D, E, F](other: Rule4[C, D, E, F]): Rule6[A, B, C, D, E, F] = r ~ WS ~ other
    def ~~[C, D, E, F, G](other: Rule5[C, D, E, F, G]): Rule7[A, B, C, D, E, F, G] = r ~ WS ~ other

    def ~~[BB >: B, C](other: ReductionRule1[BB, C]): Rule2[A, C] = r ~ WS ~ other
    def ~~[AA >: A, BB >: B, C](other: ReductionRule2[AA, BB, C]): Rule1[C] = r ~ WS ~ other

    def ~~>>[R](f: (A, B) => InputPosition => R): Rule1[R] =
      r ~~> withContext((a: A, b: B, ctx) => f(a, b)(ContextPosition(ctx)))
    def ~~>>[Z, R](f: (Z, A, B) => InputPosition => R): ReductionRule1[Z, R] =
      r ~~> withContext((z: Z, a: A, b: B, ctx) => f(z, a, b)(ContextPosition(ctx)))
    def ~~>>[Y, Z, R](f: (Y, Z, A, B) => InputPosition => R): ReductionRule2[Y, Z, R] =
      r ~~> withContext((y: Y, z: Z, a: A, b: B, ctx) => f(y, z, a, b)(ContextPosition(ctx)))
  }

  implicit class RichRule3[+A, +B, +C](r: Rule3[A, B, C]) {
    def ~~(other: Rule0): Rule3[A, B, C] = r ~ WS ~ other
    def ~~[D](other: Rule1[D]): Rule4[A, B, C, D] = r ~ WS ~ other
    def ~~[D, E](other: Rule2[D, E]): Rule5[A, B, C, D, E] = r ~ WS ~ other
    def ~~[D, E, F](other: Rule3[D, E, F]): Rule6[A, B, C, D, E, F] = r ~ WS ~ other
    def ~~[D, E, F, G](other: Rule4[D, E, F, G]): Rule7[A, B, C, D, E, F, G] = r ~ WS ~ other

    def ~~>>[R](f: (A, B, C) => InputPosition => R): Rule1[R] =
      r ~~> withContext((a: A, b: B, c: C, ctx) => f(a, b, c)(ContextPosition(ctx)))
  }

  implicit class RichRule4[+A, +B, +C, +D](r: Rule4[A, B, C, D]) {
    def ~~(other: Rule0): Rule4[A, B, C, D] = r ~ WS ~ other
    def ~~[E](other: Rule1[E]): Rule5[A, B, C, D, E] = r ~ WS ~ other
    def ~~[E, F](other: Rule2[E, F]): Rule6[A, B, C, D, E, F] = r ~ WS ~ other
    def ~~[E, F, G](other: Rule3[E, F, G]): Rule7[A, B, C, D, E, F, G] = r ~ WS ~ other

    def ~~>>[R](f: (A, B, C, D) => InputPosition => R): Rule1[R] =
      r ~~> withContext((a: A, b: B, c: C, d: D, ctx) => f(a, b, c, d)(ContextPosition(ctx)))
  }

  implicit class RichRule5[+A, +B, +C, +D, +E](r: Rule5[A, B, C, D, E]) {
    def ~~(other: Rule0): Rule5[A, B, C, D, E] = r ~ WS ~ other
    def ~~[F](other: Rule1[F]): Rule6[A, B, C, D, E, F] = r ~ WS ~ other
    def ~~[F, G](other: Rule2[F, G]): Rule7[A, B, C, D, E, F, G] = r ~ WS ~ other

    def ~~>>[R](f: (A, B, C, D, E) => (InputPosition => R)): Rule1[R] =
      r ~~> withContext((a: A, b: B, c: C, d: D, e: E, ctx) => f(a, b, c, d, e)(ContextPosition(ctx)))
  }

  implicit class RichRule6[+A, +B, +C, +D, +E, +F](r: Rule6[A, B, C, D, E, F]) {
    def ~~(other: Rule0): Rule6[A, B, C, D, E, F] = r ~ WS ~ other
    def ~~[G](other: Rule1[G]): Rule7[A, B, C, D, E, F, G] = r ~ WS ~ other

    def ~~>>[R](func: (A, B, C, D, E, F) => (InputPosition => R)): Rule1[R] =
      r ~~> withContext((a: A, b: B, c: C, d: D, e: E, f: F, ctx) => func(a, b, c, d, e, f)(ContextPosition(ctx)))
  }

  implicit class RichRule7[+A, +B, +C, +D, +E, +F, +G](r: Rule7[A, B, C, D, E, F, G]) {
    def ~~(other: Rule0): Rule7[A, B, C, D, E, F, G] = r ~ WS ~ other
  }

  implicit class RichReductionRule1[-A, +B](r: ReductionRule1[A, B]) {
    def ~~(other: Rule0): ReductionRule1[A, B] = r ~ WS ~ other
  }

  implicit class RichReductionRule2[-A, -B, +C](r: ReductionRule2[A, B, C]) {
    def ~~(other: Rule0): ReductionRule2[A, B, C] = r ~ WS ~ other
  }
}
