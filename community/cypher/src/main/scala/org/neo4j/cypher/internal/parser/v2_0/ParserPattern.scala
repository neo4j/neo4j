/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.parser.v2_0

import org.neo4j.graphdb.Direction
import org.neo4j.helpers.ThisShouldNotHappenError
import org.neo4j.cypher.internal.commands.expressions.{Literal, Expression, Identifier}
import org.neo4j.cypher.internal.commands.values.LabelName

trait ParserPattern extends Base with Labels {

  def usePattern[T](translator: AbstractPattern => Maybe[T], acceptable: Seq[T] => Boolean): Parser[Seq[T]] = Parser {
    case in =>
      usePattern(translator)(in) match {
        case Success(patterns, rest) =>
          if (acceptable(patterns))
            Success(patterns, rest)
          else
            Failure("", rest)
        case Failure(msg, rest) => Failure(msg, rest)
        case Error(msg, rest) => Error(msg, rest)
      }
  }

  def usePattern[T](translator: AbstractPattern => Maybe[T]): Parser[Seq[T]] = Parser {
    case in => translate(in, translator, pattern(in))
  }

  def usePath[T](translator: AbstractPattern => Maybe[T]): Parser[Seq[T]] = Parser {
    case in => translate(in, translator, path(in))
  }

  private def translate[T](in: Input, translator: (AbstractPattern) => Maybe[T], pattern1: ParseResult[Seq[AbstractPattern]]): ParseResult[Seq[T]] with Product with Serializable = {
    pattern1 match {
      case Success(abstractPattern, rest) =>
        val concretePattern = abstractPattern.map(p => translator(p))

        concretePattern.find(!_.success) match {
          case Some(No(msg)) => Failure(msg.mkString("\n"), rest.rest)
          case None          => Success(concretePattern.flatMap(_.values), rest)
          case _             => throw new ThisShouldNotHappenError("Andres", "This is here to stop compiler warnings.")
        }

      case Failure(msg, rest) => Failure(msg, rest)
      case Error(msg, rest) => Error(msg, rest)
    }
  }

  private def pattern: Parser[Seq[AbstractPattern]] = commaList(patternBit) ^^ (patterns => patterns.flatten)

  private def patternBit: Parser[Seq[AbstractPattern]] =
    pathAssignment |
      pathFacingOut |
      singleNode

  private def singleNode: Parser[Seq[ParsedEntity]] = {
    node ^^ (n => Seq(n))
  }

  private def pathAssignment: Parser[Seq[AbstractPattern]] = optParens(identity) ~ "=" ~ optParens(path) ^^ {
    case pathName ~ "=" ~ Seq(p: ParsedShortestPath) => Seq(p.rename(pathName))
    case pathName ~ "=" ~ patterns => Seq(ParsedNamedPath(pathName, patterns))
  }

  def node: Parser[ParsedEntity] =
      parens(nodeFromExpression) | // whatever expression, but inside parenthesis
      singleNodeDefinition | // x optLabelSeq optValues
      nodeInParenthesis | // (singleNodeDefinition)
      failure("expected an expression that is a node")


  private def values = (("="|VALUES) ~> curlyMapWithExpression) | curlyMap

  private def labelsAndValues: Parser[(LabelSpec, Map[String, Expression], Boolean)] = optLabelChoiceForm ~ opt(values) ^^ {
    case labelSpec ~ optMap =>
      val mapVal = optMap.getOrElse(Map.empty)
      val bare = labelSpec.bare && optMap.isEmpty
      (labelSpec, mapVal, bare)
  }

  private def singleNodeDefinition = identity ~ labelsAndValues ^^ {
    case name ~ labelsAndValues =>
      val (labelsVal, mapVal, bare) = labelsAndValues
      ParsedEntity(name, Identifier(name), mapVal, labelsVal, bare)
  }

  private def nodeInParenthesis = parens(optionalName ~ labelsAndValues) ^^ {
    case name ~ labelsAndValues =>
      val (labelsVal, mapVal, bare) = labelsAndValues
      ParsedEntity(name, Identifier(name), mapVal, labelsVal, bare)
  }

  private def nodeFromExpression = Parser {
    in =>
      (generatedName ~ expression).apply(in) match {
        case Success(_ ~ Identifier(name), rest) =>
          Success(ParsedEntity(name, Identifier(name), Map[String, Expression](), LabelSet.empty, true), rest)

        case Success(name ~ Literal(n: LabelName), rest) =>
          Success(ParsedEntity(name, Identifier(name), Map[String, Expression](), LabelSet(Seq(n)), true), rest)

        case Success(name ~ exp, rest) =>
          Success(ParsedEntity(name, exp, Map[String, Expression](), LabelSet.empty, true), rest)

        case x: Error           => x
        case Failure(msg, rest) => failure("expected an expression that is a node", rest)
      }
  }

  private def path: Parser[List[AbstractPattern]] = relationship | shortestPath
  private def pathFacingOut: Parser[List[AbstractPattern]] = relationshipFacingOut | shortestPath

  private def relationshipFacingOut = relationship ^^ (x => x.map(_.makeOutgoing))

  private def relationship: Parser[List[AbstractPattern]] = {
    node ~ rep1(tail) ^^ {
      case head ~ tails =>
        var start = head
        val links = tails.map {
          case Tail(_, dir, relName, relProps, end, None, types, optional) =>
            val t = ParsedRelation(relName, relProps, start, end, types, dir, optional)
            start = end
            t
          case Tail(pathName, dir, relName, relProps, end, Some((min, max)), types, optional) =>
            val relIterator = Some(relName).filter(Identifier.isNamed)
            val t = ParsedVarLengthRelation(pathName, relProps, start, end, types, dir, optional, min, max, relIterator)
            start = end
            t
        }

        List(links: _*)
    }
  }

  private def patternForShortestPath: Parser[AbstractPattern] = onlyOne("expected single path segment", relationship)

  private def shortestPath: Parser[List[AbstractPattern]] = generatedName ~ (SHORTESTPATH | ALLSHORTESTPATHS) ~ parens(patternForShortestPath) ^^ {
    case name ~ algo ~ relInfo =>
      val single = algo.startsWith("s")

      val PatternWithEnds(start, end, typez, dir, optional, maxDepth, relIterator) = relInfo

      List(ParsedShortestPath(name = name,
        props = Map(),
        start = start,
        end = end,
        typ = typez,
        dir = dir,
        optional = optional,
        maxDepth = maxDepth,
        single = single,
        relIterator = relIterator))
  }


  private def tailWithRelData: Parser[Tail] = generatedName ~ opt("<") ~ "-" ~ "[" ~ optionalName ~ opt("?") ~ opt(":" ~> rep1sep(escapableString, "|")) ~ variable_length ~ props ~ "]" ~ "-" ~ opt(">") ~ node ^^ {
    case pathName ~ l ~ "-" ~ "[" ~ rel ~ optional ~ typez ~ varLength ~ properties ~ "]" ~ "-" ~ r ~ end => Tail(pathName, direction(l, r), rel, properties, end, varLength, typez.toSeq.flatten.distinct, optional.isDefined)
  } | linkErrorMessages

  private def linkErrorMessages: Parser[Tail] =
    opt("<") ~> "-" ~> "[" ~> opt(identity) ~> opt("?") ~> opt(":" ~> rep1sep(escapableString, "|")) ~> variable_length ~> props ~> "]" ~> failure("expected -") |
      opt("<") ~> "-" ~> "[" ~> opt(identity) ~> opt("?") ~> opt(":" ~> rep1sep(escapableString, "|")) ~> variable_length ~> props ~> failure("unclosed bracket") |
      opt("<") ~> "-" ~> "[" ~> failure("expected relationship information") |
      opt("<") ~> "-" ~> failure("expected [ or -")

  private def variable_length = opt("*" ~ opt(wholeNumber) ~ opt("..") ~ opt(wholeNumber)) ^^ {
    case None => None
    case Some("*" ~ None ~ None ~ None) => Some(None, None)
    case Some("*" ~ min ~ None ~ None) => Some((min.map(_.toInt), min.map(_.toInt)))
    case Some("*" ~ min ~ _ ~ max) => Some((min.map(_.toInt), max.map(_.toInt)))
    case _ => throw new ThisShouldNotHappenError("Stefan/Andres", "This non-exhaustive match would have been a RuntimeException in the past")
  }

  private def tailWithNoRelData = generatedName ~ opt("<") ~ "-" ~ generatedName ~ "-" ~ opt(">") ~ node ^^ {
    case pathName ~ l ~ "-" ~ name ~ "-" ~ r ~ end => Tail(pathName, direction(l, r), name, Map(), end, None, Seq(), optional = false)
  }

  private def tail: Parser[Tail] = tailWithRelData | tailWithNoRelData

  private def props = opt(curlyMap) ^^ {
    case None => Map[String, Expression]()
    case Some(x) => x
  }

  private def expressionAsMap(x:Parser[Expression]):Parser[Map[String, Expression]] =
    x ^^ (x => Map[String, Expression]("*" -> x))

  private def curlyMap =
    expressionAsMap(parameter) | literalMap

  private def curlyMapWithExpression =
    expressionAsMap(expression) | literalMap

  private def literalMap = "{" ~> repsep(propertyAssignment, ",") <~ "}" ^^ (_.toMap)

  private def direction(l: Option[String], r: Option[String]): Direction = (l, r) match {
    case (None, Some(_)) => Direction.OUTGOING
    case (Some(_), None) => Direction.INCOMING
    case _ => Direction.BOTH
  }

  private def propertyAssignment: Parser[(String, Expression)] = identity ~ ":" ~ expression ^^ {
    case id ~ ":" ~ exp => (id, exp)
  }

  def expression: Parser[Expression]

  private case class Tail(pathName:String,
                          dir: Direction,
                          relName: String,
                          relProps: Map[String, Expression],
                          end: ParsedEntity,
                          varLength: Option[(Option[Int], Option[Int])],
                          types: Seq[String],
                          optional: Boolean)

  abstract sealed class Maybe[+T] {
    def values: Seq[T]
    def success: Boolean
    def ++[B >: T](other: Maybe[B]): Maybe[B]
    def map[B](f: T => B): Maybe[B]
    def seqMap[B](f:Seq[T]=>Seq[B]): Maybe[B]
  }

  case class Yes[T](values: Seq[T]) extends Maybe[T] {
    def success = true

    def ++[B >: T](other: Maybe[B]): Maybe[B] = other match {
      case Yes(otherStuff) => Yes(values ++ otherStuff)
      case No(msg) => No(msg)
    }

    def map[B](f: T => B): Maybe[B] = Yes(values.map(f))

    def seqMap[B](f: (Seq[T]) => Seq[B]): Maybe[B] = Yes(f(values))
  }

  case class No(messages: Seq[String]) extends Maybe[Nothing] {
    def values = throw new Exception("No values exists")
    def success = false

    def ++[B >: Nothing](other: Maybe[B]): Maybe[B] = other match {
      case Yes(_) => this
      case No(otherMessages) => No(messages ++ otherMessages)
    }

    def map[B](f: Nothing => B): Maybe[B] = this

    def seqMap[B](f: (Seq[Nothing]) => Seq[B]): Maybe[B] = this
  }
}
