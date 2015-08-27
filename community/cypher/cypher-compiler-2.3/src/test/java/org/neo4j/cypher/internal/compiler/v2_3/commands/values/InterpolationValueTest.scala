package org.neo4j.cypher.internal.compiler.v2_3.commands.values

import org.neo4j.cypher.internal.frontend.v2_3.helpers.NonEmptyList
import org.neo4j.cypher.internal.frontend.v2_3.parser.{MatchMany, MatchSingle, MatchText, ParsedLikePattern}
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class InterpolationValueTest extends CypherFunSuite {

  val literal = LiteralStringPart("x")
  val interpolated = InterpolatedStringPart("y")

  test("should interpolate as texts") {
    implicit val mode = TextInterpolationMode

    InterpolationValue(NonEmptyList(literal)).interpolate should equal("x")
    InterpolationValue(NonEmptyList(interpolated)).interpolate should equal("y")
    InterpolationValue(NonEmptyList(literal, interpolated)).interpolate should equal("xy")
  }

  test("should interpolate as regex") {
    implicit val mode = PatternInterpolationMode

    InterpolationValue(NonEmptyList(literal)).interpolate.toString should equal("x".r.pattern.toString)
    InterpolationValue(NonEmptyList(interpolated)).interpolate.toString should equal("\\Qy\\E".r.pattern.toString)
    InterpolationValue(NonEmptyList(literal, interpolated)).interpolate.toString should equal("x\\Qy\\E".r.pattern.toString)
  }

  test("should interpolate as like patterns") {
    implicit val mode = ParsedLikePatternInterpolationMode

    InterpolationValue(NonEmptyList(literal)).interpolate should equal(ParsedLikePattern(List(MatchText("x"))))
    InterpolationValue(NonEmptyList(interpolated)).interpolate should equal(ParsedLikePattern(List(MatchText("y"))))
    InterpolationValue(NonEmptyList(literal, interpolated)).interpolate should equal(ParsedLikePattern(List(MatchText("x"), MatchText("y"))))
    InterpolationValue(NonEmptyList(LiteralStringPart("foo%"), InterpolatedStringPart("bar_"))).interpolate should equal(
      ParsedLikePattern(List(MatchText("foo"), MatchMany, MatchText("bar_"))))
  }
}
