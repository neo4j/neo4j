package cypher.feature.parser

import cypher.feature.parser.matchers.ValueMatcher
import org.scalatest.matchers.{MatchResult, Matcher}

case class accept(value: Any) extends Matcher[ValueMatcher] {

  override def apply(matcher: ValueMatcher): MatchResult = {
    MatchResult(matches = matcher.matches(value),
                s"$matcher did not match $value",
                s"$matcher unexpectedly matched $value")
  }
}
