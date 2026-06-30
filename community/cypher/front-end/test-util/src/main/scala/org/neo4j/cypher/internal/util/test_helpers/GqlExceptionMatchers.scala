/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.cypher.internal.util.test_helpers

import org.neo4j.gqlstatus.ErrorGqlStatusObject
import org.neo4j.gqlstatus.GqlStatusInfoCodes
import org.scalatest.matchers.BeMatcher
import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher
import org.scalatest.matchers.should.Matchers.a
import org.scalatest.matchers.should.Matchers.be

import java.util.regex.Pattern

import scala.annotation.nowarn
import scala.jdk.OptionConverters.RichOptional

trait GqlExceptionMatchers {

  case class GqlExceptionMatcher(
    code: GqlStatusInfoCodes,
    statusDescription: String,
    offset: Option[Int] = None,
    line: Option[Int] = None,
    column: Option[Int] = None,
    causeMatcher: Option[GqlExceptionMatcher] = None,
    fuzzyMatch: Boolean = false,
    regexMatch: Boolean = false
  ) extends BeMatcher[ErrorGqlStatusObject] {
    override def toString(): String = s"GqlExceptionMatcher for $code"

    override def apply(left: ErrorGqlStatusObject): MatchResult = {
      validationFailures(left)
        .getOrElse(validGqlStatusObject())
    }

    private def validationFailures(left: ErrorGqlStatusObject): Option[MatchResult] = {
      invalidCode(left)
        .orElse(invalidStatusDescription(left))
        .orElse(causeExistenceCheck(left))
        .orElse(positionCheck(left))
    }

    private def invalidCode(left: ErrorGqlStatusObject): Option[MatchResult] = {
      if (left.gqlStatus() != code.getStatusString) {
        Some(MatchResult(
          matches = false,
          s"Expected GQL code ${code.getStatusString} but found ${left.gqlStatus()}",
          "Unreachable"
        ))
      } else {
        None
      }
    }

    private def invalidStatusDescription(left: ErrorGqlStatusObject): Option[MatchResult] = {
      if (!fuzzyMatch && !regexMatch && left.statusDescription() != statusDescription) {
        Some(MatchResult(
          matches = false,
          s"""The status description for ${left.gqlStatus()}:
             |${left.statusDescription()}
             |was not equal to the expected:
             |$statusDescription
             |""".stripMargin,
          "Unreachable"
        ))
      } else if (fuzzyMatch && !left.statusDescription().contains(statusDescription)) {
        Some(MatchResult(
          matches = false,
          s"""The status description for ${left.gqlStatus()}:
             |${left.statusDescription()}
             |did not contain the expected:
             |$statusDescription
             |""".stripMargin,
          "Unreachable"
        ))
      } else if (regexMatch && !left.statusDescription().matches(statusDescription)) {
        Some(MatchResult(
          matches = false,
          s"""The status description for ${left.gqlStatus()}:
             |${left.statusDescription()}
             |did not match the expected:
             |$statusDescription
             |""".stripMargin,
          "Unreachable"
        ))
      } else {
        None
      }
    }

    private def causeExistenceCheck(left: ErrorGqlStatusObject): Option[MatchResult] = {
      causeMatcher
        .map(validateCause(left))
        .getOrElse(validateNoCause(left))
    }

    private def validateNoCause(left: ErrorGqlStatusObject): Option[MatchResult] =
      left.cause().toScala.map(_ => {
        MatchResult(
          matches = false,
          s"Expected ${left.gqlStatus()} to not have a cause but found ${left.cause().get().gqlStatus()}",
          "Unreachable"
        )
      })

    private def validateCause(left: ErrorGqlStatusObject)(causeMatcher: GqlExceptionMatcher): Option[MatchResult] = {
      def addContext(m: MatchResult): MatchResult = {
        def withContext(msg: String) =
          s"""the cause of ${left.gqlStatus()} had validation failures:
             |$msg""".stripMargin
        m.copy(
          rawFailureMessage = withContext(m.rawFailureMessage),
          rawMidSentenceFailureMessage = withContext(m.rawMidSentenceFailureMessage)
        )
      }
      left.cause().toScala.map(cause => {
        // Cause exists, run cause matcher against it
        causeMatcher.validationFailures(cause).map(addContext)
      })
        .getOrElse(
          // Cause doesn't exist, return failure
          Some(MatchResult(
            matches = false,
            s"Expected ${left.gqlStatus()} to have a cause",
            "Unreachable"
          ))
        )
    }

    @nowarn("msg=eliminated by erasure")
    private def positionCheck(left: ErrorGqlStatusObject): Option[MatchResult] = {
      if (offset.nonEmpty || line.nonEmpty || column.nonEmpty) {
        left.diagnosticRecord().get("_position") match {
          case position: java.util.Map[String, Int] @unchecked =>
            offset.flatMap(validateOffset(position)).orElse(
              line.flatMap(validateLine(position)).orElse(
                column.flatMap(validateColumn(position))
              )
            )
          case null => Some(MatchResult(
              matches = false,
              s"Expected diagnosticRecord().get(\"_position\") to produce a java.util.Map[String, Int] but was null",
              "Unreachable"
            ))
          case position => Some(MatchResult(
              matches = false,
              s"Expected diagnosticRecord().get(\"_position\") to produce a java.util.Map[String, Int] but was ${position.getClass}",
              "Unreachable"
            ))
        }
      } else {
        None
      }
    }

    private def validateOffset(position: java.util.Map[String, Int])(offset: Int): Option[MatchResult] = {
      if (position.get("offset") != offset) {
        Some(MatchResult(
          matches = false,
          s"Expected offset $offset but found ${position.get("offset")}",
          "Unreachable"
        ))
      } else {
        None
      }
    }

    private def validateLine(position: java.util.Map[String, Int])(line: Int): Option[MatchResult] = {
      if (position.get("line") != line) {
        Some(MatchResult(
          matches = false,
          s"Expected line $line but found ${position.get("line")}",
          "Unreachable"
        ))
      } else {
        None
      }
    }

    private def validateColumn(position: java.util.Map[String, Int])(column: Int): Option[MatchResult] = {
      if (position.get("column") != column) {
        Some(MatchResult(
          matches = false,
          s"Expected column $column but found ${position.get("column")}",
          "Unreachable"
        ))
      } else {
        None
      }
    }

    private def validGqlStatusObject(): MatchResult = {
      MatchResult(
        matches = true,
        "Unreachable",
        "expected the GQL-status object to not be the provided gqlStatus, but all fields were the same"
      )
    }

    def withCause(causeMatcher: GqlExceptionMatcher): GqlExceptionMatcher = {
      /*
       Assume that cause matcher is not already set.
       This occurs when given the following pattern:
         exception should be(foo
                            .withCause(bar)
                            .withCause(baz)
                            )
       Test writer probably intended to do this:
         exception should be(foo
                              .withCause(bar
                                .withCause(baz)
                              )
                            )
       */
      assume(this.causeMatcher.isEmpty, "TEST SETUP ERROR: This would override an existing cause matcher.")
      this.copy(causeMatcher = Some(causeMatcher))
    }

    def withCause(
      code: GqlStatusInfoCodes,
      statusDescription: String,
      fuzzyStatusDescr: Boolean = false
    ): GqlExceptionMatcher = {
      withCause(GqlExceptionMatcher(code, statusDescription, fuzzyMatch = fuzzyStatusDescr))
    }

    def withPosition(offset: Int, line: Int, column: Int): GqlExceptionMatcher = {
      this.copy(offset = Some(offset), line = Some(line), column = Some(column))
    }

  }

  def gqlStatus(
    code: GqlStatusInfoCodes,
    statusDescription: String,
    fuzzyStatusDescr: Boolean = false,
    regexDescr: Boolean = false
  ): GqlExceptionMatcher = {
    GqlExceptionMatcher(code, statusDescription, fuzzyMatch = fuzzyStatusDescr, regexMatch = regexDescr)
  }

  def gqlStatus(
    code: GqlStatusInfoCodes,
    statusDescription: Seq[String]
  ): GqlExceptionMatcher = {
    GqlExceptionMatcher(code, statusDescription.map(Pattern.quote).mkString(".*", ".*", ".*"), regexMatch = true)
  }

  def gqlException(
    legacyMsg: String,
    gqlMatcher: GqlExceptionMatcher,
    fuzzyMsg: Boolean = false
  ): BeMatcher[Exception] =
    gqlExceptionHelper(Seq(legacyMsg), gqlMatcher, fuzzyMsg)

  def gqlException(
    legacyMsgParts: Seq[String],
    gqlMatcher: GqlExceptionMatcher
  ): BeMatcher[Exception] =
    gqlExceptionHelper(legacyMsgParts, gqlMatcher, fuzzyMsg = true)

  private def gqlExceptionHelper(
    legacyMsgParts: Seq[String],
    gqlMatcher: GqlExceptionMatcher,
    fuzzyMsg: Boolean
  ): BeMatcher[Exception] =
    BeMatcher {
      (ex: Exception) =>
        {
          val typeMatcher: Matcher[Exception] = be(a[ErrorGqlStatusObject])
          val messageMatcher: Matcher[Exception] = {
            if (legacyMsgParts.isEmpty) {
              Matcher {
                _ => MatchResult(matches = true, "Unreachable", "Did not provide any legacy message to check")
              }
            } else if (fuzzyMsg) {
              Matcher { ex =>
                MatchResult(
                  legacyMsgParts.forall(p => ex.getMessage.replace("\r\n", "\n").contains(p.replace("\r\n", "\n"))),
                  s"Message '${ex.getMessage}' did not contain all of '${legacyMsgParts.mkString("[\"", "\", \"", "\"]")}'",
                  s"Message contains all of '${legacyMsgParts.mkString("[\"", "\", \"", "\"]")}'"
                )
              }
            } else {
              Matcher { ex =>
                MatchResult(
                  // It is safe to do legacyMsgParts.head here,
                  // because if the list does not have exactly one element we would end up in one of the cases above.
                  ex.getMessage.replace("\r\n", "\n").equals(legacyMsgParts.head.replace("\r\n", "\n")),
                  s"Message '${ex.getMessage}' did not equal '${legacyMsgParts.head}'",
                  s"Message equals '${legacyMsgParts.head}'"
                )
              }
            }
          }
          val exceptionMatchResult = (typeMatcher and messageMatcher)(ex)

          if (!exceptionMatchResult.matches) {
            exceptionMatchResult
          } else {
            val gqlMatchResult: MatchResult = gqlMatcher(ex.asInstanceOf[ErrorGqlStatusObject])
            MatchResult(
              gqlMatchResult.matches,
              gqlMatchResult.rawFailureMessage,
              "expected exception to not be the provided gqlException, but all fields were the same"
            )
          }
        }
    }
}

object GqlExceptionMatchers extends GqlExceptionMatchers {

  val InvalidReferenceStatus: GqlExceptionMatcher = GqlExceptionMatcher(
    GqlStatusInfoCodes.STATUS_42001,
    "error: syntax error or access rule violation - invalid syntax"
  )

  val InvalidSyntaxStatus: GqlExceptionMatcher = GqlExceptionMatcher(
    GqlStatusInfoCodes.STATUS_42001,
    "error: syntax error or access rule violation - invalid syntax"
  )

  val Reparsesable_42I67: GqlExceptionMatcher = gqlStatus(
    GqlStatusInfoCodes.STATUS_42I67,
    "error: syntax error or access rule violation - unsupported language feature. The query is parsable in `CYPHER 25`, but it is run in `CYPHER 5`. Consider changing the database default Cypher version using `ALTER DATABASE SET DEFAULT LANGUAGE` or prefix the query with `CYPHER 25`."
  )

  def functionArgumentGqlException(legacyMsg: String, func: String, msgPart: String): BeMatcher[Exception] = {
    gqlException(
      legacyMsg,
      gqlStatus(
        GqlStatusInfoCodes.STATUS_22N38,
        s"error: data exception - invalid function argument. Invalid argument to the function $func."
      ).withCause(
        GqlStatusInfoCodes.STATUS_22N01,
        s"error: data exception - invalid type. $msgPart"
      )
    )
  }

  def functionInvalidArgumentGqlException(legacyMsg: String, func: String, msgPart: String): BeMatcher[Exception] = {
    gqlException(
      legacyMsg,
      gqlStatus(
        GqlStatusInfoCodes.STATUS_22N38,
        s"error: data exception - invalid function argument. Invalid argument to the function $func."
      ).withCause(
        GqlStatusInfoCodes.STATUS_22N03,
        s"error: data exception - specified numeric value out of range. $msgPart"
      )
    )
  }

  def typeExceptionGqlException(legacyMsg: String, msgPart: String, param: String): BeMatcher[Exception] = {
    gqlException(
      legacyMsg,
      gqlStatus(
        GqlStatusInfoCodes.STATUS_42N51,
        s"error: syntax error or access rule violation - invalid parameter. Invalid parameter $$`$param`."
      ).withCause(
        gqlStatus(
          GqlStatusInfoCodes.STATUS_22G03,
          "error: data exception - invalid value type"
        ).withCause(
          GqlStatusInfoCodes.STATUS_22N27,
          s"error: data exception - invalid entity type. $msgPart"
        )
      )
    )
  }

  def invalidTypeException(
    legacyMsg: String,
    valueString: String,
    expectedType: String,
    gotType: String
  ): BeMatcher[Exception] = {
    gqlException(
      legacyMsg,
      gqlStatus(
        GqlStatusInfoCodes.STATUS_22G03,
        "error: data exception - invalid value type"
      ).withCause(
        GqlStatusInfoCodes.STATUS_22N01,
        s"error: data exception - invalid type. Expected the value $valueString to be of type $expectedType, but was of type $gotType."
      )
    )
  }

  def cantCoerceException(legacyMsg: String, value: String, expectedType: String): BeMatcher[Exception] =
    gqlException(
      legacyMsg,
      gqlStatus(GqlStatusInfoCodes.STATUS_22G03, "error: data exception - invalid value type")
        .withCause(
          GqlStatusInfoCodes.STATUS_22N37,
          s"error: data exception - invalid coercion. Cannot coerce $value to $expectedType."
        )
    )
}
