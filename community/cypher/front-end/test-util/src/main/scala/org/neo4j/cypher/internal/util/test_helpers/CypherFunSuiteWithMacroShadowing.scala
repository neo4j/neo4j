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

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuiteWithMacroShadowing.ATypeMatcher
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuiteWithMacroShadowing.defaultPosition
import org.scalatest.Assertion
import org.scalatest.matchers.BeMatcher
import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher
import org.scalatest.matchers.TypeMatcherHelper
import org.scalatest.matchers.dsl.BeWord
import org.scalatest.matchers.dsl.ResultOfBeWordForAType
import org.scalatest.matchers.dsl.ResultOfThrownByApplication
import org.scalatest.matchers.should.Matchers

import scala.annotation.nowarn
import scala.reflect.ClassTag

/**
 * Shadows the ScalaTest 2.13 methods implemented as macros to allow calling them from Scala 3 modules.
 */
trait CypherFunSuiteWithMacroShadowing extends CypherFunSuite {

  protected def assert(x: Boolean): Unit = super.assert(x)
  protected def assert(x: Boolean, clue: Any): Unit = super.assert(x, clue)

  protected def assume(x: Boolean): Unit = super.assume(x)
  protected def assume(x: Boolean, clue: Any): Unit = super.assume(x, clue)

  @nowarn
  protected def a[A: ClassTag](implicit d: DummyImplicit): ATypeMatcher[A] = ATypeMatcher[A]()

  @nowarn
  protected def an[A: ClassTag](implicit d: DummyImplicit): ATypeMatcher[A] = ATypeMatcher[A]()

  // couldn't find a way to shadow `val matchPattern`
  def matchPatternLike(pf: PartialFunction[Any, ?]): Matcher[Any] = matchPattern.apply(pf)

  implicit final protected def `Disable ScalaTest Position macro`: org.scalactic.source.Position =
    defaultPosition
}

object CypherFunSuiteWithMacroShadowing {
  val defaultPosition: org.scalactic.source.Position = org.scalactic.source.Position("", "", -1)

  case class ATypeMatcher[A: ClassTag]() extends BeMatcher[Any] {

    def should(bw: BeWord): ResultOfBeWordForAType[A] = {
      Matchers.a[A].should(bw)
    }

    def shouldBe(tby: ResultOfThrownByApplication): Assertion = {
      Matchers.a[A].shouldBe(tby)
    }

    override def apply(left: Any): MatchResult = {
      TypeMatcherHelper.aTypeMatcher(Matchers.a[A]).apply(left)
    }
  }
}
