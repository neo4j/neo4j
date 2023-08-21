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
package org.neo4j.cypher.internal.macros

import org.neo4j.cypher.internal.util.AssertionRunner.ASSERTIONS_ENABLED

import scala.language.experimental.macros
import scala.reflect.macros.blackbox

/**
 * Utility more or less equivalent to using Java keyword assert.
 *
 * As with `assert`, `require` should not be used for checking input on public methods or similar, only to be used
 * for checking internal invariants. We should always assume that these checks are not running in production code.
 */
object AssertMacros {

  /**
   * Require that the given condition is `true`
   * @param condition the condition that is required to be true
   */
  def checkOnlyWhenAssertionsAreEnabled(condition: Boolean): Unit = macro checkOnlyWhenAssertionsAreEnabledImpl

  /**
   * Require that the given condition is `true`
   * @param condition the condition that is required to be true
   * @param msg the error message shown if requirement fails
   */
  def checkOnlyWhenAssertionsAreEnabled(condition: Boolean, msg: String): Unit =
    macro checkOnlyWhenAssertionsAreEnabledWithMsgImpl

  def checkOnlyWhenAssertionsAreEnabledImpl(c: blackbox.Context)(condition: c.Expr[Boolean]): c.universe.Tree = {
    import c.universe.Quasiquote
    // this is just a precaution to make clear that we are using this constant here
    assert(ASSERTIONS_ENABLED || !ASSERTIONS_ENABLED)
    q"""
        if(org.neo4j.cypher.internal.util.AssertionRunner.ASSERTIONS_ENABLED && !$condition) {
          throw new AssertionError("assertion failed")
        }
      """
  }

  def checkOnlyWhenAssertionsAreEnabledWithMsgImpl(c: blackbox.Context)(
    condition: c.Expr[Boolean],
    msg: c.Expr[String]
  ): c.universe.Tree = {
    import c.universe.Quasiquote
    // this is just a precaution to make clear that we are using this constant here
    assert(ASSERTIONS_ENABLED || !ASSERTIONS_ENABLED)
    q"""
        if (org.neo4j.cypher.internal.util.AssertionRunner.ASSERTIONS_ENABLED && !$condition) {
          throw new AssertionError($msg)
        }
      """
  }
}
