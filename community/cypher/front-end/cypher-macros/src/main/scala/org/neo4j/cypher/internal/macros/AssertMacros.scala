/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
  def checkOnlyWhenAssertionsAreEnabled(condition: Boolean, msg: String): Unit = macro checkOnlyWhenAssertionsAreEnabledWithMsgImpl

  def checkOnlyWhenAssertionsAreEnabledImpl(c: blackbox.Context)(condition: c.Expr[Boolean]): c.universe.Tree = {
    import c.universe.Quasiquote
    //this is just a precaution to make clear that we are using this constant here
    assert(ASSERTIONS_ENABLED || !ASSERTIONS_ENABLED)
    q"""
        if(org.neo4j.cypher.internal.util.AssertionRunner.ASSERTIONS_ENABLED && !$condition) {
          throw new AssertionError("assertion failed")
        }
      """
  }

  def checkOnlyWhenAssertionsAreEnabledWithMsgImpl(c: blackbox.Context)(condition: c.Expr[Boolean], msg: c.Expr[String]): c.universe.Tree = {
    import c.universe.Quasiquote
    //this is just a precaution to make clear that we are using this constant here
    assert(ASSERTIONS_ENABLED || !ASSERTIONS_ENABLED)
    q"""
        if (org.neo4j.cypher.internal.util.AssertionRunner.ASSERTIONS_ENABLED && !$condition) {
          throw new AssertionError($msg)
        }
      """
  }
}
