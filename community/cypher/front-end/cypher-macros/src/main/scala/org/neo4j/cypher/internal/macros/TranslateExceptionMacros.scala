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

import scala.language.experimental.macros
import scala.reflect.macros.blackbox

object TranslateExceptionMacros {

  /**
   * Wraps an expression in a try-catch block. The catch block rethrows kernel exceptions as cypher exceptions, plus some other mapping rules.
   *
   * @param f the function to wrap in try catch
   * @param tokenNameLookup this should be of type `TokenNameLookup`, which will also be checked by the compiler during macro expansion.
   *                        We avoid declaring this here to avoid circular dependencies.
   */
  def translateException[A](tokenNameLookup: AnyRef, f: A): A = macro translateExceptionImpl[A]

  def translateExceptionImpl[A: c.WeakTypeTag](c: blackbox.Context)(
    tokenNameLookup: c.Tree,
    f: c.Tree
  ): c.universe.Tree = {
    import c.universe.Quasiquote
    q"""
        try {
          $f
        } catch {
          case e: org.neo4j.exceptions.KernelException =>
            throw new org.neo4j.exceptions.CypherExecutionException(e.getUserMessage($tokenNameLookup), e)

          case e: org.neo4j.graphdb.ConstraintViolationException =>
            throw new org.neo4j.exceptions.ConstraintViolationException(e.getMessage, e)

          case e: org.neo4j.kernel.api.exceptions.ResourceCloseFailureException =>
            throw new org.neo4j.exceptions.CypherExecutionException(e.getMessage, e)

          case e: java.lang.ArithmeticException =>
            throw new org.neo4j.exceptions.ArithmeticException(e.getMessage, e)
        }
      """
  }
}
