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

  def translateExceptionImpl[A: c.WeakTypeTag](c: blackbox.Context)(tokenNameLookup: c.Tree, f: c.Tree): c.universe.Tree = {
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

  def translateIterator[A](tokenNameLookup: AnyRef, iteratorFactory: => Iterator[A]): Iterator[A] = macro translateIteratorImp[A]

  def translateIteratorImp[A](c: blackbox.Context)(tokenNameLookup: c.Tree, iteratorFactory: c.Tree)(implicit tag: c.WeakTypeTag[A]): c.universe.Tree = {
    import c.universe.Quasiquote
    import c.universe.TypeName
    import c.universe.Ident
    import c.universe.Type
    import c.universe.AppliedTypeTree

    def toTypeTree(typ: Type): c.universe.Tree = {
      val base = Ident(TypeName(typ.typeSymbol.name.toString))
      val args = typ.typeArgs.map(t => toTypeTree(t))

      if (args.isEmpty)
        base
      else
        AppliedTypeTree(base, args)
    }
    val innerTypeTree = toTypeTree(tag.tpe)

    val translatedIterator = translateExceptionImpl(c)(tokenNameLookup, iteratorFactory)
    val translatedNext = translateExceptionImpl(c)(tokenNameLookup, q"iterator.next()")
    val translatedHasNext = translateExceptionImpl(c)(tokenNameLookup, q"iterator.hasNext")

    q"""
        val iterator = $translatedIterator
        new Iterator[$innerTypeTree] {
          override def hasNext: Boolean = $translatedHasNext
          override def next(): $innerTypeTree = $translatedNext
        }
      """
  }
}
