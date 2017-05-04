/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.spi.v3_2

import org.neo4j.cypher.{ConstraintValidationException, CypherExecutionException}
import org.neo4j.graphdb.{ConstraintViolationException => KernelConstraintViolationException}
import org.neo4j.kernel.api.TokenNameLookup
import org.neo4j.kernel.api.exceptions.KernelException

trait ExceptionTranslationSupport {
  inner: TokenContext =>

  protected def translateException[A](f: => A) = try {
    f
  } catch {
    case e: KernelException => throw new CypherExecutionException(e.getUserMessage(new TokenNameLookup {
      def propertyKeyGetName(propertyKeyId: Int): String = inner.getPropertyKeyName(propertyKeyId)

      def labelGetName(labelId: Int): String = inner.getLabelName(labelId)

      def relationshipTypeGetName(relTypeId: Int): String = inner.getRelTypeName(relTypeId)
    }), e)
    case e : KernelConstraintViolationException => throw new ConstraintValidationException(e.getMessage, e)
  }

  protected def translateIterator[A](iteratorFactory: => Iterator[A]): Iterator[A] = {
    val iterator = translateException(iteratorFactory)
    new Iterator[A] {
      override def hasNext: Boolean = translateException(iterator.hasNext)
      override def next(): A = translateException(iterator.next())
    }
  }
}
