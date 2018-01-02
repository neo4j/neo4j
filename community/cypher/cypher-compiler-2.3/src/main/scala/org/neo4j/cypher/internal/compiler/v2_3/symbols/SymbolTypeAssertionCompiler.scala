/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.symbols

import org.neo4j.cypher.internal.compiler.v2_3.ExecutionContext
import org.neo4j.cypher.internal.frontend.v2_3.CypherTypeException
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.graphdb.{Node, Relationship}

import scala.annotation.tailrec
import scala.reflect.ClassTag

object SymbolTypeAssertionCompiler {
  type Cont = ExecutionContext => ExecutionContext

  def compile(requirements: Seq[(String, CypherType)]): Cont =
    compile(requirements, identity)

  @tailrec
  private def compile(remaining: Seq[(String, CypherType)], cont: Cont): Cont = remaining match {
    case Seq((name, CTNode), tl@_*) =>
      compile(tl, verifyType[Node](name, cont))
    case Seq((name, CTRelationship), tl@_*) =>
      compile(tl, verifyType[Relationship](name, cont))
    case Seq((name, _), tl@_*) =>
      compile(tl, cont)
    case _ =>
      cont
  }

  private def verifyType[T : ClassTag](name: String, cont: Cont): Cont = {
    val clazz: Class[_] = implicitly[ClassTag[T]].runtimeClass
    (context: ExecutionContext) => {
      val value = context(name)
      if (value != null && !clazz.isInstance(value)) {
        throw new CypherTypeException(s"Expected $name to be a ${clazz.getSimpleName}, but was: $value")
      }
      cont(context)
    }
  }
}
