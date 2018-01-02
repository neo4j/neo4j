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
package org.neo4j.cypher.internal.compiler.v2_3

package object helpers {
  def closing[Resource <: AutoCloseable, Result](resource: Resource)(block: => Result)(
    implicit onSuccess: Resource => Unit = (r: Resource) => {},
             onError: (Resource, Throwable) => Unit = (r: Resource, t: Throwable) => {}): Result = {
    using(resource){ _ => block }
  }

  def using[Resource <: AutoCloseable, Result](resource: Resource)(block: Resource => Result)(
    implicit onSuccess: Resource => Unit = (r: Resource) => {},
             onError: (Resource, Throwable) => Unit = (r: Resource, t: Throwable) => {}): Result = {
    var failure = false
    try {
      block(resource)
    } catch {
      case f: Throwable =>
        failure = true
        try {
          try {
            onError(resource, f)
          } finally {
            resource.close()
          }
        } catch {
          case sub: Throwable =>
            f.addSuppressed(sub)
        }
        throw f
    } finally {
      if(!failure) {
        try {
          onSuccess(resource)
        } finally {
          resource.close()
        }
      }
    }
  }
}
