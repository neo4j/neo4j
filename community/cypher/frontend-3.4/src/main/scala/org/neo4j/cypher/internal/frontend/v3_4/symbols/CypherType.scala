/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.frontend.v3_4.symbols

abstract class CypherType {
  def parentType: CypherType
  val isAbstract: Boolean = false

  def coercibleTo: Set[CypherType] = Set.empty

  def parents: Seq[CypherType] = parents(Vector.empty)
  private def parents(accumulator: Seq[CypherType]): Seq[CypherType] =
    if (this.parentType == this)
      accumulator
    else
      this.parentType.parents(accumulator :+ this.parentType)

  /*
  Determines if the class or interface represented by this
  {@code CypherType} object is either the same as, or is a
  supertype of, the class or interface represented by the
  specified {@code CypherType} parameter.
   */
  def isAssignableFrom(other: CypherType): Boolean =
    if (other == this)
      true
    else if (other.parentType == other)
      false
    else
      isAssignableFrom(other.parentType)

  def legacyIteratedType: CypherType = this

  def leastUpperBound(other: CypherType): CypherType =
    if (this.isAssignableFrom(other)) this
    else if (other.isAssignableFrom(this)) other
    else parentType leastUpperBound other.parentType

  def greatestLowerBound(other: CypherType): Option[CypherType] =
    if (this.isAssignableFrom(other)) Some(other)
    else if (other.isAssignableFrom(this)) Some(this)
    else None

  lazy val covariant: TypeSpec = TypeSpec.all constrain this
  lazy val invariant: TypeSpec = TypeSpec.exact(this)
  lazy val contravariant: TypeSpec = TypeSpec.all leastUpperBounds this

  def rewrite(f: CypherType => CypherType) = f(this)

  def toNeoTypeString: String
}
