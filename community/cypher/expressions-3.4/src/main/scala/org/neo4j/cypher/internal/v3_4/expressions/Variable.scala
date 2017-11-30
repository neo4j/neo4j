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
package org.neo4j.cypher.internal.v3_4.expressions

import org.neo4j.cypher.internal.util.v3_4.InputPosition

class Variable(val name: String)(val position: InputPosition) extends Expression {

  def copyId: Variable = copy()(position)

  def renameId(newName: String): Variable = copy(name = newName)(position)

  def bumpId: Variable = copy()(position.bumped())

  override def asCanonicalStringVal: String = name

  //--------------------------------------------------------------------------------------------------
  // The methods below are what we would get automatically if this was a case class
  //--------------------------------------------------------------------------------------------------
  def copy(name: String = this.name)(position: InputPosition): Variable = new Variable(name)(position)

  override def productElement(n: Int) =
    if (n == 0) name
    else throw new java.lang.IndexOutOfBoundsException(n.toString)

  override def productArity = 1

  override def canEqual(that: Any) = that.isInstanceOf[Variable]

  override def toString: String = s"Variable($name)"

  override def hashCode(): Int = runtime.ScalaRunTime._hashCode(Variable.this)

  override def equals(obj: scala.Any): Boolean = runtime.ScalaRunTime._equals(Variable.this, obj)
}

object Variable {
  implicit val byName: Ordering[Variable] =
    Ordering.by { (variable: Variable) =>
      (variable.name, variable.position)
    }(Ordering.Tuple2(implicitly[Ordering[String]], InputPosition.byOffset))

  def apply(name: String)(position: InputPosition) = new Variable(name)(position)

  def unapply(arg: Variable): Option[String] = Some(arg.name)
}
