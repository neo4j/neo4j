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
package org.neo4j.cypher.internal.util

import org.neo4j.cypher.internal.util.Rewritable.IteratorEq

trait ASTNode extends Product with Foldable with Rewritable {

  self =>

  def position: InputPosition

  def dup(children: Seq[AnyRef]): this.type =
    if (children.iterator eqElements this.treeChildren)
      this
    else {
      val args = children
      val hasExtraParam = Rewritable.numParameters(this) == children.length + 1
      val lastParamIsPos = Rewritable.includesPosition(this)
      val ctorArgs = if (hasExtraParam && lastParamIsPos) args :+ this.position else args
      val duped = Rewritable.copyProduct(this, ctorArgs.toArray)
      duped.asInstanceOf[self.type]
    }

  def asCanonicalStringVal: String = toString
}
