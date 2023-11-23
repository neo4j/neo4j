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

import java.lang.reflect.Method

import scala.collection.mutable

object RewritableJava {

  private val productCopyConstructors = new ThreadLocal[mutable.HashMap[Class[_], Method]]() {

    override def initialValue: mutable.HashMap[Class[_], Method] =
      new mutable.HashMap[Class[_], Method]
  }

  def copyConstructor(product: Product): Method = {
    def getCopyMethod(productClass: Class[_ <: Product]): Method = {
      try {
        productClass.getMethods.find(_.getName == "copy").get
      } catch {
        case _: NoSuchElementException =>
          throw new IllegalStateException(
            s"Failed trying to rewrite $productClass - this class does not have a `copy` method"
          )
      }
    }

    val productClass = product.getClass
    productCopyConstructors.get.getOrElseUpdate(productClass, getCopyMethod(productClass))
  }

  def copyProduct(product: Product, children: Array[AnyRef]): AnyRef = {
    val method = copyConstructor(product)
    val result = method.invoke(product, children: _*)
    result
  }

  def numParameters(product: Product): Int = {
    val constructor = copyConstructor(product)
    val params = constructor.getParameterTypes
    params.length
  }

  def includesPosition(product: Product): Boolean = {
    val constructor = copyConstructor(product)
    val params = constructor.getParameterTypes
    params.last.isAssignableFrom(classOf[InputPosition])
  }
}
