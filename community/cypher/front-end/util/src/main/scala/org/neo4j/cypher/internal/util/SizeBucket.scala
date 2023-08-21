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

object SizeBucket {

  /**
   * Compute the next closest power of 10. This number is used when planning queries where the literal being auto-parameterized has a size, 
   * e.g. a list or a string, by keeping this "bucket" as a size hint in the auto-parameter. By keeping a rough estimate of the size, we can make a more 
   * informed decision at plan time when solving predicates like `IN [...]` or `a.prop STARTS WITH 'foo'`.
   * 
   * Note estimation is exact for size 0 and 1 which is an important property of the computation since we leverage that fact
   * in other parts of the code base.
   *
   * @param size The size of the literal that is being auto-parametrized.
   * @return Next closest power of 10 for size.
   *         Examples: computeBucket(1) = 1, computeBucket(7) = 10, computeBucket(17) = 100, computeBucket(42) = 100
   */
  def computeBucket(size: Int): BucketSize = size match {
    case 0 => ExactSize(0)
    case 1 => ExactSize(1)
    case _ => ApproximateSize(Math.pow(10, Math.ceil(Math.log10(size))).toInt)
  }
}

sealed trait BucketSize {
  def toOption: Option[Int]
}

case object UnknownSize extends BucketSize {
  override def toOption: Option[Int] = None
}

case class ExactSize(size: Int) extends BucketSize with Rewritable {
  override def toOption: Option[Int] = Some(size)

  override def dup(children: Seq[AnyRef]): this.type = {
    val newSize = children.head.asInstanceOf[Int]
    if (newSize == size) this
    else copy(size = newSize).asInstanceOf[this.type]
  }
}

case class ApproximateSize(size: Int) extends BucketSize with Rewritable {
  override def toOption: Option[Int] = Some(size)

  override def dup(children: Seq[AnyRef]): this.type = {
    val newSize = children.head.asInstanceOf[Int]
    if (newSize == size) this
    else copy(size = newSize).asInstanceOf[this.type]
  }
}

object WithSizeHint {

  def unapply(bucketSize: BucketSize): Option[Int] = bucketSize match {
    case UnknownSize           => None
    case ExactSize(size)       => Some(size)
    case ApproximateSize(size) => Some(size)
  }
}
