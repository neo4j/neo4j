/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

package org.neo4j.values.storable

import org.neo4j.blob.Blob
import org.neo4j.hashing.HashFunction
import org.neo4j.values.{AnyValue, ValueMapper}

/**
  * Created by bluejoe on 2018/12/12.
  */
case class BlobValue(val blob: Blob) extends ScalarValue {
  override def unsafeCompareTo(value: Value): Int = blob.length.compareTo(value.asInstanceOf[BlobValue].blob.length)

  override def writeTo[E <: Exception](valueWriter: ValueWriter[E]): Unit = {
    valueWriter.writeBlob(blob);
  }

  override def asObjectCopy(): AnyRef = blob;

  override def valueGroup(): ValueGroup = ValueGroup.NO_VALUE;

  override def numberType(): NumberType = NumberType.NO_NUMBER;

  override def prettyPrint(): String = {
    val length = blob.length;
    val mimeType = blob.mimeType.text;
    s"(blob,length=$length, mimeType=$mimeType)"
  }

  override def equals(value: Value): Boolean =
    value.isInstanceOf[BlobValue] &&
      this.blob.length.equals(value.asInstanceOf[BlobValue].blob.length) &&
      this.blob.mimeType.code.equals(value.asInstanceOf[BlobValue].blob.mimeType.code);

  override def computeHash(): Int = {
    blob.hashCode;
  }

  override def map[T](valueMapper: ValueMapper[T]): T = this.blob.asInstanceOf[T];

  override def getTypeName: String = "BlobValue"

  override def updateHash(hashFunction: HashFunction, hash: Long): Long = hash
}

class BlobArraySupport[X <: BlobArraySupport[X]](val blobs: Array[Blob]) {
  val values: Array[AnyValue] = blobs.map(new BlobValue(_));

  def writeTo[E <: Exception](writer: ValueWriter[E]) {
    writer.beginArray(values.length, ValueWriter.ArrayType.BLOB)
    blobs.foreach(writer.writeBlob(_));
    writer.endArray()
  }

  def unsafeCompareTo(other: Value): Int = if (_equals(other)) 0 else -1;

  def _equals(other: Value): Boolean = {
    other.isInstanceOf[X] &&
      other.asInstanceOf[X].blobs.zip(blobs).map(t => t._1 == t._2).reduce(_ && _)
  }
}