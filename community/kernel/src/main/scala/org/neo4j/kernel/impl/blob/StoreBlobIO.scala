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
package org.neo4j.kernel.impl.blob

import java.io.InputStream
import java.nio.ByteBuffer

import org.apache.logging.log4j.scala.Logging
import org.neo4j.blob._
import org.neo4j.blob.utils._
import org.neo4j.kernel.impl.store.record.{PrimitiveRecord, PropertyBlock, PropertyRecord}
import org.neo4j.values.storable.{BlobArray, BlobValue}

/**
  * Created by bluejoe on 2019/3/29.
  */
object StoreBlobIO extends Logging {
  def saveAndEncodeBlobAsByteArray(ic: ContextMap, blob: Blob): Array[Byte] = {
    val bid = ic.get[BlobStorage].save(blob);
    BlobIO.pack(BlobFactory.makeEntry(bid, blob));
  }

  def saveBlob(ic: ContextMap, blob: Blob, keyId: Int, block: PropertyBlock) = {
    val bid = ic.get[BlobStorage].save(blob);
    block.setValueBlocks(BlobIO._pack(BlobFactory.makeEntry(bid, blob), keyId));
  }

  def deleteBlobArrayProperty(ic: ContextMap, blobs: BlobArray): Unit = {
    ic.get[BlobStorage].deleteBatch(
      blobs.value().map(_.asInstanceOf[BlobWithId].id));
  }

  def deleteBlobProperty(ic: ContextMap, primitive: PrimitiveRecord, propRecord: PropertyRecord, block: PropertyBlock): Unit = {
    val entry = BlobIO.unpack(block.getValueBlocks);
    ic.get[BlobStorage].delete(entry.id);
  }

  def readBlob(ic: ContextMap, bytes: Array[Byte]): Blob = {
    readBlobValue(ic, StreamUtils.convertByteArray2LongArray(bytes)).blob;
  }

  def readBlobArray(ic: ContextMap, dataBuffer: ByteBuffer, arrayLength: Int): Array[Blob] = {
    (0 to arrayLength - 1).map { x =>
      val byteLength = dataBuffer.getInt();
      val blobByteArray = new Array[Byte](byteLength);
      dataBuffer.get(blobByteArray);
      StoreBlobIO.readBlob(ic, blobByteArray);
    }.toArray
  }


  def readBlobValue(ic: ContextMap, block: PropertyBlock): BlobValue = {
    readBlobValue(ic, block.getValueBlocks);
  }

  def readBlobValue(ic: ContextMap, values: Array[Long]): BlobValue = {
    val entry = BlobIO.unpack(values);
    val storage = ic.get[BlobStorage];

    val blob = BlobFactory.makeStoredBlob(entry, new InputStreamSource {
      override def offerStream[T](consume: (InputStream) => T): T = {
        val bid = entry.id;
        storage.load(bid).getOrElse(throw new BlobNotExistException(bid)).offerStream(consume)
      }
    });

    BlobValue(blob);
  }
}

class BlobNotExistException(bid: BlobId) extends RuntimeException {

}