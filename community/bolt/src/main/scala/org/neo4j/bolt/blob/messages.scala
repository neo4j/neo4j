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
package org.neo4j.bolt.blob

import java.io.{ByteArrayInputStream, InputStream}

import org.neo4j.blob.{InputStreamSource, MimeType, Blob, BlobMessageSignature}
import org.neo4j.bolt.messaging.Neo4jPack.Unpacker
import org.neo4j.bolt.messaging.{RequestMessage, RequestMessageDecoder}
import org.neo4j.bolt.runtime.BoltResult.Visitor
import org.neo4j.bolt.runtime._
import org.neo4j.cypher.result.QueryResult
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values

/**
  * Created by bluejoe on 2019/5/7.
  */
trait RequestMessageHandler {
  @throws[Exception]
  def accepts(context: StateMachineContext);
}

class GetBlobMessage(val blobId: String) extends RequestMessage with RequestMessageHandler {
  override def safeToProcessInAnyState(): Boolean = false;

  override def toString = s"GET_BLOB(id=$blobId)";

  @throws[Exception]
  override def accepts(context: StateMachineContext) = {
    val opt: Option[Blob] = TransactionalBlobCache.get(blobId)
    if (opt.isDefined) {
      context.connectionState.onRecords(new BoltResult() {
        override def fieldNames(): Array[String] = Array("chunk-id", "chunk-start", "chunk-length", "chunk-content", "eof", "total-bytes")

        override def accept(visitor: Visitor): Unit = {
          val blob = opt.get;
          val total = blob.length;
          var transfered = 0L;

          blob.offerStream(is => {
            var i = 0;
            var read = 0;
            while (read != -1) {
              val bs = new Array[Byte](BoltServerBlobIO.CHUNK_SIZE);
              read = is.read(bs);
              if (read != -1) {
                val transfered2 = transfered + read;
                val record = new QueryResult.Record() {
                  override def fields(): Array[AnyValue] = {
                    Array(Values.of(i),
                      Values.of(transfered),
                      Values.of(read),
                      Values.of(if (read == BoltServerBlobIO.CHUNK_SIZE) bs else bs.slice(0, read)),
                      Values.of(transfered2 == total),
                      Values.of(blob.length)
                    );
                  }
                }

                visitor.visit(record);
                transfered = transfered2;
                i += 1;
              }

            }
          })
        }

        override def close(): Unit = {
          //TODO
          println("server side: CLOSE!!!!!!!!");
        }
      }, true);
    }
    //failed
    else {
      context.connectionState.markFailed(Neo4jError.fatalFrom(new InvalidBlobHandleException(blobId)))
    }
  }
}

class GetBlobMessageDecoder(val responseHandler: BoltResponseHandler) extends RequestMessageDecoder {
  override def signature(): Int = BlobMessageSignature.SIGNATURE_GET_BLOB;

  override def decode(unpacker: Unpacker): RequestMessage = {
    val blobId: String = unpacker.unpackString
    new GetBlobMessage(blobId);
  }
}

class InvalidBlobHandleException(blobId: String)
  extends RuntimeException(s"invalid blob handle: $blobId, make sure it is within an active transaction") {

}

class InlineBlob(bytes: Array[Byte], val length: Long, val mimeType: MimeType)
  extends Blob {
  override val streamSource: InputStreamSource = new InputStreamSource() {
    override def offerStream[T](consume: (InputStream) => T): T = {
      val fis = new ByteArrayInputStream(bytes);
      val t = consume(fis);
      fis.close();
      t;
    }
  };
}