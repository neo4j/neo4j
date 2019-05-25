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

import java.util.concurrent.atomic.AtomicInteger

import org.apache.commons.codec.digest.DigestUtils
import org.neo4j.blob.utils.ReflectUtils._
import org.neo4j.blob.utils._
import org.neo4j.blob._
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.api.KernelTransaction.CloseListener
import org.neo4j.kernel.impl.coreapi.{InternalTransaction, TopLevelTransaction}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.BlobValue

import scala.collection.mutable

/**
  * Created by bluejoe on 2019/4/3.
  */
object BoltServerBlobIO {
  val CHUNK_SIZE = 1024 * 10;
  val useInlineAlways = false;

  def packBlob(blob: Blob, out: org.neo4j.bolt.v1.packstream.PackOutput): BlobEntry = {
    //create a temp blodid
    val tempBlobId = BlobId.EMPTY;
    val inline = useInlineAlways || (blob.length <= BlobIO.MAX_INLINE_BLOB_BYTES);
    //write marker
    out.writeByte(if (inline) {
      BlobIO.BOLT_VALUE_TYPE_BLOB_INLINE
    }
    else {
      BlobIO.BOLT_VALUE_TYPE_BLOB_REMOTE
    });

    //write blob entry
    val entry = BlobFactory.makeEntry(tempBlobId, blob);
    BlobIO._pack(entry).foreach(out.writeLong(_));

    //write inline
    if (inline) {
      val bs = blob.toBytes();
      out.writeBytes(bs, 0, bs.length);
    }
    else {
      //cache blob here for later use
      val bid = BoltTransactionListener.currentTopLevelTransactionId();
      val blobId = TransactionalBlobCache.put(blob, bid);

      //pack blob id
      val bs = blobId.getBytes("utf-8");
      out.writeInt(bs.length);
      out.writeBytes(bs, 0, bs.length);
    }

    entry;
  }

  def unpackBlob(unpacker: org.neo4j.bolt.v1.packstream.PackStream.Unpacker): AnyValue = {
    val in = unpacker._get("in").asInstanceOf[org.neo4j.bolt.v1.packstream.PackInput];
    val byte = in.peekByte();

    byte match {
      case BlobIO.BOLT_VALUE_TYPE_BLOB_INLINE =>
        in.readByte();

        val values = for (i <- 0 to 3) yield in.readLong();
        val entry = BlobIO.unpack(values.toArray);

        //read inline
        val length = entry.length;
        val bs = new Array[Byte](length.toInt);
        in.readBytes(bs, 0, length.toInt);
        new BlobValue((new InlineBlob(bs, length, entry.mimeType)));

      case _ =>
        null;
    }
  }
}

object BoltTransactionListener {
  val _local = new ThreadLocal[TopLevelTransaction]();

  private def transactionId(kt: TopLevelTransaction) = "" + kt.hashCode();

  def currentTopLevelTransaction() = _local.get();

  def currentTopLevelTransactionId() = transactionId(_local.get());

  def onTransactionCreate(tx: InternalTransaction): Unit = {
    tx match {
      case topTx: TopLevelTransaction =>
        val kt = topTx._get("transaction").asInstanceOf[KernelTransaction];

        _local.set(topTx);
        kt.registerCloseListener(new CloseListener {
          override def notify(txId: Long): Unit = {
            TransactionalBlobCache.invalidate(transactionId(topTx));
            _local.remove();
          }
        })

      case _ => {

      }
    }
  }
}

object TransactionalBlobCache {
  val _serial = new AtomicInteger();
  //1m
  val MAX_ALIVE = 60 * 60 * 1000;

  //5s
  val CHECK_INTERVAL = 5 * 60 * 1000;

  case class Entry(id: String, blob: Blob, transactionId: String, time: Long = System.currentTimeMillis()) {

  }

  val _blobCache = mutable.Map[String, Entry]();

  //expired blob checking thread
  new Thread(new Runnable() {
    override def run(): Unit = {
      while (true) {
        Thread.sleep(CHECK_INTERVAL);
        if (!_blobCache.isEmpty) {
          val deadline = System.currentTimeMillis() - MAX_ALIVE;
          _blobCache --= _blobCache.filter(_._2.time > deadline).map(_._1);
        }
      }
    }
  }).start();

  def put(blob: Blob, transactionId: String): String = {
    val bid = DigestUtils.md5Hex(StreamUtils.covertLong2ByteArray(_serial.getAndIncrement()));
    val entry = Entry(bid, blob, transactionId);
    _blobCache += bid -> entry;
    bid;
  };

  def dump(): Unit = {
    println("<<<<<<<<<<<<<<<<<")
    _blobCache.foreach(println);
    println(">>>>>>>>>>>>>>>>>")
  }

  def get(bid: String): Option[Blob] = {
    _blobCache.get(bid).map(_.blob)
  };

  def invalidate(transactionId: String) = {
    _blobCache --= _blobCache.filter(_._2.transactionId.equals(transactionId)).map(_._1);
  };
}