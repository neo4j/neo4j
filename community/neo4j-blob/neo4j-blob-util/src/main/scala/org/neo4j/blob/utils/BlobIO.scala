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
package org.neo4j.blob.utils

import java.io.ByteArrayOutputStream

import org.neo4j.blob._
import org.neo4j.blob.utils.StreamUtils._

/**
  * Created by bluejoe on 2019/4/18.
  */
object BlobIO {
  val BOLT_VALUE_TYPE_BLOB_INLINE = 0xC5.toByte;
  val BOLT_VALUE_TYPE_BLOB_REMOTE = 0xC4.toByte;
  val BLOB_PROPERTY_TYPE = 15;
  val MAX_INLINE_BLOB_BYTES = 10240;

  def pack(entry: BlobEntry): Array[Byte] = {
    val baos = new ByteArrayOutputStream();
    _pack(entry, 0).foreach(baos.writeLong(_));

    baos.toByteArray;
  }

  def unpack(values: Array[Long]): BlobEntry = {
    val length = values(1) >> 16;
    val mimeType = values(1) & 0xFFFFL;

    val bid = new BlobId(values(2), values(3));

    val mt = MimeTypeFactory.fromCode(mimeType);
    BlobFactory.makeEntry(bid, length, mt);
  }

  def _pack(entry: BlobEntry, keyId: Int = 0): Array[Long] = {
    val values = new Array[Long](4);
    //val digest = ByteArrayUtils.convertByteArray2LongArray(blob.digest);
    /*
    blob uses 4*8 bytes: [v0][v1][v2][v3]
    v0: [____,____][____,____][____,____][____,____][[____,tttt][kkkk,kkkk][kkkk,kkkk][kkkk,kkkk] (t=type, k=keyId)
    v1: [llll,llll][llll,llll][llll,llll][llll,llll][llll,llll][llll,llll][mmmm,mmmm][mmmm,mmmm] (l=length, m=mimeType)
    v2: [iiii,iiii][iiii,iiii][iiii,iiii][iiii,iiii][iiii,iiii][iiii,iiii][iiii,iiii][iiii,iiii]
    v3: [iiii,iiii][iiii,iiii][iiii,iiii][iiii,iiii][iiii,iiii][iiii,iiii][iiii,iiii][iiii,iiii]
    */
    values(0) = keyId | (BLOB_PROPERTY_TYPE << 24);
    values(1) = entry.mimeType.code | (entry.length << 16);
    val la = StreamUtils.convertByteArray2LongArray(entry.id.asByteArray());
    values(2) = la(0);
    values(3) = la(1);

    values;
  }
}