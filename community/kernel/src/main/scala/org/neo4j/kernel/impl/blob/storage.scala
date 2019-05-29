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

import java.io.{File, FileInputStream, FileOutputStream, InputStream}
import java.util.UUID

import org.apache.commons.io.filefilter.TrueFileFilter
import org.apache.commons.io.{FileUtils, IOUtils}
import org.neo4j.blob._
import org.neo4j.blob.utils.Logging
import org.neo4j.blob.utils.StreamUtils._
import org.neo4j.kernel.impl.ConfigUtils._
import org.neo4j.kernel.impl.Configuration
import scala.collection.JavaConversions._

trait BlobStorage extends BatchBlobValueStorage {
  def save(blob: Blob): BlobId;

  def load(id: BlobId): Option[Blob];

  def delete(id: BlobId): Unit;
}

trait BatchBlobValueStorage extends Closable {
  def saveBatch(blobs: Iterable[Blob]): Iterable[BlobId];

  def loadBatch(ids: Iterable[BlobId]): Iterable[Option[Blob]];

  def deleteBatch(ids: Iterable[BlobId]): Unit;

  def iterator(): Iterator[(BlobId, Blob)];
}

trait Closable {
  def initialize(storeDir: File, conf: Configuration): Unit;

  def disconnect(): Unit;
}

object BlobStorage extends Logging {
  def of(bbvs: BatchBlobValueStorage) = {
    logger.info(s"using batch blob storage: ${bbvs}");

    new BlobStorage {
      override def delete(blobId: BlobId): Unit = {
        logger.debug(s"deleting blob: ${blobId.asLiteralString()}");

        bbvs.deleteBatch(Array(blobId))
      }

      override def load(blobId: BlobId): Option[Blob] = bbvs.loadBatch(Array(blobId)).head

      override def save(blob: Blob): BlobId = {
        bbvs.saveBatch(Array(blob)).head
      }

      override def deleteBatch(ids: Iterable[BlobId]): Unit = {
        logger.debug(s"deleting blob: ${ids.map(_.asLiteralString)}");

        bbvs.deleteBatch(ids)
      }

      override def saveBatch(blobs: Iterable[Blob]): Iterable[BlobId] = {
        bbvs.saveBatch(blobs)
      }

      override def loadBatch(ids: Iterable[BlobId]): Iterable[Option[Blob]] = bbvs.loadBatch(ids)

      override def disconnect(): Unit = bbvs.disconnect()

      override def initialize(storeDir: File, conf: Configuration): Unit = bbvs.initialize(storeDir, conf)

      override def iterator(): Iterator[(BlobId, Blob)] = bbvs.iterator();
    };
  }

  def create(conf: Configuration): BlobStorage =
    of(create(conf.getRaw("blob.storage")));

  def create(blobStorageClassName: Option[String]): BatchBlobValueStorage = {
    blobStorageClassName.map(Class.forName(_).newInstance().asInstanceOf[BatchBlobValueStorage])
      .getOrElse(createDefault())
  }

  class DefaultLocalFileSystemBlobValueStorage extends BatchBlobValueStorage with Logging {
    var _rootDir: File = _;

    override def saveBatch(blobs: Iterable[Blob]) = {
      blobs.map(blob => {
        val bid = generateId();
        val file = locateFile(bid);
        file.getParentFile.mkdirs();

        val fos = new FileOutputStream(file);
        fos.write(bid.asByteArray());
        fos.writeLong(blob.mimeType.code);
        fos.writeLong(blob.length);

        blob.offerStream { bis =>
          IOUtils.copy(bis, fos);
        }
        fos.close();

        bid;
      }
      )
    }

    override def loadBatch(ids: Iterable[BlobId]): Iterable[Option[Blob]] = {
      ids.map(id => Some(readFromBlobFile(locateFile(id))._2));
    }

    override def deleteBatch(ids: Iterable[BlobId]) = {
      ids.foreach { id =>
        locateFile(id).delete()
      }
    }

    private def generateId(): BlobId = {
      val uuid = UUID.randomUUID()
      BlobId(uuid.getMostSignificantBits, uuid.getLeastSignificantBits);
    }

    private def locateFile(bid: BlobId): File = {
      val idname = bid.asLiteralString();
      new File(_rootDir, s"${idname.substring(28, 32)}/$idname");
    }

    private def readFromBlobFile(blobFile: File): (BlobId, Blob) = {
      val fis = new FileInputStream(blobFile);
      val blobId = BlobId.readFromStream(fis);
      val mimeType = MimeTypeFactory.fromCode(fis.readLong());
      val length = fis.readLong();
      fis.close();

      val blob = BlobFactory.fromInputStreamSource(new InputStreamSource() {
        def offerStream[T](consume: (InputStream) => T): T = {
          val is = new FileInputStream(blobFile);
          //NOTE: skip
          is.skip(8 * 4);
          val t = consume(is);
          is.close();
          t;
        }
      }, length, Some(mimeType));

      (blobId, blob);
    }

    override def initialize(storeDir: File, conf: Configuration): Unit = {
      val baseDir: File = storeDir; //new File(conf.getRaw("unsupported.dbms.directories.neo4j_home").get());
      _rootDir = conf.getAsFile("blob.storage.file.dir", baseDir, new File(baseDir, "/blob"));
      _rootDir.mkdirs();
      logger.info(s"using storage dir: ${_rootDir.getCanonicalPath}");
    }

    override def disconnect(): Unit = {
    }

    override def iterator(): Iterator[(BlobId, Blob)] = {
      FileUtils.iterateFiles(_rootDir, TrueFileFilter.TRUE, TrueFileFilter.TRUE).map {
        readFromBlobFile(_)
      }
    }
  }

  def createDefault(): BatchBlobValueStorage = new DefaultLocalFileSystemBlobValueStorage();
}