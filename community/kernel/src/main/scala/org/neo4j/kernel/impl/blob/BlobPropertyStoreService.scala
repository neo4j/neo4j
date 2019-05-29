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

import java.io.File

import org.neo4j.blob.utils.Logging
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.impl.ConfigUtils._
import org.neo4j.kernel.impl.Configuration
import org.neo4j.kernel.impl.factory.DatabaseInfo
import org.neo4j.kernel.impl.proc.Procedures
import org.neo4j.kernel.lifecycle.Lifecycle

import scala.collection.mutable.ArrayBuffer

class BlobPropertyStoreService(proceduresService: Procedures, storeDir: File, neo4jConf: Config, databaseInfo: DatabaseInfo)
  extends Lifecycle with Logging {

  val configuration: Configuration = neo4jConf;
  val blobStorage: BlobStorage = BlobStorage.create(configuration);

  /////binds context
  neo4jConf.getInstanceContext.put[BlobPropertyStoreService](this);
  neo4jConf.getInstanceContext.put[BlobStorage](blobStorage);

  override def shutdown(): Unit = {
  }

  override def init(): Unit = {
  }

  override def stop(): Unit = {
    blobStorage.disconnect();
    logger.info(s"blob storage shutdown: $blobStorage");
  }

  override def start(): Unit = {
    blobStorage.initialize(new File(storeDir, neo4jConf.get(GraphDatabaseSettings.active_database)), configuration);
    registerProcedure(classOf[DefaultBlobFunctions]);
  }

  private def registerProcedure(procedures: Class[_]*) {
    for (procedure <- procedures) {
      proceduresService.registerProcedure(procedure);
      proceduresService.registerFunction(procedure);
    }
  }
}