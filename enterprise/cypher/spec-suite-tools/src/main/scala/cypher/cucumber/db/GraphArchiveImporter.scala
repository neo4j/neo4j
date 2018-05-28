/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package cypher.cucumber.db

import java.util.concurrent.TimeUnit

import java.util.{Map => JavaMap}

import cypher.cucumber.db.GraphArchive.Descriptor
import org.neo4j.graphdb.{Label, Transaction, GraphDatabaseService}

import scala.io.Source
import scala.reflect.io.Path

abstract class GraphArchiveImporter {

  def importArchive(archive: GraphArchive.Descriptor, destination: Path): Unit = {
    val db = createDatabase(archive, destination)
    try {
      createConstraints(archive, db)
      createIndices(archive, db)
      createData(archive, db)
    } finally {
      db.shutdown()
    }
  }

  def createConstraints(archive: Descriptor, db: GraphDatabaseService): Unit = {
    archive.recipe.uniqueNodeProperties.foreach {
      case (label, key) =>
        val tx = db.beginTx()
        try {
          db.schema().constraintFor(Label.label(label)).assertPropertyIsUnique(key).create()
          tx.success()
        } finally {
          tx.close()
        }
    }
  }

  def createData(archive: Descriptor, db: GraphDatabaseService): Unit = {
    archive.scripts.foreach { script =>
      val executor = new CypherExecutor(db)
      try {
        val input = Source.fromFile(script.file.jfile, "UTF-8").mkString
        val iterator = input.split(";").filter(_.trim.nonEmpty).iterator

        while (iterator.hasNext) {
          val statement = iterator.next()
          val result = executor.execute(s"CYPHER runtime=interpreted $statement", java.util.Collections.emptyMap())
          try {
            while (result.hasNext) result.next()
          } finally {
            result.close()
          }
        }
      } finally {
        executor.close()
      }
    }
  }

  def createIndices(archive: Descriptor, db: GraphDatabaseService): Unit = {
    archive.recipe.indexedNodeProperties.foreach {
      case (label, key) =>
        val tx = db.beginTx()
        try {
          db.schema().indexFor(Label.label(label)).on(key).create()
          tx.success()
        } finally {
          tx.close()
        }
    }

    val tx = db.beginTx()
    try {
      db.schema().awaitIndexesOnline(30, TimeUnit.SECONDS)
      tx.success()
    } finally {
      tx.close()
    }
  }

  protected def createDatabase(archive: GraphArchive.Descriptor, destination: Path): GraphDatabaseService

  private class CypherExecutor(db: GraphDatabaseService, batchSize: Int = 1000) {
    private var tx: Transaction = null
    private var count = 0

    def execute(statement: String, parameters: JavaMap[String, AnyRef]) =
      try {
        ensureOpen()
        db.execute(statement, parameters)
      } finally {
        count += 1
        if (count % batchSize == 0) {
          commit()
          count = 0
        }
      }

    def close(): Unit = {
      commit()
    }

    private def ensureOpen(): Unit = {
      if (tx == null)
        tx = db.beginTx()
    }

    private def commit() = {
      if (tx != null) {
        try {
          try {
            tx.success()
          } finally {
            tx.close()
          }
        } finally {
          tx = null
        }
      }
    }
  }
}


