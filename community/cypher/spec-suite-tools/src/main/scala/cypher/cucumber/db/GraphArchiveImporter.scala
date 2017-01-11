/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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


