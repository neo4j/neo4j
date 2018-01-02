/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.performance

import java.io.File
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.graphdb.DynamicRelationshipType
import org.neo4j.index.impl.lucene.LuceneBatchInserterIndexProviderNewImpl
import org.neo4j.unsafe.batchinsert.{BatchInserter, BatchInserterIndex, BatchInserters}

import scala.collection.JavaConverters._
import scala.io.Source.fromFile

class DataImportTest extends CypherFunSuite {

  val CATEGORY = DynamicRelationshipType.withName("category")
  val RATING = DynamicRelationshipType.withName("rating")

  // This test creates a database
  ignore("createDatabase") {
    val sourceDir = new File("/Users/ata/Downloads/apa/ml-10M100K")
    val targetDir = "target/perf-graph.db"
    val dir = new File(targetDir)
    deleteAll(dir)

    dir.exists() should equal(false)

    val (inserter, moviesId, moviesTitles, indexProvider, typeIdx) = createInserters(targetDir)

    val movieMap = createMovies(sourceDir, inserter, moviesTitles, moviesId, typeIdx)
    addRatings(sourceDir, movieMap.toMap, inserter, typeIdx)

    indexProvider.shutdown()
    inserter.shutdown()
  }

  private def addRatings(sourceDir: File, movies: Map[String, Long], inserter: BatchInserter, typeIdx: BatchInserterIndex) = {
    val users = scala.collection.mutable.Map[String, Long]()

    fromFile(new File(sourceDir, "ratings.dat")).getLines().foreach(ratingLine => {
      val (userId, movieId, stars) = splitRating(ratingLine)

      val user = users.getOrElseUpdate(userId, {
        val user = inserter.createNode(map("id" -> userId, "type" -> "user"))
        typeIdx.add(user, map("type"->"user"))
        user
      })
      val movie = movies(movieId)
      inserter.createRelationship(user, movie, RATING, map("stars" -> stars.toDouble.asInstanceOf[AnyRef]))
    })
    users.toMap
  }

  private def splitRating(ratingLine: String) = {
    val split = ratingLine.split("::")
    val userId = split(0)
    val movieId = split(1)
    val stars = split(2)
    (userId, movieId, stars)
  }

  def createNodeIdx(indexProvider: LuceneBatchInserterIndexProviderNewImpl, name: String, typ: String, column: String): BatchInserterIndex = {
    val moviesId = indexProvider.nodeIndex(name, Map("type" -> typ).asJava)
    moviesId.setCacheCapacity(column, 10000)
    moviesId
  }

  private def createInserters(targetDir: String) = {
    val inserter = BatchInserters.inserter(targetDir)
    val indexProvider = new LuceneBatchInserterIndexProviderNewImpl(inserter)

    val moviesTitles = createNodeIdx(indexProvider, "movieTitles", "fulltext", "title")
    val moviesId = createNodeIdx(indexProvider, "movieIds", "exact", "id")
    val typeIdx = createNodeIdx(indexProvider, "type", "exact", "type")

    (inserter, moviesId, moviesTitles, indexProvider, typeIdx)
  }

  private def createMovies(sourceDir: File, inserter: BatchInserter, moviesTitles: BatchInserterIndex, moviesId: BatchInserterIndex, typeIdx: BatchInserterIndex) = {

    val categories = scala.collection.mutable.Map[String, Long]()
    val movies = scala.collection.mutable.Map[String, Long]()
    val moviesFile = fromFile(new File(sourceDir, "movies.dat"))
    moviesFile.getLines().foreach(line => {
      val split = line.split("::")
      val props = map("id" -> split(0), "title" -> split(1), "type" -> "movie")
      val movieId = inserter.createNode(props)
      movies += split(0) -> movieId
      moviesTitles.add(movieId, map("title" -> split(1)))
      moviesId.add(movieId, map("id" -> split(0)))
      typeIdx.add(movieId, map("type" -> "movie"))

      split(2).split('|').foreach(category => {
        val categoryId = categories.getOrElseUpdate(category, {
          inserter.createNode(map("name" -> category))
        })
        inserter.createRelationship(movieId, categoryId, CATEGORY, map())
      })
    })
    moviesFile.close()
    movies
  }


  private def map(m: (String, Object)*): java.util.Map[String, Object] = m.toMap.asJava

  def deleteAll(file: File) {
    def deleteFile(dfile: File) {
      if (dfile.isDirectory) {
        val subfiles = dfile.listFiles
        if (subfiles != null)
          subfiles.foreach {
            f => deleteFile(f)
          }
      }
      dfile.delete
    }
    deleteFile(file)
  }

}
