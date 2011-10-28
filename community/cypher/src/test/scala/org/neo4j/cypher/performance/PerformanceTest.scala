/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import org.scalatest.Assertions
import org.neo4j.kernel.EmbeddedGraphDatabase
import org.neo4j.cypher.ExecutionEngine
import scala.io.Source.fromFile
import scala.util.Random
import org.junit.{Ignore, After, Before, Test}


@Ignore
class PerformanceTest extends Assertions {
  val movies: List[String] = getExistingMovies
  val r = new Random()

  var db: EmbeddedGraphDatabase = null
  var engine: ExecutionEngine = null

  @Before
  def init() {
    db = new EmbeddedGraphDatabase("target/perf-graph.db")
    engine = new ExecutionEngine(db)
  }

  @After
  def closeDown() {
    db.shutdown()
  }

  @Test
  def testGetFiveSimilarMovies() {
    val query = "START a=node:movieIds(id={id}) MATCH a<-[r1:rating]-person-[r2:rating]->b WHERE r1.stars>4 AND r2.stars>4 RETURN a.title, b.title, count(*) ORDER BY count(*) DESC LIMIT 5"

    (0 to 100).foreach( (x)=>println(engine.execute(query, Map("id"->getRandomMovie)).toList ))
  }

  def getRandomMovie: String = movies(r.nextInt(movies.length))

  private def getExistingMovies = fromFile("/Users/ata/Downloads/apa/ml-10M100K/movies.dat").getLines().map(line => line.split("::")(0)).toList

}