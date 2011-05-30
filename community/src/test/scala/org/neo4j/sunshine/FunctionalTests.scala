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
package org.neo4j.sunshine

import org.neo4j.kernel.{AbstractGraphDatabase, ImpermanentGraphDatabase}
import org.junit.Assert._
import org.neo4j.graphdb.Node
import scala.collection.JavaConverters._
import org.junit.{Ignore, Before, Test}

/**
 * Created by Andres Taylor
 * Date: 5/27/11
 * Time: 14:22 
 */
@Ignore("Somethis has changed with GraphDescription")
class FunctionalTests {
  var db: AbstractGraphDatabase = null
  var engine: ExecutionEngine = null
  val parser = new SunshineParser

  @Test def sunshineHelloWorld() {
    testQuery(
      "start n = (0) return n",
      List(Map("n" -> db.getReferenceNode)))
  }

  @Test def sunshineHelloWorldWithPattern() {
    testQuery("start a = (%a) match (a)-[:TO]->(b) return b",
      List(Map("b" -> node("b"))))
  }

  @Test def filterOnNumbericValue() {
    setProp("a", "age", 36)
    testQuery("start a = (%a) where a.age = 36 return a",
      List(Map("a" -> node("a"))))
  }

  @Before def init() {
    db = new ImpermanentGraphDatabase()
    engine = new ExecutionEngine(db)
//    val graphDescription = new GraphDescription("a TO b")
//    graphDescription.create(db)
  }

  def node(name: String): Node = {
    db.getAllNodes.asScala.find(_.getProperty("name", null) == name) match {
      case Some(node) => node
    }
  }

  def setProp(name: String, property: String, value: Any) {
    val n = node(name)
    val tx = db.beginTx()
    n.setProperty(property, value)
    tx.success()
    tx.finish()
  }

  def testQuery(query: String, expectedResult: List[Map[String, Any]]) {
    val q = query.replaceAll("%a", node("a").getId.toString)
    val parseResult = parser.parse(q)
    val result = engine.execute(parseResult)
    assertEquals(expectedResult, result.toList)
  }
}