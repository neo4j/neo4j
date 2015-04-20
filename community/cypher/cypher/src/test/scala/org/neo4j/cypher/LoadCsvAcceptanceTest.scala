/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher

import java.io.{File, PrintWriter}

import org.neo4j.cypher.internal.commons.CreateTempFileTestSupport
import org.neo4j.cypher.internal.compiler.v2_2.commands.expressions.StringHelper.RichString
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.test.TestGraphDatabaseFactory
import org.scalatest.BeforeAndAfterAll

class LoadCsvAcceptanceTest
  extends ExecutionEngineFunSuite with BeforeAndAfterAll
  with QueryStatisticsTestSupport with CreateTempFileTestSupport {

  test("import three strings") {
    val url = createCSVTempFileURL({
      writer =>
        writer.println("'Foo'")
        writer.println("'Foo'")
        writer.println("'Foo'")
    }).cypherEscape

    val result = execute(s"LOAD CSV FROM '$url' AS line CREATE (a {name: line[0]}) RETURN a.name")
    assertStats(result, nodesCreated = 3, propertiesSet = 3)
  }

  test("import three numbers") {
    val url = createCSVTempFileURL({
      writer =>
        writer.println("1")
        writer.println("2")
        writer.println("3")
    }).cypherEscape

    val result =
      execute(s"LOAD CSV FROM '$url' AS line CREATE (a {number: line[0]}) RETURN a.number")
    assertStats(result, nodesCreated = 3, propertiesSet = 3)

    result.columnAs[Long]("a.number").toList === List("")
  }

  test("import three rows numbers and strings") {
    val url = createCSVTempFileURL({
      writer =>
        writer.println("1, 'Aadvark'")
        writer.println("2, 'Babs'")
        writer.println("3, 'Cash'")
    }).cypherEscape

    val result = execute(s"LOAD CSV FROM '$url' AS line CREATE (a {name: line[0]}) RETURN a.name")
    assertStats(result, nodesCreated = 3, propertiesSet = 3)
  }

  test("import three rows with headers") {
    val url = createCSVTempFileURL({
      writer =>
        writer.println("id,name")
        writer.println("1, 'Aadvark'")
        writer.println("2, 'Babs'")
        writer.println("3, 'Cash'")
    }).cypherEscape

    val result = execute(
      s"LOAD CSV WITH HEADERS FROM '$url' AS line CREATE (a {id: line.id, name: line.name}) RETURN a.name"
    )

    assertStats(result, nodesCreated = 3, propertiesSet = 6)
  }

  test("import three rows with headers messy data") {
    val url = createCSVTempFileURL({
      writer =>
        writer.println("id,name,x")
        writer.println("1,'Aadvark',0")
        writer.println("2,'Babs'")
        writer.println("3,'Cash',1")
        writer.println("4,'Dice',\"\"")
        writer.println("5,'Emerald',")
    }).cypherEscape

    val result = execute(s"LOAD CSV WITH HEADERS FROM '$url' AS line RETURN line.x")
    assert(result.toList === List(
      Map("line.x" -> "0"),
      Map("line.x" -> null),
      Map("line.x" -> "1"),
      Map("line.x" -> ""),
      Map("line.x" -> null))
    )
  }

  test("should handle quotes") {
    val url = createCSVTempFileURL()({
      writer =>
        writer.println("String without quotes")
        writer.println("'String, with single quotes'")
        writer.println("\"String, with double quotes\"")
        writer.println(""""String with ""quotes"" in it"""")
    }).cypherEscape

    val result = execute(s"LOAD CSV FROM '$url' AS line RETURN line as string").toList
    assert(result === List(
      Map("string" -> Seq("String without quotes")),
      Map("string" -> Seq("'String", " with single quotes'")),
      Map("string" -> Seq("String, with double quotes")),
      Map("string" -> Seq("""String with "quotes" in it"""))))
  }

  test("should handle crlf line termination") {
    val url = createCSVTempFileURL({
      writer =>
        writer.print("1,'Aadvark',0\r\n")
        writer.print("2,'Babs'\r\n")
        writer.print("3,'Cash',1\r\n")
    }).cypherEscape

    val result = execute(s"LOAD CSV FROM '$url' AS line RETURN line")
    assert(result.toList === List(Map("line" -> Seq("1","'Aadvark'","0")), Map("line" -> Seq("2","'Babs'")), Map("line" -> Seq("3","'Cash'","1"))))
  }

  test("should handle lf line termination") {
    val url = createCSVTempFileURL({
      writer =>
        writer.print("1,'Aadvark',0\n")
        writer.print("2,'Babs'\n")
        writer.print("3,'Cash',1\n")
    }).cypherEscape

    val result = execute(s"LOAD CSV FROM '$url' AS line RETURN line")
    assert(result.toList === List(Map("line" -> Seq("1","'Aadvark'","0")), Map("line" -> Seq("2","'Babs'")), Map("line" -> Seq("3","'Cash'","1"))))
  }

  test("should handle cr line termination") {
    val url = createCSVTempFileURL({
      writer =>
        writer.print("1,'Aadvark',0\r")
        writer.print("2,'Babs'\r")
        writer.print("3,'Cash',1\r")
    }).cypherEscape

    val result = execute(s"LOAD CSV FROM '$url' AS line RETURN line")
    assert(result.toList === List(Map("line" -> Seq("1","'Aadvark'","0")), Map("line" -> Seq("2","'Babs'")), Map("line" -> Seq("3","'Cash'","1"))))
  }

  test("should handle custom field terminator") {
    val url = createCSVTempFileURL ({
      writer =>
        writer.println("1;'Aadvark';0")
        writer.println("2;'Babs'")
        writer.println("3;'Cash';1")
    }).cypherEscape

    val result = execute(s"LOAD CSV FROM '$url' AS line FIELDTERMINATOR ';' RETURN line")
    assert(result.toList === List(Map("line" -> Seq("1","'Aadvark'","0")), Map("line" -> Seq("2","'Babs'")), Map("line" -> Seq("3","'Cash'","1"))))
  }

  test("should open file containing strange chars with '") {
    val filename = ensureNoIllegalCharsInWindowsFilePath("cypher '%^&!@#_)(098.:,;[]{}\\~$*+-")
    val url = createCSVTempFileURL (filename)({
      writer =>
        writer.println("something")
    }).cypherEscape

    val result = execute("LOAD CSV FROM \"" + url + "\" AS line RETURN line as string").toList
    assert(result === List(Map("string" -> Seq("something"))))
  }

  test("should open file containing strange chars with \"") {
    val filename = ensureNoIllegalCharsInWindowsFilePath("cypher \"%^&!@#_)(098.:,;[]{}\\~$*+-")
    val url = createCSVTempFileURL (filename)({
      writer =>
        writer.println("something")
    })

    val result = execute(s"LOAD CSV FROM '$url' AS line RETURN line as string").toList
    assert(result === List(Map("string" -> Seq("something"))))
  }

  test("empty file does not create anything") {
    val url = createCSVTempFileURL(writer => {}).cypherEscape

    val result = execute(s"LOAD CSV FROM '$url' AS line CREATE (a {name: line[0]}) RETURN a.name")
    assertStats(result, nodesCreated = 0)
  }

  test("should be able to open relative paths with dot") {
    val url = createCSVTempFileURL(filename = "cypher", dir = "./")(
        writer =>
            writer.println("something")
    ).cypherEscape

    val result = execute(s"LOAD CSV FROM '$url' AS line CREATE (a {name: line[0]}) RETURN a.name")
    assertStats(result, nodesCreated = 1, propertiesSet = 1)
  }

  test("should be able to open relative paths with dotdot") {
    val url = createCSVTempFileURL(filename = "cypher", dir = "../")(
        writer =>
            writer.println("something")
    ).cypherEscape

    val result = execute(s"LOAD CSV FROM '$url' AS line CREATE (a {name: line[0]}) RETURN a.name")
    assertStats(result, nodesCreated = 1, propertiesSet = 1)
  }

  test("should handle null keys in maps as result value") {
    val url = createCSVTempFileURL ({
      writer =>
        writer.println("DEPARTMENT ID;DEPARTMENT NAME;")
        writer.println("010-1010;MFG Supplies;")
        writer.println("010-1011;Corporate Procurement;")
        writer.println("010-1015;MFG - Engineering HQ;")
    }).cypherEscape

    val result = execute(s"LOAD CSV WITH HEADERS FROM '$url' AS line FIELDTERMINATOR ';' RETURN *").toList
    assert(result === List(
      Map("line" -> Map("DEPARTMENT ID" -> "010-1010", "DEPARTMENT NAME" -> "MFG Supplies", null.asInstanceOf[String] -> null)),
      Map("line" -> Map("DEPARTMENT ID" -> "010-1011", "DEPARTMENT NAME" -> "Corporate Procurement", null.asInstanceOf[String] -> null)),
      Map("line" -> Map("DEPARTMENT ID" -> "010-1015", "DEPARTMENT NAME" -> "MFG - Engineering HQ", null.asInstanceOf[String] -> null))
    ))
  }

  test("should fail gracefully when loading missing file") {
    intercept[LoadExternalResourceException] {
      execute("LOAD CSV FROM 'file://./these_are_not_the_droids_you_are_looking_for.csv' AS line CREATE (a {name:line[0]})")
    }
  }

  test("should be able to download data from the web") {
    val url = s"http://127.0.0.1:${port}/test.csv".cypherEscape

    val result = executeScalar[Long](s"LOAD CSV FROM '${url}' AS line RETURN count(line)")
    result should equal(3)
  }

  test("should be able to download from a website when redirected and cookies are set") {
    val url = s"http://127.0.0.1:${port}/redirect_test.csv".cypherEscape

    val result = executeScalar[Long](s"LOAD CSV FROM '${url}' AS line RETURN count(line)")
    result should equal(3)
  }

  test("should fail gracefully when getting 404") {
    intercept[LoadExternalResourceException] {
      execute(s"LOAD CSV FROM 'http://127.0.0.1:${port}/these_are_not_the_droids_you_are_looking_for/' AS line CREATE (a {name:line[0]})")
    }
  }

  test("should fail gracefully when loading non existent (local) site") {
    intercept[LoadExternalResourceException] {
      execute("LOAD CSV FROM 'http://127.0.0.1:9999/these_are_not_the_droids_you_are_looking_for/' AS line CREATE (a {name:line[0]})")
    }
  }

  test("should fail for file urls if local file access disallowed") {
    val url = createCSVTempFileURL({
      writer =>
        writer.println("String without quotes")
    }).cypherEscape
    val db = new TestGraphDatabaseFactory()
      .newImpermanentDatabaseBuilder()
      .setConfig(GraphDatabaseSettings.allow_file_urls, "false")
      .newGraphDatabase()

    intercept[LoadExternalResourceException] {
      val engine = new ExecutionEngine(db)
      engine.execute(s"LOAD CSV FROM '$url' AS line CREATE (a {name:line[0]})")
    }
  }

  test("should reject URLs that are not file://, http://, https://, ftp://") {
    val exception = intercept[LoadExternalResourceException] {
      execute(s"LOAD CSV FROM 'morsecorba://sos' AS line CREATE (a {name:line[0]})")
    }
    exception.getMessage should equal("Invalid URL specified (unknown protocol: morsecorba)")
  }

  test("should reject invalid URLs") {
    val exception = intercept[LoadExternalResourceException] {
      execute(s"LOAD CSV FROM 'foo.bar' AS line CREATE (a {name:line[0]})")
    }
    exception.getMessage should equal("Invalid URL specified (no protocol: foo.bar)")
  }

  private def ensureNoIllegalCharsInWindowsFilePath(filename: String) = {
    // isWindows?
    if ('\\' == File.separatorChar) {
      // http://msdn.microsoft.com/en-us/library/windows/desktop/aa365247%28v=vs.85%29.aspxs
      val illegalCharsInWidnowsFilePath = "/?<>\\:*|\""
      // just replace the illegal chars with a 'a'
      illegalCharsInWidnowsFilePath.foldLeft(filename)((current, c) => current.replace(c, 'a'))
    } else {
      filename
    }
  }

  private val CSV_DATA_CONTENT = "1,1,1\n2,2,2\n3,3,3\n".getBytes
  private val CSV_PATH = "/test.csv"
  private val CSV_COOKIE_PATH = "/cookie_test.csv"
  private val CSV_REDIRECT_PATH = "/redirect_test.csv"
  private val MAGIC_COOKIE = "neoCookie=Magic"
  private var httpServer: HttpServerTestSupport = _
  private var port = -1

  override def beforeAll() {
    val  builder = new HttpServerTestSupportBuilder()
    builder.onPathReplyWithData(CSV_PATH, CSV_DATA_CONTENT)

    builder.onPathReplyWithData(CSV_COOKIE_PATH, CSV_DATA_CONTENT)
    builder.onPathReplyOnlyWhen(CSV_COOKIE_PATH, HttpServerTestSupport.hasCookie(MAGIC_COOKIE))

    builder.onPathRedirectTo(CSV_REDIRECT_PATH, CSV_COOKIE_PATH)
    builder.onPathTransformResponse(CSV_REDIRECT_PATH, HttpServerTestSupport.setCookie(MAGIC_COOKIE))

    httpServer = builder.build()
    httpServer.start()
    port = httpServer.boundInfo.getPort
    assert(port > 0)
  }

  override def afterAll() {
    httpServer.stop()
  }

  private def createFile(f: PrintWriter => Unit): String = createFile()(f)
  private def createFile(filename: String = "cypher", dir: String = null)(f: PrintWriter => Unit): String = createTempFileURL(filename, ".csv")(f).cypherEscape
}
