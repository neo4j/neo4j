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
package org.neo4j.internal.cypher.acceptance

import java.io.{File, PrintWriter}
import java.net.{URL, URLConnection, URLStreamHandler, URLStreamHandlerFactory}

import org.neo4j.cypher._
import org.neo4j.cypher.internal.compiler.v2_3.test_helpers.CreateTempFileTestSupport
import org.neo4j.cypher.internal.frontend.v2_3.helpers.StringHelper.RichString
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.graphdb.security.URLAccessRule
import org.neo4j.kernel.GraphDatabaseAPI
import org.neo4j.test.TestGraphDatabaseFactory
import org.scalatest.BeforeAndAfterAll

class LoadCsvAcceptanceTest
  extends ExecutionEngineFunSuite with BeforeAndAfterAll
  with QueryStatisticsTestSupport with CreateTempFileTestSupport {

  def csvUrls(f: PrintWriter => Unit) = Seq(
    createCSVTempFileURL(f),
    createGzipCSVTempFileURL(f),
    createZipCSVTempFileURL(f)
  )

  test("import three strings") {
    val urls = csvUrls({
      writer =>
        writer.println("'Foo'")
        writer.println("'Foo'")
        writer.println("'Foo'")
    })

    for (url <- urls) {
      val result = execute(s"LOAD CSV FROM '$url' AS line CREATE (a {name: line[0]}) RETURN a.name")
      assertStats(result, nodesCreated = 3, propertiesSet = 3)
    }
  }

  test("import three numbers") {
    val urls = csvUrls({
      writer =>
        writer.println("1")
        writer.println("2")
        writer.println("3")
    })
    for (url <- urls) {
      val result =
        execute(s"LOAD CSV FROM '$url' AS line CREATE (a {number: line[0]}) RETURN a.number")
      assertStats(result, nodesCreated = 3, propertiesSet = 3)

      result.columnAs[Long]("a.number").toList === List("")
    }
  }

  test("import three rows numbers and strings") {
    val urls = csvUrls({
      writer =>
        writer.println("1, 'Aadvark'")
        writer.println("2, 'Babs'")
        writer.println("3, 'Cash'")
    })
    for (url <- urls) {
      val result = execute(s"LOAD CSV FROM '$url' AS line CREATE (a {name: line[0]}) RETURN a.name")
      assertStats(result, nodesCreated = 3, propertiesSet = 3)
    }
  }

  test("import three rows with headers") {
    val urls = csvUrls({
      writer =>
        writer.println("id,name")
        writer.println("1, 'Aadvark'")
        writer.println("2, 'Babs'")
        writer.println("3, 'Cash'")
    })
    for (url <- urls) {
      val result = execute(
        s"LOAD CSV WITH HEADERS FROM '$url' AS line CREATE (a {id: line.id, name: line.name}) RETURN a.name"
      )

      assertStats(result, nodesCreated = 3, propertiesSet = 6)
    }
  }

  test("import three rows with headers messy data") {
    val urls = csvUrls({
      writer =>
        writer.println("id,name,x")
        writer.println("1,'Aardvark',0")
        writer.println("2,'Babs'")
        writer.println("3,'Cash',1")
        writer.println("4,'Dice',\"\"")
        writer.println("5,'Emerald',")
    })
    for (url <- urls) {
      val result = execute(s"LOAD CSV WITH HEADERS FROM '$url' AS line RETURN line.x")
      assert(result.toList === List(
        Map("line.x" -> "0"),
        Map("line.x" -> null),
        Map("line.x" -> "1"),
        Map("line.x" -> ""),
        Map("line.x" -> null))
      )
    }
  }

  test("import three rows with headers messy data with predicate") {
    val urls = csvUrls({
      writer =>
        writer.println("id,name,x")
        writer.println("1,'Aardvark',0")
        writer.println("2,'Babs'")
        writer.println("3,'Cash',1")
        writer.println("4,'Dice',\"\"")
        writer.println("5,'Emerald',")
    })
    for (url <- urls) {
      val result = execute(s"LOAD CSV WITH HEADERS FROM '$url' AS line WITH line WHERE line.x IS NOT NULL RETURN line.name")
      assert(result.toList === List(
        Map("line.name" -> "'Aardvark'"),
        Map("line.name" -> "'Cash'"),
        Map("line.name" -> "'Dice'"))
      )
    }
  }

  test("should handle quotes") {
    val urls = csvUrls({
      writer =>
        writer.println("String without quotes")
        writer.println("'String, with single quotes'")
        writer.println("\"String, with double quotes\"")
        writer.println( """"String with ""quotes"" in it"""")
    })
    for (url <- urls) {
      val result = execute(s"LOAD CSV FROM '$url' AS line RETURN line as string").toList
      assert(result === List(
        Map("string" -> Seq("String without quotes")),
        Map("string" -> Seq("'String", " with single quotes'")),
        Map("string" -> Seq("String, with double quotes")),
        Map("string" -> Seq( """String with "quotes" in it"""))))
    }
  }

  test("should handle crlf line termination") {
    val urls = csvUrls({
      writer =>
        writer.print("1,'Aadvark',0\r\n")
        writer.print("2,'Babs'\r\n")
        writer.print("3,'Cash',1\r\n")
    })

    for (url <- urls) {
      val result = execute(s"LOAD CSV FROM '$url' AS line RETURN line")
      assert(result.toList === List(Map("line" -> Seq("1", "'Aadvark'", "0")), Map("line" -> Seq("2", "'Babs'")),
        Map("line" -> Seq("3", "'Cash'", "1"))))
    }
  }

  test("should handle lf line termination") {
    val urls = csvUrls({
      writer =>
        writer.print("1,'Aadvark',0\n")
        writer.print("2,'Babs'\n")
        writer.print("3,'Cash',1\n")
    })
    for (url <- urls) {
      val result = execute(s"LOAD CSV FROM '$url' AS line RETURN line")
      assert(result.toList === List(Map("line" -> Seq("1", "'Aadvark'", "0")), Map("line" -> Seq("2", "'Babs'")),
        Map("line" -> Seq("3", "'Cash'", "1"))))
    }
  }

  test("should handle cr line termination") {
    val urls = csvUrls({
      writer =>
        writer.print("1,'Aadvark',0\r")
        writer.print("2,'Babs'\r")
        writer.print("3,'Cash',1\r")
    })
    for (url <- urls) {
      val result = execute(s"LOAD CSV FROM '$url' AS line RETURN line")
      assert(result.toList === List(Map("line" -> Seq("1", "'Aadvark'", "0")), Map("line" -> Seq("2", "'Babs'")),
        Map("line" -> Seq("3", "'Cash'", "1"))))
    }
  }

  test("should handle custom field terminator") {
    val urls = csvUrls({
      writer =>
        writer.println("1;'Aadvark';0")
        writer.println("2;'Babs'")
        writer.println("3;'Cash';1")
    })
    for (url <- urls) {
      val result = execute(s"LOAD CSV FROM '$url' AS line FIELDTERMINATOR ';' RETURN line")
      assert(result.toList === List(Map("line" -> Seq("1", "'Aadvark'", "0")), Map("line" -> Seq("2", "'Babs'")),
        Map("line" -> Seq("3", "'Cash'", "1"))))
    }
  }

  test("should open file containing strange chars with '") {
    val filename = ensureNoIllegalCharsInWindowsFilePath("cypher '%^&!@#_)(098.:,;[]{}\\~$*+-")
    val url = createCSVTempFileURL(filename)({
      writer =>
        writer.println("something")
    })

    val result = execute("LOAD CSV FROM \"" + url + "\" AS line RETURN line as string").toList
    assert(result === List(Map("string" -> Seq("something"))))
  }

  test("should open file containing strange chars with \"") {
    val filename = ensureNoIllegalCharsInWindowsFilePath("cypher \"%^&!@#_)(098.:,;[]{}\\~$*+-")
    val url = createCSVTempFileURL(filename)({
      writer =>
        writer.println("something")
    })

    val result = execute(s"LOAD CSV FROM '$url' AS line RETURN line as string").toList
    assert(result === List(Map("string" -> Seq("something"))))
  }

  test("empty file does not create anything") {
    val urls = csvUrls(writer => {})
    for (url <- urls) {
      val result = execute(s"LOAD CSV FROM '$url' AS line CREATE (a {name: line[0]}) RETURN a.name")
      assertStats(result, nodesCreated = 0)
    }
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
    val urls = csvUrls({
      writer =>
        writer.println("DEPARTMENT ID;DEPARTMENT NAME;")
        writer.println("010-1010;MFG Supplies;")
        writer.println("010-1011;Corporate Procurement;")
        writer.println("010-1015;MFG - Engineering HQ;")
    })
    for (url <- urls) {
      val result = execute(s"LOAD CSV WITH HEADERS FROM '$url' AS line FIELDTERMINATOR ';' RETURN *").toList
      assert(result === List(
        Map("line" -> Map("DEPARTMENT ID" -> "010-1010", "DEPARTMENT NAME" -> "MFG Supplies",
          null.asInstanceOf[String] -> null)),
        Map("line" -> Map("DEPARTMENT ID" -> "010-1011", "DEPARTMENT NAME" -> "Corporate Procurement",
          null.asInstanceOf[String] -> null)),
        Map("line" -> Map("DEPARTMENT ID" -> "010-1015", "DEPARTMENT NAME" -> "MFG - Engineering HQ",
          null.asInstanceOf[String] -> null))
      ))
    }
  }

  test("should fail gracefully when loading missing file") {
    intercept[LoadExternalResourceException] {
      execute("LOAD CSV FROM 'file:///./these_are_not_the_droids_you_are_looking_for.csv' AS line CREATE (a {name:line[0]})")
    }
  }

  test("should be able to download data from the web") {
    val url = s"http://127.0.0.1:$port/test.csv".cypherEscape

    val result = executeScalar[Long](s"LOAD CSV FROM '${url}' AS line RETURN count(line)")
    result should equal(3)
  }

  test("should be able to download from a website when redirected and cookies are set") {
    val url = s"http://127.0.0.1:$port/redirect_test.csv".cypherEscape

    val result = executeScalar[Long](s"LOAD CSV FROM '${url}' AS line RETURN count(line)")
    result should equal(3)
  }

  test("should fail gracefully when getting 404") {
    intercept[LoadExternalResourceException] {
      execute(s"LOAD CSV FROM 'http://127.0.0.1:$port/these_are_not_the_droids_you_are_looking_for/' AS line CREATE (a {name:line[0]})")
    }
  }

  test("should fail gracefully when loading non existent (local) site") {
    intercept[LoadExternalResourceException] {
      execute("LOAD CSV FROM 'http://127.0.0.1:9999/these_are_not_the_droids_you_are_looking_for/' AS line CREATE (a {name:line[0]})")
    }
  }

  test("should reject URLs that are not valid") {
    intercept[LoadExternalResourceException] {
      execute(s"LOAD CSV FROM 'morsecorba://sos' AS line CREATE (a {name:line[0]})")
    }.getMessage should equal("Invalid URL 'morsecorba://sos': unknown protocol: morsecorba")

    intercept[LoadExternalResourceException] {
      execute(s"LOAD CSV FROM '://' AS line CREATE (a {name:line[0]})")
    }.getMessage should equal("Invalid URL '://': no protocol: ://")

    intercept[LoadExternalResourceException] {
      execute(s"LOAD CSV FROM 'foo.bar' AS line CREATE (a {name:line[0]})")
    }.getMessage should equal("Invalid URL 'foo.bar': no protocol: foo.bar")

    intercept[LoadExternalResourceException] {
      execute(s"LOAD CSV FROM 'jar:file:///tmp/bar.jar' AS line CREATE (a {name:line[0]})")
    }.getMessage should equal("Invalid URL 'jar:file:///tmp/bar.jar': no !/ in spec")

    intercept[LoadExternalResourceException] {
      execute("LOAD CSV FROM 'file://./blah.csv' AS line CREATE (a {name:line[0]})")
    }.getMessage should equal("Cannot load from URL 'file://./blah.csv': file URL may not contain an authority section (i.e. it should be 'file:///')")

    intercept[LoadExternalResourceException] {
      execute("LOAD CSV FROM 'file:///tmp/blah.csv?q=foo' AS line CREATE (a {name:line[0]})")
    }.getMessage should equal("Cannot load from URL 'file:///tmp/blah.csv?q=foo': file URL may not contain a query component")
  }

  test("should deny URLs for blocked protocols") {
    intercept[LoadExternalResourceException] {
      execute(s"LOAD CSV FROM 'jar:file:///tmp/bar.jar!/blah/foo.csv' AS line CREATE (a {name:line[0]})")
    }.getMessage should equal("Cannot load from URL 'jar:file:///tmp/bar.jar!/blah/foo.csv': loading resources via protocol 'jar' is not permitted")
  }

  test("should fail for file urls if local file access disallowed") {
    val db = new TestGraphDatabaseFactory()
      .newImpermanentDatabaseBuilder()
      .setConfig(GraphDatabaseSettings.allow_file_urls, "false")
      .newGraphDatabase()

    intercept[LoadExternalResourceException] {
      val engine = new ExecutionEngine(db)
      engine.execute(s"LOAD CSV FROM 'file:///tmp/blah.csv' AS line CREATE (a {name:line[0]})")
    }.getMessage should endWith(": configuration property 'allow_file_urls' is false")
  }

  test("should allow paths relative to authorized directory") {
    val dir = createTempDirectory("loadcsvroot")
    pathWrite(dir.resolve("tmp/blah.csv"))(
      writer =>
        writer.println("something")
    )

    val db = new TestGraphDatabaseFactory()
      .newImpermanentDatabaseBuilder()
      .setConfig(GraphDatabaseSettings.load_csv_file_url_root, dir.toString)
      .newGraphDatabase()

    val result = new ExecutionEngine(db).execute(s"LOAD CSV FROM 'file:///tmp/blah.csv' AS line RETURN line[0] AS field")
    result.toList should equal(List(Map("field" -> "something")))
  }

  test("should restrict file urls to be rooted within an authorized directory") {
    val dir = createTempDirectory("loadcsvroot")

    val db = new TestGraphDatabaseFactory()
      .newImpermanentDatabaseBuilder()
      .setConfig(GraphDatabaseSettings.load_csv_file_url_root, dir.toString)
      .newGraphDatabase()

    intercept[LoadExternalResourceException] {
      new ExecutionEngine(db)
        .execute(s"LOAD CSV FROM 'file:///../foo.csv' AS line RETURN line[0] AS field").size
    }.getMessage should endWith(" file URL points outside configured import directory")
  }

  test("should apply protocol rules set at db construction") {
    val url = createCSVTempFileURL({
      writer =>
        writer.println("something")
    })

    URL.setURLStreamHandlerFactory(new URLStreamHandlerFactory {
      override def createURLStreamHandler(protocol: String): URLStreamHandler =
        if (protocol != "testproto")
          null
        else
          new URLStreamHandler {
            override def openConnection(u: URL): URLConnection = new URL(url).openConnection()
          }
    })

    val db = new TestGraphDatabaseFactory()
      .addURLAccessRule( "testproto", new URLAccessRule {
        override def validate(gdb: GraphDatabaseAPI, url: URL): URL = url
      })
      .newImpermanentDatabaseBuilder()
      .newGraphDatabase()

    val result = new ExecutionEngine(db).execute(s"LOAD CSV FROM 'testproto://foo.bar' AS line RETURN line[0] AS field")
    result.toList should equal(List(Map("field" -> "something")))
  }

  test("eager queries should be handled correctly") {
    val urls = csvUrls({
      writer =>
        writer.println("id,title,country,year")
        writer.println("1,Wall Street,USA,1987")
        writer.println("2,The American President,USA,1995")
        writer.println("3,The Shawshank Redemption,USA,1994")
    })
    for (url <- urls) {
      execute(s"LOAD CSV WITH HEADERS FROM '$url' AS csvLine " +
        "MERGE (country:Country {name: csvLine.country}) " +
        "CREATE (movie:Movie {id: toInt(csvLine.id), title: csvLine.title, year:toInt(csvLine.year)})" +
        "CREATE (movie)-[:MADE_IN]->(country)")


      //make sure three unique movies are created
      val result = execute("match (m:Movie) return m.id AS id ORDER BY m.id").toList

      result should equal(List(Map("id" -> 1), Map("id" -> 2), Map("id" -> 3)))
      //empty database
      execute("MATCH (n) DETACH DELETE n")
    }
  }

  test("should be able to use expression as url") {
    val url = createCSVTempFileURL({
      writer =>
        writer.println("'Foo'")
        writer.println("'Foo'")
        writer.println("'Foo'")
    }).cypherEscape
    val first = url.substring(0, url.length / 2)
    val second = url.substring(url.length / 2)
    createNode(Map("prop" -> second))

    val result = execute(s"MATCH (n) WITH n, '$first' as prefix  LOAD CSV FROM prefix + n.prop AS line CREATE (a {name: line[0]}) RETURN a.name")
    assertStats(result, nodesCreated = 3, propertiesSet = 3)
  }

  test("empty headers file should not throw") {
    val urls = csvUrls({ _ => {} })
    for (url <- urls) {
      val result = execute(
        s"LOAD CSV WITH HEADERS FROM '$url' AS line RETURN count(*)"
      )

      result.toList should equal(List(Map("count(*)" -> 0)))
    }
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
  private def createFile(filename: String = "cypher", dir: String = null)(f: PrintWriter => Unit): String = createTempFileURL(filename, ".csv")(f)
}
