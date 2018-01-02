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
package org.neo4j.cypher

import java.io.ByteArrayOutputStream
import java.util.zip.{DeflaterOutputStream, GZIPOutputStream}
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import org.eclipse.jetty.server.handler.{AbstractHandler, ContextHandler, ContextHandlerCollection}
import org.eclipse.jetty.server.{Handler, Request, Server}
import org.scalatest.BeforeAndAfterAll

class LoadCsvCompressionAcceptanceTest extends ExecutionEngineFunSuite with BeforeAndAfterAll {
  private val CSV =
    """a1,b1,c1,d1
      |a2,b2,c2,d2""".stripMargin

  private val server = new TestServer

  override protected def afterAll() = {
    server.stop()
  }

  override protected def beforeAll() = {
    server.start()
  }

  test("should handle uncompressed csv over http") {
    val result = execute("LOAD CSV FROM 'http://localhost:8080/csv' AS lines RETURN lines")

    result.toList should equal(List(
      Map("lines" -> Seq("a1", "b1", "c1", "d1")),
      Map("lines" -> Seq("a2", "b2", "c2", "d2"))
      ))
  }

  test("should handle gzipped csv over http") {
    val result = execute("LOAD CSV FROM 'http://localhost:8080/gzip' AS lines RETURN lines")

    result.toList should equal(List(
      Map("lines" -> Seq("a1", "b1", "c1", "d1")),
      Map("lines" -> Seq("a2", "b2", "c2", "d2"))
    ))
  }

  test("should handle deflated csv over http") {
    val result = execute("LOAD CSV FROM 'http://localhost:8080/deflate' AS lines RETURN lines")

    result.toList should equal(List(
      Map("lines" -> Seq("a1", "b1", "c1", "d1")),
      Map("lines" -> Seq("a2", "b2", "c2", "d2"))
    ))
  }

   /*
    * Simple server that handles csv requests in plain text, gzip and deflate
    */
  private class TestServer {
    private val server: Server = new Server(8080)
    private val handlers = new ContextHandlerCollection()
    addHandler("/csv", new CsvHandler)
    addHandler("/gzip", new GzipCsvHandler)
    addHandler("/deflate", new DeflateCsvHandler)
    server.setHandler(handlers)

    def start() = {
      server.start()
    }

    def stop() = server.stop()

    private def addHandler(path: String, handler: Handler): Unit = {
      val contextHandler = new ContextHandler()
      contextHandler.setContextPath(path)
      contextHandler.setHandler(handler)
      handlers.addHandler(contextHandler)
    }
  }

  /*
   * Returns csv in plain text
   */
  private class CsvHandler extends AbstractHandler {

    override def handle(s: String, request: Request, httpServletRequest: HttpServletRequest,
                        httpServletResponse: HttpServletResponse): Unit = {
      httpServletResponse.setContentType("text/csv")
      httpServletResponse.setStatus(HttpServletResponse.SC_OK)
      httpServletResponse.getWriter.print(CSV)
      request.setHandled(true)
    }
  }

  /*
   * Returns csv compressed with gzip
   */
  private class GzipCsvHandler extends AbstractHandler {

    override def handle(s: String, request: Request, httpServletRequest: HttpServletRequest,
                        httpServletResponse: HttpServletResponse): Unit = {
      httpServletResponse.setContentType("text/csv")
      httpServletResponse.setStatus(HttpServletResponse.SC_OK)
      httpServletResponse.setHeader("content-encoding", "gzip")
      //write compressed data to a byte array
      val stream = new ByteArrayOutputStream(CSV.length)
      val gzipStream = new GZIPOutputStream(stream)
      gzipStream.write(CSV.getBytes)
      gzipStream.close()
      val compressed = stream.toByteArray
      stream.close()

      //respond with the compressed data
      httpServletResponse.getOutputStream.write(compressed)
      request.setHandled(true)
    }
  }

  /*
   * Returns csv compressed with deflate
   */
  private class DeflateCsvHandler extends AbstractHandler {

    override def handle(s: String, request: Request, httpServletRequest: HttpServletRequest,
                        httpServletResponse: HttpServletResponse): Unit = {
      httpServletResponse.setContentType("text/csv")
      httpServletResponse.setStatus(HttpServletResponse.SC_OK)
      httpServletResponse.setHeader("content-encoding", "deflate")

      //write deflated data to byte array
      val stream = new ByteArrayOutputStream(CSV.length)
      val deflateStream = new DeflaterOutputStream(stream)
      deflateStream.write(CSV.getBytes)
      deflateStream.close()
      val compressed = stream.toByteArray
      stream.close()

      //respond with the deflated data
      httpServletResponse.getOutputStream.write(compressed)
      request.setHandled(true)
    }
  }
}
