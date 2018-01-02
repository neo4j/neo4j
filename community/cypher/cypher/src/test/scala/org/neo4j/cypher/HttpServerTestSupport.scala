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

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import java.net.{InetAddress, InetSocketAddress}
import java.io.IOException
import java.util.concurrent.Executors
import scala.collection.mutable
import scala.collection.mutable.HashMap

trait HttpServerTestSupport {
  def boundInfo: InetSocketAddress
  def start()
  def stop()
}

class HttpServerTestSupportBuilder {
  val ASK_OS_TO_PROVIDE_A_PORT = 0
  private var port = ASK_OS_TO_PROVIDE_A_PORT
  private var allowedMethods: Set[String] = Set()
  private val mapping = new mutable.HashMap[String, (HttpExchange => Unit)]()
  private val filters = new mutable.HashMap[String, (HttpExchange => Boolean)]()
  private val transformations = new mutable.HashMap[String, (HttpExchange => HttpExchange)]()

  def withPort(newPort: Int) {
    assert(newPort >= 0 && newPort < 65536)
    port = newPort
  }

  def onPathReplyWithData(path: String, data: Array[Byte]) {
    assert(path != null && !path.isEmpty)
    assert(data != null)
    allowedMethods = allowedMethods + HttpReplyer.GET
    mapping(path) = HttpReplyer.sendResponse(data)
  }

  def onPathRedirectTo(path: String, redirectTo: String) {
    assert(path != null && !path.isEmpty)
    assert(redirectTo != null && !redirectTo.isEmpty)
    allowedMethods = allowedMethods + HttpReplyer.GET
    mapping(path) = HttpReplyer.sendRedirect(redirectTo)
  }

  def onPathReplyOnlyWhen(path: String, predicate: HttpExchange => Boolean) {
    assert(path != null && !path.isEmpty)
    assert(mapping.contains(path))
    filters(path) = predicate
  }

  def onPathTransformResponse(path: String, transformation: HttpExchange => HttpExchange) {
    assert(path != null && !path.isEmpty)
    assert(mapping.contains(path))
    transformations(path) = transformation
  }

  def build(): HttpServerTestSupport = {
    new HttpServerTestSupportImpl(port, allowedMethods, mapping.toMap, filters.toMap, transformations.toMap)
  }

  private class HttpServerTestSupportImpl(port: Int, allowedMethods: Set[String],
                                  mapping: Map[String, (HttpExchange => Unit)],
                                  filters: Map[String, (HttpExchange => Boolean)],
                                  transformations: Map[String, (HttpExchange => HttpExchange)])
    extends HttpServerTestSupport {

    private var optServer: Option[HttpServer] = None

    private def provideServer = {
      try {
        val address = new InetSocketAddress(InetAddress.getLoopbackAddress, port)
        HttpServer.create(address, 0)
      } catch {
        case (ex: IOException) =>
          throw new IllegalStateException("Error in creating and/or binding the server.", ex)
      }
    }

    def boundInfo = optServer.get.getAddress

    def start {
      optServer = Some(provideServer)
      val server = optServer.get

      server.createContext("/", new HttpHandler {
        def handle(exchange: HttpExchange) {
          if (!allowedMethods.contains(exchange.getRequestMethod)) {
            HttpReplyer.sendMethodNotAllowed(exchange)
            return
          }

          val path = exchange.getRequestURI.getPath
          if (mapping.contains(path)) {
            if (filters.getOrElse(path, {_: HttpExchange => true})(exchange)) {
              val reply = transformations.getOrElse(path, identity[HttpExchange](_))(exchange)
              mapping(path)(reply)
            }
          }

          HttpReplyer.sendNotFound(exchange)
        }
      })

      server.setExecutor(Executors.newFixedThreadPool(1))
      server.start()
    }

    def stop() {
      optServer.foreach(server => server.stop(0))
    }
  }

  private object HttpReplyer {
    private val NO_DATA = Array[Byte]()
    val GET = "GET"
    val POST = "POST"

    def sendResponse(data: Array[Byte])(exchange: HttpExchange) {
      sendResponse(200, data)(exchange)
    }

    def sendRedirect(location: String)(exchange: HttpExchange) {
      exchange.getResponseHeaders.set("Location", location)
      sendResponse(307, NO_DATA)(exchange)
    }

    def sendNotFound(exchange: HttpExchange) {
      sendResponse(404, NO_DATA)(exchange)
    }

    def sendMethodNotAllowed(exchange: HttpExchange) {
      sendResponse(405, NO_DATA)(exchange)
    }

    def sendBadRequest(exchange: HttpExchange) {
      sendResponse(400, NO_DATA)(exchange)
    }

    def sendPermissionDenied(exchange: HttpExchange) {
      sendResponse(403, NO_DATA)(exchange)
    }

    private def sendResponse(statusCode: Int, data: Array[Byte])(exchange: HttpExchange) {
      exchange.sendResponseHeaders(statusCode, data.length)
      val os = exchange.getResponseBody
      try {
        os.write(data)
      } finally {
        os.close()
      }
    }
  }
}

object HttpServerTestSupport {
  def hasCookie(cookie: String)(exchange: HttpExchange) = {
    val cookieString = Option(exchange.getRequestHeaders.getFirst("Cookie"))
    cookieString.exists(cookie => cookie.split(";").contains(cookie))
  }

  def setCookie(cookie: String)(exchange: HttpExchange) = {
    exchange.getResponseHeaders.set("Set-Cookie", cookie)
    exchange
  }
}

