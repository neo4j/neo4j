/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import com.sun.net.httpserver.HttpServer
import org.neo4j.test.ports.PortAuthority

import java.io.IOException
import java.net.InetSocketAddress
import java.util.concurrent.Executors

import scala.collection.mutable

trait HttpServerTestSupport {
  def boundInfo: InetSocketAddress
  def start(): Unit
  def stop(): Unit
}

class HttpServerTestSupportBuilder {
  private var allowedMethods: Set[String] = Set()
  private val mapping = new mutable.HashMap[String, HttpExchange => Unit]()
  private val filters = new mutable.HashMap[String, HttpExchange => Boolean]()
  private val transformations = new mutable.HashMap[String, HttpExchange => HttpExchange]()

  def onPathReplyWithData(path: String, data: Array[Byte]): Unit = {
    assert(path != null && !path.isEmpty)
    assert(data != null)
    allowedMethods = allowedMethods + HttpReplyer.GET
    mapping(path) = HttpReplyer.sendResponse(data)
  }

  def onPathRedirectTo(path: String, redirectTo: String): Unit = {
    assert(path != null && !path.isEmpty)
    assert(redirectTo != null && !redirectTo.isEmpty)
    allowedMethods = allowedMethods + HttpReplyer.GET
    mapping(path) = HttpReplyer.sendRedirect(redirectTo)
  }

  def onPathPermanentRedirectTo(path: String, redirectTo: String): Unit = {
    assert(path != null && !path.isEmpty)
    assert(redirectTo != null && !redirectTo.isEmpty)
    allowedMethods = allowedMethods + HttpReplyer.GET
    mapping(path) = HttpReplyer.sendPermanentRedirect(redirectTo)
  }

  def onPathReplyOnlyWhen(path: String, predicate: HttpExchange => Boolean): Unit = {
    assert(path != null && !path.isEmpty)
    assert(mapping.contains(path))
    filters(path) = predicate
  }

  def onPathTransformResponse(path: String, transformation: HttpExchange => HttpExchange): Unit = {
    assert(path != null && !path.isEmpty)
    assert(mapping.contains(path))
    transformations(path) = transformation
  }

  def build(): HttpServerTestSupport = {
    // Have PortAuthority allocate a port, use boundInfo to lookup the port later
    new HttpServerTestSupportImpl(
      PortAuthority.allocatePort(),
      allowedMethods,
      mapping.toMap,
      filters.toMap,
      transformations.toMap
    )
  }

  private class HttpServerTestSupportImpl(
    port: Int,
    allowedMethods: Set[String],
    mapping: Map[String, HttpExchange => Unit],
    filters: Map[String, HttpExchange => Boolean],
    transformations: Map[String, HttpExchange => HttpExchange]
  ) extends HttpServerTestSupport {

    private var optServer: Option[HttpServer] = None

    private def provideServer = {
      try {
        val address = new InetSocketAddress(port)
        HttpServer.create(address, 0)
      } catch {
        case ex: IOException =>
          throw new IllegalStateException("Error in creating and/or binding the server.", ex)
      }
    }

    def boundInfo = optServer.get.getAddress

    def start(): Unit = {
      optServer = Some(provideServer)
      val server = optServer.get

      server.createContext(
        "/",
        new HttpHandler {
          def handle(exchange: HttpExchange): Unit = {
            if (!allowedMethods.contains(exchange.getRequestMethod)) {
              HttpReplyer.sendMethodNotAllowed(exchange)
              return
            }

            val path = exchange.getRequestURI.getPath
            if (mapping.contains(path)) {
              if (filters.getOrElse(path, { (_: HttpExchange) => true })(exchange)) {
                val reply = transformations.getOrElse(path, identity[HttpExchange](_))(exchange)
                mapping(path)(reply)
              }
            }

            HttpReplyer.sendNotFound(exchange)
          }
        }
      )

      server.setExecutor(Executors.newFixedThreadPool(1))
      server.start()
    }

    def stop(): Unit = {
      optServer.foreach(server => server.stop(0))
    }
  }

  private object HttpReplyer {
    private val NO_DATA = Array[Byte]()
    val GET = "GET"
    val POST = "POST"

    def sendResponse(data: Array[Byte])(exchange: HttpExchange): Unit = {
      sendResponse(200, data)(exchange)
    }

    def sendPermanentRedirect(location: String)(exchange: HttpExchange): Unit = {
      exchange.getResponseHeaders.set("Location", location)
      sendResponse(301, NO_DATA)(exchange)
    }

    def sendRedirect(location: String)(exchange: HttpExchange): Unit = {
      exchange.getResponseHeaders.set("Location", location)
      sendResponse(307, NO_DATA)(exchange)
    }

    def sendNotFound(exchange: HttpExchange): Unit = {
      sendResponse(404, NO_DATA)(exchange)
    }

    def sendMethodNotAllowed(exchange: HttpExchange): Unit = {
      sendResponse(405, NO_DATA)(exchange)
    }

    def sendBadRequest(exchange: HttpExchange): Unit = {
      sendResponse(400, NO_DATA)(exchange)
    }

    def sendPermissionDenied(exchange: HttpExchange): Unit = {
      sendResponse(403, NO_DATA)(exchange)
    }

    private def sendResponse(statusCode: Int, data: Array[Byte])(exchange: HttpExchange): Unit = {
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

  def hasUserAgent(userAgent: String)(exchange: HttpExchange) = {
    val userAgentString = Option(exchange.getRequestHeaders.getFirst("User-Agent"))
    userAgentString.contains(userAgent)
  }

  def setCookie(cookie: String)(exchange: HttpExchange) = {
    exchange.getResponseHeaders.set("Set-Cookie", cookie)
    exchange
  }
}
