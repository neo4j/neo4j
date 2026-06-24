/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.scoping.inspection_tool

import java.net.URI

import scala.annotation.nowarn

object FrontendInspector extends cask.MainRoutes {

  private val serverUri = URI.create("http://localhost:8080/")

  override def host: String = "localhost"
  override def port: Int = 8080
  override def debugMode: Boolean = true

  @nowarn("msg=match may not be exhaustive")
  @cask.get("/")
  def index(query: String = ""): cask.Response[String] = {
    htmlResponse(Renderer.page(query))
  }

  @nowarn("msg=match may not be exhaustive")
  @cask.get("/inspect")
  def inspect(query: String): cask.Response[String] = {
    htmlResponse(Renderer.renderInspection(query))
  }

  override def main(args: Array[String]): Unit = {
    val serverThread = new Thread(
      new Runnable {
        override def run(): Unit = FrontendInspector.super.main(args)
      },
      "frontend-inspector-server"
    )
    serverThread.setDaemon(false)
    serverThread.start()
    Runtime.awaitServerReady(serverUri)
    println(s"Frontend Inspector is ready at $serverUri")
    Runtime.openBrowser(serverUri)
    serverThread.join()
  }

  initialize()

  private def htmlResponse(body: String): cask.Response[String] =
    cask.Response(
      data = body,
      headers = Seq("Content-Type" -> "text/html; charset=utf-8")
    )
}
