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

import java.awt.Desktop
import java.net.HttpURLConnection
import java.net.URI
import java.net.URL
import java.util.Locale
import java.util.concurrent.TimeUnit

object Runtime {

  def awaitServerReady(uri: URI): Unit = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15)
    var ready = false
    while (!ready && System.nanoTime() < deadline) {
      ready = canReach(uri)
      if (!ready) {
        Thread.sleep(150L)
      }
    }
    if (!ready) {
      throw new IllegalStateException(s"FrontendInspector did not become ready at $uri")
    }
  }

  private def canReach(uri: URI): Boolean = {
    val connection = new URL(uri.toString).openConnection().asInstanceOf[HttpURLConnection]
    connection.setConnectTimeout(500)
    connection.setReadTimeout(500)
    connection.setRequestMethod("GET")
    try {
      val status = connection.getResponseCode
      status >= 200 && status < 500
    } catch {
      case _: Throwable => false
    } finally {
      connection.disconnect()
    }
  }

  def openBrowser(uri: URI): Unit = {
    if (Desktop.isDesktopSupported) {
      val desktop = Desktop.getDesktop
      if (desktop.isSupported(Desktop.Action.BROWSE)) {
        desktop.browse(uri)
        return
      }
    }
    val osName = System.getProperty("os.name", "").toLowerCase(Locale.ROOT)
    val command =
      if (osName.contains("mac")) Seq("open", uri.toString)
      else if (osName.contains("win")) Seq("rundll32", "url.dll,FileProtocolHandler", uri.toString)
      else Seq("xdg-open", uri.toString)
    new ProcessBuilder(command: _*).start()
  }
}
