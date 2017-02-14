/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cypher.cucumber.db

import java.io.IOException
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path => JPath, SimpleFileVisitor}
import java.util
import java.util.function.Consumer

import scala.reflect.io.Path

object DeleteDirectory {
  private val directoriesToDelete: util.Set[Path] = new util.LinkedHashSet[Path]()

  Runtime.getRuntime.addShutdownHook(new Thread(new Runnable() {
    override def run(): Unit = {
      directoriesToDelete.forEach(new Consumer[Path] {
        override def accept(path: Path): Unit = now(path)
      })
    }
  }))

  def onExit(path: Path): Path = {
    directoriesToDelete.add(path)
    path
  }

  def now(path: Path) = {
    if (path.exists) {
      Files.walkFileTree(path.jfile.toPath, new SimpleFileVisitor[JPath] {
        override def visitFile(file: JPath, attributes: BasicFileAttributes) =
          cont(Files.delete(file))

        override def postVisitDirectory(dir: JPath, exc: IOException) =
          cont(Files.delete(dir))

        private def cont(f: => Unit) = {
          f; FileVisitResult.CONTINUE
        }
      })
    }
  }
}
