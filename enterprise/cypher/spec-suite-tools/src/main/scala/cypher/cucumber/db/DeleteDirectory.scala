/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
