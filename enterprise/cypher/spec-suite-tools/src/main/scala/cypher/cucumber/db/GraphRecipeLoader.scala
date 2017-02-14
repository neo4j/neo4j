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

import java.io.FileInputStream
import java.security.{DigestInputStream, MessageDigest}

import cypher.cucumber.db.GraphRecipe.CypherScript

import scala.io.Codec
import scala.reflect.io.File
import scala.util.Try

object GraphRecipeLoader {
  def forRepository(repository: GraphFileRepository) =
    cachedGraph(loadGraph(repository))

  private def cachedGraph(factory: GraphRecipeLoader) =
    new scala.collection.mutable.HashMap[String, GraphRecipe.Descriptor[CypherScript]]() {
      override def apply(key: String) = getOrElseUpdate(key, factory(key))
    }

  private def loadGraph(repository: GraphFileRepository)(name: String) = {
    import org.json4s._
    import org.json4s.native.JsonMethods._
    implicit val formats = DefaultFormats + new GraphRecipe.AdviceSerializer

    val file = repository.graphImportFile(name)
    val json =
      Try(file.slurp(Codec.UTF8))
        .toOption
        .getOrElse(throw new IllegalArgumentException(s"$file should exist"))


    val parsed = parse(json)
    val extracted = parsed.extract[GraphRecipe.Descriptor[String]]
    val resolved = extracted.mapScripts(resolveScriptFile(repository))
    resolved
  }

  private def resolveScriptFile(repository: GraphFileRepository)(name: String) = {
    val file = repository.graphScriptFile(name)
    val hash = FileContentsDigest(file)
    CypherScript(file, hash)
  }

  private object FileContentsDigest {
    private val digest = MessageDigest.getInstance("SHA-1")

    def apply(file: File) = {
      val stream = new DigestInputStream(new FileInputStream(file.jfile), digest)
      val buffer = new Array[Byte](16384)
      try {
        while (stream.read(buffer) != -1) ()
        hexBytes(digest.digest())
      } finally {
        stream.close()
      }
    }

    private def hexBytes(bytes: Array[Byte]) =
      bytes.map("%02X" format _).mkString
  }
}
