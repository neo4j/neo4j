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
