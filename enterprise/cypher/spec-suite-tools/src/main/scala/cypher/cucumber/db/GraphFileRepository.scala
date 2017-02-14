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

import java.util.UUID

import scala.reflect.io.{File, Path}

class GraphFileRepository(root: Path) {
  self =>

  val graphRecipeLoader = GraphRecipeLoader.forRepository(self)

  def graphImportFile(name: String) = root / File(s"$name.json")
  def graphScriptFile(name: String) = root / File(s"$name.cypher")
  def graphArchivePath(descriptor: GraphArchive.Descriptor) = root / Path(descriptor.toString)
  def graphArchiveImportStatusFile(descriptor: GraphArchive.Descriptor) = graphArchivePath(descriptor) / "IMPORTED"
  def temporarySnapshotPath = DeleteDirectory.onExit(root / Path(UUID.randomUUID().toString))
}


