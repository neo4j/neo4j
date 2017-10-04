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

import java.nio.file.Files

import scala.reflect.io.Path

class GraphArchiveLibrary(val repository: GraphFileRepository) {

  def recipe(name: String) = repository.graphRecipeLoader(name)

  def lendForReadOnlyUse(archiveUse: GraphArchive.Use.ReadOnly)(implicit importer: GraphArchiveImporter): Path =
    locateArchive(archiveUse.archive)

  def snapshotForUpdatingUse(archiveUse: GraphArchive.Use.Updating)(implicit importer: GraphArchiveImporter): Path = {
    val archivePath = locateArchive(archiveUse.archive)
    val snapshotPath = repository.temporarySnapshotPath
    Files.copy(archivePath.jfile.toPath, snapshotPath.jfile.toPath)
    snapshotPath
  }

  private def locateArchive(archiveDescriptor: GraphArchive.Descriptor)(implicit importer: GraphArchiveImporter): Path = {
    val archivePath = repository.graphArchivePath(archiveDescriptor)

    val statusFile = repository.graphArchiveImportStatusFile(archiveDescriptor)
    if (!statusFile.exists) {
      DeleteDirectory.now(archivePath)
      archivePath.createDirectory(failIfExists = true)
      importer.importArchive(archiveDescriptor, archivePath)
      statusFile.createFile(failIfExists = true)
    }

    validateArchivePath(archivePath)
  }

  private def validateArchivePath(archivePath: Path): Path =
    if (archivePath.canRead)
      archivePath
    else
      throw new IllegalStateException("Cannot read archive path: " + archivePath.toString())
}
