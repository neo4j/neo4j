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
