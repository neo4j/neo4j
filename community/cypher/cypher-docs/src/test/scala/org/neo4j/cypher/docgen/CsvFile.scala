/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.docgen

import java.io.{PrintWriter, File}

class CsvFile(fileName: String)(implicit csvFilesDir: File) {
  def withContents(lines: Seq[String]*): String = {
    val csvFile = new File(csvFilesDir, fileName)
    val writer = new PrintWriter(csvFile, "UTF-8")
    lines.foreach(line => writer.println(line.mkString("\"", "\",\"", "\"")))
    writer.flush()
    writer.close()
    csvFile.toURI.toURL.toString.replace("\\", "\\\\")
  }
}
