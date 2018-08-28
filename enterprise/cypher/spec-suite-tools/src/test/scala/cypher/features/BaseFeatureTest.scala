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
package cypher.features

import java.io.File
import java.net.URL
import java.nio.file.{FileSystems, Files, Paths}
import java.util

import org.opencypher.tools.tck.api.CypherTCK.{featureSuffix, featuresPath, parseFeature}
import org.opencypher.tools.tck.api.{Feature, Scenario}

import scala.collection.JavaConverters._
import scala.io.{Codec, Source}

abstract class BaseFeatureTest {

  //  ---- TCK methods
  // TODO: Remove those when M12 is released

  def parseFilesystemFeatures(directory: File): Seq[Feature] = {
    require(directory.isDirectory)
    val featureFileNames = directory.listFiles.filter(_.getName.endsWith(featureSuffix))
    featureFileNames.map(parseFilesystemFeature)
  }

  def parseFilesystemFeature(file: File): Feature = {
    parseFeature(file.getAbsolutePath, Source.fromFile(file)(Codec.UTF8).mkString)
  }

  def parseClasspathFeature(pathUrl: URL): Feature = {
    parseFeature(pathUrl.toString, Source.fromURL(pathUrl)(Codec.UTF8).mkString)
  }

  def allTckScenarios: Seq[Scenario] = parseClasspathFeatures(featuresPath).flatMap(_.scenarios)

  def parseClasspathFeatures(path: String): Seq[Feature] = {
    val resource = getClass.getResource(path).toURI
    val fs = FileSystems.newFileSystem(resource, new util.HashMap[String, String]) // Needed to support `Paths.get` below
    try {
      val directoryPath = Paths.get(resource)
      val paths = Files.newDirectoryStream(directoryPath).asScala.toSeq
      val featurePathStrings = paths.map(path => path.toString).filter(_.endsWith(featureSuffix))
      val featureUrls = featurePathStrings.map(getClass.getResource(_))
      featureUrls.map(parseClasspathFeature)
    } finally {
      fs.close()
    }
  }
  // ---- TCK methods end

  def filterScenarios(allScenarios: Seq[Scenario], featureToRun: String, scenarioToRun: String): Seq[Scenario] = {
    if (featureToRun.nonEmpty) {
      val filteredFeature = allScenarios.filter(s => s.featureName.contains(featureToRun))
      if (scenarioToRun.nonEmpty) {
        filteredFeature.filter(s => s.name.contains(scenarioToRun))
      } else
        filteredFeature
    } else if (scenarioToRun.nonEmpty) {
      allScenarios.filter(s => s.name.contains(scenarioToRun))
    } else
      allScenarios
  }
}
