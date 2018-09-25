/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package cypher.features

import java.io.File
import java.net.{URI, URL}
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

  // When running externally from e.g. TeamCity we will get a jar and can use the same procedure as for allTckScenarios
  // When running locally we need to use the file system instead
  def allAcceptanceScenarios: Seq[Scenario] = {
    val packageURL: URL = classOf[BaseFeatureTest].getProtectionDomain.getCodeSource.getLocation
    val resourcePath: String = "/acceptance/features"

    if (packageURL.toString.contains("jar"))
      parseClasspathFeatures(resourcePath).flatMap(_.scenarios)
    else {
      val featuresURI: URI = new URL(packageURL.toString + resourcePath).toURI
      parseFilesystemFeatures(new File(featuresURI)).flatMap(_.scenarios) //TODO: Change this back to the following line when TCK M12 is released
      //val all = CypherTCK.parseFilesystemFeatures(new File(featuresURI)).flatMap(_.scenarios)
    }
  }

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
