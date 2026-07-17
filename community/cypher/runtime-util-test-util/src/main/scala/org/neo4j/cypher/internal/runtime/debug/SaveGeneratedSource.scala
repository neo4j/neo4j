/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.debug

import org.neo4j.codegen.api.CodeGeneration.GENERATED_SOURCE_LOCATION_PROPERTY
import org.neo4j.cypher.internal.util.test_helpers.StackableBeforeAndAfterEach
import org.scalatest.Suite

import java.io.File
import java.nio.file.FileVisitResult
import java.nio.file.FileVisitResult.CONTINUE
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.SimpleFileVisitor
import java.nio.file.attribute.BasicFileAttributes

/**
 * This trait allows debugging generated queries, by generating queries through java source, then making sure that
 * the generated source is written to a place where Intellij can find it.
 *
 * How to use:
 * 1. Add this trait to the test class containing your test.
 * 2. Prefix your query with `CYPHER debug=generate_java_source`
 * 3. Mark `[your-maven-module]/target/generated-test-sources/cypher` as "Generated Sources Root".
 * 4. Make sure you have a breakpoint set to somewhere before execution enters the generated code,
 *    but after the code has been generated.
 * 5. When you run you test, as the first breakpoint triggers, find the directory
 *    `[your-maven-module]/target/generated-test-sources/cypher`, right click and select "Synchronize 'cypher'"
 *    If you have not done so before, this is a good time to "Mark Directory as" "Generated Sources Root".
 * 6. Now you should see the source file for the generated query, and be able to set breakpoints in that code,
 *    as well as stepping through it.
 * 7. Note that every time you re-run your test, you will have to repeat steps 4 to 6, since new code will be
 *    generated each time.
 */
trait SaveGeneratedSource extends StackableBeforeAndAfterEach {
  self: Suite =>
  val saveGeneratedSourceEnabled: Boolean
  val keepSourceFilesAfterTestFinishes: Boolean = false
  val logSaveLocation: Boolean = true

  private var generatedSources: Option[Path] = None

  registerBeforeEach {
    if (saveGeneratedSourceEnabled) {
      // Resolve the generated source location relative to classpath of the test class that mixes in this trait.
      val classPathUrl = getClass.getProtectionDomain.getCodeSource.getLocation
      val classPath = Paths.get(classPathUrl.toURI)
      val modulePath = classPath.resolve(Paths.get("..", "..")).normalize()
      // If the module directory is resolved correctly, we assign the generated source location
      if (
        Files.isDirectory(modulePath.resolve(Paths.get("src", "test", "scala")).resolve(getClass.getName.replace(
          ".",
          File.pathSeparator
        )).getParent)
        && Files.isDirectory(modulePath.resolve("target"))
      ) {
        setLocation(modulePath.resolve("target").resolve("generated-test-sources").resolve("cypher"))
      } else {
        throw new IllegalArgumentException(
          s"Could not resolve directory for saving generated source code relative to the module directory '$modulePath'."
        )
      }
    }
  }

  private def setLocation(location: Path) = {
    if (logSaveLocation) System.err.println(s"Will save generated sources to $location")
    generatedSources = Some(location)
    System.setProperty(GENERATED_SOURCE_LOCATION_PROPERTY, location.toString)
  }

  registerAfterEach {
    if (saveGeneratedSourceEnabled) {
      System.clearProperty("org.neo4j.cypher.DEBUG.generated_source_location")
      if (!keepSourceFilesAfterTestFinishes) {
        generatedSources.foreach { location =>
          Files.walkFileTree(
            location,
            new SimpleFileVisitor[Path] {
              override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
                Files.delete(file)
                CONTINUE
              }
            }
          )
        }
      }
    }
  }
}
