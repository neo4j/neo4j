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
package cypher.feature.parser.reporting

import java.io.File
import javax.imageio.ImageIO

import cypher.feature.parser.reporting.CombinationChartWriter.UPPER_BOUND
import org.neo4j.cypher.internal.compiler.v3_2.ast.{QueryTag, QueryTags}
import org.scalatest.FunSuite
import org.scalatest.Matchers._

import scala.collection.JavaConverters._

class CombinationChartWriterTest extends FunSuite {

  test("should generate simple png image") {
    val writer = new CombinationChartWriter(new File("target"), "test")

    val tags = List("a", "b", "c")

    val data = List(List(Int.box(-1), Int.box(25), Int.box(50)).asJava,
                    List(Int.box(25), Int.box(-1), Int.box(75)).asJava,
                    List(Int.box(50), Int.box(75), Int.box(-1)).asJava).asJava

    writer.dumpPNG(data)

    val file = new File("target", "test.png")
    file.exists() shouldBe true
    ImageIO.read(file).getHeight shouldBe tags.length * 10
  }

  test("should generate png from tags") {
    val writer = new CombinationChartWriter(new File("target"), "tags")

    val tags: List[QueryTag] = QueryTags.all.toList

    val data = tags.map { tag =>
      tags.map { otherTag =>
        Integer.valueOf(Math.abs((tag.toString + otherTag.toString).toSet.hashCode) % UPPER_BOUND)
      }.asJava
    }.asJava

    writer.dumpPNG(data)

    val file = new File("target", "tags.png")
    file.exists() shouldBe true
    ImageIO.read(file).getHeight shouldBe QueryTags.all.size * 10
  }

  test("should generate simple html table") {
    val writer = new CombinationChartWriter(new File("target"), "table")

    val tags = List("a", "b", "c")

    val data = tags.map { tag =>
      tags.map { otherTag =>
        if (tag == otherTag) Int.box(-1)
        else Integer.valueOf(Math.abs((tag + otherTag).toSet.hashCode) % UPPER_BOUND)
      }.asJava
    }.asJava

    writer.dumpHTML(data, tags.asJava)

    val file = new File("target", "table.html")
    file.exists() shouldBe true
  }

  test("should generate html from tags") {
    val writer = new CombinationChartWriter(new File("target"), "tags")

    val tags = QueryTags.all.toList

    val data = tags.map { tag =>
      tags.map { otherTag =>
        Integer.valueOf(Math.abs((tag.toString + otherTag.toString).toSet.hashCode) % UPPER_BOUND)
      }.asJava
    }.asJava

    writer.dumpHTML(data, tags.map(_.toString).asJava)

    val file = new File("target", "tags.html")
    file.exists() shouldBe true
  }

}
