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
package org.neo4j.internal.cypher.acceptance.comparisonsupport

case class Versions(versions: Version*) {
  def +(other: Version): Versions = {
    val newVersions = if (!versions.contains(other)) versions :+ other else versions
    Versions(newVersions: _*)
  }
}

object Versions {
  implicit def versionToVersions(version: Version): Versions = Versions(version)

  val orderedVersions: Seq[Version] = Seq(V2_3, V3_1, V3_4, V3_5)

  val oldest: Version = orderedVersions.head
  val latest: Version = orderedVersions.last
  val all = Versions(orderedVersions: _*)

  def definedBy(preParserArgs: Array[String]): Versions = {
    val versions = all.versions.filter(_.isDefinedBy(preParserArgs))
    if (versions.nonEmpty) Versions(versions: _*) else all
  }

  object V2_3 extends Version("2.3")

  object V3_1 extends Version("3.1")

  object V3_4 extends Version("3.4") {
    // 3.4 has 3.5 runtime
    override val acceptedRuntimeVersionNames = Set("3.5")
  }

  object V3_5 extends Version("3.5") {
    // 3.5 may fall back to 3.1 deprecated features
    override val acceptedRuntimeVersionNames = Set("3.5", "3.1")
    override val acceptedPlannerVersionNames = Set("3.5", "3.1")
  }

}

case class Version(name: String) {
  // inclusive
  def ->(other: Version): Versions = {
    val fromIndex = Versions.orderedVersions.indexOf(this)
    val toIndex = Versions.orderedVersions.indexOf(other) + 1
    Versions(Versions.orderedVersions.slice(fromIndex, toIndex): _*)
  }

  val acceptedRuntimeVersionNames: Set[String] = Set(name)
  val acceptedPlannerVersionNames: Set[String] = Set(name)

  def isDefinedBy(preParserArgs: Array[String]): Boolean = preParserArgs.contains(name)
}
