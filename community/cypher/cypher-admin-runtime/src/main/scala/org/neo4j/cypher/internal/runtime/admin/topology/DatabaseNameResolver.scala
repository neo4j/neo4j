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
package org.neo4j.cypher.internal.runtime.admin.topology

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.DatabaseName
import org.neo4j.cypher.internal.ast.NamespacedName
import org.neo4j.cypher.internal.ast.ParameterName
import org.neo4j.cypher.internal.ast.ParameterProvider
import org.neo4j.cypher.internal.util.AssertionRunner
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModelConstants.DEFAULT_NAMESPACE
import org.neo4j.exceptions.InternalException
import org.neo4j.kernel.database.DatabaseReference
import org.neo4j.kernel.database.DatabaseReferenceImpl
import org.neo4j.kernel.database.DatabaseReferenceRepository
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.database.NormalizedDatabaseName

import scala.jdk.CollectionConverters.ListHasAsScala
import scala.jdk.CollectionConverters.SetHasAsScala

class DatabaseNameResolver(referenceResolver: DatabaseReferenceRepository) {

  private[topology] def resolveDatabaseNameToReference(
    namedDatabase: DatabaseName,
    params: ParameterProvider,
    cypherVersion: CypherVersion,
    ignoreNullInput: Boolean
  ): Set[DatabaseReference] = {
    val databaseReferences = referenceResolver.getAllDatabaseReferences.asScala
    val (name, namespace): (NormalizedDatabaseName, Option[NormalizedDatabaseName]) =
      namedDatabase match {
        case nn @ NamespacedName(_, namespace) =>
          val normalizedNamespace = namespace.map(new NormalizedDatabaseName(_))
          if (cypherVersion == CypherVersion.Cypher5) {
            normalizedNamespace match {
              case Some(ns) =>
                val deprecatedName = ns.name() + "." + nn.name
                databaseReferences.find(dr => dr.isComposite && dr.alias().equals(ns))
                  .map(_ => (new NormalizedDatabaseName(nn.name), normalizedNamespace))
                  .getOrElse((
                    new NormalizedDatabaseName(deprecatedName),
                    None
                  ))
              case None => (new NormalizedDatabaseName(nn.name), normalizedNamespace)
            }
          } else {
            (new NormalizedDatabaseName(nn.name), normalizedNamespace)
          }
        case pn: ParameterName =>
          val (namespace, name, _, _) = pn.getNameParts(
            params,
            DEFAULT_NAMESPACE,
            allowAndPassThroughNullInput = ignoreNullInput
          )
          if (name == null) {
            (null, None)
          } else {
            val normalizedNamespace = namespace.map(new NormalizedDatabaseName(_))
            normalizedNamespace match {
              case None => (new NormalizedDatabaseName(name), None)
              case Some(ns) =>
                databaseReferences.find(dr => dr.isComposite && dr.alias().equals(ns))
                  .map(_ => (new NormalizedDatabaseName(name), normalizedNamespace))
                  .getOrElse((
                    new NormalizedDatabaseName(ns.name() + "." + name),
                    None
                  ))
            }
          }
      }

    def assertAtMostOne(seq: Iterable[DatabaseReference]): Unit = {
      if (AssertionRunner.isAssertionsEnabled && seq.size > 1) {
        throw InternalException.internalError(
          this.getClass.getSimpleName,
          "SHOW DATABASE by name should only return 0 or 1 databases.",
          "SHOW DATABASE by name should only return 0 or 1 databases"
        )
      }
    }

    def displayName(namespace: Option[NormalizedDatabaseName], name: NormalizedDatabaseName): String = {
      (namespace, name) match {
        case (Some(ns), name) => s"${ns.name()}.${name.name()}"
        case (None, name)     => name.name()
      }
    }

    def primaryByNamedDatabaseId(namedDatabaseId: NamedDatabaseId): Option[DatabaseReference] = {
      val matching = databaseReferences.filter(_.isPrimary).filter(_.namedDatabaseId() == namedDatabaseId)
      assertAtMostOne(matching)
      matching.headOption
    }

    val filteredReferences: Set[DatabaseReference] = cypherVersion match {
      case CypherVersion.Cypher5 =>
        // Cypher 5: find reference by namespace/name split
        namespace match {
          case None => databaseReferences.collect {
              // ignore null values
              case _ if name == null => Set.empty
              // database
              case ref if ref.isPrimary && ref.alias().equals(name) => Set(ref)
              // alias
              case ref: DatabaseReferenceImpl.Internal if ref.alias().equals(name) =>
                primaryByNamedDatabaseId(ref.namedDatabaseId())
            }.flatten.toSet
          case Some(namespace) => databaseReferences.collect {
              // ignore null values
              case _ if name == null => Set.empty
              // composite constituent
              case c: DatabaseReferenceImpl.Composite if c.alias().equals(namespace) =>
                c.constituents().asScala
                  .filter(r => r.alias().equals(name))
                  .flatMap(dr => primaryByNamedDatabaseId(dr.namedDatabaseId()))
            }.flatten.toSet
        }
      case _ =>
        // Cypher 25+: find reference by full/display name
        databaseReferences.collect {
          // ignore null values
          case _ if name == null => Set.empty
          // database
          case ref if ref.isPrimary && ref.fullName().name().equals(displayName(namespace, name)) => Set(ref)
          // composite constituent
          case c: DatabaseReferenceImpl.Composite =>
            c.constituents().asScala
              .filter(constituent => constituent.fullName().name().equals(displayName(namespace, name)))
              .flatMap(dr => primaryByNamedDatabaseId(dr.namedDatabaseId()))
          // alias
          case ref: DatabaseReferenceImpl.Internal if ref.fullName().name().equals(displayName(namespace, name)) =>
            primaryByNamedDatabaseId(ref.namedDatabaseId())
        }.flatten.toSet
    }

    assertAtMostOne(filteredReferences)
    filteredReferences
  }
}
