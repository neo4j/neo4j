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

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.MapBasedParameterProvider
import org.neo4j.cypher.internal.ast.NamespacedName
import org.neo4j.cypher.internal.ast.ParameterName
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.exceptions.ParameterWrongTypeException
import org.neo4j.kernel.database.DatabaseIdFactory
import org.neo4j.kernel.database.DatabaseReferenceImpl
import org.neo4j.kernel.database.DatabaseReferenceRepository
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualValues

import java.util.UUID

import scala.jdk.CollectionConverters.SetHasAsJava

class DatabaseNameResolverTest extends CypherFunSuite {

  private val pos = InputPosition.NONE

  // Primary database "mydb"
  private val primaryDbId = DatabaseIdFactory.from("mydb", UUID.randomUUID())
  private val primaryRef = new DatabaseReferenceImpl.Internal(new NormalizedDatabaseName("mydb"), primaryDbId, true)

  // Non-primary alias "myalias" pointing to the same underlying db as primaryRef
  private val aliasRef = new DatabaseReferenceImpl.Internal(new NormalizedDatabaseName("myalias"), primaryDbId, false)

  // Constituent db "constituent1" — primary reference in root namespace
  private val constituentDbId = DatabaseIdFactory.from("constituent1", UUID.randomUUID())

  private val constituentPrimary =
    new DatabaseReferenceImpl.Internal(new NormalizedDatabaseName("constituent1"), constituentDbId, true)

  // Same constituent but namespaced under "mycomposite" — this lives inside the composite
  private val constituentInComposite = new DatabaseReferenceImpl.Internal(
    new NormalizedDatabaseName("constituent1"),
    new NormalizedDatabaseName("mycomposite"),
    constituentDbId,
    false
  )

  // Composite "mycomposite" whose constituent is constituentInComposite
  private val compositeDbId = DatabaseIdFactory.from("mycomposite", UUID.randomUUID())

  private val compositeRef = new DatabaseReferenceImpl.Composite(
    new NormalizedDatabaseName("mycomposite"),
    compositeDbId,
    Set[org.neo4j.kernel.database.DatabaseReference](constituentInComposite).asJava
  )

  // Legacy graph.db
  private val oldPrimaryDb = DatabaseIdFactory.from("graph.db", UUID.randomUUID())

  private val oldPrimaryRef =
    new DatabaseReferenceImpl.Internal(new NormalizedDatabaseName("graph.db"), oldPrimaryDb, true)

  private val allRefs: java.util.Set[org.neo4j.kernel.database.DatabaseReference] =
    Set[org.neo4j.kernel.database.DatabaseReference](
      primaryRef,
      aliasRef,
      constituentPrimary,
      constituentInComposite,
      compositeRef,
      oldPrimaryRef
    ).asJava

  private def makeResolver(): DatabaseNameResolver = {
    val repo = mock[DatabaseReferenceRepository]
    when(repo.getAllDatabaseReferences).thenReturn(allRefs)
    new DatabaseNameResolver(repo)
  }

  // --- NamespacedName + Cypher5 (resolves by alias()) ---

  test("Cypher5: NamespacedName resolves primary database by alias") {
    val refs = makeResolver().resolveDatabaseNameToReference(
      NamespacedName("mydb")(pos),
      null,
      CypherVersion.Cypher5,
      ignoreNullInput = false
    )
    refs shouldBe Set(primaryRef)
  }

  test("Cypher5: NamespacedName resolves alias to its primary database") {
    val refs = makeResolver().resolveDatabaseNameToReference(
      NamespacedName("myalias")(pos),
      null,
      CypherVersion.Cypher5,
      ignoreNullInput = false
    )
    refs shouldBe Set(primaryRef)
  }

  test("Cypher5: NamespacedName returns empty set for unknown database") {
    val refs = makeResolver().resolveDatabaseNameToReference(
      NamespacedName("unknown")(pos),
      null,
      CypherVersion.Cypher5,
      ignoreNullInput = false
    )
    refs shouldBe empty
  }

  test("Cypher5: NamespacedName with namespace resolves composite constituent") {
    val refs = makeResolver().resolveDatabaseNameToReference(
      NamespacedName(List("constituent1"), Some("mycomposite"))(pos),
      null,
      CypherVersion.Cypher5,
      ignoreNullInput = false
    )
    refs shouldBe Set(constituentPrimary)
  }

  test("Cypher5: NamespacedName with namespace returns empty when constituent not found in composite") {
    val refs = makeResolver().resolveDatabaseNameToReference(
      NamespacedName(List("unknown"), Some("mycomposite"))(pos),
      null,
      CypherVersion.Cypher5,
      ignoreNullInput = false
    )
    refs shouldBe empty
  }

  test(
    "Cypher5: NamespacedName with namespace uses dotted-name fallback when namespace is not a composite"
  ) {
    val refs = makeResolver().resolveDatabaseNameToReference(
      NamespacedName(List("db"), Some("graph"))(pos),
      null,
      CypherVersion.Cypher5,
      ignoreNullInput = false
    )
    refs shouldBe Set(oldPrimaryRef)
  }

  // --- NamespacedName + Cypher25 (resolves by fullName()) ---

  test("Cypher25: NamespacedName resolves primary database by fullName") {
    val refs = makeResolver().resolveDatabaseNameToReference(
      NamespacedName("mydb")(pos),
      null,
      CypherVersion.Cypher25,
      ignoreNullInput = false
    )
    refs shouldBe Set(primaryRef)
  }

  test("Cypher25: NamespacedName resolves alias to its primary database by fullName") {
    val refs = makeResolver().resolveDatabaseNameToReference(
      NamespacedName("myalias")(pos),
      null,
      CypherVersion.Cypher25,
      ignoreNullInput = false
    )
    refs shouldBe Set(primaryRef)
  }

  test("Cypher25: NamespacedName with namespace resolves composite constituent by fullName") {
    val refs = makeResolver().resolveDatabaseNameToReference(
      NamespacedName(List("constituent1"), Some("mycomposite"))(pos),
      null,
      CypherVersion.Cypher25,
      ignoreNullInput = false
    )
    refs shouldBe Set(constituentPrimary)
  }

  test("Cypher25: NamespacedName returns empty set for unknown database") {
    val refs = makeResolver().resolveDatabaseNameToReference(
      NamespacedName("unknown")(pos),
      null,
      CypherVersion.Cypher25,
      ignoreNullInput = false
    )
    refs shouldBe empty
  }

  test(
    "Cypher25: NamespacedName with namespace uses dotted-name fallback when namespace is not a composite"
  ) {
    val refs = makeResolver().resolveDatabaseNameToReference(
      NamespacedName(List("db"), Some("graph"))(pos),
      null,
      CypherVersion.Cypher25,
      ignoreNullInput = false
    )
    refs shouldBe Set(oldPrimaryRef)
  }

  // --- ParameterName ---

  private val dbParam = "dbParam"
  private val param = ExplicitParameter(dbParam, CTString)(pos)
  private val paramName = ParameterName(param)(pos)

  for (cypherversion <- CypherVersion.values()) {

    test(s"${cypherversion.description}: ParameterName: null (NoValue) input with ignoreNullInput returns empty set") {
      val provider = MapBasedParameterProvider(VirtualValues.map(Array(dbParam), Array(Values.NO_VALUE)))
      val refs = makeResolver().resolveDatabaseNameToReference(
        paramName,
        provider,
        cypherversion,
        ignoreNullInput = true
      )
      refs shouldBe empty
    }

    test(
      s"${cypherversion.description}: ParameterName: null (NoValue) input without ignoreNullInput returns empty set"
    ) {
      val provider = MapBasedParameterProvider(VirtualValues.map(Array(dbParam), Array(Values.NO_VALUE)))
      the[ParameterWrongTypeException] thrownBy makeResolver().resolveDatabaseNameToReference(
        paramName,
        provider,
        cypherversion,
        ignoreNullInput = false
      ) should have message s"Expected parameter $$$dbParam to have type String but was NO_VALUE"
    }

    test(s"${cypherversion.description}: ParameterName: simple name resolves to primary database") {
      val provider = MapBasedParameterProvider(VirtualValues.map(Array(dbParam), Array(Values.stringValue("mydb"))))
      val refs = makeResolver().resolveDatabaseNameToReference(
        paramName,
        provider,
        cypherversion,
        ignoreNullInput = false
      )
      refs shouldBe Set(primaryRef)
    }

    test(
      s"${cypherversion.description}: ParameterName: dotted name resolves composite constituent when namespace is a known composite"
    ) {
      val provider =
        MapBasedParameterProvider(VirtualValues.map(
          Array(dbParam),
          Array(Values.stringValue("mycomposite.constituent1"))
        ))
      val refs = makeResolver().resolveDatabaseNameToReference(
        paramName,
        provider,
        cypherversion,
        ignoreNullInput = false
      )
      refs shouldBe Set(constituentPrimary)
    }

    test(
      s"${cypherversion.description}: ParameterName: dotted name uses full literal name when namespace is not a known composite"
    ) {
      val provider =
        MapBasedParameterProvider(VirtualValues.map(Array(dbParam), Array(Values.stringValue("graph.db"))))
      val refs = makeResolver().resolveDatabaseNameToReference(
        paramName,
        provider,
        cypherversion,
        ignoreNullInput = false
      )
      refs shouldBe Set(oldPrimaryRef)
    }

    test(
      s"${cypherversion.description}: ParameterName: dotted name uses full literal name when namespace is not a known composite and reference does not exist"
    ) {
      val provider =
        MapBasedParameterProvider(VirtualValues.map(Array(dbParam), Array(Values.stringValue("other.name"))))
      val refs = makeResolver().resolveDatabaseNameToReference(
        paramName,
        provider,
        cypherversion,
        ignoreNullInput = false
      )
      refs shouldBe empty
    }
  }
}
