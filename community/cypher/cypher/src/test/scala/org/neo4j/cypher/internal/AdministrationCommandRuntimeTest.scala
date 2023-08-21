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
package org.neo4j.cypher.internal;

import org.neo4j.cypher.internal.ast.NamespacedName
import org.neo4j.cypher.internal.ast.ParameterName
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DEFAULT_NAMESPACE
import org.neo4j.exceptions.ParameterWrongTypeException
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualValues;

class AdministrationCommandRuntimeTest extends CypherFunSuite {

  test("databaseNameFields should convert namespaced name to parameters") {
    val databaseNameFields = AdministrationCommandRuntime.getDatabaseNameFields(
      "name",
      NamespacedName(List("a", "b"), Some("c"))(InputPosition.NONE)
    )
    databaseNameFields.keys shouldBe Array(
      "__internal_name",
      "__internal_name_namespace",
      "__internal_name_displayName"
    )
    databaseNameFields.values shouldBe Array(
      Values.stringValue("a.b"),
      Values.stringValue("c"),
      Values.stringValue("c.a.b")
    )
  }

  test("databaseNameFields should convert default namespaced name to parameters") {
    val databaseNameFields = AdministrationCommandRuntime.getDatabaseNameFields(
      "name",
      NamespacedName(List("a", "b"), None)(InputPosition.NONE)
    )
    databaseNameFields.keys shouldBe Array(
      "__internal_name",
      "__internal_name_namespace",
      "__internal_name_displayName"
    )
    databaseNameFields.values shouldBe Array(
      Values.stringValue("a.b"),
      Values.stringValue(DEFAULT_NAMESPACE),
      Values.stringValue("a.b")
    )
  }

  test("databaseNameFields should convert default namespaced name to parameters if it is specified explicitly") {
    val databaseNameFields = AdministrationCommandRuntime.getDatabaseNameFields(
      "name",
      NamespacedName(List("a", "b"), Some(DEFAULT_NAMESPACE))(InputPosition.NONE)
    )
    databaseNameFields.keys shouldBe Array(
      "__internal_name",
      "__internal_name_namespace",
      "__internal_name_displayName"
    )
    databaseNameFields.values shouldBe Array(
      Values.stringValue("a.b"),
      Values.stringValue(DEFAULT_NAMESPACE),
      Values.stringValue("a.b")
    )
  }

  test("databaseNameFields should convert parameter to namespaced parameter") {
    val databaseNameFields = AdministrationCommandRuntime.getDatabaseNameFields(
      "name",
      ParameterName(ExplicitParameter("param", CTString)(InputPosition.NONE))(InputPosition.NONE)
    )
    databaseNameFields.keys shouldBe Array(
      "__internal_param",
      "__internal_param_namespace",
      "__internal_param_displayName"
    )

    val initialParams = VirtualValues.map(databaseNameFields.keys, databaseNameFields.values)
    val convertedParams =
      databaseNameFields.nameConverter(null, initialParams.updatedWith("param", Values.stringValue("a.b.c")))
    convertedParams.get("__internal_param") shouldBe Values.stringValue("b.c")
    convertedParams.get("__internal_param_namespace") shouldBe Values.stringValue("a")
    convertedParams.get("__internal_param_displayName") shouldBe Values.stringValue("a.b.c")
  }

  test("databaseNameFields should convert parameter to default namespaced parameter") {
    val databaseNameFields = AdministrationCommandRuntime.getDatabaseNameFields(
      "name",
      ParameterName(ExplicitParameter("param", CTString)(InputPosition.NONE))(InputPosition.NONE)
    )
    databaseNameFields.keys shouldBe Array(
      "__internal_param",
      "__internal_param_namespace",
      "__internal_param_displayName"
    )

    val initialParams = VirtualValues.map(databaseNameFields.keys, databaseNameFields.values)
    val convertedParams =
      databaseNameFields.nameConverter(null, initialParams.updatedWith("param", Values.stringValue("a")))
    convertedParams.get("__internal_param") shouldBe Values.stringValue("a")
    convertedParams.get("__internal_param_namespace") shouldBe Values.stringValue(DEFAULT_NAMESPACE)
    convertedParams.get("__internal_param_displayName") shouldBe Values.stringValue("a")
  }

  test("databaseNameFields should convert parameter to default namespaced parameter if it is specified explicitly") {
    val databaseNameFields = AdministrationCommandRuntime.getDatabaseNameFields(
      "name",
      ParameterName(ExplicitParameter("param", CTString)(InputPosition.NONE))(InputPosition.NONE)
    )
    databaseNameFields.keys shouldBe Array(
      "__internal_param",
      "__internal_param_namespace",
      "__internal_param_displayName"
    )

    val initialParams = VirtualValues.map(databaseNameFields.keys, databaseNameFields.values)
    val convertedParams = databaseNameFields.nameConverter(
      null,
      initialParams.updatedWith("param", Values.stringValue(s"$DEFAULT_NAMESPACE.a"))
    )
    convertedParams.get("__internal_param") shouldBe Values.stringValue("a")
    convertedParams.get("__internal_param_namespace") shouldBe Values.stringValue(DEFAULT_NAMESPACE)
    convertedParams.get("__internal_param_displayName") shouldBe Values.stringValue("a")
  }

  test("databaseNameFields should fail convert parameter with incorrect type") {
    val databaseNameFields = AdministrationCommandRuntime.getDatabaseNameFields(
      "name",
      ParameterName(ExplicitParameter("param", CTInteger)(InputPosition.NONE))(InputPosition.NONE)
    )
    databaseNameFields.keys shouldBe Array(
      "__internal_param",
      "__internal_param_namespace",
      "__internal_param_displayName"
    )

    the[ParameterWrongTypeException] thrownBy (databaseNameFields.nameConverter(
      null,
      VirtualValues.map(Array("param"), Array(Values.intValue(42)))
    )) should
      have message "Expected parameter $param to have type String but was Int(42)"
  }

}
