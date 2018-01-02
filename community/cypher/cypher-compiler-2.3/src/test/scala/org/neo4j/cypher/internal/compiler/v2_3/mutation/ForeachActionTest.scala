/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.mutation

import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.{Add, Literal, Collection, Property, Identifier}
import org.neo4j.cypher.internal.compiler.v2_3.commands.values.{UnresolvedLabel, UnresolvedProperty}
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class ForeachActionTest extends CypherFunSuite {

  test("foreach with create") { // FOREACH( i in ["foo"] | CREATE (r)-[:REL]->(c {id: i.prop}) )
  val to = RelationshipEndpoint(Identifier("c"), Map("id" -> Property(Identifier("i"), UnresolvedProperty("prop"))), Seq(UnresolvedLabel("apa")))
    val from = RelationshipEndpoint("r")

    val objectUnderTest = ForeachAction(Collection(Literal("foo")), "i", Seq(CreateRelationship("  UNNAMED1", from, to, "REL", Map.empty)))

    objectUnderTest.symbolTableDependencies should be(empty)
  }

  test("nested foreach with creation in each foreach") {
    // FOREACH( i in ["foo"] | CREATE (r)-[:REL]->(c {id: i.prop}) FOREACH j in ["foo"] |CREATE (r)-[:REL]->(c {id: i.prop}) )
    val to1 = RelationshipEndpoint(Identifier("  UNNAMED23"), Map("id" -> Add(Property(Identifier("c"), UnresolvedProperty("prop")), Identifier("j"))), Seq(UnresolvedLabel("apa")))
    val from1 = RelationshipEndpoint("c")
    val innerForeach = ForeachAction(Collection(Literal("foo")), "j", Seq(CreateRelationship("  UNNAMED1", from1, to1, "REL", Map.empty)))


    val to2 = RelationshipEndpoint(Identifier("c"), Map("id" -> Property(Identifier("i"), UnresolvedProperty("prop"))), Seq(UnresolvedLabel("apa")))
    val from2 = RelationshipEndpoint("r")
    val objectUnderTest = ForeachAction(Collection(Literal("foo")), "i", Seq(CreateRelationship("  UNNAMED2", from2, to2, "REL", Map.empty), innerForeach))

    objectUnderTest.symbolTableDependencies should be(empty)
  }
}
