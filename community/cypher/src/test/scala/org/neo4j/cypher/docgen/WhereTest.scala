/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import org.junit.Test
import org.junit.Assert._
import org.neo4j.graphdb.{Relationship, Node}

class WhereTest extends DocumentingTestBase {
  def graphDescription = List("Andres KNOWS Tobias", "Andres KNOWS Peter")

  override val properties = Map(
    "Andres" -> Map("age" -> 36l, "belt" -> "white"),
    "Tobias" -> Map("age" -> 25l),
    "Peter" -> Map("age" -> 34l)
  )

  def section = "Where"

  @Test def filter_on_property() {
    testQuery(
      title = "Filter on node property",
      text = "To filter on a property, write your clause after the `WHERE` keyword.",
      queryText = """start n=node(%Andres%, %Tobias%) where n.age < 30 return n""",
      returns = """The node.""",
      assertions = (p) => assertEquals(List(node("Tobias")), p.columnAs[Node]("n").toList))
  }

  @Test def boolean_operations() {
    testQuery(
      title = "Boolean operations",
      text = "You can use the expected boolean operators `AND` and `OR`, and also the boolean function `NOT()`.",
      queryText = """start n=node(%Andres%, %Tobias%) where (n.age < 30 and n.name = "Tobias") or not(n.name = "Tobias")  return n""",
      returns = """The node.""",
      assertions = (p) => assertEquals(List(node("Andres"), node("Tobias")), p.columnAs[Node]("n").toList))
  }

  @Test def regular_expressions() {
    testQuery(
      title = "Regular expressions",
      text = "You can match on regular expressions by using `=~ /regexp/`, like this:",
      queryText = """start n=node(%Andres%, %Tobias%) where n.name =~ /Tob.*/ return n""",
      returns = """The node named Tobias.""",
      assertions = (p) => assertEquals(List(node("Tobias")), p.columnAs[Node]("n").toList))
  }

  @Test def regular_expressions_escaped() {
    testQuery(
      title = "Escaping in regular expressions",
      text = "If you need a forward slash inside of your regular expression, escape it just like you expect to.",
      queryText = """start n=node(%Andres%, %Tobias%) where n.name =~ /Some\/thing/ return n""",
      returns = """No nodes match this regular expression.""",
      assertions = (p) => assertEquals(List(), p.toList))
  }

  @Test def regular_expressions_case_insensitive() {
    testQuery(
      title = "Case insensitive regular expressions",
      text = "By pre-pending a regular expression with (?i), the whole expression becomes case insensitive.",
      queryText = """start n=node(%Andres%, %Tobias%) where n.name =~ /(?i)ANDR.*/ return n""",
      returns = """The node with name 'Andres' is returned.""",
      assertions = (p) => assertEquals(List(Map("n" -> node("Andres"))), p.toList))
  }

  @Test def has_property() {
    testQuery(
      title = "Property exists",
      text = "To only include nodes/relationships that have a property, just write out the identifier and the property you expect it to have.",
      queryText = """start n=node(%Andres%, %Tobias%) where has(n.belt) return n""",
      returns = """The node named Andres.""",
      assertions = (p) => assertEquals(List(node("Andres")), p.columnAs[Node]("n").toList))
  }

  @Test def compare_if_property_exists() {
    testQuery(
      title = "Default true if property is missing",
      text = "If you want to compare a property on a graph element, but only if it exists, use the nullable property syntax. " +
        "You can use a question mark if you want missing property to return true, like:",
      queryText = """start n=node(%Andres%, %Tobias%) where n.belt? = 'white' return n""",
      returns = "All nodes, even those without the belt property",
      assertions = (p) => assertEquals(List(node("Andres"), node("Tobias")), p.columnAs[Node]("n").toList))
  }

  @Test def compare_if_property_exists_default_false() {
    testQuery(
      title = "Default false if property is missing",
      text = "When you need missing property to evaluate to false, use the exclamation mark.",
      queryText = """start n=node(%Andres%, %Tobias%) where n.belt! = 'white' return n""",
      returns = "No nodes without the belt property are returned.",
      assertions = (p) => assertEquals(List(node("Andres")), p.columnAs[Node]("n").toList))
  }

  @Test def filter_on_relationship_type() {
    testQuery(
      title = "Filtering on relationship type",
      text = "You can put the exact relationship type in the `MATCH` pattern, but sometimes you want to be able to do more " +
        "advanced filtering on the type. You can use the special property `TYPE` to compare the type with something else. " +
        "In this example, the query does a regular expression comparison with the name of the relationship type.",
      queryText = """start n=node(%Andres%) match (n)-[r]->() where type(r) =~ /K.*/ return r""",
      returns = """The relationship that has a type whose name starts with K.""",
      assertions = (p) => assertEquals("KNOWS", p.columnAs[Relationship]("r").toList.head.getType.name()))
  }

  @Test def filter_on_null() {
    testQuery(
      title = "Filter on null values",
      text = "Sometimes you might want to test if a value or an identifier is null. This is done just like SQL does it, with IS NULL." +
        " Also like SQL, the negative is IS NOT NULL, althought NOT(IS NULL x) also works.",
      queryText = """start a=node(%Tobias%), b=node(%Andres%, %Peter%) match a<-[r?]-b where r is null return b""",
      returns = "Nodes that Tobias is not connected to",
      assertions = (p) => assertEquals(List(Map("b" -> node("Peter"))), p.toList))
  }

  @Test def has_relationship_to() {
    testQuery(
      title = "Filter on relationships",
      text = """To filter out subgraphs based on relationships between nodes, you use a limited part of the iconigraphy in the match clause. You can only describe the relationship with direction and optional type. These are all valid expressions:
[source,cypher]
----
WHERE a-->b
WHERE a<--b
WHERE a<-[:KNOWS]-b
WHERE a-[:KNOWS]-b
----

Note that you can not introduce new identifiers here. Although it might look very similar to the `MATCH` clause, the
`WHERE` clause is all about eliminating matched subgraphs. `MATCH a-->b` is very different from `WHERE a-->b`; the first will
produce a subgraph for every relationship between `a` and `b`, and the latter will eliminate any matched subgraphs where `a` and `b`
do not have a relationship between them.
      """,
      queryText = """start a=node(%Tobias%), b=node(%Andres%, %Peter%) where a<--b return b""",
      returns = "Nodes that Tobias is not connected to",
      assertions = (p) => assertEquals(List(Map("b" -> node("Andres"))), p.toList))
  }

  @Test def in_operator() {
    testQuery(
      title = "IN operator",
      text = "To check if an element exists in a collection, you can use the IN operator.",
      queryText = """start a=node(%Andres%, %Tobias%, %Peter%) where a.name IN ["Peter", "Tobias"] return a""",
      returns = "This query shows how to check if a property exists in a literal collection.",
      assertions = (p) => assertEquals(List(node("Tobias"),node("Peter")), p.columnAs[Node]("a").toList))
  }
}