/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import org.neo4j.visualization.graphviz.GraphStyle
import org.neo4j.visualization.graphviz.AsciiDocSimpleStyle

class WhereTest extends DocumentingTestBase {
  override def graphDescription = List(
    "Andres KNOWS Tobias",
    "Andres:Swedish KNOWS Peter")

  override val properties = Map(
    "Andres" -> Map("age" -> 36l, "belt" -> "white"),
    "Tobias" -> Map("age" -> 25l),
    "Peter" -> Map("age" -> 34l)
  )

  override protected def getGraphvizStyle: GraphStyle = 
    AsciiDocSimpleStyle.withAutomaticRelationshipTypeColors()

  def section = "Where"

  @Test def filter_on_node_label() {
    testQuery(
      title = "Filter on node label",
      text = "To filter nodes by label, write a label predicate after the `WHERE` keyword using `WHERE n:foo`.",
      queryText = """match (n) where n:Swedish return n""",
      optionalResultExplanation = """The "+Andres+" node will be returned.""",
      assertions = (p) => assertEquals(List(node("Andres")), p.columnAs[Node]("n").toList))
  }

  @Test def filter_on_property() {
    testQuery(
      title = "Filter on node property",
      text = "To filter on a property, write your clause after the `WHERE` keyword. Filtering on relationship properties works just the same way.",
      queryText = """match (n) where n.age < 30 return n""",
      optionalResultExplanation = """The "+Tobias+" node will be returned.""",
      assertions = (p) => assertEquals(List(node("Tobias")), p.columnAs[Node]("n").toList))
  }

  @Test def boolean_operations() {
    testQuery(
      title = "Boolean operations",
      text = "You can use the expected boolean operators `AND` and `OR`, and also the boolean function `NOT`. " +
        "See <<cypher-working-with-null>> for more information on how this works with +NULL+.",
      queryText = """match (n) where n.name = 'Peter' xor (n.age < 30 and n.name = "Tobias") or not (n.name = "Tobias" or n.name="Peter") return n""",
      optionalResultExplanation = "This query shows how boolean operators can be used.",
      assertions = (p) => assertEquals(nodes("Andres", "Tobias", "Peter").toSet, p.columnAs[Node]("n").toSet))
  }

  @Test def regular_expressions() {
    testQuery(
      title = "Regular expressions",
      text = "You can match on regular expressions by using `=~ \"regexp\"`, like this:",
      queryText = """match (n) where n.name =~ 'Tob.*' return n""",
      optionalResultExplanation = """The "+Tobias+" node will be returned.""",
      assertions = (p) => assertEquals(List(node("Tobias")), p.columnAs[Node]("n").toList))
  }

  @Test def regular_expressions_escaped() {
    testQuery(
      title = "Escaping in regular expressions",
      text = "If you need a forward slash inside of your regular expression, escape it. Remember that back slash needs " +
             "to be escaped in string literals",
      queryText = """match (n) where n.name =~ 'Some\\/thing' return n""",
      optionalResultExplanation = """No nodes match this regular expression.""",
      assertions = (p) => assertEquals(List(), p.toList))
  }

  @Test def regular_expressions_case_insensitive() {
    testQuery(
      title = "Case insensitive regular expressions",
      text = "By pre-pending a regular expression with `(?i)`, the whole expression becomes case insensitive.",
      queryText = """match (n) where n.name =~ '(?i)ANDR.*' return n""",
      optionalResultExplanation = """The node with name "+Andres+" is returned.""",
      assertions = (p) => assertEquals(List(Map("n" -> node("Andres"))), p.toList))
  }

  @Test def has_property() {
    testQuery(
      title = "Property exists",
      text = "To only include nodes/relationships that have a property, use the `HAS()` function and just write out the identifier and the property you expect it to have.",
      queryText = """match (n) where has(n.belt) return n""",
      optionalResultExplanation = """The node named "+Andres+" is returned.""",
      assertions = (p) => assertEquals(List(node("Andres")), p.columnAs[Node]("n").toList))
  }

  @Test def compare_if_property_exists() {
    testQuery(
      title = "Default to true if property is missing",
      text = "If you want to compare a property on a graph element, but only if it exists, you can compare the " +
        "property against both the value you are looking for and +NULL+, like:",
      queryText = """match (n) where n.belt = 'white' or n.belt IS NULL return n order by n.name""",
      optionalResultExplanation = "This returns all nodes, even those without the belt property.",
      assertions = (p) => assertEquals(List(node("Andres"), node("Peter"), node("Tobias")), p.columnAs[Node]("n").toList))
  }

  @Test def compare_if_property_exists_default_false() {
    testQuery(
      title = "Default to false if property is missing",
      text = "As missing properties evaluate to +NULL+, the comparision in the example will evaluate to +FALSE+ for nodes without the +belt+ property.",
      queryText = """match (n) where n.belt = 'white' return n""",
      optionalResultExplanation = "Only nodes with the belt property are returned.",
      assertions = (p) => assertEquals(List(node("Andres")), p.columnAs[Node]("n").toList))
  }

  @Test def filter_on_relationship_type() {
    testQuery(
      title = "Filtering on relationship type",
      text = "You can put the exact relationship type in the `MATCH` pattern, but sometimes you want to be able to do more " +
        "advanced filtering on the type. You can use the special property `TYPE` to compare the type with something else. " +
        "In this example, the query does a regular expression comparison with the name of the relationship type.",
      queryText = """match (n)-[r]->() where n.name='Andres' and type(r) =~ 'K.*' return r""",
      optionalResultExplanation = """This returns relationships that has a type whose name starts with K.""",
      assertions = (p) => assertEquals("KNOWS", p.columnAs[Relationship]("r").toList.head.getType.name()))
  }

  @Test def filter_on_null() {
    testQuery(
      title = "Filter on NULL",
      text = "Sometimes you might want to test if a value or an identifier is +NULL+. This is done just like SQL does it, " +
        "with `IS NULL`. Also like SQL, the negative is `IS NOT NULL`, although `NOT(IS NULL x)` also works.",
      queryText = """match (person) where person.name = 'Peter' AND person.belt is null return person""",
      optionalResultExplanation = "Nodes that have name 'Peter' but no belt property are returned.",
      assertions = (p) => assertEquals(List(Map("person" -> node("Peter"))), p.toList))
  }

  @Test def filter_on_patterns() {
    testQuery(
      title = "Filter on patterns",
      text = """Patterns are expressions in Cypher, expressions that return a collection of paths. Collection
expressions are also predicates -- an empty collection represents `false`, and a non-empty represents `true`.

So, patterns are not only expressions, they are also predicates. The only limitation to your pattern is that you must be
able to express it in a single path. You can not use commas between multiple paths like you do in `MATCH`. You can achieve
the same effect by combining multiple patterns with `AND`.

Note that you can not introduce new identifiers here. Although it might look very similar to the `MATCH` patterns, the
`WHERE` clause is all about eliminating matched subgraphs. `MATCH (a)-[*]->(b)` is very different from `WHERE (a)-[*]->(b)`; the
first will produce a subgraph for every path it can find between `a` and `b`, and the latter will eliminate any matched
subgraphs where `a` and `b` do not have a directed relationship chain between them.
             """,
      queryText = """match (tobias {name: 'Tobias'}), (others) where others.name IN ['Andres', 'Peter'] and (tobias)<--(others) return others""",
      optionalResultExplanation = "Nodes that have an outgoing relationship to the \"+Tobias+\" node are returned.",
      assertions = (p) => assertEquals(List(Map("others" -> node("Andres"))), p.toList))
  }

  @Test def predicate_with_properties() {
    testQuery(
      title = "Filter on patterns with properties",
      text = """You can also add properties to your patterns:""",
      queryText = """match (n) where (n)-[:KNOWS]-({name:'Tobias'}) return n""",
      optionalResultExplanation = "Finds all nodes that have a +KNOWS+ relationship to a node with the name +Tobias+.",
      assertions = (p) => assertEquals(List(Map("n" -> node("Andres"))), p.toList))
  }

  @Test def has_not_relationship_to() {
    testQuery(
      title = "Filter on patterns using NOT",
      text = """The `NOT` function can be used to exclude a pattern. """,
      queryText = """MATCH (persons), (peter {name: 'Peter'}) where not (persons)-->(peter) return persons""",
      optionalResultExplanation = "Nodes that do not have an outgoing relationship to the \"+Peter+\" node are returned.",
      assertions = (p) => assertEquals(List(Map("persons" -> node("Tobias")),Map("persons" -> node("Peter"))), p.toList))
  }

  @Test def in_operator() {
    testQuery(
      title = "IN operator",
      text = "To check if an element exists in a collection, you can use the `IN` operator.",
      queryText = """match (a) where a.name IN ["Peter", "Tobias"] return a""",
      optionalResultExplanation = "This query shows how to check if a property exists in a literal collection.",
      assertions = (p) => assertEquals(List(node("Tobias"),node("Peter")), p.columnAs[Node]("a").toList))
  }
}
