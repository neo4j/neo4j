/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import org.neo4j.graphdb.Node
import org.neo4j.cypher.ExecutionResult

class FunctionsTest extends DocumentingTestBase {
  def graphDescription = List(
    "A:foo:bar KNOWS B",
    "A KNOWS C",
    "B KNOWS D",
    "C KNOWS D",
    "B MARRIED E:Spouse")

  override val properties = Map(
    "A" -> Map("name" -> "Alice", "age" -> 38, "eyes" -> "brown"),
    "B" -> Map("name" -> "Bob", "age" -> 25, "eyes" -> "blue"),
    "C" -> Map("name" -> "Charlie", "age" -> 53, "eyes" -> "green"),
    "D" -> Map("name" -> "Daniel", "age" -> 54, "eyes" -> "brown"),
    "E" -> Map("name" -> "Eskil", "age" -> 41, "eyes" -> "blue", "array" -> Array("one", "two", "three"))
  )

  def section = "functions"

  val common_arguments = List(
    "collection" -> "An expression that returns a collection",
    "identifier" -> "This is the identifier that can be used from the predicate.",
    "predicate" -> "A predicate that is tested against all items in the collection."
  )

  @Test def all() {
    testThis(
      title = "ALL",
      syntax = "ALL(identifier in collection WHERE predicate)",
      arguments = common_arguments,
      text = """Tests whether a predicate holds for all element of this collection collection.""",
      queryText = """match p=a-[*1..3]->b where a.name='Alice' and b.name='Daniel' and all(x in nodes(p) WHERE x.age > 30) return p""",
      returns = """All nodes in the returned paths will have an `age` property of at least 30.""",
      assertions = (p) => assertEquals(1, p.toSeq.length))
  }

  @Test def any() {
    testThis(
      title = "ANY",
      syntax = "ANY(identifier in collection WHERE predicate)",
      arguments = common_arguments,
      text = """Tests whether a predicate holds for at least one element in the collection.""",
      queryText = """match a where a.name='Eskil' and any(x in a.array WHERE x = "one") return a""",
      returns = """All nodes in the returned paths has at least one `one` value set in the array property named `array`.""",
      assertions = (p) => assertEquals(List(Map("a"->node("E"))), p.toList))
  }

  @Test def none() {
    testThis(
      title = "NONE",
      syntax = "NONE(identifier in collection WHERE predicate)",
      arguments = common_arguments,
      text = """Returns true if the predicate holds for no element in the collection.""",
      queryText = """match p=n-[*1..3]->b where n.name='Alice' and NONE(x in nodes(p) WHERE x.age = 25) return p""",
      returns = """No nodes in the returned paths has a `age` property set to `25`.""",
      assertions = (p) => assertEquals(2, p.toSeq.length))
  }

  @Test def single() {
    testThis(
      title = "SINGLE",
      syntax = "SINGLE(identifier in collection WHERE predicate)",
      arguments = common_arguments,
      text = """Returns true if the predicate holds for exactly one of the elements in the collection.""",
      queryText = """match p=n-->b where n.name='Alice' and SINGLE(var in nodes(p) WHERE var.eyes = "blue") return p""",
      returns = """Exactly one node in every returned path will have the `eyes` property set to `"blue"`.""",
      assertions = (p) => assertEquals(1, p.toSeq.length))
  }

  @Test def relationship_type() {
    testThis(
      title = "TYPE",
      syntax = "TYPE( relationship )",
      arguments = List("relationship" -> "A relationship."),
      text = """Returns a string representation of the relationship type.""",
      queryText = """match (n)-[r]->() where n.name='Alice' return type(r)""",
      returns = """The relationship type of `r` is returned by the query.""",
      assertions = (p) => assertEquals("KNOWS", p.columnAs[String]("type(r)").toList.head))
  }

  @Test def length() {
    testThis(
      title = "LENGTH",
      syntax = "LENGTH( collection )",
      arguments = List("collection" -> "An expression that returns a collection"),
      text = """To return or filter on the length of a collection, use the `LENGTH()` function.""",
      queryText = """match p=a-->b-->c where a.name='Alice' return length(p)""",
      returns = """The length of the path `p` is returned by the query.""",
      assertions = (p) => assertEquals(2, p.columnAs[Int]("length(p)").toList.head))
  }

  @Test def labels() {
    testThis(
      title = "LABELS",
      syntax = "LABELS( node )",
      arguments = List("node" -> "Any expression that returns a single node"),
      text = """Returns a collection of string representations for the labels attached to a node.""",
      queryText = """match a where a.name='Alice' return labels(a)""",
      returns = """The labels of `n` is returned by the query.""",
      assertions = {
        (p) =>
          val iter: Iterable[String] = p.columnAs[Iterable[String]]("labels(a)").next()
          assert(iter.toList === List("foo", "bar"))
      }
    )
  }

  @Test def extract() {
    testThis(
      title = "EXTRACT",
      syntax = "EXTRACT( identifier in collection : expression )",
      arguments = List(
        "collection" -> "An expression that returns a collection",
        "identifier" -> "The closure will have an identifier introduced in it's context. Here you decide which identifier to use.",
        "expression" -> "This expression will run once per value in the collection, and produces the result collection."
      ),
      text = """To return a single property, or the value of a function from a collection of nodes or relationships,
 you can use `EXTRACT`. It will go through a collection, run an expression on every element, and return the results
 in an collection with these values. It works like the `map` method in functional languages such as Lisp and Scala.""",
      queryText = """match p=a-->b-->c where a.name='Alice' and b.name='Bob' and c.name='Daniel' return extract(n in nodes(p) : n.age)""",
      returns = """The age property of all nodes in the path are returned.""",
      assertions = (p) => assertEquals(List(Map("extract(n in nodes(p) : n.age)" -> List(38, 25, 54))), p.toList))
  }

  @Test def reduce() {
    testThis(
      title = "REDUCE",
      syntax = "REDUCE( accumulator = initial,  identifier in collection : expression )",
      arguments = List(
        "accumulator" -> "An identifier that will hold the result and the partial results as the collection is iterated",
        "initial"    -> "An expression that runs once to give a starting value to the accumulator",
        "collection" -> "An expression that returns a collection",
        "identifier" -> "The closure will have an identifier introduced in it's context. Here you decide which identifier to use.",
        "expression" -> "This expression will run once per value in the collection, and produces the result value."
      ),
      text = """To run an expression against individual elements of a collection, and store the result of the expression in
 an accumulator, you can use `REDUCE`. It will go through a collection, run an expression on every element, storing the partial result 
 in the accumulator. It works like the `fold` or `reduce` method in functional languages such as Lisp and Scala.""",
      queryText = """match p=a-->b-->c where a.name='Alice' and b.name='Bob' and c.name='Daniel' return reduce(totalAge = 0, n in nodes(p) : totalAge + n.age)""",
      returns = """The age property of all nodes in the path are summed and returned as a single value.""",
      assertions = (p) => assertEquals(List(Map("reduce(totalAge = 0, n in nodes(p) : totalAge + n.age)" -> 117)), p.toList))
  }

  @Test def head() {
    testThis(
      title = "HEAD",
      syntax = "HEAD( expression )",
      arguments = List(
        "expression" -> "This expression should return a collection of some kind."
      ),
      text = "`HEAD` returns the first element in a collection.",
      queryText = """match a where a.name='Eskil' return a.array, head(a.array)""",
      returns = "The first node in the path is returned.",
      assertions = (p) => assertEquals(List("one"), p.columnAs[List[_]]("head(a.array)").toList))
  }

  @Test def last() {
    testThis(
      title = "LAST",
      syntax = "LAST( expression )",
      arguments = List(
        "expression" -> "This expression should return a collection of some kind."
      ),
      text = "`LAST` returns the last element in a collection.",
      queryText = """match a where a.name='Eskil' return a.array, last(a.array)""",
      returns = "The last node in the path is returned.",
      assertions = (p) => assertEquals(List("three"), p.columnAs[List[_]]("last(a.array)").toList))
  }

  @Test def tail() {
    testThis(
      title = "TAIL",
      syntax = "TAIL( expression )",
      arguments = List(
        "expression" -> "This expression should return a collection of some kind."
      ),
      text = "`TAIL` returns all but the first element in a collection.",
      queryText = """match a where a.name='Eskil' return a.array, tail(a.array)""",
      returns = "This returns the property named `array` and all elements of that property except the first one.",
      assertions = (p) => {
        val toList = p.columnAs[Iterable[_]]("tail(a.array)").toList.head.toList
        assert(toList === List("two","three"))
      })
  }

  @Test def filter() {
    testThis(
      title = "FILTER",
      syntax = "FILTER(identifier in collection : predicate)",
      arguments = common_arguments,
      text = "`FILTER` returns all the elements in a collection that comply to a predicate.",
      queryText = """match a where a.name='Eskil' return a.array, filter(x in a.array : length(x) = 3)""",
      returns = "This returns the property named `array` and a list of values in it, which have the length `3`.",
      assertions = (p) => {
        val array = p.columnAs[Iterable[_]]("filter(x in a.array : length(x) = 3)").toList.head
        assert(List("one","two") === array.toList)
      })
  }

  @Test def nodes_in_path() {
    testThis(
      title = "NODES",
      syntax = "NODES( path )",
      arguments = List("path" -> "A path."),
      text = """Returns all nodes in a path.""",
      queryText = """match p=a-->b-->c where a.name='Alice' and c.name='Eskil' return NODES(p)""",
      returns = """All the nodes in the path `p` are returned by the example query.""",
      assertions = (p) => assert(List(node("A"), node("B"), node("E")) === p.columnAs[List[Node]]("NODES(p)").toList.head)
    )
  }

  @Test def rels_in_path() {
    testThis(
      title = "RELATIONSHIPS",
      syntax = "RELATIONSHIPS( path )",
      arguments = List("path" -> "A path."),
      text = """Returns all relationships in a path.""",
      queryText = """match p=a-->b-->c where a.name='Alice' and c.name='Eskil' return RELATIONSHIPS(p)""",
      returns = """All the relationships in the path `p` are returned.""",
      assertions = (p) => assert(2 === p.columnAs[Seq[Node]]("RELATIONSHIPS(p)").toSeq.head.length)
    )
  }

  @Test def id() {
    testThis(
      title = "ID",
      syntax = "ID( property-container )",
      arguments = List("property-container" -> "A node or a relationship."),
      text = """Returns the id of the relationship or node.""",
      queryText = """match a return ID(a)""",
      returns = """This returns the node id for three nodes.""",
      assertions = (p) => assert(Seq(1,2,3,4,5) === p.columnAs[Int]("ID(a)").toSeq)
    )
  }

  @Test def coalesce() {
    testThis(
      title = "COALESCE",
      syntax = "COALESCE( expression [, expression]* )",
      arguments = List("expression" -> "The expression that might return null."),
      text = """Returns the first non-+null+ value in the list of expressions passed to it.""",
      queryText = """match a where a.name='Alice' return coalesce(a.hairColour?, a.eyes?)""",
      returns = """""",
      assertions = (p) => assert(Seq("brown") === p.columnAs[String]("coalesce(a.hairColour?, a.eyes?)").toSeq)
    )
  }

  @Test def abs() {
    testThis(
      title = "ABS",
      syntax = "ABS( expression )",
      arguments = List("expression" -> "A numeric expression."),
      text = "`ABS` returns the absolute value of a number.",
      queryText = """match a, e where a.name = 'Alice' and e.name = 'Eskil' return a.age, e.age, abs(a.age - e.age)""",
      returns = "The absolute value of the age difference is returned.",
      assertions = (p) => assert(List(Map("abs(a.age - e.age)"->3.0, "a.age"->38, "e.age"->41)) === p.toList)
    )
  }

  @Test def round() {
    testThis(
      title = "ROUND",
      syntax = "ROUND( expression )",
      arguments = List("expression" -> "A numerical expression."),
      text = "`ROUND` returns the numerical expression, rounded to the nearest integer.",
      queryText = """match a return round(3.141592) limit 1""",
      returns = "",
      assertions = (p) => assert(List(Map("round(3.141592)"->3)) === p.toList)
    )
  }

  @Test def sqrt() {
    testThis(
      title = "SQRT",
      syntax = "SQRT( expression )",
      arguments = List("expression" -> "A numerical expression"),
      text = "`SQRT` returns the square root of a number.",
      queryText = """match n return sqrt(256) limit 1""",
      returns = "",
      assertions = (p) => assert(List(Map("sqrt(256)"->16))=== p.toList)
    )
  }

  @Test def sign() {
    testThis(
      title = "SIGN",
      syntax = "SIGN( expression )",
      arguments = List("expression" -> "A numerical expression"),
      text = "`SIGN` returns the signum of a number -- zero if the expression is zero, `-1` for any negative number, and `1` for any positive number.",
      queryText = "match n return sign(-17), sign(0.1) limit 1",
      returns = "",
      assertions = (p) => assert(List(Map("sign(-17)"-> -1, "sign(0.1)"->1)) === p.toList)
    )
  }

  @Test def range() {
    testThis(
      title = "RANGE",
      syntax = "RANGE( start, end [, step] )",
      arguments = List(
        "start" -> "A numerical expression.",
        "end" -> "A numerical expression.",
        "step" -> "A numerical expression."
      ),
      text = "Returns numerical values in a range with a non-zero step value step. Range is inclusive in both ends.",
      queryText = "match n return range(0,10), range(2,18,3) limit 1",
      returns = "Two lists of numbers are returned.",
      assertions = (p) => assert(List(Map(
        "range(0,10)"-> List(0,1,2,3,4,5,6,7,8,9,10),
        "range(2,18,3)"->List(2,5,8,11,14,17)
      )) === p.toList)
    )
  }

  @Test def replace() {
    testThis(
      title = "REPLACE",
      syntax = "REPLACE( original, search, replace )",
      arguments = List("original" -> "An expression that returns a string",
                       "search" -> "An expression that returns a string to search for",
                       "replace" -> "An expression that returns the string to replace the search string with"),
      text = "`REPLACE` returns a string with the search string replaced by the replace string. It replaces all occurrences.",
      queryText = "match a return replace(\"hello\", \"l\", \"w\") limit 1",
      returns = "A string.",
      assertions = (p) => assert(Seq("hewwo") === p.columnAs[String]("replace(\"hello\", \"l\", \"w\")").toSeq)
    )
  }

  @Test def left() {
    testThis(
      title = "LEFT",
      syntax = "LEFT( original, length )",
      arguments = List("original" -> "An expression that returns a string",
                       "n" -> "An expression that returns a positive number"),
      text = "`LEFT` returns a string containing the left n characters of the original string.",
      queryText = "match n return left(\"hello\", 3) limit 1",
      returns = "A String.",
      assertions = (p) => assert(Seq("hel") === p.columnAs[String]("left(\"hello\", 3)").toSeq)
    )
  }

  @Test def right() {
    testThis(
      title = "RIGHT",
      syntax = "RIGHT( original, length )",
      arguments = List("original" -> "An expression that returns a string",
                       "n" -> "An expression that returns a positive number"),
      text = "`RIGHT` returns a string containing the right n characters of the original string.",
      queryText = "match n return right(\"hello\", 3) limit 1",
      returns = "A string.",
      assertions = (p) => assert(Seq("llo") === p.columnAs[String]("right(\"hello\", 3)").toSeq)
    )
  }

  @Test def substring() {
    testThis(
      title = "SUBSTRING",
      syntax = "SUBSTRING( original, start [, length] )",
      arguments = List("original" -> "An expression that returns a string",
                       "start" -> "An expression that returns a positive number",
                       "length" -> "An expression that returns a positive number"),
      text = "`SUBSTRING` returns a substring of the original, with a 0-based index start and length. If length is omitted, it returns a substring from start until the end of the string.",
      queryText = "match n return substring(\"hello\", 1, 3), substring(\"hello\", 2) limit 1",
      returns = "A string.",
      assertions = (p) => assert(List(Map("substring(\"hello\", 1, 3)" -> "ell", "substring(\"hello\", 2)" -> "llo")) === p.toList)
    )
  }

  @Test def lower() {
    testThis(
      title = "LOWER",
      syntax = "LOWER( original )",
      arguments = List("original" -> "An expression that returns a string"),
      text = "`LOWER` returns the original string in lowercase.",
      queryText = "match n return lower(\"HELLO\") limit 1",
      returns = "A string.",
      assertions = (p) => assert(List(Map("lower(\"HELLO\")" -> "hello")) === p.toList)
    )
  }

  @Test def upper() {
    testThis(
      title = "UPPER",
      syntax = "UPPER( original )",
      arguments = List("original" -> "An expression that returns a string"),
      text = "`UPPER` returns the original string in uppercase.",
      queryText = "match a return upper(\"hello\") limit 1",
      returns = "A string.",
      assertions = (p) => assert(List(Map("upper(\"hello\")" -> "HELLO")) === p.toList)
    )
  }

  @Test def ltrim() {
    testThis(
      title = "LTRIM",
      syntax = "LTRIM( original )",
      arguments = List("original" -> "An expression that returns a string"),
      text = "`LTRIM` returns the original string with whitespace removed from the left side.",
      queryText = "match n return ltrim(\"   hello\") limit 1",
      returns = "A string.",
      assertions = (p) => assert(List(Map("ltrim(\"   hello\")" -> "hello")) === p.toList)
    )
  }

  @Test def rtrim() {
    testThis(
      title = "RTRIM",
      syntax = "RTRIM( original )",
      arguments = List("original" -> "An expression that returns a string"),
      text = "`RTRIM` returns the original string with whitespace removed from the right side.",
      queryText = "match n return rtrim(\"hello   \") limit 1",
      returns = "A string.",
      assertions = (p) => assert(List(Map("rtrim(\"hello   \")" -> "hello")) === p.toList)
    )
  }

  @Test def trim() {
    testThis(
      title = "TRIM",
      syntax = "TRIM( original )",
      arguments = List("original" -> "An expression that returns a string"),
      text = "`TRIM` returns the original string with whitespace removed from both sides.",
      queryText = "match n return trim(\"   hello   \") limit 1",
      returns = "A string.",
      assertions = (p) => assert(List(Map("trim(\"   hello   \")" -> "hello")) === p.toList)
    )
  }

  @Test def str() {
    testThis(
      title = "STR",
      syntax = "STR( expression )",
      arguments = List("expression" -> "An expression that returns anything"),
      text = "`STR` returns a string representation of the expression.",
      queryText = "match a return str(1) limit 1",
      returns = "A string.",
      assertions = (p) => assert(List(Map("str(1)" -> "1")) === p.toList)
    )
  }

  @Test def now() {
    testThis(
      title = "TIMESTAMP",
      syntax = "TIMESTAMP()",
      arguments = List.empty,
      text = "`TIMESTAMP` returns the difference, measured in milliseconds, between the current time and midnight, " +
        "January 1, 1970 UTC. It will return the same value during the whole one query, even if the query is a long " +
        "running one.",
      queryText = "start n=node(1) return timestamp()",
      returns = "The time in milliseconds.",
      assertions = (p) => assert(
        p.toList.head("timestamp()") match {
          // this should pass unless your machine is really slow
          case x: Long => System.currentTimeMillis - x < 100000
          case _       => false
        })
    )
  }

  @Test def startNode() {
    testThis(
      title = "STARTNODE",
      syntax = "STARTNODE( relationship )",
      arguments = List("relationship" -> "An expression that returns a relationship"),
      text = "`STARTNODE` returns the starting node of a relationship",
      queryText = "MATCH (x:foo)-[r]-() return startNode(r)",
      returns = "",
      assertions = (p) => assert(p.toList.head("startNode(r)") === node("A")))
  }

  @Test def endNode() {
    testThis(
      title = "ENDNODE",
      syntax = "ENDNODE( relationship )",
      arguments = List("relationship" -> "An expression that returns a relationship"),
      text = "`ENDNODE` returns the end node of a relationship",
      queryText = "MATCH (x:foo)-[r]-() return endNode(r)",
      returns = "",
      assertions = (p) => assert(p.toList.head("endNode(r)") === node("B")))
  }

  private def testThis(title: String, syntax: String, arguments: List[(String, String)], text: String, queryText: String, returns: String, assertions: (ExecutionResult => Unit)*) {
    val argsText = arguments.map(x => "* _" + x._1 + ":_ " + x._2).mkString("\r\n\r\n")
    val fullText = String.format("""%s

*Syntax:* `%s`

*Arguments:*

%s""", text, syntax, argsText)
    testQuery(title, fullText, queryText, returns, assertions: _*)
  }
}
