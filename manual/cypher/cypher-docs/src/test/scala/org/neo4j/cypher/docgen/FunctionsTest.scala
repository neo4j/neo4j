/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.junit.Assert._
import org.junit.Test
import org.neo4j.cypher.internal.compiler.v3_0.executionplan.InternalExecutionResult
import org.neo4j.graphdb.Node
import org.neo4j.visualization.graphviz.{AsciiDocSimpleStyle, GraphStyle}

class FunctionsTest extends DocumentingTestBase {
  override def graphDescription = List(
    "A:foo:bar KNOWS B",
    "A KNOWS C",
    "B KNOWS D",
    "C KNOWS D",
    "B MARRIED E:Spouse")

  override val properties = Map(
    "A" -> Map[String, Any]("name" -> "Alice", "age" -> 38, "eyes" -> "brown"),
    "B" -> Map[String, Any]("name" -> "Bob", "age" -> 25, "eyes" -> "blue"),
    "C" -> Map[String, Any]("name" -> "Charlie", "age" -> 53, "eyes" -> "green"),
    "D" -> Map[String, Any]("name" -> "Daniel", "age" -> 54, "eyes" -> "brown"),
    "E" -> Map[String, Any]("name" -> "Eskil", "age" -> 41, "eyes" -> "blue", "array" -> Array("one", "two", "three"))
  )

  override protected def getGraphvizStyle: GraphStyle =
    AsciiDocSimpleStyle.withAutomaticRelationshipTypeColors()

  def section = "functions"

  val common_arguments = List(
    "list" -> "An expression that returns a list",
    "variable" -> "This is the variable that can be used from the predicate.",
    "predicate" -> "A predicate that is tested against all items in the list."
  )

  @Test def all() {


    testThis(
      title = "all()",
      syntax = "all(variable IN list WHERE predicate)",
      arguments = common_arguments,
      text = """Tests whether a predicate holds for all elements of this list.""",
      queryText = """match p=(a)-[*1..3]->(b) where a.name='Alice' and b.name='Daniel' and all(x in nodes(p) WHERE x.age > 30) return p""",
      returns = """All nodes in the returned paths will have an `age` property of at least 30.""",
      assertions = (p) => assertEquals(1, p.toSeq.length))
  }

  @Test def any() {
    testThis(
      title = "any()",
      syntax = "any(variable IN list WHERE predicate)",
      arguments = common_arguments,
      text = """Tests whether a predicate holds for at least one element in the list.""",
      queryText = """match (a) where a.name='Eskil' and any(x in a.array WHERE x = "one") return a""",
      returns = """All nodes in the returned paths has at least one `one` value set in the array property named `array`.""",
      assertions = (p) => assertEquals(List(Map("a"->node("E"))), p.toList))
  }

  @Test def none() {
    testThis(
      title = "none()",
      syntax = "none(variable in list WHERE predicate)",
      arguments = common_arguments,
      text = """Returns true if the predicate holds for no element in the list.""",
      queryText = """match p=(n)-[*1..3]->(b) where n.name='Alice' and NONE(x in nodes(p) WHERE x.age = 25) return p""",
      returns = """No nodes in the returned paths has a `age` property set to `25`.""",
      assertions = (p) => assertEquals(2, p.toSeq.length))
  }

  @Test def single() {
    testThis(
      title = "single()",
      syntax = "single(variable in list WHERE predicate)",
      arguments = common_arguments,
      text = """Returns true if the predicate holds for exactly one of the elements in the list.""",
      queryText = """match p=(n)-->(b) where n.name='Alice' and SINGLE(var in nodes(p) WHERE var.eyes = "blue") return p""",
      returns = """Exactly one node in every returned path will have the `eyes` property set to `"blue"`.""",
      assertions = (p) => assertEquals(1, p.toSeq.length))
  }

  @Test def exists() {
    testThis(
      title = "exists()",
      syntax = "exists( pattern-or-property )",
      arguments = List("pattern-or-property" -> "A pattern or a property (in the form 'variable.prop')."),
      text = """Returns true if a match for the pattern exists in the graph, or the property exists in the node, relationship or map.""",
      queryText = """match (n) where EXISTS(n.name) return n.name AS name, EXISTS( (n)-[:MARRIED]->() ) AS is_married""",
      returns = """This query returns all the nodes with a name property along with a boolean true/false indicating if they are married.""",
      assertions = (p) => assertEquals(5, p.toSeq.length))
  }

  @Test def relationship_type() {
    testThis(
      title = "type()",
      syntax = "type( relationship )",
      arguments = List("relationship" -> "A relationship."),
      text = """Returns a string representation of the relationship type.""",
      queryText = """match (n)-[r]->() where n.name='Alice' return type(r)""",
      returns = """The relationship type of `r` is returned by the query.""",
      assertions = (p) => assertEquals("KNOWS", p.columnAs[String]("type(r)").toList.head))
  }

  @Test def size() {
    testThis(
      title = "size()",
      syntax = "size( list )",
      arguments = List("list" -> "An expression that returns a list"),
      text = """To return or filter on the size of a list, use the `size()` function.""",
      queryText = """return size(['Alice', 'Bob']) as col""",
      returns = """The number of items in the list is returned by the query.""",
      assertions = (col) => assertEquals(2, col.columnAs[Long]("col").toList.head))
  }

  @Test def size2() {
    testThis(
      title = "Size of pattern expression",
      syntax = "size( pattern expression )",
      arguments = List("pattern expression" -> "A pattern expression that returns a list"),
      text = """
               |This is the same `size()` method described before,
               |but instead of passing in a list directly, you provide a pattern expression
               |that can be used in a match query to provide a new set of results.
               |The size of the result is calculated, not the length of the expression itself.
               |""".stripMargin,
      queryText = """match (a) where a.name='Alice' return size( (a)-->()-->() ) as fof""",
      returns = """The number of sub-graphs matching the pattern expression is returned by the query.""",
      assertions = (p) => assertEquals(3, p.columnAs[Long]("fof").toList.head))
  }

  @Test def length() {
    testThis(
      title = "length()",
      syntax = "length( path )",
      arguments = List("path" -> "An expression that returns a path"),
      text = """To return or filter on the length of a path, use the `length()` function.""",
      queryText = """match p=(a)-->(b)-->(c) where a.name='Alice' return length(p)""",
      returns = """The length of the path `p` is returned by the query.""",
      assertions = (p) => assertEquals(2, p.columnAs[Long]("length(p)").toList.head))
  }

  @Test def lengthString() {
    testThis(
      title = "Length of string",
      syntax = "length( string )",
      arguments = List("string" -> "An expression that returns a string"),
      text = """To return or filter on the length of a string, use the `length()` function.""",
      queryText = """match (a) where length(a.name) > 6 return length(a.name)""",
      returns = """The length of the name `Charlie` is returned by the query.""",
      assertions = (p) => assertEquals(7, p.columnAs[Long]("length(a.name)").toList.head))
  }

  @Test def labels() {
    testThis(
      title = "labels()",
      syntax = "labels( node )",
      arguments = List("node" -> "Any expression that returns a single node"),
      text = """Returns a list of string representations for the labels attached to a node.""",
      queryText = """match (a) where a.name='Alice' return labels(a)""",
      returns = """The labels of `n` is returned by the query.""",
      assertions = {
        (p) =>
          val iter: Iterable[String] = p.columnAs[Iterable[String]]("labels(a)").next()
          assert(iter.toSet === Set("foo", "bar"))
      }
    )
  }

  @Test def keys() {
    testThis(
      title = "keys()",
      syntax = "keys(  property-container )",
      arguments = List("property-container" -> "A node, a relationship, or a literal map."),
      text = """Returns a list of string representations for the property names of a node, relationship, or map.""",
      queryText = """match (a) where a.name='Alice' return keys(a)""",
      returns = """The name of the properties of `n` is returned by the query.""",
      assertions = {
        (p) =>
          val iter: Iterable[String] = p.columnAs[Iterable[String]]("keys(a)").next()
          assert(iter.toSet === Set("name", "age", "eyes"))
      }
    )
  }

  @Test def extract() {
    testThis(
      title = "extract()",
      syntax = "extract( variable IN list | expression )",
      arguments = List(
        "list" -> "An expression that returns a list",
        "variable" -> "The closure will have a variable introduced in it's context. Here you decide which variable to use.",
        "expression" -> "This expression will run once per value in the list, and produces the result list."
      ),
      text = """To return a single property, or the value of a function from a list of nodes or relationships,
 you can use `extract()`. It will go through a list, run an expression on every element, and return the results
 in a list with these values. It works like the `map` method in functional languages such as Lisp and Scala.""",
      queryText = """match p=(a)-->(b)-->(c) where a.name='Alice' and b.name='Bob' and c.name='Daniel' return extract(n in nodes(p) | n.age) AS extracted""",
      returns = """The age property of all nodes in the path are returned.""",
      assertions = (p) => assertEquals(List(Map("extracted" -> List(38, 25, 54))), p.toList))
  }

  @Test def reduce() {
    testThis(
      title = "reduce()",
      syntax = "reduce( accumulator = initial, variable IN list | expression )",
      arguments = List(
        "accumulator" -> "A variable that will hold the result and the partial results as the list is iterated",
        "initial"    -> "An expression that runs once to give a starting value to the accumulator",
        "list" -> "An expression that returns a list",
        "variable" -> "The closure will have a variable introduced in its context. Here you decide which variable to use.",
        "expression" -> "This expression will run once per value in the list, and produces the result value."
      ),
      text = """To run an expression against individual elements of a list, and store the result of the expression in
 an accumulator, you can use `reduce()`. It will go through a list, run an expression on every element, storing the partial result
 in the accumulator. It works like the `fold` or `reduce` method in functional languages such as Lisp and Scala.""",
      queryText = """match p=(a)-->(b)-->(c) where a.name='Alice' and b.name='Bob' and c.name='Daniel' return reduce(totalAge = 0, n in nodes(p) | totalAge + n.age) AS reduction""",
      returns = """The age property of all nodes in the path are summed and returned as a single value.""",
      assertions = (p) => assertEquals(List(Map("reduction" -> 117)), p.toList))
  }

  @Test def head() {
    testThis(
      title = "head()",
      syntax = "head( expression )",
      arguments = List(
        "expression" -> "This expression should return a list of some kind."
      ),
      text = "`head()` returns the first element in a list.",
      queryText = """match (a) where a.name='Eskil' return a.array, head(a.array)""",
      returns = "The first node in the path is returned.",
      assertions = (p) => assertEquals(List("one"), p.columnAs[List[_]]("head(a.array)").toList))
  }

  @Test def last() {
    testThis(
      title = "last()",
      syntax = "last( expression )",
      arguments = List(
        "expression" -> "This expression should return a list of some kind."
      ),
      text = "`last()` returns the last element in a list.",
      queryText = """match (a) where a.name='Eskil' return a.array, last(a.array)""",
      returns = "The last node in the path is returned.",
      assertions = (p) => assertEquals(List("three"), p.columnAs[List[_]]("last(a.array)").toList))
  }

  @Test def tail() {
    testThis(
      title = "tail()",
      syntax = "tail( expression )",
      arguments = List(
        "expression" -> "This expression should return a list of some kind."
      ),
      text = "`tail()` returns all but the first element in a list.",
      queryText = """match (a) where a.name='Eskil' return a.array, tail(a.array)""",
      returns = "This returns the property named `array` and all elements of that property except the first one.",
      assertions = (p) => {
        val toList = p.columnAs[Iterable[_]]("tail(a.array)").toList.head.toList
        assert(toList === List("two","three"))
      })
  }

  @Test def filter() {
    testThis(
      title = "filter()",
      syntax = "filter(variable IN list WHERE predicate)",
      arguments = common_arguments,
      text = "`filter()` returns all the elements in a list that comply to a predicate.",
      queryText = """match (a) where a.name='Eskil' return a.array, filter(x in a.array WHERE size(x) = 3)""",
      returns = "This returns the property named `array` and a list of values in it, which have size `3`.",
      assertions = (p) => {
        val array = p.columnAs[Iterable[_]]("filter(x in a.array WHERE size(x) = 3)").toList.head
        assert(List("one","two") === array.toList)
      })
  }

  @Test def nodes_in_path() {
    testThis(
      title = "nodes()",
      syntax = "nodes( path )",
      arguments = List("path" -> "A path."),
      text = """Returns all nodes in a path.""",
      queryText = """match p=(a)-->(b)-->(c) where a.name='Alice' and c.name='Eskil' return nodes(p)""",
      returns = """All the nodes in the path `p` are returned by the example query.""",
      assertions = (p) => assert(List(node("A"), node("B"), node("E")) === p.columnAs[Seq[Node]]("nodes(p)").toList.head)
    )
  }

  @Test def rels_in_path() {
    testThis(
      title = "relationships()",
      syntax = "relationships( path )",
      arguments = List("path" -> "A path."),
      text = """Returns all relationships in a path.""",
      queryText = """match p=(a)-->(b)-->(c) where a.name='Alice' and c.name='Eskil' return relationships(p)""",
      returns = """All the relationships in the path `p` are returned.""",
      assertions = (p) => assert(2 === p.columnAs[Seq[Node]]("relationships(p)").toSeq.head.length)
    )
  }

  @Test def id() {
    testThis(
      title = "id()",
      syntax = "id( property-container )",
      arguments = List("property-container" -> "A node or a relationship."),
      text = """Returns the id of the relationship or node.""",
      queryText = """match (a) return id(a)""",
      returns = """This returns the node id for three nodes.""",
      assertions = (p) => assert(Seq(0,1,2,3,4) === p.columnAs[Long]("id(a)").toSeq)
    )
  }

  @Test def coalesce() {
    testThis(
      title = "coalesce()",
      syntax = "coalesce( expression [, expression]* )",
      arguments = List("expression" -> "The expression that might return NULL."),
      text = """Returns the first non-++NULL++ value in the list of expressions passed to it.
In case all arguments are +NULL+, +NULL+ will be returned.""",
      queryText = """match (a) where a.name='Alice' return coalesce(a.hairColor, a.eyes)""",
      returns = """""",
      assertions = (p) => assert(Seq("brown") === p.columnAs[String]("coalesce(a.hairColor, a.eyes)").toSeq)
    )
  }

  @Test def range() {
    testThis(
      title = "range()",
      syntax = "range( start, end [, step] )",
      arguments = List(
        "start" -> "A numerical expression.",
        "end" -> "A numerical expression.",
        "step" -> "A numerical expression."
      ),
      text = "`range()` returns numerical values in a range. The default distance between values in the range is `1`. The r is inclusive in both ends.",
      queryText = "return range(0,10), range(2,18,3)",
      returns = "Two lists of numbers in the given ranges are returned.",
      assertions = (p) => assert(List(Map(
        "range(0,10)"-> List(0,1,2,3,4,5,6,7,8,9,10),
        "range(2,18,3)"->List(2,5,8,11,14,17)
      )) === p.toList)
    )
  }

  @Test def replace() {
    testThis(
      title = "replace()",
      syntax = "replace( original, search, replace )",
      arguments = List("original" -> "An expression that returns a string",
                       "search" -> "An expression that returns a string to search for",
                       "replace" -> "An expression that returns the string to replace the search string with"),
      text = "`replace()` returns a string with the search string replaced by the replace string. It replaces all occurrences.",
      queryText = "return replace(\"hello\", \"l\", \"w\")",
      returns = "",
      assertions = (p) => assert(Seq("hewwo") === p.columnAs[String]("replace(\"hello\", \"l\", \"w\")").toSeq)
    )
  }

  @Test def split() {
    testThis(
      title = "split()",
      syntax = "split( original, splitPattern )",
      arguments = List(
        "original" -> "An expression that returns a string",
        "splitPattern" -> "The string to split the original string with"),
      text = "`split()` returns the sequence of strings which are delimited by split patterns.",
      queryText = """return split("one,two", ",")""",
      returns = "",
      assertions = (p) => {
        assert(List(Map(
          """split("one,two", ",")""" -> List("one", "two")
        )) === p.toList)
      }
    )
  }

  @Test def left() {
    testThis(
      title = "left()",
      syntax = "left( original, length )",
      arguments = List("original" -> "An expression that returns a string",
                       "n" -> "An expression that returns a positive number"),
      text = "`left()` returns a string containing the left n characters of the original string.",
      queryText = "return left(\"hello\", 3)",
      returns = "",
      assertions = (p) => assert(Seq("hel") === p.columnAs[String]("left(\"hello\", 3)").toSeq)
    )
  }

  @Test def right() {
    testThis(
      title = "right()",
      syntax = "right( original, length )",
      arguments = List("original" -> "An expression that returns a string",
                       "n" -> "An expression that returns a positive number"),
      text = "`right()` returns a string containing the right n characters of the original string.",
      queryText = "return right(\"hello\", 3)",
      returns = "",
      assertions = (p) => assert(Seq("llo") === p.columnAs[String]("right(\"hello\", 3)").toSeq)
    )
  }

  @Test def substring() {
    testThis(
      title = "substring()",
      syntax = "substring( original, start [, length] )",
      arguments = List("original" -> "An expression that returns a string",
                       "start" -> "An expression that returns a positive number",
                       "length" -> "An expression that returns a positive number"),
      text = "`substring()` returns a substring of the original, with a 0-based index start and length. If length is omitted, it returns a substring from start until the end of the string.",
      queryText = "return substring(\"hello\", 1, 3), substring(\"hello\", 2)",
      returns = "",
      assertions = (p) => assert(List(Map("substring(\"hello\", 1, 3)" -> "ell", "substring(\"hello\", 2)" -> "llo")) === p.toList)
    )
  }

  @Test def lower() {
    testThis(
      title = "lower()",
      syntax = "lower( original )",
      arguments = List("original" -> "An expression that returns a string"),
      text = "`lower()` returns the original string in lowercase.",
      queryText = "return lower(\"HELLO\")",
      returns = "",
      assertions = (p) => assert(List(Map("lower(\"HELLO\")" -> "hello")) === p.toList)
    )
  }

  @Test def upper() {
    testThis(
      title = "upper()",
      syntax = "upper( original )",
      arguments = List("original" -> "An expression that returns a string"),
      text = "`upper()` returns the original string in uppercase.",
      queryText = "return upper(\"hello\")",
      returns = "",
      assertions = (p) => assert(List(Map("upper(\"hello\")" -> "HELLO")) === p.toList)
    )
  }

  @Test def reverse() {
    testThis(
      title = "reverse()",
      syntax = "reverse( original )",
      arguments = List("original" -> "An expression that returns a string"),
      text = "`reverse()` returns the original string reversed.",
      queryText = "return reverse(\"anagram\")",
      returns = "",
      assertions = (p) => assert(List(Map("reverse(\"anagram\")" -> "margana")) === p.toList)
    )
  }

  @Test def ltrim() {
    testThis(
      title = "ltrim()",
      syntax = "ltrim( original )",
      arguments = List("original" -> "An expression that returns a string"),
      text = "`ltrim()` returns the original string with whitespace removed from the left side.",
      queryText = "return ltrim(\"   hello\")",
      returns = "",
      assertions = (p) => assert(List(Map("ltrim(\"   hello\")" -> "hello")) === p.toList)
    )
  }

  @Test def rtrim() {
    testThis(
      title = "rtrim()",
      syntax = "rtrim( original )",
      arguments = List("original" -> "An expression that returns a string"),
      text = "`rtrim()` returns the original string with whitespace removed from the right side.",
      queryText = "return rtrim(\"hello   \")",
      returns = "",
      assertions = (p) => assert(List(Map("rtrim(\"hello   \")" -> "hello")) === p.toList)
    )
  }

  @Test def trim() {
    testThis(
      title = "trim()",
      syntax = "trim( original )",
      arguments = List("original" -> "An expression that returns a string"),
      text = "`trim()` returns the original string with whitespace removed from both sides.",
      queryText = "return trim(\"   hello   \")",
      returns = "",
      assertions = (p) => assert(List(Map("trim(\"   hello   \")" -> "hello")) === p.toList)
    )
  }

  @Test def toInt() {
    testThis(
      title = "toInt()",
      syntax = "toInt( expression )",
      arguments = List("expression" -> "An expression that returns anything"),
      text = "`toInt()` converts the argument to an integer. A string is parsed as if it was an integer number. If the " +
        "parsing fails, +NULL+ will be returned. A floating point number will be cast into an integer.",
      queryText = "return toInt(\"42\"), toInt(\"not a number\")",
      returns = "",
      assertions = (p) => assert(List(Map("toInt(\"42\")" -> 42, "toInt(\"not a number\")" -> null)) === p.toList)
    )
  }

  @Test def toFloat() {
    testThis(
      title = "toFloat()",
      syntax = "toFloat( expression )",
      arguments = List("expression" -> "An expression that returns anything"),
      text = "`toFloat()` converts the argument to a float. A string is parsed as if it was an floating point number. " +
        "If the parsing fails, +NULL+ will be returned. An integer will be cast to a floating point number.",
      queryText = "return toFloat(\"11.5\"), toFloat(\"not a number\")",
      returns = "",
      assertions = (p) => assert(List(Map("toFloat(\"11.5\")" -> 11.5, "toFloat(\"not a number\")" -> null)) === p.toList)
    )
  }

  @Test def toStringFunc() {
    testThis(
      title = "toString()",
      syntax = "toString( expression )",
      arguments = List("expression" -> "An expression that returns a number, a boolean, or a string"),
      text = "`toString()` converts the argument to a string. It converts integral and floating point numbers and booleans to strings, and if called with a string will leave it unchanged.",
      queryText = "return toString(11.5), toString(\"already a string\"), toString(true)",
      returns = "",
      assertions = (p) => assert(List(Map("toString(11.5)" -> "11.5",
                                          "toString(\"already a string\")" -> "already a string",
                                          "toString(true)" -> "true")) === p.toList)
    )
  }

  @Test def propertiesFunc() {
    testThis(
      title = "properties()",
      syntax = "properties( expression )",
      arguments = List("expression" -> "An expression that returns a node, a relationship, or a map"),
      text = "`properties()` converts the arguments to a map of its properties. " +
        "If the argument is a node or a relationship, the returned map is a map of its properties ." +
        "If the argument is already a map, it is returned unchanged.",
      queryText = "create (p:Person {name: 'Stefan', city: 'Berlin'}) return properties(p)",
      returns = "",
      assertions = (p) => assert(List(
        Map("properties(p)" -> Map("name" -> "Stefan", "city" -> "Berlin"))) === p.toList)
    )
  }

  @Test def now() {
    testThis(
      title = "timestamp()",
      syntax = "timestamp()",
      arguments = List.empty,
      text = "`timestamp()` returns the difference, measured in milliseconds, between the current time and midnight, " +
        "January 1, 1970 UTC. It will return the same value during the whole one query, even if the query is a long " +
        "running one.",
      queryText = "return timestamp()",
      returns = "The time in milliseconds is returned.",
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
      title = "startNode()",
      syntax = "startNode( relationship )",
      arguments = List("relationship" -> "An expression that returns a relationship"),
      text = "`startNode()` returns the starting node of a relationship",
      queryText = "MATCH (x:foo)-[r]-() return startNode(r)",
      returns = "",
      assertions = (p) => assert(p.toList.head("startNode(r)") === node("A")))
  }

  @Test def endNode() {
    testThis(
      title = "endNode()",
      syntax = "endNode( relationship )",
      arguments = List("relationship" -> "An expression that returns a relationship"),
      text = "`endNode()` returns the end node of a relationship",
      queryText = "MATCH (x:foo)-[r]-() return endNode(r)",
      returns = "",
      assertions = (p) => assert(p.toList.head("endNode(r)") === node("C")))
  }

  private def testThis(title: String, syntax: String, arguments: List[(String, String)], text: String, queryText: String,
                       returns: String, assertions: (InternalExecutionResult => Unit)) {
    val argsText = arguments.map(x => "* _" + x._1 + ":_ " + x._2).mkString("\r\n\r\n")
    val fullText = String.format("""%s

*Syntax:* `%s`

*Arguments:*

%s""", text, syntax, argsText)
    testQuery(title, fullText, queryText, returns, assertions = assertions)
  }
}
