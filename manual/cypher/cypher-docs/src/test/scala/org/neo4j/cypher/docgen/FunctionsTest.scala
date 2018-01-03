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
package org.neo4j.cypher.docgen

import org.junit.Assert._
import org.junit.Test
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.InternalExecutionResult
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
    "collection" -> "An expression that returns a collection",
    "identifier" -> "This is the identifier that can be used from the predicate.",
    "predicate" -> "A predicate that is tested against all items in the collection."
  )

  @Test def all() {
    testThis(
      title = "ALL",
      syntax = "ALL(identifier in collection WHERE predicate)",
      arguments = common_arguments,
      text = """Tests whether a predicate holds for all elements of this collection.""",
      queryText = """match p=(a)-[*1..3]->(b) where a.name='Alice' and b.name='Daniel' and all(x in nodes(p) WHERE x.age > 30) return p""",
      returns = """All nodes in the returned paths will have an `age` property of at least 30.""",
      assertions = (p) => assertEquals(1, p.toSeq.length))
  }

  @Test def any() {
    testThis(
      title = "ANY",
      syntax = "ANY(identifier in collection WHERE predicate)",
      arguments = common_arguments,
      text = """Tests whether a predicate holds for at least one element in the collection.""",
      queryText = """match (a) where a.name='Eskil' and any(x in a.array WHERE x = "one") return a""",
      returns = """All nodes in the returned paths has at least one `one` value set in the array property named `array`.""",
      assertions = (p) => assertEquals(List(Map("a"->node("E"))), p.toList))
  }

  @Test def none() {
    testThis(
      title = "NONE",
      syntax = "NONE(identifier in collection WHERE predicate)",
      arguments = common_arguments,
      text = """Returns true if the predicate holds for no element in the collection.""",
      queryText = """match p=(n)-[*1..3]->(b) where n.name='Alice' and NONE(x in nodes(p) WHERE x.age = 25) return p""",
      returns = """No nodes in the returned paths has a `age` property set to `25`.""",
      assertions = (p) => assertEquals(2, p.toSeq.length))
  }

  @Test def single() {
    testThis(
      title = "SINGLE",
      syntax = "SINGLE(identifier in collection WHERE predicate)",
      arguments = common_arguments,
      text = """Returns true if the predicate holds for exactly one of the elements in the collection.""",
      queryText = """match p=(n)-->(b) where n.name='Alice' and SINGLE(var in nodes(p) WHERE var.eyes = "blue") return p""",
      returns = """Exactly one node in every returned path will have the `eyes` property set to `"blue"`.""",
      assertions = (p) => assertEquals(1, p.toSeq.length))
  }

  @Test def exists() {
    testThis(
      title = "EXISTS",
      syntax = "EXISTS( pattern-or-property )",
      arguments = List("pattern-or-property" -> "A pattern or a property (in the form 'identifier.prop')."),
      text = """Returns true if a match for the pattern exists in the graph, or the property exists in the node, relationship or map.""",
      queryText = """match (n) where EXISTS(n.name) return n.name AS name, EXISTS( (n)-[:MARRIED]->() ) AS is_married""",
      returns = """This query returns all the nodes with a name property along with a boolean true/false indicating if they are married.""",
      assertions = (p) => assertEquals(5, p.toSeq.length))
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

  @Test def size() {
    testThis(
      title = "SIZE",
      syntax = "SIZE( collection )",
      arguments = List("collection" -> "An expression that returns a collection"),
      text = """To return or filter on the size of a collection, use the `SIZE()` function.""",
      queryText = """return size(['Alice', 'Bob']) as col""",
      returns = """The number of items in the collection is returned by the query.""",
      assertions = (col) => assertEquals(2, col.columnAs[Int]("col").toList.head))
  }

  @Test def size2() {
    testThis(
      title = "SIZE of pattern expression",
      syntax = "SIZE( pattern expression )",
      arguments = List("pattern expression" -> "A pattern expression that returns a collection"),
      text = """
               |This is the same `SIZE()` method described before,
               |but instead of passing in a collection directly, you provide a pattern expression
               |that can be used in a match query to provide a new set of results.
               |The size of the result is calculated, not the length of the expression itself.
               |""".stripMargin,
      queryText = """match (a) where a.name='Alice' return size( (a)-->()-->() ) as fof""",
      returns = """The number of sub-graphs matching the pattern expression is returned by the query.""",
      assertions = (p) => assertEquals(3, p.columnAs[Int]("fof").toList.head))
  }

  @Test def length() {
    testThis(
      title = "LENGTH",
      syntax = "LENGTH( path )",
      arguments = List("path" -> "An expression that returns a path"),
      text = """To return or filter on the length of a path, use the `LENGTH()` function.""",
      queryText = """match p=(a)-->(b)-->(c) where a.name='Alice' return length(p)""",
      returns = """The length of the path `p` is returned by the query.""",
      assertions = (p) => assertEquals(2, p.columnAs[Int]("length(p)").toList.head))
  }

  @Test def lengthString() {
    testThis(
      title = "LENGTH of string",
      syntax = "LENGTH( string )",
      arguments = List("string" -> "An expression that returns a string"),
      text = """To return or filter on the length of a string, use the `LENGTH()` function.""",
      queryText = """match (a) where length(a.name) > 6 return length(a.name)""",
      returns = """The length of the name `Charlie` is returned by the query.""",
      assertions = (p) => assertEquals(7, p.columnAs[Int]("length(a.name)").toList.head))
  }

  @Test def labels() {
    testThis(
      title = "LABELS",
      syntax = "LABELS( node )",
      arguments = List("node" -> "Any expression that returns a single node"),
      text = """Returns a collection of string representations for the labels attached to a node.""",
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
      title = "KEYS",
      syntax = "KEYS(  property-container )",
      arguments = List("property-container" -> "A node, a relationship, or a literal map."),
      text = """Returns a collection of string representations for the property names of a node, relationship, or map.""",
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
      title = "EXTRACT",
      syntax = "EXTRACT( identifier in collection | expression )",
      arguments = List(
        "collection" -> "An expression that returns a collection",
        "identifier" -> "The closure will have an identifier introduced in it's context. Here you decide which identifier to use.",
        "expression" -> "This expression will run once per value in the collection, and produces the result collection."
      ),
      text = """To return a single property, or the value of a function from a collection of nodes or relationships,
 you can use `EXTRACT`. It will go through a collection, run an expression on every element, and return the results
 in an collection with these values. It works like the `map` method in functional languages such as Lisp and Scala.""",
      queryText = """match p=(a)-->(b)-->(c) where a.name='Alice' and b.name='Bob' and c.name='Daniel' return extract(n in nodes(p) | n.age) AS extracted""",
      returns = """The age property of all nodes in the path are returned.""",
      assertions = (p) => assertEquals(List(Map("extracted" -> List(38, 25, 54))), p.toList))
  }

  @Test def reduce() {
    testThis(
      title = "REDUCE",
      syntax = "REDUCE( accumulator = initial,  identifier in collection | expression )",
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
      queryText = """match p=(a)-->(b)-->(c) where a.name='Alice' and b.name='Bob' and c.name='Daniel' return reduce(totalAge = 0, n in nodes(p) | totalAge + n.age) AS reduction""",
      returns = """The age property of all nodes in the path are summed and returned as a single value.""",
      assertions = (p) => assertEquals(List(Map("reduction" -> 117)), p.toList))
  }

  @Test def head() {
    testThis(
      title = "HEAD",
      syntax = "HEAD( expression )",
      arguments = List(
        "expression" -> "This expression should return a collection of some kind."
      ),
      text = "`HEAD` returns the first element in a collection.",
      queryText = """match (a) where a.name='Eskil' return a.array, head(a.array)""",
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
      queryText = """match (a) where a.name='Eskil' return a.array, last(a.array)""",
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
      queryText = """match (a) where a.name='Eskil' return a.array, tail(a.array)""",
      returns = "This returns the property named `array` and all elements of that property except the first one.",
      assertions = (p) => {
        val toList = p.columnAs[Iterable[_]]("tail(a.array)").toList.head.toList
        assert(toList === List("two","three"))
      })
  }

  @Test def filter() {
    testThis(
      title = "FILTER",
      syntax = "FILTER(identifier in collection WHERE predicate)",
      arguments = common_arguments,
      text = "`FILTER` returns all the elements in a collection that comply to a predicate.",
      queryText = """match (a) where a.name='Eskil' return a.array, filter(x in a.array WHERE size(x) = 3)""",
      returns = "This returns the property named `array` and a list of values in it, which have size `3`.",
      assertions = (p) => {
        val array = p.columnAs[Iterable[_]]("filter(x in a.array WHERE size(x) = 3)").toList.head
        assert(List("one","two") === array.toList)
      })
  }

  @Test def nodes_in_path() {
    testThis(
      title = "NODES",
      syntax = "NODES( path )",
      arguments = List("path" -> "A path."),
      text = """Returns all nodes in a path.""",
      queryText = """match p=(a)-->(b)-->(c) where a.name='Alice' and c.name='Eskil' return nodes(p)""",
      returns = """All the nodes in the path `p` are returned by the example query.""",
      assertions = (p) => assert(List(node("A"), node("B"), node("E")) === p.columnAs[Seq[Node]]("nodes(p)").toList.head)
    )
  }

  @Test def rels_in_path() {
    testThis(
      title = "RELATIONSHIPS",
      syntax = "RELATIONSHIPS( path )",
      arguments = List("path" -> "A path."),
      text = """Returns all relationships in a path.""",
      queryText = """match p=(a)-->(b)-->(c) where a.name='Alice' and c.name='Eskil' return relationships(p)""",
      returns = """All the relationships in the path `p` are returned.""",
      assertions = (p) => assert(2 === p.columnAs[Seq[Node]]("relationships(p)").toSeq.head.length)
    )
  }

  @Test def id() {
    testThis(
      title = "ID",
      syntax = "ID( property-container )",
      arguments = List("property-container" -> "A node or a relationship."),
      text = """Returns the id of the relationship or node.""",
      queryText = """match (a) return id(a)""",
      returns = """This returns the node id for three nodes.""",
      assertions = (p) => assert(Seq(0,1,2,3,4) === p.columnAs[Int]("id(a)").toSeq)
    )
  }

  @Test def coalesce() {
    testThis(
      title = "COALESCE",
      syntax = "COALESCE( expression [, expression]* )",
      arguments = List("expression" -> "The expression that might return NULL."),
      text = """Returns the first non-++NULL++ value in the list of expressions passed to it.
In case all arguments are +NULL+, +NULL+ will be returned.""",
      queryText = """match (a) where a.name='Alice' return coalesce(a.hairColor, a.eyes)""",
      returns = """""",
      assertions = (p) => assert(Seq("brown") === p.columnAs[String]("coalesce(a.hairColor, a.eyes)").toSeq)
    )
  }

  @Test def abs() {
    testThis(
      title = "ABS",
      syntax = "ABS( expression )",
      arguments = List("expression" -> "A numeric expression."),
      text = "`ABS` returns the absolute value of a number.",
      queryText = """match (a), (e) where a.name = 'Alice' and e.name = 'Eskil' return a.age, e.age, abs(a.age - e.age)""",
      returns = "The absolute value of the age difference is returned.",
      assertions = (p) => assert(List(Map("abs(a.age - e.age)"->3.0, "a.age"->38, "e.age"->41)) === p.toList)
    )
  }

  @Test def acos() {
    testThis(
      title = "ACOS",
      syntax = "ACOS( expression )",
      arguments = List("expression" -> "A numeric expression."),
      text = "`ACOS` returns the arccosine of the expression, in radians.",
      queryText = """return acos(0.5)""",
      returns = "The arccosine of 0.5.",
      assertions = (p) => assertEquals(1.0471975511965979, p.toList.head("acos(0.5)").asInstanceOf[Double], 0.000001)
    )
  }

  @Test def asin() {
    testThis(
      title = "ASIN",
      syntax = "ASIN( expression )",
      arguments = List("expression" -> "A numeric expression."),
      text = "`ASIN` returns the arcsine of the expression, in radians.",
      queryText = """return asin(0.5)""",
      returns = "The arcsine of 0.5.",
      assertions = (p) => assertEquals(0.5235987755982989, p.toList.head("asin(0.5)").asInstanceOf[Double], 0.000001)
    )
  }

  @Test def atan() {
    testThis(
      title = "ATAN",
      syntax = "ATAN( expression )",
      arguments = List("expression" -> "A numeric expression."),
      text = "`ATAN` returns the arctangent of the expression, in radians.",
      queryText = """return atan(0.5)""",
      returns = "The arctangent of 0.5.",
      assertions = (p) => assertEquals(0.4636476090008061, p.toList.head("atan(0.5)").asInstanceOf[Double], 0.000001)
    )
  }

  @Test def atan2() {
    testThis(
      title = "ATAN2",
      syntax = "ATAN2( expression , expression)",
      arguments = List("expression" -> "A numeric expression for y.", "expression" -> "A numeric expression for x."),
      text = "`ATAN2` returns the arctangent2 of a set of coordinates, in radians.",
      queryText = """return atan2(0.5, 0.6)""",
      returns = "The arctangent2 of 0.5, 0.6.",
      assertions = (p) => assertEquals(0.6947382761967033, p.toList.head("atan2(0.5, 0.6)").asInstanceOf[Double], 0.000001)
    )
  }

  @Test def ceil() {
    testThis(
      title = "CEIL",
      syntax = "CEIL( expression )",
      arguments = List("expression" -> "A numeric expression."),
      text = "`CEIL` returns the smallest integer greater than or equal to the number.",
      queryText = """return ceil(0.1)""",
      returns = "The ceil of 0.1",
      assertions = (p) => assertEquals(1.0, p.toList.head("ceil(0.1)").asInstanceOf[Double], 0.000001)
    )
  }

  @Test def cos() {
    testThis(
      title = "COS",
      syntax = "COS( expression )",
      arguments = List("expression" -> "A numeric expression."),
      text = "`COS` returns the cosine of the expression.",
      queryText = """return cos(0.5)""",
      returns = "The cosine of 0.5 is returned.",
      assertions = (p) => assertEquals(0.87758256189, p.toList.head("cos(0.5)").asInstanceOf[Double], 0.000001)
    )
  }

  @Test def cot() {
    testThis(
      title = "COT",
      syntax = "COT( expression )",
      arguments = List("expression" -> "A numeric expression."),
      text = "`COT` returns the cotangent of the expression.",
      queryText = """return cot(0.5)""",
      returns = "The cotangent of 0.5 is returned.",
      assertions = (p) => assertEquals(1.830487721712452, p.toList.head("cot(0.5)").asInstanceOf[Double], 0.000001)
    )
  }

  @Test def degrees() {
    testThis(
      title = "DEGREES",
      syntax = "DEGREES( expression )",
      arguments = List("expression" -> "A numeric expression."),
      text = "`DEGREES` converts radians to degrees.",
      queryText = """return degrees(3.14159)""",
      returns = "The number of degrees in something close to pi.",
      assertions = (p) => assertEquals(180.0, p.toList.head("degrees(3.14159)").asInstanceOf[Double], 0.001)
    )
  }

  @Test def e() {
    testThis(
      title = "E",
      syntax = "E()",
      arguments = List.empty,
      text = "`E` returns the constant, e.",
      queryText = """return e()""",
      returns = "The constant e is returned (the base of natural log).",
      assertions = (p) => assertEquals(2.718281828459045, p.toList.head("e()").asInstanceOf[Double], 0.000001)
    )
  }

  @Test def exp() {
    testThis(
      title = "EXP",
      syntax = "EXP( expression )",
      arguments = List("expression" -> "A numeric expression."),
      text = "`EXP` returns the value e raised to the power of the expression.",
      queryText = """return exp(2)""",
      returns = "The exp of 2 is returned: e^2^.",
      assertions = (p) => assertEquals(7.38905609893065, p.toList.head("exp(2)").asInstanceOf[Double], 0.000001)
    )
  }

  @Test def floor() {
    testThis(
      title = "FLOOR",
      syntax = "FLOOR( expression )",
      arguments = List("expression" -> "A numeric expression."),
      text = "`FLOOR` returns the greatest integer less than or equal to the expression.",
      queryText = """return floor(0.9)""",
      returns = "The floor of 0.9 is returned.",
      assertions = (p) => assertEquals(0.0, p.toList.head("floor(0.9)").asInstanceOf[Double], 0.000001)
    )
  }

  @Test def haversin() {
    testThis(
      title = "HAVERSIN",
      syntax = "HAVERSIN( expression )",
      arguments = List("expression" -> "A numeric expression."),
      text = "`HAVERSIN` returns half the versine of the expression.",
      queryText = """return haversin(0.5)""",
      returns = "The haversine of 0.5 is returned.",
      assertions = (p) => assertEquals(0.061208719054813, p.toList.head("haversin(0.5)").asInstanceOf[Double], 0.000001)
    )
  }

  @Test def haversin_spherical_distance() {
    testQuery(
      title = "Spherical distance using the haversin function",
      text =
"The +haversin+ function may be used to compute the distance on the surface of a sphere between two points (each given " +
"by their latitude and longitude). In this example the spherical distance (in km) between Berlin in Germany " +
"(at lat 52.5, lon 13.4) and San Mateo in California (at lat 37.5, lon -122.3) is calculated using an average " +
"earth radius of 6371 km.",
      queryText = """CREATE (ber:City {lat: 52.5, lon: 13.4}), (sm:City {lat: 37.5, lon: -122.3}) RETURN 2 * 6371 * asin(sqrt(haversin(radians( sm.lat - ber.lat )) + cos(radians( sm.lat )) * cos(radians( ber.lat )) * haversin(radians( sm.lon - ber.lon )))) AS dist""",
      optionalResultExplanation = "The distance between Berlin and San Mateo is returned (about 9129 km).",
      assertions = (p) => assertEquals(9129, p.toList.head("dist").asInstanceOf[Double], 1)
    )
  }

  @Test def log() {
    testThis(
      title = "LOG",
      syntax = "LOG( expression )",
      arguments = List("expression" -> "A numeric expression."),
      text = "`LOG` returns the natural logarithm of the expression.",
      queryText = """return log(27)""",
      returns = "The log of 27 is returned.",
      assertions = (p) => assertEquals(3.295836866004329, p.toList.head("log(27)").asInstanceOf[Double], 0.000001)
    )
  }

  @Test def log10() {
    testThis(
      title = "LOG10",
      syntax = "LOG10( expression )",
      arguments = List("expression" -> "A numeric expression."),
      text = "`LOG10` returns the base 10 logarithm of the expression.",
      queryText = """return log10(27)""",
      returns = "The log10 of 27 is returned.",
      assertions = (p) => assertEquals(1.4313637641589874, p.toList.head("log10(27)").asInstanceOf[Double], 0.000001)
    )
  }

  @Test def pi() {
    testThis(
      title = "PI",
      syntax = "PI()",
      arguments = List.empty,
      text = "`PI` returns the mathematical constant pi.",
      queryText = """return pi()""",
      returns = "The constant pi is returned.",
      assertions = (p) => assertEquals(3.141592653589793, p.toList.head("pi()").asInstanceOf[Double], 0.000001)
    )
  }

  @Test def radians() {
    testThis(
      title = "RADIANS",
      syntax = "RADIANS( expression )",
      arguments = List("expression" -> "A numeric expression."),
      text = "`RADIANS` converts degrees to radians.",
      queryText = """return radians(180)""",
      returns = "The number of radians in 180 is returned (pi).",
      assertions = (p) => assertEquals(3.141592653589793, p.toList.head("radians(180)").asInstanceOf[Double], 0.000001)
    )
  }

  @Test def sin() {
    testThis(
      title = "SIN",
      syntax = "SIN( expression )",
      arguments = List("expression" -> "A numeric expression."),
      text = "`SIN` returns the sine of the expression.",
      queryText = """return sin(0.5)""",
      returns = "The sine of 0.5 is returned.",
      assertions = (p) => assertEquals(0.479425538604203, p.toList.head("sin(0.5)").asInstanceOf[Double], 0.000001)
    )
  }

  @Test def tan() {
    testThis(
      title = "TAN",
      syntax = "TAN( expression )",
      arguments = List("expression" -> "A numeric expression."),
      text = "`TAN` returns the tangent of the expression.",
      queryText = """return tan(0.5)""",
      returns = "The tangent of 0.5 is returned.",
      assertions = (p) => assertEquals(0.5463024898437905, p.toList.head("tan(0.5)").asInstanceOf[Double], 0.000001)
    )
  }

  @Test def round() {
    testThis(
      title = "ROUND",
      syntax = "ROUND( expression )",
      arguments = List("expression" -> "A numerical expression."),
      text = "`ROUND` returns the numerical expression, rounded to the nearest integer.",
      queryText = """return round(3.141592)""",
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
      queryText = """return sqrt(256)""",
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
      queryText = "return sign(-17), sign(0.1)",
      returns = "",
      assertions = (p) => assert(List(Map("sign(-17)"-> -1, "sign(0.1)"->1)) === p.toList)
    )
  }

  @Test def rand() {
    testThis(
      title = "RAND",
      syntax = "RAND( expression )",
      arguments = List("expression" -> "A numeric expression."),
      text = "`RAND` returns a random double between 0 and 1.0.",
      queryText = """return rand() as x1""",
      returns = "A random number is returned.",
      assertions = (p) => assert(p.toList.head("x1").asInstanceOf[Double] >= 0)
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
      queryText = "return range(0,10), range(2,18,3)",
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
      queryText = "return replace(\"hello\", \"l\", \"w\")",
      returns = "",
      assertions = (p) => assert(Seq("hewwo") === p.columnAs[String]("replace(\"hello\", \"l\", \"w\")").toSeq)
    )
  }

  @Test def split() {
    testThis(
      title = "SPLIT",
      syntax = "SPLIT( original, splitPattern )",
      arguments = List(
        "original" -> "An expression that returns a string",
        "splitPattern" -> "The string to split the original string with"),
      text = "`SPLIT` returns the sequence of strings witch are delimited by split patterns.",
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
      title = "LEFT",
      syntax = "LEFT( original, length )",
      arguments = List("original" -> "An expression that returns a string",
                       "n" -> "An expression that returns a positive number"),
      text = "`LEFT` returns a string containing the left n characters of the original string.",
      queryText = "return left(\"hello\", 3)",
      returns = "",
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
      queryText = "return right(\"hello\", 3)",
      returns = "",
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
      queryText = "return substring(\"hello\", 1, 3), substring(\"hello\", 2)",
      returns = "",
      assertions = (p) => assert(List(Map("substring(\"hello\", 1, 3)" -> "ell", "substring(\"hello\", 2)" -> "llo")) === p.toList)
    )
  }

  @Test def lower() {
    testThis(
      title = "LOWER",
      syntax = "LOWER( original )",
      arguments = List("original" -> "An expression that returns a string"),
      text = "`LOWER` returns the original string in lowercase.",
      queryText = "return lower(\"HELLO\")",
      returns = "",
      assertions = (p) => assert(List(Map("lower(\"HELLO\")" -> "hello")) === p.toList)
    )
  }

  @Test def upper() {
    testThis(
      title = "UPPER",
      syntax = "UPPER( original )",
      arguments = List("original" -> "An expression that returns a string"),
      text = "`UPPER` returns the original string in uppercase.",
      queryText = "return upper(\"hello\")",
      returns = "",
      assertions = (p) => assert(List(Map("upper(\"hello\")" -> "HELLO")) === p.toList)
    )
  }

  @Test def reverse() {
    testThis(
      title = "REVERSE",
      syntax = "REVERSE( original )",
      arguments = List("original" -> "An expression that returns a string"),
      text = "`REVERSE` returns the original string reversed.",
      queryText = "return reverse(\"anagram\")",
      returns = "",
      assertions = (p) => assert(List(Map("reverse(\"anagram\")" -> "margana")) === p.toList)
    )
  }

  @Test def ltrim() {
    testThis(
      title = "LTRIM",
      syntax = "LTRIM( original )",
      arguments = List("original" -> "An expression that returns a string"),
      text = "`LTRIM` returns the original string with whitespace removed from the left side.",
      queryText = "return ltrim(\"   hello\")",
      returns = "",
      assertions = (p) => assert(List(Map("ltrim(\"   hello\")" -> "hello")) === p.toList)
    )
  }

  @Test def rtrim() {
    testThis(
      title = "RTRIM",
      syntax = "RTRIM( original )",
      arguments = List("original" -> "An expression that returns a string"),
      text = "`RTRIM` returns the original string with whitespace removed from the right side.",
      queryText = "return rtrim(\"hello   \")",
      returns = "",
      assertions = (p) => assert(List(Map("rtrim(\"hello   \")" -> "hello")) === p.toList)
    )
  }

  @Test def trim() {
    testThis(
      title = "TRIM",
      syntax = "TRIM( original )",
      arguments = List("original" -> "An expression that returns a string"),
      text = "`TRIM` returns the original string with whitespace removed from both sides.",
      queryText = "return trim(\"   hello   \")",
      returns = "",
      assertions = (p) => assert(List(Map("trim(\"   hello   \")" -> "hello")) === p.toList)
    )
  }

  @Test def str() {
    testThis(
      title = "STR",
      syntax = "STR( expression )",
      arguments = List("expression" -> "An expression that returns anything"),
      text = "`STR` returns a string representation of the expression. If the expression returns a string the result will" +
        "be wrapped in quotation marks.",
      queryText = "return str(1), str(\"hello\")",
      returns = "",
      assertions = (p) => assert(List(Map("str(1)" -> "1",
                                          "str(\"hello\")" -> "\"hello\"")) === p.toList)
    )
  }

  @Test def toInt() {
    testThis(
      title = "TOINT",
      syntax = "TOINT( expression )",
      arguments = List("expression" -> "An expression that returns anything"),
      text = "`TOINT` converts the argument to an integer. A string is parsed as if it was an integer number. If the " +
        "parsing fails, +NULL+ will be returned. A floating point number will be cast into an integer.",
      queryText = "return toInt(\"42\"), toInt(\"not a number\")",
      returns = "",
      assertions = (p) => assert(List(Map("toInt(\"42\")" -> 42, "toInt(\"not a number\")" -> null)) === p.toList)
    )
  }

  @Test def toFloat() {
    testThis(
      title = "TOFLOAT",
      syntax = "TOFLOAT( expression )",
      arguments = List("expression" -> "An expression that returns anything"),
      text = "`TOFLOAT` converts the argument to a float. A string is parsed as if it was an floating point number. " +
        "If the parsing fails, +NULL+ will be returned. An integer will be cast to a floating point number.",
      queryText = "return toFloat(\"11.5\"), toFloat(\"not a number\")",
      returns = "",
      assertions = (p) => assert(List(Map("toFloat(\"11.5\")" -> 11.5, "toFloat(\"not a number\")" -> null)) === p.toList)
    )
  }

  @Test def toStringFunc() {
    testThis(
      title = "TOSTRING",
      syntax = "TOSTRING( expression )",
      arguments = List("expression" -> "An expression that returns a number or a string"),
      text = "`TOSTRING` converts the argument to a string. It converts integral and floating point numbers to strings, and if called with a string will leave it unchanged.",
      queryText = "return toString(11.5), toString(\"already a string\")",
      returns = "",
      assertions = (p) => assert(List(Map("toString(11.5)" -> "11.5", "toString(\"already a string\")" -> "already a string")) === p.toList)
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
