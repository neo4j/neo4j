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
package org.neo4j.cypher.internal.parser.v2_0

import util.parsing.combinator.JavaTokenParsers


trait Strings extends JavaTokenParsers {
  def KEYWORDS =
    START | CREATE | SET | DELETE | FOREACH | MATCH | WHERE | VALUES | WITH |
    RETURN | SKIP | LIMIT | ORDER | BY | ASC | DESC | ON | WHEN | CASE | THEN | ELSE | DROP

  // KEYWORDS
  def START = ignoreCase("start")
  def CREATE = ignoreCase("create")
  def SET = ignoreCase("set")
  def DELETE = ignoreCase("delete")
  def FOREACH = ignoreCase("foreach")
  def MATCH = ignoreCase("match")
  def WHERE = ignoreCase("where")
  def VALUES = ignoreCase("values")
  def WITH = ignoreCase("with")
  def RETURN = ignoreCase("return")
  def SKIP = ignoreCase("skip")
  def LIMIT = ignoreCase("limit")
  def ORDER = ignoreCase("order")
  def BY = ignoreCase("by")
  def ASC = ignoreCase("asc")|ignoreCase("ascending")
  def DESC = ignoreCase("desc")|ignoreCase("descending")
  def ON = ignoreCase("on")
  def WHEN = ignoreCase("when")
  def CASE = ignoreCase("case")
  def THEN = ignoreCase("then")
  def ELSE = ignoreCase("else")
  def DROP = ignoreCase("drop")

  // SHOULD THESE ALSO BE KEYWORDS?
  def UNIQUE = ignoreCase("unique")
  def UNION = ignoreCase("union")
  def ALL = ignoreCase("all")
  def NULL = ignoreCase("null")
  def TRUE = ignoreCase("true")
  def FALSE = ignoreCase("false")
  def DISTINCT = ignoreCase("distinct")
  def END = ignoreCase("end")
  def IS = ignoreCase("is")
  def NOT = ignoreCase("not")
  def HAS = ignoreCase("has")
  def ANY = ignoreCase("any")
  def NONE = ignoreCase("none")
  def SINGLE = ignoreCase("single")
  def OR = ignoreCase("or")
  def AND = ignoreCase("and")
  def AS = ignoreCase("as")
  def NODE = ignoreCase("node")
  def RELATIONSHIP = (ignoreCase("relationship") | ignoreCase("rel")) ^^^ "rel"
  def REMOVE = ignoreCase("remove")
  def IN = ignoreCase("in")
  def INDEX = ignoreCase("index")

  //FUNCTIONS
  def EXTRACT = ignoreCase("extract")
  def REDUCE = ignoreCase("reduce")
  def COALESCE = ignoreCase("coalesce")
  def FILTER = ignoreCase("filter")   //"count", "sum", "min", "max", "avg", "collect"
  def COUNT = ignoreCase("count")
  def SUM = ignoreCase("sum")
  def MIN = ignoreCase("min")
  def MAX = ignoreCase("max")
  def AVG = ignoreCase("avg")
  def COLLECT = ignoreCase("collect")
  def PERCENTILE_CONT = ignoreCase("percentile_cont")
  def PERCENTILE_DISC = ignoreCase("percentile_disc")

  def SHORTESTPATH = ignoreCase("shortestPath")
  def ALLSHORTESTPATHS = ignoreCase("allshortestpaths")



  private def ignoreCase(str: String): Parser[String] =
    ("""(?i)\b""" + str + """\b""").r ^^ (x => x.toLowerCase) |
      failure("expected " + str.toUpperCase)
}