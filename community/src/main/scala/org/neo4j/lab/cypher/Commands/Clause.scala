package org.neo4j.lab.cypher.commands

import org.neo4j.graphdb.Direction

/**
 * Created by Andres Taylor
 * Date: 4/16/11
 * Time: 13:29 
 */


abstract sealed class Clause {
  def ++(other: Clause): Clause = And(this, other)
}

case class StringEquals(variable: String, propName: String, value: String) extends Clause

case class NumberLargerThan(variable: String, propName: String, value: Float) extends Clause

case class And(a: Clause, b: Clause) extends Clause

case class Or(a: Clause, b: Clause) extends Clause
