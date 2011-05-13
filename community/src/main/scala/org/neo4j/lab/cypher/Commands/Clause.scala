package org.neo4j.lab.cypher.commands

import org.neo4j.graphdb.Direction

/**
 * Created by Andres Taylor
 * Date: 4/16/11
 * Time: 13:29 
 */


abstract sealed class Clause {
  //  def and(otherField: Clause): Clause = And(this, otherField)
  //
  //  def or(otherField: Clause): Clause = Or(this, otherField)
}

case class StringEquals(variable: String, propName: String, value: String) extends Clause

case class NumberLargerThan(variable: String, propName: String, value: Float) extends Clause

case class And(a: Clause, b: Clause) extends Clause

case class Or(a: Clause, b: Clause) extends Clause

case class RelatedTo(left:String, right:String, rel:String, relType:String, direction:Direction) extends Clause


//case class NumberEquals(variable:String, propName: String, value: Number) extends Clause
//
//case class BooleanEquals(f: String, value: Boolean) extends Clause
//
//case class In(field: String, values: String*) extends Clause
//
//case class And(lClause: Clause, rClause: Clause) extends Clause
//
//case class Or(lClause: Clause, rClause: Clause) extends Clause