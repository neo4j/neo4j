/*
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
package org.neo4j.cypher.internal.compiler.v2_3.birk.il

import org.neo4j.cypher.internal.compiler.v2_3.birk.il.ProjectionInstruction.{add, sub, parameter}
import org.scalatest._

class ProjectionInstructionCompilationTest extends FunSuite with Matchers with CodeGenSugar {
  // addition

  { // literal + literal
    def adding( lhs:ProjectionInstruction, rhs:ProjectionInstruction ) = evaluate(
      ProjectProperties( Seq.empty, ProduceResults( Map.empty, Map.empty,
        Map( "result" -> add( lhs, rhs ).projectedVariable.name ) ) ) )

    verifyAddition( adding, new SimpleOperands[ProjectionInstruction]("literal") {
      override def value( value: Any ) = literal( value )
    } )
  }

  { // parameter + parameter
    val addition: ProjectionInstruction = add( parameter( "lhs" ), parameter( "rhs" ) )
    val clazz = compile( ProjectProperties( Seq( addition ), ProduceResults( Map.empty, Map.empty,
      Map( "result" -> addition.projectedVariable.name ) ) ) )
    def adding( lhs:Any, rhs:Any ) = evaluate( newInstance( clazz, params = Map( "lhs" -> lhs, "rhs" -> rhs ) ) )

    verifyAddition( adding, new SimpleOperands[Any]("parameter") {
      override def value( value: Any ) = value
    } )
  }

  { // literal + parameter
    def adding( lhs:ProjectionInstruction, rhs: Any ) = {
      val addition: ProjectionInstruction = add( lhs, parameter( "rhs" ) )
      evaluate( newInstance( compile( ProjectProperties( Seq( addition ), ProduceResults( Map.empty, Map.empty,
        Map( "result" -> addition.projectedVariable.name ) ) ) ), params = Map( "rhs" -> rhs ) ) )
    }

    verifyAddition( adding, new Operands[ProjectionInstruction,Any]("literal","parameter") {
      override def lhs( value: Any ) = literal(value)
      override def rhs( value: Any ) = value
    } )
  }

  { // parameter + literal
    def adding( lhs:Any, rhs: ProjectionInstruction ) = {
      val addition: ProjectionInstruction = add( parameter( "lhs" ), rhs )
      evaluate( newInstance( compile( ProjectProperties( Seq( addition ), ProduceResults( Map.empty, Map.empty,
        Map( "result" -> addition.projectedVariable.name ) ) ) ), params = Map( "lhs" -> lhs ) ) )
    }

    verifyAddition( adding, new Operands[Any, ProjectionInstruction]("parameter","literal") {
      override def lhs( value: Any ) = value
      override def rhs( value: Any ) = literal(value)
    } )
  }

  // subtraction

  { // literal - literal
    def subtracting( lhs:ProjectionInstruction, rhs:ProjectionInstruction ) = evaluate(
      ProjectProperties( Seq.empty, ProduceResults( Map.empty, Map.empty,
        Map( "result" -> sub( lhs, rhs ).projectedVariable.name ) ) ) )

    verifySubtraction( subtracting, new SimpleOperands[ProjectionInstruction]("literal") {
      override def value( value: Any ) = literal( value )
    } )
  }

  { // parameter - parameter
    val subtraction: ProjectionInstruction = sub( parameter( "lhs" ), parameter( "rhs" ) )
    val clazz = compile( ProjectProperties( Seq( subtraction ), ProduceResults( Map.empty, Map.empty,
      Map( "result" -> subtraction.projectedVariable.name ) ) ) )
    def subtracting( lhs:Any, rhs:Any ) = evaluate( newInstance( clazz, params = Map( "lhs" -> lhs, "rhs" -> rhs ) ) )

    verifySubtraction( subtracting, new SimpleOperands[Any]("parameter") {
      override def value( value: Any ) = value
    } )
  }

  { // literal - parameter
    def subtracting( lhs:ProjectionInstruction, rhs: Any ) = {
      val subtraction: ProjectionInstruction = sub( lhs, parameter( "rhs" ) )
      evaluate( newInstance( compile( ProjectProperties( Seq( subtraction ), ProduceResults( Map.empty, Map.empty,
        Map( "result" -> subtraction.projectedVariable.name ) ) ) ), params = Map( "rhs" -> rhs ) ) )
    }

    verifySubtraction( subtracting, new Operands[ProjectionInstruction,Any]("literal","parameter") {
      override def lhs( value: Any ) = literal(value)
      override def rhs( value: Any ) = value
    } )
  }

  { // parameter - literal
    def subtracting( lhs:Any, rhs: ProjectionInstruction ) = {
      val subtraction: ProjectionInstruction = sub( parameter( "lhs" ), rhs )
      evaluate( newInstance( compile( ProjectProperties( Seq( subtraction ), ProduceResults( Map.empty, Map.empty,
        Map( "result" -> subtraction.projectedVariable.name ) ) ) ), params = Map( "lhs" -> lhs ) ) )
    }

    verifySubtraction( subtracting, new Operands[Any, ProjectionInstruction]("parameter","literal") {
      override def lhs( value: Any ) = value
      override def rhs( value: Any ) = literal(value)
    } )
  }

  type Operator[Lhs,Rhs] = (Lhs, Rhs) => List[Map[String, Any]]

  private def verifyAddition[Lhs,Rhs]( add: Operator[Lhs,Rhs], value: Operands[Lhs,Rhs] ) = {
    verify( add, 7, "+", 9, value, 16 )
    verify( add, "abc", "+", 7, value, "abc7" )
    verify( add, 9, "+", "abc", value, "9abc" )
    verify( add, 3.14, "+", "abc", value, "3.14abc" )
    verify( add, "abc", "+", 3.14, value, "abc3.14" )
    verify( add, 7, "+", 3.14, value, 10.14 )
    verify( add, 11.6, "+", 3, value, 14.6 )
    verify( add, 2.5, "+", 4.5, value, 7.0 )
    verify( add, Long.MaxValue, "+", Long.MinValue, value, -1 )
  }

  private def verifySubtraction[Lhs,Rhs]( sub: Operator[Lhs,Rhs], value: Operands[Lhs,Rhs] ) = {
    verify( sub, 9, "-", 7, value, 2 )
    verify( sub, Long.MaxValue, "-", Int.MaxValue, value, Long.MaxValue - Int.MaxValue )
    verify( sub, 3.25, "-", 3, value, 0.25 )
    verify( sub, 3.21, "-", 1.23, value, 1.98 )
    verify( sub, -1, "-", -2, value, 1 )
    verify( sub, -1.25, "-", -2.5, value, 1.25 )
  }

  abstract class Operands[Lhs,Rhs](val left:String, val right:String) {
    def lhs( value:Any ): Lhs
    def rhs( value:Any ): Rhs
  }
  abstract class SimpleOperands[Value](v:String) extends Operands[Value,Value](v,v) {
    override def lhs( v:Any ) = value(v)
    override def rhs( v:Any ) = value(v)
    def value( value:Any ): Value
  }

  def verify[Lhs,Rhs]( operator: Operator[Lhs,Rhs], lhs: Any, op:String, rhs: Any, value: Operands[Lhs, Rhs], result: Any ) =
    test(s"${lhs.getClass.getSimpleName} ${value.left} ($lhs) $op ${rhs.getClass.getSimpleName} ${value.right} ($rhs)") {
      operator( value.lhs(lhs), value.rhs(rhs) ) shouldEqual List( Map( "result" -> result ) )
    }

  def literal(value: Any): ProjectionInstruction =
  {
    value match {
      case v:Int => ProjectionInstruction.literal(v)
      case v:Long => ProjectionInstruction.literal(v)
      case v:Double => ProjectionInstruction.literal(v)
      case v:String => ProjectionInstruction.literal(v)
      case v:Boolean => ProjectionInstruction.literal(v)
    }
  }
}
