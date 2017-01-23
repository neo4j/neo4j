/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.spi.v3_2.codegen


import org.neo4j.codegen.FieldReference.field
import org.neo4j.codegen.Parameter.param
import org.neo4j.codegen._
import org.neo4j.cypher.internal.codegen.{CompiledOrderabilityUtils, CompiledEquivalenceUtils}
import org.neo4j.cypher.internal.compiler.v3_2.codegen.ir.expressions.{RepresentationType, CodeGenType}
import org.neo4j.cypher.internal.compiler.v3_2.codegen.spi._
import org.neo4j.cypher.internal.compiler.v3_2.helpers._
import org.neo4j.cypher.internal.frontend.v3_2.symbols

import scala.collection.mutable

class AuxGenerator(val packageName: String, val generator: CodeGenerator) {

  import GeneratedQueryStructure.{lowerType, method, typeRef}

  private val types: scala.collection.mutable.Map[_ >: TupleDescriptor, TypeReference] = mutable.Map.empty
  private var nameId = 0

  def typeReference(tupleDescriptor: TupleDescriptor) = tupleDescriptor match {
    case t: SimpleTupleDescriptor => simpleTypeReference(t)
    case t: HashableTupleDescriptor => hashableTypeReference(t)
    case t: OrderableTupleDescriptor => comparableTypeReference(t)
  }

  def simpleTypeReference(tupleDescriptor: SimpleTupleDescriptor): TypeReference = {
    types.getOrElseUpdate(tupleDescriptor, using(generator.generateClass(packageName, newSimpleTupleTypeName())) { clazz =>
      tupleDescriptor.structure.foreach {
        case (fieldName, fieldType: CodeGenType) => clazz.field(lowerType(fieldType), fieldName)
      }
      clazz.handle()
    })
  }

  def hashableTypeReference(tupleDescriptor: HashableTupleDescriptor): TypeReference = {
    types.getOrElseUpdate(tupleDescriptor, using(generator.generateClass(packageName, newHashableTupleTypeName())) { clazz =>
      tupleDescriptor.structure.foreach {
        case (fieldName, fieldType) => clazz.field(lowerType(fieldType), fieldName)
      }
      clazz.field(classOf[Int], "hashCode")
      clazz.generate(MethodTemplate.method(classOf[Int], "hashCode")
                       .returns(ExpressionTemplate.get(ExpressionTemplate.self(clazz.handle()), classOf[Int], "hashCode")).build())

      using(clazz.generateMethod(typeRef[Boolean], "equals", param(typeRef[Object], "other"))) {body =>
        val otherName = s"other$nameId"
        body.assign(body.declare(clazz.handle(), otherName), Expression.cast(clazz.handle(), body.load("other")))

        body.returns(tupleDescriptor.structure.map {
          case (fieldName, fieldType) =>
            val fieldReference = field(clazz.handle(), lowerType(fieldType), fieldName)
            Expression.invoke(method[CompiledEquivalenceUtils, Boolean]("equals", typeRef[Object], typeRef[Object]),

                              Expression.box(
                                Expression.get(body.self(), fieldReference)),
                              Expression.box(
                                Expression.get(body.load(otherName), fieldReference)))
        }.reduceLeft(Expression.and))
      }
      clazz.handle()
    })
  }

  private def sortResultSign(sortOrder: SortOrder): Int = sortOrder match {
    case Ascending => -1
    case Descending => +1
  }

  private def lessThanSortResult(sortOrder: SortOrder) = sortResultSign(sortOrder)
  private def greaterThanSortResult(sortOrder: SortOrder) = -sortResultSign(sortOrder)

  def comparableTypeReference(tupleDescriptor: OrderableTupleDescriptor): TypeReference = {
    types.getOrElseUpdate(tupleDescriptor,
      using(generator.generateClass(packageName, newComparableTupleTypeName(),
      typeRef[Comparable[_]])) { clazz =>
      tupleDescriptor.structure.foreach {
        case (fieldName, fieldType: CodeGenType) => clazz.field(lowerType(fieldType), fieldName)
      }
      using(clazz.generateMethod(typeRef[Int], "compareTo", param(typeRef[Object], "other"))) { l1 =>
        val otherName = s"other$nameId"
        l1.assign(l1.declare(clazz.handle(), otherName), Expression.cast(clazz.handle(), l1.load("other")))

        tupleDescriptor.sortItems.foreach { sortItem =>
          val SortItem(fieldName, sortOrder) = sortItem
          val codeGenType = tupleDescriptor.structure(fieldName)
          val fieldType: TypeReference = lowerType(codeGenType)
          val fieldReference = field(clazz.handle(), fieldType, fieldName)
          val thisValueName = s"thisValue_$fieldName"
          val otherValueName = s"otherValue_$fieldName"
          using(l1.block()) { l2 =>
            codeGenType match {
              // TODO: Primitive nodes and relationships including correct ordering of nulls
              // TODO: Extract shared code between cases
              case CodeGenType(symbols.CTInteger, reprType) => {
                /*
                E.g.
                long thisValue_a = this.a
                long otherValue_a = other.a
                if (thisValue_a < otherValue_a) {
                  return -1;
                } else if (otherValue_a < thisValue_a) {
                  return +1;
                }
                ...
                */
                val isPrimitive = RepresentationType.isPrimitive(reprType)
                val thisValueVariable: LocalVariable = l2.declare(fieldType, thisValueName)
                val otherValueVariable: LocalVariable = l2.declare(fieldType, otherValueName)
                val thisField = Expression.get(l2.self(), fieldReference)
                val otherField = Expression.get(l2.load(otherName), fieldReference)

                l2.assign(thisValueVariable, if (isPrimitive) thisField else Expression.box(thisField))
                l2.assign(otherValueVariable, if (isPrimitive) otherField else Expression.box(otherField))

                using(l2.ifStatement(Expression.lt(thisValueVariable, otherValueVariable))) { l3 =>
                  l3.returns(Expression.constant(lessThanSortResult(sortOrder)))
                }
                using(l2.ifStatement(Expression.lt(otherValueVariable, thisValueVariable))) { l3 =>
                  l3.returns(Expression.constant(greaterThanSortResult(sortOrder)))
                }
              }
              case CodeGenType(symbols.CTFloat, reprType) => {
                // We use Double.compare(double, double) which handles float equality properly
                /*
                E.g.
                double thisValue_a = this.a
                double otherValue_a = other.a
                compare_a = Double.compare(thisValue_a, otherValue_a) == 0)
                if ( !(compare_a == 0) ) {
                  return -1;
                } else if (otherValue_a < thisValue_a) {
                  return +1;
                }
                ...
                */
                val isPrimitive = RepresentationType.isPrimitive(reprType)
                val compareResultName = s"compare_$fieldName"
                val compareResult = l2.declare(typeRef[Int], compareResultName)
                val thisField = Expression.get(l2.self(), fieldReference)
                val otherField = Expression.get(l2.load(otherName), fieldReference)

                // Invoke compare with the parameter order of the fields based on the sort order
                val (lhs, rhs) = sortOrder match {
                  case Ascending =>
                    (thisField, otherField)
                  case Descending =>
                    (otherField, thisField)
                }
                l2.assign(compareResult,
                  Expression.invoke(method[java.lang.Double, Int]("compare", typeRef[Double], typeRef[Double]),
                    if (isPrimitive) lhs else Expression.box(lhs),
                    if (isPrimitive) rhs else Expression.box(rhs)))
                using(l2.ifNotStatement(Expression.equal(compareResult, Expression.constant(0)))) { l3 =>
                  l3.returns(compareResult)
                }
              }
              case CodeGenType(symbols.CTBoolean, reprType) => {
                /*
                E.g.
                boolean thisValue_a = this.a
                boolean otherValue_a = other.a

                if (thisValue_a != otherValue_a) {
                  return (thisValue_a) ? 1 : -1
                }
                ...
                */
                val isPrimitive = RepresentationType.isPrimitive(reprType)
                val thisValueVariable: LocalVariable = l2.declare(fieldType, thisValueName)
                val otherValueVariable: LocalVariable = l2.declare(fieldType, otherValueName)

                val thisField = Expression.get(l2.self(), fieldReference)
                val otherField = Expression.get(l2.load(otherName), fieldReference)

                l2.assign(thisValueVariable, if (isPrimitive) thisField else Expression.box(thisField))
                l2.assign(otherValueVariable, if (isPrimitive) otherField else Expression.box(otherField))

                using(l2.ifNotStatement(Expression.equal(thisValueVariable, otherValueVariable))) { l3 =>
                  l3.returns(Expression.ternary(thisValueVariable,
                    Expression.constant(greaterThanSortResult(sortOrder)),
                    Expression.constant(lessThanSortResult(sortOrder))))
                }
              }
              case _ => {
                // Use CompiledOrderabilityUtils.compare which handles mixed-types according to Cypher orderability semantics
                val compareResultName = s"compare_$fieldName"
                val compareResult = l2.declare(typeRef[Int], compareResultName)
                val thisField = Expression.box(Expression.get(l2.self(), fieldReference))
                val otherField = Expression.box(Expression.get(l2.load(otherName), fieldReference))

                // Invoke compare with the parameter order of the fields based on the sort order
                val (lhs, rhs) = sortOrder match {
                  case Ascending =>
                    (thisField, otherField)
                  case Descending =>
                    (otherField, thisField)
                }
                l2.assign(compareResult,
                  Expression.invoke(method[CompiledOrderabilityUtils, Int]("compare", typeRef[Object], typeRef[Object]),
                    lhs, rhs))
                using(l2.ifNotStatement(Expression.equal(compareResult, Expression.constant(0)))) { l3 =>
                  l3.returns(compareResult)
                }
              }
            }
          }
        }
        l1.returns(Expression.constant(0))
      }
      clazz.handle()
    })
  }

  private def newSimpleTupleTypeName() = {
    val name = "SimpleTupleType" + nameId
    nameId += 1
    name
  }

  private def newHashableTupleTypeName() = {
    val name = "HashableTupleType" + nameId
    nameId += 1
    name
  }

  private def newComparableTupleTypeName() = {
    val name = "ComparableTupleType" + nameId
    nameId += 1
    name
  }
}
