/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.spi.v3_4.codegen

import java.util

import org.neo4j.codegen.FieldReference.field
import org.neo4j.codegen.Parameter.param
import org.neo4j.codegen._
import org.neo4j.cypher.internal.util.v3_4.symbols
import org.neo4j.cypher.internal.codegen.CompiledEquivalenceUtils
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.CodeGenContext
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.expressions._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.spi._
import org.neo4j.cypher.internal.compiler.v3_4.common.CypherOrderability
import org.neo4j.cypher.internal.frontend.v3_4.helpers._
import org.neo4j.values.storable.{Value, Values}
import org.neo4j.values.{AnyValue, AnyValues}

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
          // Primitive types
          case (fieldName, fieldType) if fieldType.isPrimitive =>
            val fieldReference = field(clazz.handle(), lowerType(fieldType), fieldName)
            Expression.equal(Expression.get(body.self(), fieldReference), Expression.get(body.load(otherName), fieldReference))

          // AnyValue
          case (fieldName, fieldType @ CypherCodeGenType(_, _: AnyValueType)) =>
            val fieldReference = field(clazz.handle(), lowerType(fieldType), fieldName)
            Expression.invoke(Expression.get(body.self(), fieldReference),
              method[AnyValue, Boolean]("equals", typeRef[Object]), Expression.get(body.load(otherName), fieldReference))

          // Fallback case
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
        val otherTupleName = s"other$nameId"
        l1.assign(l1.declare(clazz.handle(), otherTupleName), Expression.cast(clazz.handle(), l1.load("other")))

        tupleDescriptor.sortItems.foreach { sortItem =>
          val SortItem(fieldName, sortOrder) = sortItem
          val sanitizedFieldName = CodeGenContext.sanitizedName(fieldName)
          val codeGenType = tupleDescriptor.structure(sanitizedFieldName)
          val fieldType: TypeReference = lowerType(codeGenType)
          val fieldReference = field(clazz.handle(), fieldType, sanitizedFieldName)
          val thisValueName = s"thisValue_$sanitizedFieldName"
          val otherValueName = s"otherValue_$sanitizedFieldName"

          // Helper for generating code expressions for the field values to compare, with proper boxing
          def extractFields(block: CodeBlock, reprType: RepresentationType): (Expression, Expression) = {
            val isPrimitive = RepresentationType.isPrimitive(reprType)
            val thisField = Expression.get(block.self(), fieldReference)
            val otherField = Expression.get(block.load(otherTupleName), fieldReference)
            if(isPrimitive)
              (thisField, otherField)
            else
              (Expression.box(thisField), Expression.box(otherField))
          }

          // Helper for generating code for extracting the field values to compare,
          // and assigning them to local variables
          def assignComparatorVariablesFor(block: CodeBlock, reprType: RepresentationType): (LocalVariable, LocalVariable) = {
            val (thisField, otherField) = extractFields(block, reprType)
            val thisValueVariable: LocalVariable = block.declare(fieldType, thisValueName)
            val otherValueVariable: LocalVariable = block.declare(fieldType, otherValueName)

            block.assign(thisValueVariable, thisField)
            block.assign(otherValueVariable, otherField)

            (thisValueVariable, otherValueVariable)
          }

          def rearrangeInSortOrder(lhs: Expression, rhs: Expression, sortOrder: SortOrder): (Expression, Expression) =
            sortOrder match {
              case Ascending =>
                (lhs, rhs)
              case Descending =>
                (rhs, lhs)
            }

          using(l1.block()) { l2 =>
            codeGenType match {
              // TODO: Primitive nodes and relationships including correct ordering of nulls
              // TODO: Extract shared code between cases
              case CypherCodeGenType(symbols.CTInteger, reprType) if RepresentationType.isPrimitive(reprType) => {
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
                val (thisValueVariable, otherValueVariable) = assignComparatorVariablesFor(l2, reprType)

                using(l2.ifStatement(Expression.lt(thisValueVariable, otherValueVariable))) { l3 =>
                  l3.returns(Expression.constant(lessThanSortResult(sortOrder)))
                }
                using(l2.ifStatement(Expression.lt(otherValueVariable, thisValueVariable))) { l3 =>
                  l3.returns(Expression.constant(greaterThanSortResult(sortOrder)))
                }
              }
              case CypherCodeGenType(symbols.CTFloat, reprType) if RepresentationType.isPrimitive(reprType) => {
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
                val compareResultName = s"compare_$sanitizedFieldName"
                val compareResult = l2.declare(typeRef[Int], compareResultName)
                val (thisField, otherField) = extractFields(l2, reprType)

                // Invoke compare with the parameter order of the fields based on the sort order
                val (lhs, rhs) = rearrangeInSortOrder(thisField, otherField, sortOrder)

                l2.assign(compareResult,
                  Expression.invoke(method[java.lang.Double, Int]("compare", typeRef[Double], typeRef[Double]), lhs, rhs))
                using(l2.ifStatement(Expression.notEqual(compareResult, Expression.constant(0)))) { l3 =>
                  l3.returns(compareResult)
                }
              }
              case CypherCodeGenType(symbols.CTBoolean, reprType) if RepresentationType.isPrimitive(reprType) => {
                /*
                E.g.
                boolean thisValue_a = this.a
                boolean otherValue_a = other.a

                if (thisValue_a != otherValue_a) {
                  return (thisValue_a) ? 1 : -1
                }
                ...
                */
                val (thisValueVariable, otherValueVariable) = assignComparatorVariablesFor(l2, reprType)

                using(l2.ifStatement(Expression.notEqual(thisValueVariable, otherValueVariable))) { l3 =>
                  l3.returns(Expression.ternary(thisValueVariable,
                    Expression.constant(greaterThanSortResult(sortOrder)),
                    Expression.constant(lessThanSortResult(sortOrder))))
                }
              }
              case _ => {
                // Use CypherOrderability.compare which handles mixed-types according to Cypher orderability semantics
                val compareResultName = s"compare_$sanitizedFieldName"
                val compareResult = l2.declare(typeRef[Int], compareResultName)

                val (thisField, otherField) = extractFields(l2, ReferenceType) // NOTE: Always force boxing in this case

                // Invoke compare with the parameter order of the fields based on the sort order
                val (lhs, rhs) = rearrangeInSortOrder(thisField, otherField, sortOrder)

                val compareExpression = codeGenType.repr match {
                  case ValueType =>
                    val comparator = Templates.valueComparator
                    Expression.invoke(comparator, method[util.Comparator[Value], Int]("compare", typeRef[Object], typeRef[Object]),
                      Expression.cast(typeRef[Value], lhs), Expression.cast(typeRef[Value], rhs))

                  case _: AnyValueType =>
                    val comparator = Templates.anyValueComparator
                    Expression.invoke(comparator, method[util.Comparator[AnyValue], Int]("compare", typeRef[Object], typeRef[Object]), lhs, rhs)

                  case _ =>
                    Expression.invoke(method[CypherOrderability, Int]("compare", typeRef[Object], typeRef[Object]), lhs, rhs)
                }

                l2.assign(compareResult, compareExpression)
                using(l2.ifStatement(Expression.notEqual(compareResult, Expression.constant(0)))) { l3 =>
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
