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
import org.neo4j.cypher.internal.codegen.CompiledEquivalenceUtils
import org.neo4j.cypher.internal.compiler.v3_2.codegen.ir.expressions.CodeGenType
import org.neo4j.cypher.internal.compiler.v3_2.helpers._

import scala.collection.mutable

class AuxGenerator(val packageName: String, val generator: CodeGenerator) {

  import GeneratedQueryStructure.{lowerType, method, typeRef}


  private val types: scala.collection.mutable.Map[Map[String, CodeGenType], TypeReference] = mutable.Map.empty
  private var nameId = 0


  def typeReference(structure: Map[String, CodeGenType]): TypeReference = {
    types.getOrElseUpdate(structure, using(generator.generateClass(packageName, newValueTypeName())) { clazz =>
      structure.foreach {
        case (fieldName, fieldType: CodeGenType) => clazz.field(lowerType(fieldType), fieldName)
      }
      clazz.handle()
    })
  }

  def hashKey(structure: Map[String, CodeGenType]): TypeReference = {
    types.getOrElseUpdate(structure, using(generator.generateClass(packageName, newKeyTypeName())) { clazz =>
      structure.foreach {
        case (fieldName, fieldType) => clazz.field(lowerType(fieldType), fieldName)
      }
      clazz.field(classOf[Int], "hashCode")
      clazz.generate(MethodTemplate.method(classOf[Int], "hashCode")
                       .returns(ExpressionTemplate.get(ExpressionTemplate.self(clazz.handle()), classOf[Int], "hashCode")).build())

      using(clazz.generateMethod(typeRef[Boolean], "equals", param(typeRef[Object], "other"))) {body =>
        val otherName = s"other$nameId"
        body.assign(body.declare(clazz.handle(), otherName), Expression.cast(clazz.handle(), body.load("other")))

        body.returns(structure.map {
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

  private def newValueTypeName() = {
    val name = "ValueType" + nameId
    nameId += 1
    name
  }

  private def newKeyTypeName() = {
    val name = "KeyType" + nameId
    nameId += 1
    name
  }
}
