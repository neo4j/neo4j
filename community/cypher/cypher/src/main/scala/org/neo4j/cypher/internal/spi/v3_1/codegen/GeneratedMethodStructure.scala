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
package org.neo4j.cypher.internal.spi.v3_1.codegen

import java.util

import org.neo4j.codegen.Expression.{not, or, _}
import org.neo4j.codegen.MethodReference.methodReference
import org.neo4j.codegen._
import org.neo4j.collection.primitive.hopscotch.LongKeyIntValueTable
import org.neo4j.collection.primitive.{PrimitiveLongIntMap, PrimitiveLongIterator, PrimitiveLongObjectMap}
import org.neo4j.cypher.internal.codegen.CompiledConversionUtils.CompositeKey
import org.neo4j.cypher.internal.codegen._
import org.neo4j.cypher.internal.compiler.v3_1.ast.convert.commands.DirectionConverter.toGraphDb
import org.neo4j.cypher.internal.compiler.v3_1.codegen._
import org.neo4j.cypher.internal.compiler.v3_1.codegen.ir.expressions.{Parameter => _, _}
import org.neo4j.cypher.internal.compiler.v3_1.helpers._
import org.neo4j.cypher.internal.compiler.v3_1.planDescription.Id
import org.neo4j.cypher.internal.frontend.v3_1.symbols.{CTNode, CTRelationship}
import org.neo4j.cypher.internal.frontend.v3_1.{ParameterNotFoundException, SemanticDirection, symbols}
import org.neo4j.cypher.internal.spi.v3_1.codegen.Methods._
import org.neo4j.cypher.internal.spi.v3_1.codegen.Templates.{createNewInstance, handleKernelExceptions, newRelationshipDataExtractor, tryCatch}
import org.neo4j.graphdb.Direction
import org.neo4j.kernel.api.index.IndexDescriptor
import org.neo4j.kernel.impl.api.RelationshipDataExtractor
import org.neo4j.kernel.impl.api.store.RelationshipIterator

import scala.collection.mutable

case class GeneratedMethodStructure(fields: Fields, generator: CodeBlock, aux: AuxGenerator, tracing: Boolean = true,
                                    events: List[String] = List.empty,
                                    locals: mutable.Map[String, LocalVariable] = mutable.Map.empty)
                                   (implicit context: CodeGenContext)
  extends MethodStructure[Expression] {

  import GeneratedQueryStructure._
  import TypeReference.parameterizedType

  private case class HashTable(valueType: TypeReference, listType: TypeReference, tableType: TypeReference,
                               get: MethodReference, put: MethodReference, add: MethodReference)

  private implicit class RichTableType(tableType: RecordingJoinTableType) {

    def extractHashTable(): HashTable = tableType match {
      case LongToListTable(structure, localMap) =>
        // compute the participating types
        val valueType = aux.typeReference(structure)
        val listType = parameterizedType(classOf[util.ArrayList[_]], valueType)
        val tableType = parameterizedType(classOf[PrimitiveLongObjectMap[_]], valueType)
        // the methods we use on those types
        val get = methodReference(tableType, typeRef[Object], "get", typeRef[Long])
        val put = methodReference(tableType, typeRef[Object], "put", typeRef[Long], typeRef[Object])
        val add = methodReference(listType, typeRef[Boolean], "add", typeRef[Object])

        HashTable(valueType, listType, tableType, get, put, add)

      case LongsToListTable(structure, localMap) =>
        // compute the participating types
        val valueType = aux.typeReference(structure)
        val listType = parameterizedType(classOf[util.ArrayList[_]], valueType)
        val tableType = parameterizedType(classOf[util.HashMap[_, _]], typeRef[CompositeKey], valueType)
        // the methods we use on those types
        val get = methodReference(tableType, typeRef[Object], "get", typeRef[Object])
        val put = methodReference(tableType, typeRef[Object], "put", typeRef[Object], typeRef[Object])
        val add = methodReference(listType, typeRef[Boolean], "add", typeRef[Object])
        HashTable(valueType, listType, tableType, get, put, add)
    }
  }

  override def nextNode(targetVar: String, iterVar: String) =
    generator.assign(typeRef[Long], targetVar, invoke(generator.load(iterVar), nextLong))

  override def createRelExtractor(relVar: String) =
    generator.assign(typeRef[RelationshipDataExtractor], relExtractor(relVar), newRelationshipDataExtractor)


  override def nextRelationshipAndNode(toNodeVar: String, iterVar: String, direction: SemanticDirection,
                                       fromNodeVar: String,
                                       relVar: String) = {
    val extractor = relExtractor(relVar)
    val start = invoke(generator.load(extractor), startNode)
    val end = invoke(generator.load(extractor), endNode)

    generator.expression(
      pop(
        invoke(generator.load(iterVar), relationshipVisit,
               invoke(generator.load(iterVar), fetchNextRelationship),
               generator.load(extractor))))
    generator.assign(typeRef[Long], toNodeVar, toGraphDb(direction) match {
      case Direction.INCOMING => start
      case Direction.OUTGOING => end
      case Direction.BOTH => ternary(equal(start, generator.load(fromNodeVar)), end, start)
    })
    generator.assign(typeRef[Long], relVar, invoke(generator.load(extractor), getRelationship))
  }

  private def relExtractor(relVar: String) = s"${relVar}Extractor"

  override def nextRelationship(iterVar: String, ignored: SemanticDirection, relVar: String) = {
    val extractor = relExtractor(relVar)
    generator.expression(
      pop(
        invoke(generator.load(iterVar), relationshipVisit,
               invoke(generator.load(iterVar), fetchNextRelationship),
               generator.load(extractor))))
    generator.assign(typeRef[Long], relVar, invoke(generator.load(extractor), getRelationship))
  }

  override def allNodesScan(iterVar: String) =
    generator.assign(typeRef[PrimitiveLongIterator], iterVar, invoke(readOperations, nodesGetAll))

  override def labelScan(iterVar: String, labelIdVar: String) =
    generator.assign(typeRef[PrimitiveLongIterator], iterVar,
                     invoke(readOperations, nodesGetForLabel, generator.load(labelIdVar)))

  override def lookupLabelId(labelIdVar: String, labelName: String) =
    generator.assign(typeRef[Int], labelIdVar,
                     invoke(readOperations, labelGetForName, constant(labelName)))

  override def lookupRelationshipTypeId(typeIdVar: String, typeName: String) =
    generator.assign(typeRef[Int], typeIdVar, invoke(readOperations, relationshipTypeGetForName, constant(typeName)))

  override def hasNextNode(iterVar: String) =
    invoke(generator.load(iterVar), hasNextLong)

  override def hasNextRelationship(iterVar: String) =
    invoke(generator.load(iterVar), hasMoreRelationship)

  override def whileLoop(test: Expression)(block: MethodStructure[Expression] => Unit) =
    using(generator.whileLoop(test)) { body =>
      block(copy(generator = body))
    }

  override def forEach(varName: String, codeGenType: CodeGenType, iterable: Expression)
                      (block: MethodStructure[Expression] => Unit) =
    using(generator.forEach(Parameter.param(lowerType(codeGenType), varName), iterable)) { body =>
      block(copy(generator = body))
    }

  override def ifStatement(test: Expression)(block: (MethodStructure[Expression]) => Unit) = {
    using(generator.ifStatement(test)) { body =>
      block(copy(generator = body))
    }
  }

  override def ifNotStatement(test: Expression)(block: (MethodStructure[Expression]) => Unit) = {
    using(generator.ifNotStatement(test)) { body =>
      block(copy(generator = body))
    }
  }

  override def ternaryOperator(test: Expression, onTrue: Expression, onFalse: Expression): Expression =
    ternary(test, onTrue, onFalse)

  override def returnSuccessfully() {
    //close all outstanding events
    for (event <- events) {
      generator.expression(
        invoke(generator.load(event),
               method[QueryExecutionEvent, Unit]("close")))
    }
    generator.expression(invoke(generator.self(), fields.success))
    generator.expression(invoke(generator.self(), fields.close))
    generator.returns()
  }

  override def declareCounter(name: String, initialValue: Expression): Unit = {
    val variable = generator.declare(typeRef[Int], name)
    locals += (name -> variable)
    generator.assign(variable, invoke(mathCastToInt, initialValue))
  }

  override def decreaseCounterAndCheckForZero(name: String): Expression = {
    val local = locals(name)
    generator.assign(local, subtract(local, constant(1)))
    equal(constant(0), local)
  }

  override def counterEqualsZero(name: String): Expression = {
    val local = locals(name)
    equal(constant(0), local)
  }

  override def setInRow(column: String, value: Expression) =
    generator.expression(invoke(resultRow, set, constant(column), value))

  override def visitorAccept() = tryCatch(generator) { onSuccess =>
    using(
      onSuccess.ifNotStatement(
        invoke(onSuccess.load("visitor"),
               visit, onSuccess.load("row")))) { body =>
      // NOTE: we are in this if-block if the visitor decided to terminate early (by returning false)
      //close all outstanding events
      for (event <- events) {
        body.expression(invoke(generator.load(event),
                               method[QueryExecutionEvent, Unit]("close")))
      }
      body.expression(invoke(body.self(), fields.success))
      body.expression(invoke(body.self(), fields.close))
      body.returns()
    }
  }(exception = param[Throwable]("e")) { onError =>
    onError.expression(invoke(onError.self(), fields.close))
    onError.throwException(onError.load("e"))
  }


  override def materializeNode(nodeIdVar: String) =
    invoke(nodeManager, newNodeProxyById, generator.load(nodeIdVar))

  override def node(nodeIdVar: String) = createNewInstance(typeRef[NodeIdWrapper], (typeRef[Long], generator.load(nodeIdVar)))

  override def nullablePrimitive(varName: String, codeGenType: CodeGenType, onSuccess: Expression) = codeGenType match {
    case CodeGenType(CTNode, IntType) | CodeGenType(CTRelationship, IntType) =>
      ternary(
        equal(nullValue(codeGenType), generator.load(varName)),
        nullValue(codeGenType),
        onSuccess)
    case _ => ternaryOnNull(generator.load(varName), constant(null), onSuccess)
  }

  override def nullableReference(varName: String, codeGenType: CodeGenType, onSuccess: Expression) = codeGenType match {
    case CodeGenType(CTNode, IntType) | CodeGenType(CTRelationship, IntType) =>
      ternary(
        equal(nullValue(codeGenType), generator.load(varName)),
        constant(null),
        onSuccess)
    case _ => ternaryOnNull(generator.load(varName), constant(null), onSuccess)
  }

  override def materializeRelationship(relIdVar: String) =
    invoke(nodeManager, newRelationshipProxyById, generator.load(relIdVar))

  override def relationship(relIdVar: String) = createNewInstance(typeRef[RelationshipIdWrapper],
                                                                  (typeRef[Long], generator.load(relIdVar)))

  override def trace[V](planStepId: String)(block: MethodStructure[Expression] => V) = if (!tracing) block(this)
  else {
    val eventName = s"event_$planStepId"
    generator.assign(typeRef[QueryExecutionEvent], eventName, traceEvent(planStepId))
    val result = block(copy(events = eventName :: events, generator = generator))
    generator.expression(invoke(generator.load(eventName), method[QueryExecutionEvent, Unit]("close")))
    result
  }

  private def traceEvent(planStepId: String) =
    invoke(tracer, executeOperator,
           get(FieldReference.staticField(generator.owner(), typeRef[Id], planStepId)))

  override def incrementDbHits() = if (tracing) generator.expression(invoke(loadEvent, Methods.dbHit))

  override def incrementRows() = if (tracing) generator.expression(invoke(loadEvent, Methods.row))

  private def loadEvent = generator.load(events.headOption.getOrElse(throw new IllegalStateException("no current trace event")))

  override def expectParameter(key: String, variableName: String) = {
    using(
      generator.ifNotStatement(invoke(params, mapContains, constant(key)))) { block =>
      block.throwException(parameterNotFoundException(key))
    }
    generator.assign(typeRef[Object], variableName, invoke(loadParameter,
                                                           invoke(params, mapGet, constantExpression(key))))
  }

  override def constantExpression(value: Object) = value match {
    case n: java.lang.Byte => constant(n.toLong)
    case n: java.lang.Short => constant(n.toLong)
    case n: java.lang.Character => constant(n.toLong)
    case n: java.lang.Integer => constant(n.toLong)
    case n: java.lang.Float => constant(n.toDouble)
    case _ => constant(value)
  }

  override def notExpression(value: Expression): Expression = not(value)

  override def threeValuedNotExpression(value: Expression): Expression = invoke(Methods.not, value)

  override def threeValuedEqualsExpression(lhs: Expression, rhs: Expression) = invoke(Methods.ternaryEquals, lhs, rhs)

  override def equalityExpression(lhs: Expression, rhs: Expression, codeGenType: CodeGenType) =
    if (codeGenType.isPrimitive) equal(lhs, rhs)
    else invoke(lhs, Methods.equals, rhs)

  override def orExpression(lhs: Expression, rhs: Expression) = or(lhs, rhs)

  override def threeValuedOrExpression(lhs: Expression, rhs: Expression) = invoke(Methods.or, lhs, rhs)

  override def markAsNull(varName: String, codeGenType: CodeGenType) =
    generator.assign(lowerType(codeGenType), varName, nullValue(codeGenType))


  override def isNull(varName: String, codeGenType: CodeGenType) =
    equal(nullValue(codeGenType), generator.load(varName))

  override def notNull(varName: String, codeGenType: CodeGenType) = not(isNull(varName, codeGenType))

  override def box(expression: Expression, cType: CodeGenType) = cType match {
    case CodeGenType(symbols.CTBoolean, BoolType) => invoke(Methods.boxBoolean, expression)
    case CodeGenType(symbols.CTInteger, IntType) => invoke(Methods.boxLong, expression)
    case CodeGenType(symbols.CTFloat, FloatType) => invoke(Methods.boxDouble, expression)
    case _ => expression
  }

  override def unbox(expression: Expression, cType: CodeGenType) = cType match {
    case c if c.isPrimitive => expression
    case CodeGenType(symbols.CTBoolean, ReferenceType) => invoke(expression, Methods.unboxBoolean)
    case CodeGenType(symbols.CTInteger, ReferenceType) => invoke(expression, Methods.unboxLong)
    case CodeGenType(symbols.CTFloat, ReferenceType) => invoke(expression, Methods.unboxDouble)
    case CodeGenType(symbols.CTNode, ReferenceType) => invoke(expression, Methods.unboxNode)
    case CodeGenType(symbols.CTRelationship, ReferenceType) => invoke(expression, Methods.unboxRel)
    case _ => throw new IllegalStateException(s"$expression cannot be unboxed")
  }

  override def toFloat(expression: Expression) = toDouble(expression)

  override def nodeGetAllRelationships(iterVar: String, nodeVar: String, direction: SemanticDirection) = {
    val local = generator.declare(typeRef[RelationshipIterator], iterVar)
    handleKernelExceptions(generator, fields.ro, fields.close) { body =>
      body.assign(local, invoke(readOperations, Methods.nodeGetAllRelationships, body.load(nodeVar), dir(direction)))
    }
  }

  override def nodeGetRelationships(iterVar: String, nodeVar: String, direction: SemanticDirection,
                                    typeVars: Seq[String]) = {
    val local = generator.declare(typeRef[RelationshipIterator], iterVar)
    handleKernelExceptions(generator, fields.ro, fields.close) { body =>
      body.assign(local, invoke(readOperations, Methods.nodeGetRelationships,
                                body.load(nodeVar), dir(direction),
                                newArray(typeRef[Int], typeVars.map(body.load): _*)))
    }
  }

  override def connectingRelationships(iterVar: String, fromNode: String, direction: SemanticDirection,
                                       toNode: String) = {
    val local = generator.declare(typeRef[RelationshipIterator], iterVar)
    handleKernelExceptions(generator, fields.ro, fields.close) { body =>
      body.assign(local, invoke(Methods.allConnectingRelationships,
                                readOperations, body.load(fromNode), dir(direction),
                                body.load(toNode)))
    }
  }

  override def connectingRelationships(iterVar: String, fromNode: String, direction: SemanticDirection,
                                       typeVars: Seq[String], toNode: String) = {
    val local = generator.declare(typeRef[RelationshipIterator], iterVar)
    handleKernelExceptions(generator, fields.ro, fields.close) { body =>
      body.assign(local, invoke(Methods.connectingRelationships, readOperations, body.load(fromNode), dir(direction),
                                body.load(toNode),
                                newArray(typeRef[Int], typeVars.map(body.load): _*)))
    }
  }

  override def loadVariable(varName: String) = generator.load(varName)

  override def add(lhs: Expression, rhs: Expression) = math(Methods.mathAdd, lhs, rhs)

  override def subtract(lhs: Expression, rhs: Expression) = math(Methods.mathSub, lhs, rhs)

  override def multiply(lhs: Expression, rhs: Expression) = math(Methods.mathMul, lhs, rhs)

  override def divide(lhs: Expression, rhs: Expression) = math(Methods.mathDiv, lhs, rhs)

  override def modulus(lhs: Expression, rhs: Expression) = math(Methods.mathMod, lhs, rhs)

  private def math(method: MethodReference, lhs: Expression, rhs: Expression): Expression =
    invoke(method, lhs, rhs)

  private def readOperations = get(generator.self(), fields.ro)

  private def nodeManager = get(generator.self(), fields.entityAccessor)

  private def resultRow = generator.load("row")

  private def tracer = get(generator.self(), fields.tracer)

  private def params = get(generator.self(), fields.params)

  private def parameterNotFoundException(key: String) =
    invoke(newInstance(typeRef[ParameterNotFoundException]),
           MethodReference.constructorReference(typeRef[ParameterNotFoundException], typeRef[String]),
           constant(s"Expected a parameter named $key"))

  private def dir(dir: SemanticDirection): Expression = dir match {
    case SemanticDirection.INCOMING => Templates.incoming
    case SemanticDirection.OUTGOING => Templates.outgoing
    case SemanticDirection.BOTH => Templates.both
  }

  override def asList(values: Seq[Expression]) = Templates.asList[Object](values)

  override def toSet(value: Expression) =
    createNewInstance(typeRef[util.HashSet[Object]], (typeRef[util.Collection[_]], value))

  override def castToCollection(value: Expression) = invoke(Methods.toCollection, value)

  override def asMap(map: Map[String, Expression]) = {
    invoke(Methods.createMap,
           newArray(typeRef[Object], map.flatMap {
             case (key, value) => Seq(constant(key), value)
           }.toSeq: _*))
  }

  override def invokeMethod(resultType: JoinTableType, resultVar: String, methodName: String)
                           (block: MethodStructure[Expression] => Unit) = {
    val returnType: TypeReference = joinTableType(resultType)
    generator.assign(returnType, resultVar,
                     invoke(generator.self(), methodReference(generator.owner(), returnType, methodName)))
    using(generator.classGenerator().generateMethod(returnType, methodName)) { body =>
      block(copy(generator = body, events = List.empty))
      body.returns(body.load(resultVar))
    }
  }

  override def allocateProbeTable(tableVar: String, tableType: JoinTableType) =
    generator.assign(joinTableType(tableType), tableVar, allocate(tableType))

  private def joinTableType(resultType: JoinTableType): TypeReference = {
    val returnType = resultType match {
      case LongToCountTable => typeRef[PrimitiveLongIntMap]
      case LongsToCountTable => TypeReference
        .parameterizedType(classOf[util.HashMap[_, _]], classOf[CompositeKey], classOf[java.lang.Integer])
      case LongToListTable(structure, _) => parameterizedType(classOf[PrimitiveLongObjectMap[_]],
                                                              parameterizedType(
                                                                classOf[util.ArrayList[_]],
                                                                aux.typeReference(structure)))
      case LongsToListTable(structure, _) => TypeReference
        .parameterizedType(classOf[util.HashMap[_, _]], typeRef[CompositeKey],
                           parameterizedType(classOf[util.ArrayList[_]], aux.typeReference(structure)))
    }
    returnType
  }

  private def allocate(resultType: JoinTableType): Expression = resultType match {
    case LongToCountTable => Templates.newCountingMap
    case LongToListTable(_, _) => Templates.newLongObjectMap
    case LongsToCountTable => createNewInstance(joinTableType(LongsToCountTable))
    case typ: LongsToListTable => createNewInstance(joinTableType(typ))
  }

  override def updateProbeTableCount(tableVar: String, tableType: CountingJoinTableType,
                                     keyVars: Seq[String]) = tableType match {
    case LongToCountTable =>
      assert(keyVars.size == 1)
      val keyVar = keyVars.head
      val countName = context.namer.newVarName()
      generator.assign(typeRef[Int], countName,
                       invoke(generator.load(tableVar), countingTableGet, generator.load(keyVar)))
      generator.expression(
        pop(
          invoke(generator.load(tableVar), countingTablePut, generator.load(keyVar),
                 ternary(
                   equal(generator.load(countName), get(staticField[LongKeyIntValueTable, Int]("NULL"))),
                   constant(1), add(generator.load(countName), constant(1))))))

    case LongsToCountTable =>
      val countName = context.namer.newVarName()
      val keyName = context.namer.newVarName()
      generator.assign(typeRef[CompositeKey], keyName,
                       invoke(compositeKey,
                              newArray(typeRef[Long], keyVars.map(generator.load): _*)))
      generator.assign(typeRef[java.lang.Integer], countName,
                       cast(typeRef[java.lang.Integer],
                            invoke(generator.load(tableVar), countingTableCompositeKeyGet,
                                   generator.load(keyName))
                       ))
      generator.expression(
        pop(
          invoke(generator.load(tableVar), countingTableCompositeKeyPut,
                 generator.load(keyName),
                 ternaryOnNull(generator.load(countName),
                               invoke(boxInteger,
                                      constant(1)), invoke(boxInteger,
                                                           add(
                                                             invoke(generator.load(countName),
                                                                    unboxInteger),
                                                             constant(1)))))))
  }

  override def probe(tableVar: String, tableType: JoinTableType, keyVars: Seq[String])
                    (block: MethodStructure[Expression] => Unit) = tableType match {
    case LongToCountTable =>
      assert(keyVars.size == 1)
      val keyVar = keyVars.head
      val times = generator.declare(typeRef[Int], context.namer.newVarName())
      generator.assign(times, invoke(generator.load(tableVar), countingTableGet, generator.load(keyVar)))
      using(generator.whileLoop(gt(times, constant(0)))) { body =>
        block(copy(generator = body))
        body.assign(times, subtract(times, constant(1)))
      }
    case LongsToCountTable =>
      val times = generator.declare(typeRef[Int], context.namer.newVarName())
      val intermediate = generator.declare(typeRef[java.lang.Integer], context.namer.newVarName())
      generator.assign(intermediate,
                       cast(typeRef[Integer],
                            invoke(generator.load(tableVar),
                                   countingTableCompositeKeyGet,
                                   invoke(compositeKey,
                                          newArray(typeRef[Long],
                                                   keyVars.map(generator.load): _*)))))
      generator.assign(times,
                       ternaryOnNull(
                         intermediate,
                         constant(-1),
                         invoke(intermediate, unboxInteger)))

      using(generator.whileLoop(gt(times, constant(0)))) { body =>
        block(copy(generator = body))
        body.assign(times, subtract(times, constant(1)))
      }

    case tableType@LongToListTable(structure, localVars) =>
      assert(keyVars.size == 1)
      val keyVar = keyVars.head

      val hashTable = tableType.extractHashTable()
      // generate the code
      val list = generator.declare(hashTable.listType, context.namer.newVarName())
      val elementName = context.namer.newVarName()
      generator.assign(list, invoke(generator.load(tableVar), hashTable.get, generator.load(keyVar)))
      using(generator.ifNonNullStatement(list)) { onTrue =>
        using(onTrue.forEach(Parameter.param(hashTable.valueType, elementName), list)) { forEach =>
          localVars.foreach {
            case (l, f) =>
              val fieldType = lowerType(structure(f))
              forEach.assign(fieldType, l, get(forEach.load(elementName),
                                               field(structure, structure(f), f)))
          }
          block(copy(generator = forEach))
        }
      }

    case tableType@LongsToListTable(structure, localVars) =>
      val hashTable = tableType.extractHashTable()
      val list = generator.declare(hashTable.listType, context.namer.newVarName())
      val elementName = context.namer.newVarName()

      generator.assign(list,
                       cast(hashTable.listType,
                            invoke(generator.load(tableVar), hashTable.get,
                                   invoke(compositeKey,
                                          newArray(typeRef[Long],
                                                   keyVars.map(generator.load): _*))
                            )))
      using(generator.ifNonNullStatement(list)) { onTrue =>
        using(onTrue.forEach(Parameter.param(hashTable.valueType, elementName), list)) { forEach =>
          localVars.foreach {
            case (l, f) =>
              val fieldType = lowerType(structure(f))
              forEach.assign(fieldType, l, get(forEach.load(elementName),
                                                   field(structure, structure(f), f)))
          }
          block(copy(generator = forEach))
        }
      }
  }

  override def putField(structure: Map[String, CodeGenType], value: Expression, fieldType: CodeGenType,
                        fieldName: String,
                        localVar: String) = {
    generator.put(value, field(structure, fieldType, fieldName), generator.load(localVar))
  }

  override def updateProbeTable(structure: Map[String, CodeGenType], tableVar: String,
                                tableType: RecordingJoinTableType,
                                keyVars: Seq[String], element: Expression) = tableType match {
    case _: LongToListTable =>
      assert(keyVars.size == 1)
      val keyVar = keyVars.head
      val hashTable = tableType.extractHashTable()
      // generate the code
      val listName = context.namer.newVarName()
      val list = generator.declare(hashTable.listType, listName) // ProbeTable list;
      // list = tableVar.get(keyVar);
      generator.assign(list,
                       cast(hashTable.listType,
                            invoke(
                              generator.load(tableVar), hashTable.get,
                              generator.load(keyVar))))
      using(generator.ifNullStatement(list)) { onTrue => // if (null == list)
        // list = new ListType();
        onTrue.assign(list, createNewInstance(hashTable.listType))
        onTrue.expression(
          // tableVar.put(keyVar, list);
          pop(
            invoke(
              generator.load(tableVar), hashTable.put, generator.load(keyVar), generator.load(listName))))
      }
      // list.add( element );
      generator.expression(
        pop(
          invoke(list, hashTable.add, element))
      )

    case _: LongsToListTable =>
      val hashTable = tableType.extractHashTable()
      // generate the code
      val listName = context.namer.newVarName()
      val keyName = context.namer.newVarName()
      val list = generator.declare(hashTable.listType, listName) // ProbeTable list;
      generator.assign(typeRef[CompositeKey], keyName,
                       invoke(compositeKey,
                              newArray(typeRef[Long], keyVars.map(generator.load): _*)))
      // list = tableVar.get(keyVar);
      generator.assign(list,
                       cast(hashTable.listType,
                            invoke(generator.load(tableVar), hashTable.get, generator.load(keyName))))
      using(generator.ifNullStatement(generator.load(listName))) { onTrue => // if (null == list)
        // list = new ListType();
        onTrue.assign(list, createNewInstance(hashTable.listType))
        // tableVar.put(keyVar, list);
        onTrue.expression(
          pop(
            invoke(generator.load(tableVar), hashTable.put, generator.load(keyName),
                   generator.load(listName))))
      }
      // list.add( element );
      generator.expression(
        pop(
          invoke(list, hashTable.add, element)))
  }

  override def declareProperty(propertyVar: String) = {
    val localVariable = generator.declare(typeRef[Object], propertyVar)
    locals += (propertyVar -> localVariable)
    generator.assign(localVariable, constant(null))
  }

  override def declare(varName: String, codeGenType: CodeGenType) = {
    val localVariable = generator.declare(lowerType(codeGenType), varName)
    locals += (varName -> localVariable)
    generator.assign(localVariable, nullValue(codeGenType))
  }

  override def hasLabel(nodeVar: String, labelVar: String, predVar: String) = {
    val local = locals(predVar)

    handleKernelExceptions(generator, fields.ro, fields.close) { inner =>
      val invoke = Expression.invoke(readOperations, nodeHasLabel, inner.load(nodeVar), inner.load(labelVar))
      inner.assign(local, invoke)
      generator.load(predVar)
    }
  }

  override def relType(relVar: String, typeVar: String) = {
    val variable = locals(typeVar)
    val typeOfRel = invoke(generator.load(relExtractor(relVar)), typeOf)
    handleKernelExceptions(generator, fields.ro, fields.close) { inner =>
      val res = invoke(readOperations, relationshipTypeGetName, typeOfRel)
      inner.assign(variable, res)
      generator.load(variable.name())
    }
  }

  override def projectVariable(variableName: String, expression: Expression) = {
    // java.lang.Object is an ok type for result variables because we only put them into result row
    val resultType = typeRef[Object]
    val localVariable = generator.declare(resultType, variableName)
    generator.assign(localVariable, expression)
  }

  override def declareFlag(name: String, initialValue: Boolean) = {
    val localVariable = generator.declare(typeRef[Boolean], name)
    locals += (name -> localVariable)
    generator.assign(localVariable, constant(initialValue))
  }

  override def updateFlag(name: String, newValue: Boolean) = {
    generator.assign(locals(name), constant(newValue))
  }

  override def declarePredicate(name: String) = {
    locals += (name -> generator.declare(typeRef[Boolean], name))
  }

  override def nodeGetPropertyForVar(nodeIdVar: String, propIdVar: String, propValueVar: String) = {
    val local = locals(propValueVar)
    handleKernelExceptions(generator, fields.ro, fields.close) { body =>
      body.assign(local, invoke(readOperations, nodeGetProperty, body.load(nodeIdVar), body.load(propIdVar)))
    }
  }

  override def nodeGetPropertyById(nodeIdVar: String, propId: Int, propValueVar: String) = {
    val local = locals(propValueVar)
    handleKernelExceptions(generator, fields.ro, fields.close) { body =>
      body.assign(local, invoke(readOperations, nodeGetProperty, body.load(nodeIdVar), constant(propId)))
    }
  }

  override def nodeIdSeek(nodeIdVar: String, expression: Expression)(block: MethodStructure[Expression] => Unit) = {
    generator.assign(typeRef[Long], nodeIdVar, invoke(Methods.mathCastToLong, expression))
    using(generator.ifStatement(
      gt(generator.load(nodeIdVar), constant(-1L)),
      invoke(readOperations, nodeExists, generator.load(nodeIdVar))
    )) { ifBody =>
      block(copy(generator = ifBody))
    }
  }

  override def relationshipGetPropertyForVar(relIdVar: String, propIdVar: String, propValueVar: String) = {
    val local = locals(propValueVar)
    handleKernelExceptions(generator, fields.ro, fields.close) { body =>
      body.assign(local,
                  invoke(readOperations, relationshipGetProperty, body.load(relIdVar), body.load(propIdVar)))
    }
  }

  override def relationshipGetPropertyById(relIdVar: String, propId: Int, propValueVar: String) = {
    val local = locals(propValueVar)
    handleKernelExceptions(generator, fields.ro, fields.close) { body =>
      body.assign(local, invoke(readOperations, relationshipGetProperty, body.load(relIdVar), constant(propId)))
    }
  }

  override def lookupPropertyKey(propName: String, propIdVar: String) =
    generator.assign(typeRef[Int], propIdVar, invoke(readOperations, propertyKeyGetForName, constant(propName)))

  override def newIndexDescriptor(descriptorVar: String, labelVar: String, propKeyVar: String) = {
    generator.assign(typeRef[IndexDescriptor], descriptorVar,
                     createNewInstance(typeRef[IndexDescriptor], (typeRef[Int], generator.load(labelVar)),
                                       (typeRef[Int], generator.load(propKeyVar)))
    )
  }

  override def indexSeek(iterVar: String, descriptorVar: String, value: Expression) = {
    val local = generator.declare(typeRef[PrimitiveLongIterator], iterVar)
    handleKernelExceptions(generator, fields.ro, fields.close) { body =>
      body.assign(local, invoke(readOperations, nodesGetFromIndexLookup, generator.load(descriptorVar), value))
    }
  }

  override def indexUniqueSeek(nodeVar: String, descriptorVar: String, value: Expression) = {
    val local = generator.declare(typeRef[Long], nodeVar)
    handleKernelExceptions(generator, fields.ro, fields.close) { body =>
      body.assign(local,
                  invoke(readOperations, nodeGetUniqueFromIndexLookup, generator.load(descriptorVar), value))
    }
  }

  override def coerceToBoolean(propertyExpression: Expression): Expression =
    invoke(coerceToPredicate, propertyExpression)

  override def newTableValue(targetVar: String, structure: Map[String, CodeGenType]) = {
    val valueType = aux.typeReference(structure)
    generator.assign(valueType, targetVar, createNewInstance(valueType))
    generator.load(targetVar)
  }

  private def field(structure: Map[String, CodeGenType], fieldType: CodeGenType, fieldName: String) =
    FieldReference.field(aux.typeReference(structure), lowerType(fieldType), fieldName)
}



