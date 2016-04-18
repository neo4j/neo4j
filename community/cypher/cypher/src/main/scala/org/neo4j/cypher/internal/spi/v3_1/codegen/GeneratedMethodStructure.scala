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

import org.neo4j.codegen._
import org.neo4j.collection.primitive.hopscotch.LongKeyIntValueTable
import org.neo4j.collection.primitive.{PrimitiveLongIntMap, PrimitiveLongIterator, PrimitiveLongObjectMap}
import org.neo4j.cypher.internal.codegen.CompiledConversionUtils.CompositeKey
import org.neo4j.cypher.internal.codegen._
import org.neo4j.cypher.internal.compiler.v3_1.ast.convert.commands.DirectionConverter
import org.neo4j.cypher.internal.compiler.v3_1.codegen._
import org.neo4j.cypher.internal.compiler.v3_1.helpers._
import org.neo4j.cypher.internal.compiler.v3_1.planDescription.Id
import org.neo4j.cypher.internal.frontend.v3_1.symbols.CypherType
import org.neo4j.cypher.internal.frontend.v3_1.{ParameterNotFoundException, SemanticDirection, symbols}
import org.neo4j.graphdb.Direction
import org.neo4j.kernel.api.index.IndexDescriptor
import org.neo4j.kernel.impl.api.RelationshipDataExtractor
import org.neo4j.kernel.impl.api.store.RelationshipIterator

case class GeneratedMethodStructure(fields: Fields, generator: CodeBlock, aux: AuxGenerator, tracing: Boolean = false,
                                    event: Option[String] = None, var locals: Map[String, LocalVariable] = Map.empty)
                                   (implicit context: CodeGenContext)
  extends MethodStructure[Expression] {

  import GeneratedQueryStructure._

  private case class HashTable(valueType: TypeReference, listType: TypeReference, tableType: TypeReference,
                               get: MethodReference, put: MethodReference, add: MethodReference)

  private implicit class RichTableType(tableType: RecordingJoinTableType) {

    def extractHashTable(): HashTable = tableType match {
      case LongToListTable(structure, localMap) =>
        // compute the participating types
        val valueType = aux.typeReference(structure)
        val listType = TypeReference.parameterizedType(classOf[util.ArrayList[_]], valueType)
        val tableType = TypeReference.parameterizedType(classOf[PrimitiveLongObjectMap[_]], valueType)
        // the methods we use on those types
        val get = MethodReference.methodReference(tableType, typeRef[Object], "get", typeRef[Long])
        val put = MethodReference.methodReference(tableType, typeRef[Object], "put", typeRef[Long], typeRef[Object])
        val add = MethodReference.methodReference(listType, typeRef[Boolean], "add", typeRef[Object])

        HashTable(valueType, listType, tableType, get, put, add)

      case LongsToListTable(structure, localMap) =>
        // compute the participating types
        val valueType = aux.typeReference(structure)
        val listType = TypeReference.parameterizedType(classOf[util.ArrayList[_]], valueType)
        val tableType = TypeReference.parameterizedType(classOf[util.HashMap[_, _]], typeRef[CompositeKey], valueType)
        // the methods we use on those types
        val get = MethodReference.methodReference(tableType, typeRef[Object], "get", typeRef[Object])
        val put = MethodReference.methodReference(tableType, typeRef[Object], "put", typeRef[Object], typeRef[Object])
        val add = MethodReference.methodReference(listType, typeRef[Boolean], "add", typeRef[Object])
        HashTable(valueType, listType, tableType, get, put, add)
    }
  }

  override def nextNode(targetVar: String, iterVar: String) =
    generator.assign(typeRef[Long], targetVar, Expression.invoke(generator.load(iterVar), Methods.nextLong))

  override def createRelExtractor(relVar: String) =
    generator.assign(typeRef[RelationshipDataExtractor], relExtractor(relVar), Templates.newRelationshipDataExtractor)


  override def nextRelationshipAndNode(toNodeVar: String, iterVar: String, direction: SemanticDirection,
                                       fromNodeVar: String,
                                       relVar: String) = {
    val extractor = relExtractor(relVar)
    val startNode = Expression.invoke(generator.load(extractor), Methods.startNode)
    val endNode = Expression.invoke(generator.load(extractor), Methods.endNode)

    generator.expression(
      Expression.pop(
        Expression.invoke(generator.load(iterVar), Methods.relationshipVisit,
                          Expression.invoke(generator.load(iterVar), Methods.nextRelationship),
                          generator.load(extractor))))
    generator.assign(typeRef[Long], toNodeVar, DirectionConverter.toGraphDb(direction) match {
      case Direction.INCOMING => startNode
      case Direction.OUTGOING => endNode
      case Direction.BOTH => Expression
        .ternary(Expression.eq(startNode, generator.load(fromNodeVar), typeRef[Long]), endNode, startNode)
    })
    generator.assign(typeRef[Long], relVar, Expression.invoke(generator.load(extractor), Methods.relationship))
  }

  private def relExtractor(relVar: String) = s"${relVar}Extractor"

  override def nextRelationship(iterVar: String, ignored: SemanticDirection, relVar: String) = {
    val extractor = relExtractor(relVar)
    generator.expression(Expression.invoke(generator.load(iterVar), Methods.relationshipVisit,
                                           Expression.invoke(generator.load(iterVar), Methods.nextRelationship),
                                           generator.load(extractor)))
    generator.assign(typeRef[Long], relVar, Expression.invoke(generator.load(extractor), Methods.relationship))
  }

  override def allNodesScan(iterVar: String) =
    generator.assign(typeRef[PrimitiveLongIterator], iterVar, Expression.invoke(readOperations, Methods.nodesGetAll))

  override def labelScan(iterVar: String, labelIdVar: String) =
    generator.assign(typeRef[PrimitiveLongIterator], iterVar,
                     Expression.invoke(readOperations, Methods.nodesGetForLabel, generator.load(labelIdVar)))

  override def lookupLabelId(labelIdVar: String, labelName: String) =
    generator.assign(typeRef[Int], labelIdVar,
                     Expression.invoke(readOperations, Methods.labelGetForName, Expression.constant(labelName)))

  override def lookupRelationshipTypeId(typeIdVar: String, typeName: String) =
    generator.assign(typeRef[Int], typeIdVar, Expression
      .invoke(readOperations, Methods.relationshipTypeGetForName, Expression.constant(typeName)))

  override def hasNextNode(iterVar: String) =
    Expression.invoke(generator.load(iterVar), Methods.hasNextLong)

  override def hasNextRelationship(iterVar: String) =
    Expression.invoke(generator.load(iterVar), Methods.hasNextRelationship)

  override def whileLoop(test: Expression)(block: MethodStructure[Expression] => Unit) =
    using(generator.whileLoop(test)) { body =>
      block(copy(generator = body))
    }

  override def forEach(varName: String, cypherType: CypherType, iterable: Expression)
                      (block: MethodStructure[Expression] => Unit) =
    using(generator.forEach(Parameter.param(lowerType(cypherType), varName), iterable)) { body =>
      block(copy(generator = body))
    }

  override def ifStatement(test: Expression)(block: (MethodStructure[Expression]) => Unit) = {
    using(generator.ifStatement(test)) { body =>
      block(copy(generator = body))
    }
  }

  override def ternaryOperator(test: Expression, onTrue: Expression, onFalse: Expression): Expression =
    Expression.ternary(test, onTrue, onFalse)

  override def returnSuccessfully() {
    generator.expression(Expression.invoke(generator.self(), fields.success))
    generator.returns()
  }

  override def declareCounter(name: String, initialValue: Expression): Unit = {
    val variable = generator.declare(typeRef[Int], name)
    locals = locals + (name -> variable)
    generator.assign(variable, Expression.invoke(Methods.mathCastToInt, initialValue))
  }

  override def decreaseCounterAndCheckForZero(name: String): Expression = {
    val local = locals(name)
    generator.assign(local, Expression.subtractInts(local, Expression.constant(1)))
    Expression.eq(Expression.constant(0), local, typeRef[Int])
  }

  override def counterEqualsZero(name: String): Expression = {
    val local = locals(name)
    Expression.eq(Expression.constant(0), local, typeRef[Int])
  }

  override def setInRow(column: String, value: Expression) =
    generator.expression(Expression.invoke(resultRow, Methods.set, Expression.constant(column), value))

  override def visitorAccept() = using(generator.ifStatement(Expression.not(
    Expression.invoke(generator.load("visitor"), Methods.visit, generator.load("row"))))) { body =>
    // NOTE: we are in this if-block if the visitor decided to terminate early (by returning false)
    body.expression(Expression.invoke(body.self(), fields.success))
    body.expression(Expression.invoke(body.self(), fields.close))
    body.returns()
  }

  override def materializeNode(nodeIdVar: String) = Expression
    .invoke(nodeManager, Methods.newNodeProxyById, generator.load(nodeIdVar))

  override def node(nodeIdVar: String) = Templates.newInstance(typeRef[NodeIdWrapper], generator.load(nodeIdVar))

  override def nullable(varName: String, cypherType: CypherType, onSuccess: Expression) = {
    Expression.ternary(
      Expression.eq(nullValue(cypherType), generator.load(varName), lowerType(cypherType)),
      Expression.constant(null),
      onSuccess)
  }

  override def materializeRelationship(relIdVar: String) = Expression
    .invoke(nodeManager, Methods.newRelationshipProxyById, generator.load(relIdVar))

  override def relationship(relIdVar: String) = Templates
    .newInstance(typeRef[RelationshipIdWrapper], generator.load(relIdVar))

  override def trace[V](planStepId: String)(block: MethodStructure[Expression] => V) = if (!tracing) block(this)
  else {
    val eventName = s"event_$planStepId"
    generator.assign(typeRef[QueryExecutionEvent], eventName, traceEvent(planStepId))
    val result = block(copy(event = Some(eventName), generator = generator))
    Expression.invoke(tracer, Methods.executeOperator,
                      Expression.invoke(generator.load(eventName),
                                        GeneratedQueryStructure.method[QueryExecutionEvent, Unit]("close")))
    result
  }

  private def traceEvent(planStepId: String) =
    Expression.invoke(tracer, Methods.executeOperator,
                      Expression.get(FieldReference.staticField(generator.owner(), typeRef[Id], planStepId)))

  override def incrementDbHits() = if (tracing) generator.expression(Expression.invoke(loadEvent, Methods.dbHit))

  override def incrementRows() = if (tracing) generator.expression(Expression.invoke(loadEvent, Methods.row))

  private def loadEvent = generator.load(event.getOrElse(throw new IllegalStateException("no current trace event")))

  override def expectParameter(key: String, variableName: String) = {
    using(
      generator.ifStatement(Expression.not(Expression.invoke(params, Methods.mapContains, Expression.constant(key)))))
    { block =>
      block.throwException(parameterNotFoundException(key))
    }
    generator.assign(typeRef[Object], variableName, Expression.invoke(Methods.loadParameter,
                                                                      Expression.invoke(params, Methods.mapGet,
                                                                                        Expression.constant(key))))
  }

  override def constant(value: Object) = value match {
    case n: java.lang.Byte => Expression.constant(n.toLong)
    case n: java.lang.Short => Expression.constant(n.toLong)
    case n: java.lang.Character => Expression.constant(n.toLong)
    case n: java.lang.Integer => Expression.constant(n.toLong)
    case n: java.lang.Float => Expression.constant(n.toDouble)
    case _ => Expression.constant(value)
  }

  override def not(value: Expression): Expression = Expression.not(value)

  override def threeValuedNot(value: Expression): Expression = Expression.invoke(Methods.not, value)

  override def threeValuedEquals(lhs: Expression, rhs: Expression) = Expression.invoke(Methods.ternaryEquals, lhs, rhs)

  override def eq(lhs: Expression, rhs: Expression, cypherType: CypherType) = Expression
    .eq(lhs, rhs, lowerType(cypherType))

  override def or(lhs: Expression, rhs: Expression) = Expression.or(lhs, rhs)

  override def threeValuedOr(lhs: Expression, rhs: Expression) = Expression.invoke(Methods.or, lhs, rhs)

  override def markAsNull(varName: String, cypherType: CypherType) =
    generator.assign(lowerType(cypherType), varName, nullValue(cypherType))

  override def notNull(varName: String, cypherType: CypherType) =
    Expression.not(Expression.eq(nullValue(cypherType), generator.load(varName), lowerType(cypherType)))

  override def box(expression: Expression, cType: CypherType) = cType match {
    case symbols.CTBoolean => Expression.invoke(Methods.boxBoolean, expression)
    case symbols.CTInteger => Expression.invoke(Methods.boxLong, expression)
    case symbols.CTFloat => Expression.invoke(Methods.boxDouble, expression)
    case _ => expression
  }

  override def toFloat(expression: Expression) = Expression.toDouble(expression)

  //TODO remove nodeGetAllRelationships again and do proper try catch
  override def nodeGetAllRelationships(iterVar: String, nodeVar: String, direction: SemanticDirection) = {
    val local = generator.declare(typeRef[RelationshipIterator], iterVar)
    Templates.handleKernelExceptions(generator, fields.ro, fields.close) { body =>
      body.assign(local, Expression
        .invoke(readOperations, Methods.nodeGetAllRelationships, body.load(nodeVar), dir(direction)))
    }
  }

  override def nodeGetRelationships(iterVar: String, nodeVar: String, direction: SemanticDirection,
                                    typeVars: Seq[String]) = {
    val local = generator.declare(typeRef[RelationshipIterator], iterVar)
    Templates.handleKernelExceptions(generator, fields.ro, fields.close) { body =>
      body.assign(local, Expression.invoke(readOperations, Methods.nodeGetRelationships,
                                           body.load(nodeVar), dir(direction),
                                           Expression.newArray(typeRef[Int], typeVars.map(body.load): _*)))
    }
  }

  override def connectingRelationships(iterVar: String, fromNode: String, direction: SemanticDirection,
                                       toNode: String) = {
    val local = generator.declare(typeRef[RelationshipIterator], iterVar)
    Templates.handleKernelExceptions(generator, fields.ro, fields.close) { body =>
      body.assign(local, Expression
        .invoke(Methods.allConnectingRelationships, readOperations, body.load(fromNode), dir(direction),
                body.load(toNode)))
    }
  }

  override def connectingRelationships(iterVar: String, fromNode: String, direction: SemanticDirection,
                                       typeVars: Seq[String], toNode: String) = {
    val local = generator.declare(typeRef[RelationshipIterator], iterVar)
    Templates.handleKernelExceptions(generator, fields.ro, fields.close) { body =>
      val args = Seq(readOperations, body.load(fromNode), dir(direction), body.load(toNode)) ++ typeVars.map(body.load)
      body.assign(local, Expression.invoke(Methods.connectingRelationships, args: _*))
    }
  }

  override def load(varName: String) = generator.load(varName)

  override def add(lhs: Expression, rhs: Expression) = math(Methods.mathAdd, lhs, rhs)

  override def subtract(lhs: Expression, rhs: Expression) = math(Methods.mathSub, lhs, rhs)

  override def multiply(lhs: Expression, rhs: Expression) = math(Methods.mathMul, lhs, rhs)

  override def divide(lhs: Expression, rhs: Expression) = math(Methods.mathDiv, lhs, rhs)

  override def mod(lhs: Expression, rhs: Expression) = math(Methods.mathMod, lhs, rhs)

  private def math(method: MethodReference, lhs: Expression, rhs: Expression): Expression =
  // TODO: generate specialized versions for specific types
    Expression.invoke(method, lhs, rhs)

  private def readOperations = Expression.get(generator.self(), fields.ro)

  private def nodeManager = Expression.get(generator.self(), fields.entityAccessor)

  private def resultRow = generator.load("row")

  private def tracer = Expression.get(generator.self(), fields.tracer)

  private def params = Expression.get(generator.self(), fields.params)

  private def parameterNotFoundException(key: String) =
    Expression.invoke(Expression.newInstance(typeRef[ParameterNotFoundException]),
                      MethodReference.constructorReference(typeRef[ParameterNotFoundException], typeRef[String]),
                      Expression.constant(s"Expected a parameter named $key"))

  private def dir(dir: SemanticDirection): Expression = dir match {
    case SemanticDirection.INCOMING => Templates.incoming
    case SemanticDirection.OUTGOING => Templates.outgoing
    case SemanticDirection.BOTH => Templates.both
  }

  override def asList(values: Seq[Expression]) = Templates.asList[Object](values)

  override def toSet(value: Expression) =
    Templates.newInstance(typeRef[util.HashSet[Object]], value)

  override def castToCollection(value: Expression) = Expression.invoke(Methods.toCollection, value)

  override def asMap(map: Map[String, Expression]) = {
    Expression.invoke(Methods.createMap,
                      Expression.newArray(typeRef[Object], map.flatMap {
                        case (key, value) => Seq(Expression.constant(key), value)
                      }.toSeq: _*))
  }

  override def method(resultType: JoinTableType, resultVar: String, methodName: String)
                     (block: MethodStructure[Expression] => Unit) = {
    val returnType: TypeReference = joinTableType(resultType)
    generator.assign(returnType, resultVar, Expression
      .invoke(generator.self(), MethodReference.methodReference(generator.owner(), returnType, methodName)))
    using(generator.classGenerator().generateMethod(returnType, methodName)) { body =>
      block(copy(generator = body, event = None))
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
      case LongToListTable(structure, _) => TypeReference.parameterizedType(classOf[PrimitiveLongObjectMap[_]],
                                                                            TypeReference.parameterizedType(
                                                                              classOf[util.ArrayList[_]],
                                                                              aux.typeReference(structure)))
      case LongsToListTable(structure, _) => TypeReference
        .parameterizedType(classOf[util.HashMap[_, _]], typeRef[CompositeKey],
                           TypeReference.parameterizedType(classOf[util.ArrayList[_]], aux.typeReference(structure)))
    }
    returnType
  }

  private def allocate(resultType: JoinTableType): Expression = resultType match {
    case LongToCountTable => Templates.newCountingMap
    case LongToListTable(_, _) => Templates.newLongObjectMap
    case LongsToCountTable => Templates.newInstance(joinTableType(LongsToCountTable))
    case typ: LongsToListTable => Templates.newInstance(joinTableType(typ))
  }

  override def updateProbeTableCount(tableVar: String, tableType: CountingJoinTableType,
                                     keyVars: Seq[String]) = tableType match {
    case LongToCountTable =>
      assert(keyVars.size == 1)
      val keyVar = keyVars.head
      val countName = context.namer.newVarName()
      generator.assign(typeRef[Int], countName,
                       Expression.invoke(generator.load(tableVar), Methods.countingTableGet, generator.load(keyVar)))
      generator.expression(
        Expression.pop(
          Expression.invoke(generator.load(tableVar), Methods.countingTablePut, generator.load(keyVar),
                            Expression.ternary(
                              Expression.eq(generator.load(countName), Expression
                                .get(staticField[LongKeyIntValueTable, Int]("NULL")), typeRef[Int]),
                              Expression.constant(1),
                              Expression.addInts(generator.load(countName), Expression.constant(1))))))
    case LongsToCountTable =>
      val countName = context.namer.newVarName()
      val keyName = context.namer.newVarName()
      generator.assign(typeRef[CompositeKey], keyName,
                       Expression.invoke(Methods.compositeKey,
                                         Expression.newArray(typeRef[Long], keyVars.map(generator.load): _*)))
      generator.assign(typeRef[java.lang.Integer], countName,
                       Expression.cast(typeRef[java.lang.Integer],
                                       Expression.invoke(generator.load(tableVar), Methods.countingTableCompositeKeyGet,
                                                         generator.load(keyName))
                       ))
      generator.expression(
        Expression.pop(
          Expression.invoke(generator.load(tableVar), Methods.countingTableCompositeKeyPut,
                            generator.load(keyName), Expression.ternary(
              Expression.eq(generator.load(countName), Expression.constant(null), typeRef[Object]),
              Expression.invoke(Methods.boxInteger,
                                Expression.constant(1)),
              Expression.invoke(Methods.boxInteger,
                                Expression.addInts(
                                  Expression.invoke(generator.load(countName), Methods.unboxInteger),
                                  Expression.constant(1)))))))
  }

  override def probe(tableVar: String, tableType: JoinTableType, keyVars: Seq[String])
                    (block: MethodStructure[Expression] => Unit) = tableType match {
    case LongToCountTable =>
      assert(keyVars.size == 1)
      val keyVar = keyVars.head
      val times = generator.declare(typeRef[Int], context.namer.newVarName())
      generator.assign(times, Expression
        .invoke(generator.load(tableVar), Methods.countingTableGet, generator.load(keyVar)))
      using(generator.whileLoop(Expression.gt(times, Expression.constant(0), typeRef[Int]))) { body =>
        block(copy(generator = body))
        body.assign(times, Expression.subtractInts(times, Expression.constant(1)))
      }
    case LongsToCountTable =>
      val times = generator.declare(typeRef[Int], context.namer.newVarName())
      val intermediate = generator.declare(typeRef[java.lang.Integer], context.namer.newVarName())
      generator.assign(intermediate,
                       Expression.cast(typeRef[Integer],
                       Expression.invoke(generator.load(tableVar),
                                                       Methods.countingTableCompositeKeyGet,
                                                       Expression.invoke(Methods.compositeKey,
                                                                         Expression.newArray(typeRef[Long],
                                                                         keyVars.map(generator.load): _*)))))
      generator.assign(times,
                       Expression.ternary(
                         Expression.eq(generator.load(intermediate.name()), Expression.constant(null), typeRef[Object]),
                         Expression.constant(-1),
                         Expression.invoke(generator.load(intermediate.name()), Methods.unboxInteger)))

      using(generator.whileLoop(Expression.gt(times, Expression.constant(0), typeRef[Int]))) { body =>
        block(copy(generator = body))
        body.assign(times, Expression.subtractInts(times, Expression.constant(1)))
      }

    case tableType@LongToListTable(structure, localVars) =>
      assert(keyVars.size == 1)
      val keyVar = keyVars.head

      val hashTable = tableType.extractHashTable()
      // generate the code
      val list = generator.declare(hashTable.listType, context.namer.newVarName())
      val elementName = context.namer.newVarName()
      generator.assign(list, Expression.invoke(generator.load(tableVar), hashTable.get, generator.load(keyVar)))
      using(generator.ifStatement(Expression.not(Expression.eq(list, Expression.constant(null), typeRef[Object]))))
      { onTrue =>
        using(onTrue.forEach(Parameter.param(hashTable.valueType, elementName), list)) { forEach =>
          localVars.foreach {
            case (local, field) =>
              val fieldType = lowerType(structure(field))
              forEach.assign(fieldType, local, Expression
                .get(forEach.load(elementName), FieldReference.field(hashTable.valueType, fieldType, field)))
          }
          block(copy(generator = forEach))
        }
      }

    case tableType@LongsToListTable(structure, localVars) =>
      val hashTable = tableType.extractHashTable()
      val list = generator.declare(hashTable.listType, context.namer.newVarName())
      val elementName = context.namer.newVarName()

      generator.assign(list,
                       Expression.cast(hashTable.listType,
                         Expression.invoke(generator.load(tableVar), hashTable.get,
                          Expression.invoke(Methods.compositeKey,
                                  Expression.newArray(typeRef[Long],
                                    keyVars.map(generator.load): _*))
                         )))
      using(generator.ifStatement(Expression.not(Expression.eq(list, Expression.constant(null), typeRef[Object]))))
      { onTrue =>
        using(onTrue.forEach(Parameter.param(hashTable.valueType, elementName), list)) { forEach =>
          localVars.foreach {
            case (local, field) =>
              val fieldType = lowerType(structure(field))
              forEach.assign(fieldType, local, Expression
                .get(forEach.load(elementName), FieldReference.field(hashTable.valueType, fieldType, field)))
          }
          block(copy(generator = forEach))
        }
      }
  }


  override def putField(structure: Map[String, CypherType], value: Expression, fieldType: CypherType, fieldName: String,
                        localVar: String) = {
    generator.put(value, field(structure, fieldType, fieldName), generator.load(localVar))
  }

  override def updateProbeTable(structure: Map[String, CypherType], tableVar: String, tableType: RecordingJoinTableType,
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
                    Expression.cast(hashTable.listType,
                       Expression.invoke(
                         generator.load(tableVar), hashTable.get,
                         generator.load(keyVar))))
      using(generator.ifStatement(Expression.eq(Expression.constant(null), generator.load(listName), typeRef[Object]))) { onTrue => // if (null == list)
        // list = new ListType();
        onTrue.assign(list, Templates.newInstance(hashTable.listType))
        onTrue.expression(
          // tableVar.put(keyVar, list);
          Expression.pop(
            Expression.invoke(
              generator.load(tableVar), hashTable.put, generator.load(keyVar), generator.load(listName))))
      }
      // list.add( element );
      generator.expression(
        Expression.pop(
          Expression.invoke(list, hashTable.add, element))
      )

    case _: LongsToListTable =>
      val hashTable = tableType.extractHashTable()
      // generate the code
      val listName = context.namer.newVarName()
      val keyName = context.namer.newVarName()
      val list = generator.declare(hashTable.listType, listName) // ProbeTable list;
      generator.assign(typeRef[CompositeKey], keyName,
                Expression.invoke(Methods.compositeKey,
                                  Expression.newArray(typeRef[Long], keyVars.map(generator.load): _*)))
      // list = tableVar.get(keyVar);
      generator.assign(list,
                       Expression.cast(hashTable.listType,
                        Expression.invoke(generator.load(tableVar), hashTable.get, generator.load(keyName))))
      using(generator.ifStatement(Expression.eq(Expression.constant(null), generator.load(listName), typeRef[Object])))
      { onTrue => // if (null == list)
        // list = new ListType();
        onTrue.assign(list, Templates.newInstance(hashTable.listType))
        // tableVar.put(keyVar, list);
        onTrue.expression(
          Expression.pop(
            Expression.invoke(generator.load(tableVar), hashTable.put, generator.load(keyName),
                                            generator.load(listName))))
      }
      // list.add( element );
      generator.expression(
        Expression.pop(
          Expression.invoke(list, hashTable.add, element)))
  }

  override def declareProperty(propertyVar: String) = {
    val localVariable = generator.declare(typeRef[Object], propertyVar)
    locals = locals + (propertyVar -> localVariable)
    generator.assign(localVariable, Expression.constant(null))
  }

  override def declare(varName: String, cypherType: CypherType) = {
    val localVariable = generator.declare(lowerType(cypherType), varName)
    locals = locals + (varName -> localVariable)
    generator.assign(localVariable, nullValue(cypherType))
  }

  override def hasLabel(nodeVar: String, labelVar: String, predVar: String) = {
    val local = locals(predVar)

    Templates.handleKernelExceptions(generator, fields.ro, fields.close) { inner =>
      val invoke = Expression.invoke(readOperations, Methods.nodeHasLabel, inner.load(nodeVar), inner.load(labelVar))
      inner.assign(local, invoke)
      generator.load(predVar)
    }
  }

  override def relType(relVar: String, typeVar: String) = {
    val variable = locals(typeVar)
    val typeOfRel = Expression.invoke(generator.load(relExtractor(relVar)), Methods.typeOf)
    Templates.handleKernelExceptions(generator, fields.ro, fields.close) { inner =>
      val invoke = Expression.invoke(readOperations, Methods.relationshipTypeGetName, typeOfRel)
      inner.assign(variable, invoke)
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
    locals = locals + (name -> localVariable)
    generator.assign(localVariable, Expression.constant(initialValue))
  }

  override def updateFlag(name: String, newValue: Boolean) = {
    generator.assign(locals(name), Expression.constant(newValue))
  }

  override def declarePredicate(name: String) = {
    locals = locals + (name -> generator.declare(typeRef[Boolean], name))
  }


  override def nodeGetPropertyForVar(nodeIdVar: String, propIdVar: String, propValueVar: String) = {
    val local = locals(propValueVar)
    Templates.handleKernelExceptions(generator, fields.ro, fields.close) { body =>

      body.assign(local, Expression
        .invoke(readOperations, Methods.nodeGetProperty, body.load(nodeIdVar), body.load(propIdVar)))

    }
  }

  override def nodeGetPropertyById(nodeIdVar: String, propId: Int, propValueVar: String) = {
    val local = locals(propValueVar)
    Templates.handleKernelExceptions(generator, fields.ro, fields.close) { body =>
      body.assign(local, Expression
        .invoke(readOperations, Methods.nodeGetProperty, body.load(nodeIdVar), Expression.constant(propId)))
    }
  }

  override def relationshipGetPropertyForVar(relIdVar: String, propIdVar: String, propValueVar: String) = {
    val local = locals(propValueVar)
    Templates.handleKernelExceptions(generator, fields.ro, fields.close) { body =>
      body.assign(local, Expression
        .invoke(readOperations, Methods.relationshipGetProperty, body.load(relIdVar), body.load(propIdVar)))
    }
  }

  override def relationshipGetPropertyById(relIdVar: String, propId: Int, propValueVar: String) = {
    val local = locals(propValueVar)
    Templates.handleKernelExceptions(generator, fields.ro, fields.close) { body =>
      body.assign(local, Expression
        .invoke(readOperations, Methods.relationshipGetProperty, body.load(relIdVar), Expression.constant(propId)))
    }
  }

  override def lookupPropertyKey(propName: String, propIdVar: String) =
    generator.assign(typeRef[Int], propIdVar, Expression
      .invoke(readOperations, Methods.propertyKeyGetForName, Expression.constant(propName)))

  override def newIndexDescriptor(descriptorVar: String, labelVar: String, propKeyVar: String) = {
    generator.assign(typeRef[IndexDescriptor], descriptorVar,
                     Templates
                       .newInstance(typeRef[IndexDescriptor], generator.load(labelVar), generator.load(propKeyVar))
    )
  }

  override def indexSeek(iterVar: String, descriptorVar: String, value: Expression) = {
    val local = generator.declare(typeRef[PrimitiveLongIterator], iterVar)
    Templates.handleKernelExceptions(generator, fields.ro, fields.close) { body =>
      body.assign(local, Expression
        .invoke(readOperations, Methods.nodesGetFromIndexLookup, generator.load(descriptorVar), value))
    }
  }

  override def indexUniqueSeek(nodeVar: String, descriptorVar: String, value: Expression) = {
    val local = generator.declare(typeRef[Long], nodeVar)
    Templates.handleKernelExceptions(generator, fields.ro, fields.close) { body =>
      body.assign(local, Expression
        .invoke(readOperations, Methods.nodeGetUniqueFromIndexLookup, generator.load(descriptorVar), value))
    }
  }

  override def coerceToBoolean(propertyExpression: Expression): Expression =
    Expression.invoke(Methods.coerceToPredicate, propertyExpression)

  override def newTableValue(targetVar: String, structure: Map[String, CypherType]) = {
    val valueType = aux.typeReference(structure)
    generator.assign(valueType, targetVar, Templates.newInstance(valueType))
    generator.load(targetVar)
  }

  private def field(structure: Map[String, CypherType], fieldType: CypherType, fieldName: String) = {
    FieldReference.field(aux.typeReference(structure), lowerType(fieldType), fieldName)
  }

}



