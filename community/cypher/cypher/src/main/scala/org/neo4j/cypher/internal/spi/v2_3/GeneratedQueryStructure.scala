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
package org.neo4j.cypher.internal.spi.v2_3

import java.util

import org.neo4j.codegen
import org.neo4j.codegen.CodeGeneratorOption._
import org.neo4j.codegen.ExpressionTemplate._
import org.neo4j.codegen.MethodReference._
import org.neo4j.codegen.TypeReference._
import org.neo4j.codegen._
import org.neo4j.codegen.source.SourceVisitor
import org.neo4j.collection.primitive.hopscotch.LongKeyIntValueTable
import org.neo4j.collection.primitive.{Primitive, PrimitiveLongIntMap, PrimitiveLongIterator, PrimitiveLongObjectMap}
import org.neo4j.cypher.internal.codegen.CompiledConversionUtils.CompositeKey
import org.neo4j.cypher.internal.codegen.{CompiledConversionUtils, CompiledExpandUtils, CompiledMathHelper, NodeIdWrapper, RelationshipIdWrapper}
import org.neo4j.cypher.internal.compiler.v2_3.ast.convert.commands.DirectionConverter
import org.neo4j.cypher.internal.compiler.v2_3.codegen._
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{GeneratedQuery, GeneratedQueryExecution, SuccessfulCloseable}
import org.neo4j.cypher.internal.compiler.v2_3.helpers._
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.{Id, InternalPlanDescription}
import org.neo4j.cypher.internal.compiler.v2_3.planner.CantCompileQueryException
import org.neo4j.cypher.internal.compiler.v2_3.{ExecutionMode, TaskCloser}
import org.neo4j.cypher.internal.frontend.v2_3.symbols.CypherType
import org.neo4j.cypher.internal.frontend.v2_3.{SemanticDirection, CypherExecutionException, ParameterNotFoundException, symbols}
import org.neo4j.graphdb.{Relationship, Node, Direction}
import org.neo4j.graphdb.Result.{ResultRow, ResultVisitor}
import org.neo4j.helpers.collection.MapUtil
import org.neo4j.kernel.api.exceptions.KernelException
import org.neo4j.kernel.api.index.IndexDescriptor
import org.neo4j.kernel.api.{ReadOperations, Statement, StatementTokenNameLookup, TokenNameLookup}
import org.neo4j.kernel.impl.api.store.RelationshipIterator
import org.neo4j.kernel.impl.api.{RelationshipDataExtractor, RelationshipVisitor}
import org.neo4j.kernel.impl.core.NodeManager

import scala.collection.mutable

object GeneratedQueryStructure extends CodeStructure[GeneratedQuery] {
  override def generateQuery(packageName: String, className: String, columns: Seq[String], operatorIds: Map[String, Id], sourceSink: Option[SourceSink])
                            (block: MethodStructure[_] => Unit)(implicit codeGenContext: CodeGenContext) = {
    val generator: codegen.CodeGenerator = try {
      codegen.CodeGenerator.generateCode(classOf[CodeStructure[_]].getClassLoader, sourceSink.map(sink => new SourceVisitor {
        override protected def visitSource(reference: TypeReference, sourceCode: CharSequence): Unit =
          sink.apply(reference.name(), sourceCode.toString)
      }).getOrElse(BLANK_OPTION))
    } catch {
      case e: Exception => throw new CantCompileQueryException(e.getMessage, e)
    }
    val execution = using(generator.generateClass(packageName, className+"Execution", typeRef[GeneratedQueryExecution], typeRef[SuccessfulCloseable])) { clazz =>
      // fields
      val fields = Fields(
        closer = clazz.field(typeRef[TaskCloser], "closer"),
        ro = clazz.field(typeRef[ReadOperations], "ro"),
        nodeManager = clazz.field(typeRef[NodeManager], "nodeManager"),
        executionMode = clazz.field(typeRef[ExecutionMode], "executionMode"),
        description = clazz.field(typeRef[java.util.function.Supplier[InternalPlanDescription]], "description"),
        tracer = clazz.field(typeRef[QueryExecutionTracer], "tracer"),
        params = clazz.field(typeRef[util.Map[String, Object]], "params"),
        closeable = clazz.field(typeRef[SuccessfulCloseable], "closeable"),
        success = clazz.generate(Templates.SUCCESS))
      // the "COLUMNS" static field
      clazz.staticField(typeRef[util.List[String]], "COLUMNS", Templates.asList(
        columns.map(key => Expression.constant(key))))

      // the operator id fields
      operatorIds.keys.foreach { opId =>
        clazz.staticField(typeRef[Id], opId)
      }

      // simple methods
      clazz.generate(Templates.CONSTRUCTOR)
      clazz.generate(Templates.SET_SUCCESSFUL_CLOSEABLE)
      val close = clazz.generate(Templates.CLOSE)
      clazz.generate(Templates.EXECUTION_MODE)
      clazz.generate(Templates.EXECUTION_PLAN_DESCRIPTION)
      clazz.generate(Templates.JAVA_COLUMNS)

      using(clazz.generate(MethodDeclaration.method(typeRef[Unit], "accept",
        Parameter.param(parameterizedType(classOf[ResultVisitor[_]], typeParameter("E")), "visitor")).
        parameterizedWith("E", extending(typeRef[Exception])).
        throwsException(typeParameter("E")))) { method =>
        using(method.tryBlock()) { body =>
          body.assign(typeRef[ResultRowImpl], "row", Templates.newResultRow)
          body.assign(typeRef[RelationshipDataExtractor], "rel", Templates.newRelationshipDataExtractor)
          block(Method(fields, body, new AuxGenerator(packageName, generator)))
          body.expression(Expression.invoke(body.self(), fields.success))
          using(body.finallyBlock()) { then =>
            then.expression(Expression.invoke(body.self(), close))
          }
        }
      }
      clazz.handle()
    }
    val query = using(generator.generateClass(packageName,className, typeRef[GeneratedQuery])) { clazz =>
      using(clazz.generateMethod(typeRef[GeneratedQueryExecution], "execute",
        param[TaskCloser]("closer"),
        param[Statement]("statement"),
        param[NodeManager]("nodeManager"),
        param[ExecutionMode]("executionMode"),
        param[java.util.function.Supplier[InternalPlanDescription]]("description"),
        param[QueryExecutionTracer]("tracer"),
        param[util.Map[String, Object]]("params"))) { execute =>
        execute.returns(Expression.invoke(Expression.newInstance(execution), MethodReference.constructorReference(execution,
          typeRef[TaskCloser],
          typeRef[Statement],
          typeRef[NodeManager],
          typeRef[ExecutionMode],
          typeRef[java.util.function.Supplier[InternalPlanDescription]],
          typeRef[QueryExecutionTracer],
          typeRef[util.Map[String, Object]]),
          execute.load("closer"),
          execute.load("statement"),
          execute.load("nodeManager"),
          execute.load("executionMode"),
          execute.load("description"),
          execute.load("tracer"),
          execute.load("params")))
      }
      clazz.handle()
    }.newInstance().asInstanceOf[GeneratedQuery]
    val clazz: Class[_] = execution.loadClass()
    operatorIds.foreach {
      case (key, id) => setStaticField(clazz, key, id)
    }
    query
  }

  def method[O <: AnyRef, R](name: String, params: TypeReference*)(implicit owner: Manifest[O], returns: Manifest[R]): MethodReference =
    MethodReference.methodReference(typeReference(owner), typeReference(returns), name, params: _*)

  def staticField[O <: AnyRef, R](name: String)(implicit owner: Manifest[O], fieldType: Manifest[R]): FieldReference =
    FieldReference.staticField(typeReference(owner), typeReference(fieldType), name)

  def param[T <: AnyRef](name: String)(implicit manifest: Manifest[T]): Parameter =
    Parameter.param(typeReference(manifest), name)

  def typeRef[T](implicit manifest: Manifest[T]): TypeReference = typeReference(manifest)

  def typeReference(manifest: Manifest[_]): TypeReference = {
    val arguments = manifest.typeArguments
    val base = TypeReference.typeReference(manifest.runtimeClass)
    if(arguments.nonEmpty) {
      TypeReference.parameterizedType(base, arguments.map(typeReference): _*)
    } else {
      base
    }
  }

  def lowerType(cType: CypherType): TypeReference = cType match {
    case symbols.CTNode => typeRef[Long]
    case symbols.CTRelationship => typeRef[Long]
    case symbols.CTAny => typeRef[Object]
  }

  def nullValue(cType: CypherType) = cType match {
    case symbols.CTNode => Expression.constant(-1L)
    case symbols.CTRelationship => Expression.constant(-1L)
    case symbols.CTAny => Expression.constant(null)
  }
}

private case class Fields(closer: FieldReference,
                          ro: FieldReference,
                          nodeManager: FieldReference,
                          executionMode: FieldReference,
                          description: FieldReference,
                          tracer: FieldReference,
                          params: FieldReference,
                          closeable: FieldReference,
                          success: MethodReference)

private class AuxGenerator(val packageName: String, val generator: codegen.CodeGenerator) {
  private val types: mutable.Map[Map[String, CypherType], TypeReference] = mutable.Map.empty
  private var nameId = 0

  def typeReference(structure: Map[String, CypherType]): TypeReference = {
    types.getOrElseUpdate(structure, using(generator.generateClass(packageName, newName())) { clazz =>
      structure.foreach {
        case (fieldName, fieldType) => clazz.field(GeneratedQueryStructure.lowerType(fieldType), fieldName)
      }
      clazz.handle()
    })
  }
  private def newName() = {
    val name = "ValueType" + nameId
    nameId += 1
    name
  }
}

private case class Method(fields: Fields, generator: CodeBlock, aux:AuxGenerator, tracing: Boolean = true,
                          event: Option[String] = None, var locals:Map[String,LocalVariable]=Map.empty)(implicit context: CodeGenContext)
  extends MethodStructure[Expression] {
  import GeneratedQueryStructure.typeRef

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
        val get = MethodReference.methodReference(tableType, listType, "get", typeRef[Long])
        val put = MethodReference.methodReference(tableType, listType, "put", typeRef[Long], listType)
        val add = MethodReference.methodReference(listType, typeRef[Boolean], "add", valueType)

        HashTable(valueType, listType, tableType, get, put, add)

      case LongsToListTable(structure, localMap) =>
        // compute the participating types
        val valueType = aux.typeReference(structure)
        val listType = TypeReference.parameterizedType(classOf[util.ArrayList[_]], valueType)
        val tableType = TypeReference.parameterizedType(classOf[util.HashMap[_, _]], typeRef[CompositeKey], valueType)
        // the methods we use on those types
        val get = MethodReference.methodReference(tableType, listType, "get", typeRef[CompositeKey])
        val put = MethodReference.methodReference(tableType, listType, "put", typeRef[CompositeKey], listType)
        val add = MethodReference.methodReference(listType, typeRef[Boolean], "add", valueType)
        HashTable(valueType, listType, tableType, get, put, add)
    }
  }

  override def nextNode(targetVar: String, iterVar: String) =
    generator.assign(typeRef[Long], targetVar, Expression.invoke(generator.load(iterVar), Methods.nextLong))

  override def nextRelationshipAndNode(toNodeVar: String, iterVar: String, direction: SemanticDirection, fromNodeVar: String,
                                       relVar: String) = {
    val startNode = Expression.invoke(generator.load("rel"), Methods.startNode)
    val endNode = Expression.invoke(generator.load("rel"), Methods.endNode)
    generator.expression(Expression.invoke(generator.load(iterVar), Methods.relationshipVisit,
      Expression.invoke(generator.load(iterVar), Methods.nextLong),
      generator.load("rel")))
    generator.assign(typeRef[Long], toNodeVar, DirectionConverter.toGraphDb(direction) match {
      case Direction.INCOMING => startNode
      case Direction.OUTGOING => endNode
      case Direction.BOTH => Expression.ternary(Expression.eq(startNode, generator.load(fromNodeVar)), endNode, startNode)
    })
    generator.assign(typeRef[Long], relVar, Expression.invoke(generator.load("rel"), Methods.relationship))
  }

  override def nextRelationship(iterVar: String, ignored: SemanticDirection, relVar: String) = {
    generator.expression(Expression.invoke(generator.load(iterVar), Methods.relationshipVisit,
      Expression.invoke(generator.load(iterVar), Methods.nextLong),
      generator.load("rel")))
    generator.assign(typeRef[Long], relVar, Expression.invoke(generator.load("rel"), Methods.relationship))
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

  override def hasNext(iterVar: String) =
    Expression.invoke(generator.load(iterVar), Methods.hasNext)

  override def whileLoop(test: Expression)(block: MethodStructure[Expression] => Unit) =
    using(generator.whileLoop(test)) { body =>
      block(copy(generator = body))
    }

  override def forEach(varName: String, cypherType: CypherType, iterable: Expression)(block: MethodStructure[Expression] => Unit) =
    using(generator.forEach(Parameter.param(GeneratedQueryStructure.lowerType(cypherType), varName), iterable)) { body =>
      block(copy(generator = body))
    }

  override def ifStatement(test: Expression)(block: (MethodStructure[Expression]) => Unit) = {
    using(generator.ifStatement(test)) { body =>
      block(copy(generator = body))
    }
  }

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
    generator.assign(local, Expression.sub(local, Expression.constant(1)))
    Expression.eq(Expression.constant(0), local)
  }

  override def counterEqualsZero(name: String): Expression = {
    val local = locals(name)
    Expression.eq(Expression.constant(0), local)
  }

  override def setInRow(column: String, value: Expression) =
    generator.expression(Expression.invoke(resultRow, Methods.set, Expression.constant(column), value))

  override def visitorAccept() = using(generator.ifStatement(Expression.not(
    Expression.invoke(generator.load("visitor"), Methods.visit, generator.load("row"))))) { body =>
    // NOTE: we are in this if-block if the visitor decided to terminate early (by returning false)
    body.expression(Expression.invoke(body.self(), fields.success))
    body.returns()
  }

  override def materializeNode(nodeIdVar: String) = Expression.invoke(nodeManager, Methods.newNodeProxyById, generator.load(nodeIdVar))

  override def node(nodeIdVar: String) = Templates.newInstance(typeRef[NodeIdWrapper], generator.load(nodeIdVar))


  override def nullable(varName: String, cypherType: CypherType, onSuccess: Expression) = {
    Expression.ternary(
      Expression.eq(GeneratedQueryStructure.nullValue(cypherType), generator.load(varName)),
      Expression.constant(null),
      onSuccess)
  }

  override def materializeRelationship(relIdVar: String) = Expression.invoke(nodeManager, Methods.newRelationshipProxyById, generator.load(relIdVar))

  override def relationship(relIdVar: String) = Templates.newInstance(typeRef[RelationshipIdWrapper], generator.load(relIdVar))

  override def trace[V](planStepId: String)(block: MethodStructure[Expression]=>V) = if(!tracing) block(this)
  else {
    val eventName = s"event_$planStepId"
    using(generator.tryBlock(typeRef[QueryExecutionEvent], eventName, traceEvent(planStepId))) { body =>
      block(copy(event = Some(eventName),
        generator = body))
    }
  }

  private def traceEvent(planStepId: String) = Expression.invoke(tracer, Methods.executeOperator,
    Expression.get(FieldReference.staticField(generator.owner(), typeRef[Id], planStepId)))

  override def incrementDbHits() = if(tracing) generator.expression(Expression.invoke(loadEvent, Methods.dbHit))

  override def incrementRows() = if(tracing) generator.expression(Expression.invoke(loadEvent, Methods.row))

  private def loadEvent = generator.load(event.getOrElse(throw new IllegalStateException("no current trace event")))

  override def expectParameter(key: String, variableName: String) = {
    using(generator.ifStatement(Expression.not(Expression.invoke(params, Methods.mapContains, Expression.constant(key))))) { block =>
      block.throwException(parameterNotFoundException(key))
    }
    generator.assign(typeRef[Object], variableName, Expression.invoke(Methods.loadParameter,
      Expression.invoke(params, Methods.mapGet, Expression.constant(key))))
  }

  override def constant(value: Object) = Expression.constant(value)

  override def not(value: Expression): Expression = Expression.not(value)

  override def ternaryNot(value: Expression): Expression = Expression.invoke(Methods.not, value)

  override def ternaryEquals(lhs: Expression, rhs: Expression) = Expression.invoke(Methods.ternaryEquals, lhs, rhs)

  override def eq(lhs: Expression, rhs: Expression) = Expression.eq(lhs, rhs)

  override def or(lhs: Expression, rhs: Expression) = Expression.or(lhs, rhs)

  override def ternaryOr(lhs: Expression, rhs: Expression) = Expression.invoke(Methods.or, lhs, rhs)

  override def markAsNull(varName: String, cypherType: CypherType) =
    generator.assign(GeneratedQueryStructure.lowerType(cypherType), varName, GeneratedQueryStructure.nullValue(cypherType))

  override def notNull(varName: String, cypherType: CypherType) =
      Expression.not(Expression.eq(GeneratedQueryStructure.nullValue(cypherType), generator.load(varName)))


  override def nodeGetAllRelationships(iterVar: String, nodeVar: String, direction: SemanticDirection) = {
    val local = generator.declare(typeRef[RelationshipIterator], iterVar)
    Templates.handleExceptions(generator, fields.ro) { body =>
      body.assign(local, Expression.invoke(readOperations, Methods.nodeGetAllRelationships, body.load(nodeVar), dir(direction)))
    }
  }

  override def nodeGetRelationships(iterVar: String, nodeVar: String, direction: SemanticDirection, typeVars: Seq[String]) = {
    val local = generator.declare(typeRef[RelationshipIterator], iterVar)
    Templates.handleExceptions(generator, fields.ro) { body =>
      val args = Seq(body.load(nodeVar), dir(direction)) ++ typeVars.map(body.load)
      body.assign(local, Expression.invoke(readOperations, Methods.nodeGetRelationships, args: _*))
    }
  }

  override def connectingRelationships(iterVar: String, fromNode: String, direction: SemanticDirection, toNode: String) = {
    val local = generator.declare(typeRef[RelationshipIterator], iterVar)
    Templates.handleExceptions(generator, fields.ro) { body =>
      body.assign(local, Expression.invoke(Methods.allConnectingRelationships, readOperations, body.load(fromNode), dir(direction), body.load(toNode)))
    }
  }

  override def connectingRelationships(iterVar: String, fromNode: String, direction: SemanticDirection, typeVars: Seq[String], toNode: String) = {
    val local = generator.declare(typeRef[RelationshipIterator], iterVar)
    Templates.handleExceptions(generator, fields.ro) { body =>
      val args = Seq(readOperations, body.load(fromNode), dir(direction),  body.load(toNode)) ++ typeVars.map(body.load)
      body.assign(local, Expression.invoke(Methods.connectingRelationships, args:_*))
    }
  }

  override def load(varName: String) = generator.load(varName)

  override def add(lhs: Expression, rhs: Expression) = math(Methods.mathAdd, lhs, rhs)

  override def sub(lhs: Expression, rhs: Expression) = math(Methods.mathSub, lhs, rhs)

  override def mul(lhs: Expression, rhs: Expression) = math(Methods.mathMul, lhs, rhs)

  override def div(lhs: Expression, rhs: Expression) = math(Methods.mathDiv, lhs, rhs)

  override def mod(lhs: Expression, rhs: Expression) = math(Methods.mathMod, lhs, rhs)

  private def math(method: MethodReference, lhs: Expression, rhs: Expression): Expression =
    // TODO: generate specialized versions for specific types
    Expression.invoke(method, lhs, rhs)

  private def readOperations = Expression.get(generator.self(), fields.ro)

  private def nodeManager = Expression.get(generator.self(), fields.nodeManager)

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

  override def asList(values: Seq[Expression]) = Templates.asList(values)

  override def toSet(value: Expression) =
    Templates.newInstance(typeRef[util.HashSet[Object]], value)

  override def castToCollection(value: Expression) = Expression.invoke(Methods.toCollection, value)

  override def asMap(map: Map[String, Expression]) = {
    Expression.invoke(Methods.arrayAsList, map.flatMap {
      case (key, value) => Seq(Expression.constant(key), value)
    }.toSeq: _*)
  }

  override def method(resultType: JoinTableType, resultVar: String, methodName: String)(block: MethodStructure[Expression]=>Unit) = {
    val returnType: TypeReference = joinTableType(resultType)
    generator.assign(returnType, resultVar, Expression.invoke(generator.self(),MethodReference.methodReference(generator.owner(),returnType, methodName)))
    using(generator.classGenerator().generateMethod(returnType, methodName)) { body =>
      body.assign(typeRef[RelationshipDataExtractor], "rel", Templates.newRelationshipDataExtractor)
      block(copy(generator = body, event = None))
      body.returns(body.load(resultVar))
    }
  }

  override def allocateProbeTable(tableVar: String, tableType: JoinTableType) =
    generator.assign(joinTableType(tableType), tableVar, allocate(tableType))


  private def joinTableType(resultType: JoinTableType): TypeReference = {
    val returnType = resultType match {
      case LongToCountTable => typeRef[PrimitiveLongIntMap]
      case LongsToCountTable => TypeReference.parameterizedType(classOf[util.HashMap[_, _]], classOf[CompositeKey], classOf[java.lang.Integer])
      case LongToListTable(structure,_) => TypeReference.parameterizedType(classOf[PrimitiveLongObjectMap[_]],
        TypeReference.parameterizedType(classOf[util.ArrayList[_]], aux.typeReference(structure)))
      case LongsToListTable(structure,_) => TypeReference.parameterizedType(classOf[util.HashMap[_,_]], typeRef[CompositeKey],
         TypeReference.parameterizedType(classOf[util.ArrayList[_]], aux.typeReference(structure)))
    }
    returnType
  }

  private def allocate(resultType: JoinTableType): Expression = resultType match {
    case LongToCountTable => Templates.newCountingMap
    case LongToListTable(_,_) => Templates.newLongObjectMap
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
      generator.expression(Expression.invoke(generator.load(tableVar), Methods.countingTablePut, generator.load(keyVar),
                                             Expression.ternary(
                                               Expression.eq(generator.load(countName), Expression
                                                 .get(GeneratedQueryStructure.staticField[LongKeyIntValueTable, Int]("NULL"))),
                                               Expression.constant(1),
                                               Expression.add(generator.load(countName), Expression.constant(1)))))
    case LongsToCountTable =>
      val countName = context.namer.newVarName()
      val keyName = context.namer.newVarName()
      generator.assign(typeRef[CompositeKey], keyName, Expression.invoke(Methods.compositeKey, keyVars.map(generator.load): _*))
      generator.assign(typeRef[java.lang.Integer], countName, Expression.invoke(generator.load(tableVar), Methods.countingTableCompositeKeyGet, generator.load(keyName)))
      generator.expression(Expression.invoke(generator.load(tableVar), Methods.countingTableCompositeKeyPut,
                                             generator.load(keyName), Expression.ternary(
          Expression.eq(generator.load(countName), Expression.constant(null)),
          Expression.constant(1),
          Expression.add(generator.load(countName), Expression.constant(1)))))
  }

  override def probe(tableVar: String, tableType: JoinTableType, keyVars: Seq[String])(block: MethodStructure[Expression] => Unit) = tableType match {
    case LongToCountTable =>
      assert(keyVars.size == 1)
      val keyVar = keyVars.head
      val times = generator.declare(typeRef[Int], context.namer.newVarName())
      generator.assign(times, Expression.invoke(generator.load(tableVar), Methods.countingTableGet, generator.load(keyVar)))
      using(generator.whileLoop(Expression.gt(times, Expression.constant(0)))) { body =>
        block(copy(generator=body))
        body.assign(times, Expression.sub(times, Expression.constant(1)))
      }
    case LongsToCountTable =>
      val times = generator.declare(typeRef[Int], context.namer.newVarName())
      val intermediate = generator.declare(typeRef[java.lang.Integer], context.namer.newVarName())
      generator.assign(intermediate, Expression.invoke(generator.load(tableVar),
        Methods.countingTableCompositeKeyGet,
        Expression.invoke(Methods.compositeKey, keyVars.map(generator.load): _*)))
      generator.assign(times,
        Expression.ternary(
          Expression.eq(generator.load(intermediate.name()), Expression.constant(null)),
          Expression.constant(-1), generator.load(intermediate.name())))

      using(generator.whileLoop(Expression.gt(times, Expression.constant(0)))) { body =>
        block(copy(generator=body))
        body.assign(times, Expression.sub(times, Expression.constant(1)))
      }

    case tableType@LongToListTable(structure,localVars) =>
      assert(keyVars.size == 1)
      val keyVar = keyVars.head

      val hashTable = tableType.extractHashTable()
      // generate the code
      val list = generator.declare(hashTable.listType, context.namer.newVarName())
      val elementName = context.namer.newVarName()
      generator.assign(list, Expression.invoke(generator.load(tableVar), hashTable.get, generator.load(keyVar)))
      using(generator.ifStatement(Expression.not(Expression.eq(list, Expression.constant(null))))) { onTrue =>
        using(onTrue.forEach(Parameter.param(hashTable.valueType, elementName), list)) { forEach =>
          localVars.foreach {
            case (local, field) =>
              val fieldType = GeneratedQueryStructure.lowerType(structure(field))
              forEach.assign(fieldType, local, Expression.get(forEach.load(elementName),FieldReference.field(hashTable.valueType, fieldType, field)))
          }
          block(copy(generator=forEach))
        }
      }

    case tableType@LongsToListTable(structure,localVars) =>
      val hashTable = tableType.extractHashTable()
      val list = generator.declare(hashTable.listType, context.namer.newVarName())
      val elementName = context.namer.newVarName()

      generator.assign(list, Expression.invoke(generator.load(tableVar),hashTable.get, Expression.invoke(Methods.compositeKey, keyVars.map(generator.load): _*)))
      using(generator.ifStatement(Expression.not(Expression.eq(list, Expression.constant(null))))) { onTrue =>
        using(onTrue.forEach(Parameter.param(hashTable.valueType, elementName), list)) { forEach =>
          localVars.foreach {
            case (local, field) =>
              val fieldType = GeneratedQueryStructure.lowerType(structure(field))
              forEach.assign(fieldType, local, Expression.get(forEach.load(elementName),FieldReference.field(hashTable.valueType, fieldType, field)))
          }
          block(copy(generator=forEach))
        }
      }
  }



  override def putField(structure: Map[String, CypherType], value: Expression, fieldType: CypherType, fieldName: String, localVar: String) = {
    generator.put(value, field(structure, fieldType, fieldName), generator.load(localVar))
  }

  override def updateProbeTable(structure: Map[String, CypherType], tableVar: String, tableType: RecordingJoinTableType, keyVars: Seq[String], element: Expression) = tableType match {
    case _: LongToListTable =>
      assert(keyVars.size == 1)
      val keyVar = keyVars.head
      val hashTable = tableType.extractHashTable()
      // generate the code
      val listName = context.namer.newVarName()
      val list = generator.declare(hashTable.listType, listName) // ProbeTable list;
      generator.assign(list, Expression
        .invoke(generator.load(tableVar), hashTable.get, generator.load(keyVar))) // list = tableVar.get(keyVar);
      using(generator.ifStatement(Expression.eq(Expression.constant(null), generator.load(listName))))
      { onTrue => // if (null == list)
        onTrue.assign(list, Templates.newInstance(hashTable.listType)) // list = new ListType();
        onTrue.expression(Expression.invoke(generator.load(tableVar), hashTable.put, generator.load(keyVar),
                                            generator.load(listName))) // tableVar.put(keyVar, list);
      }
      generator.expression(Expression.invoke(list, hashTable.add, element)) // list.add( element );

    case _: LongsToListTable =>
      val hashTable = tableType.extractHashTable()
      // generate the code
      val listName = context.namer.newVarName()
      val keyName = context.namer.newVarName()
      val list = generator.declare(hashTable.listType, listName) // ProbeTable list;
      generator
        .assign(typeRef[CompositeKey], keyName, Expression.invoke(Methods.compositeKey, keyVars.map(generator.load): _*))
      generator.assign(list, Expression
        .invoke(generator.load(tableVar), hashTable.get, generator.load(keyName))) // list = tableVar.get(keyVar);
      using(generator.ifStatement(Expression.eq(Expression.constant(null), generator.load(listName))))
      { onTrue => // if (null == list)
        onTrue.assign(list, Templates.newInstance(hashTable.listType)) // list = new ListType();
        onTrue.expression(Expression.invoke(generator.load(tableVar), hashTable.put, generator.load(keyName),
                                            generator.load(listName))) // tableVar.put(keyVar, list);
      }
      generator.expression(Expression.invoke(list, hashTable.add, element)) // list.add( element );


  }

  override def declareProperty(propertyVar: String) = {
    val localVariable = generator.declare(typeRef[Object], propertyVar)
    locals = locals + (propertyVar -> localVariable)
    generator.assign(localVariable, Expression.constant(null))
  }

  override def hasLabel(nodeVar: String, labelVar: String, predVar: String) =  {
     val local = locals(predVar)
    Templates.handleExceptions(generator, fields.ro) { inner =>
      val invoke = Expression.invoke(readOperations, Methods.nodeHasLabel, inner.load(nodeVar), inner.load(labelVar))
      inner.assign(local, invoke)
      generator.load(predVar)
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
    Templates.handleExceptions(generator, fields.ro) { body =>

      body.assign(local, Expression.invoke(readOperations, Methods.nodeGetProperty, body.load(nodeIdVar), body.load(propIdVar)))

    }
  }

  override def nodeGetPropertyById(nodeIdVar: String, propId: Int, propValueVar: String) = {
    val local = locals(propValueVar)
    Templates.handleExceptions(generator, fields.ro) { body =>
      body.assign(local, Expression.invoke(readOperations, Methods.nodeGetProperty, body.load(nodeIdVar), Expression.constant(propId)))
    }
  }

  override def relationshipGetPropertyForVar(relIdVar: String, propIdVar: String, propValueVar: String) = {
    val local = locals(propValueVar)
    Templates.handleExceptions(generator, fields.ro) { body =>
      body.assign(local, Expression.invoke(readOperations, Methods.relationshipGetProperty, body.load(relIdVar), body.load(propIdVar)))
    }
  }

  override def relationshipGetPropertyById(relIdVar: String, propId: Int, propValueVar: String) = {
    val local = locals(propValueVar)
    Templates.handleExceptions(generator, fields.ro) { body =>
      body.assign(local, Expression.invoke(readOperations, Methods.relationshipGetProperty, body.load(relIdVar), Expression.constant(propId)))
    }
  }

  override def lookupPropertyKey(propName: String, propIdVar: String) =
    generator.assign(typeRef[Int], propIdVar, Expression.invoke(readOperations, Methods.propertyKeyGetForName ,Expression.constant(propName)))

  override def newIndexDescriptor(descriptorVar: String, labelVar: String, propKeyVar: String) = {
    generator.assign(typeRef[IndexDescriptor], descriptorVar,
      Templates.newInstance(typeRef[IndexDescriptor], generator.load(labelVar), generator.load(propKeyVar))
    )
  }

  override def indexSeek(iterVar: String, descriptorVar: String, value: Expression) = {
    val local = generator.declare(typeRef[PrimitiveLongIterator], iterVar)
    Templates.handleExceptions(generator, fields.ro) { body =>
      body.assign(local, Expression.invoke(readOperations, Methods.nodesGetFromIndexLookup, generator.load(descriptorVar), value ))
    }
  }

  override def indexUniqueSeek(nodeVar: String, descriptorVar: String, value: Expression) = {
    val local = generator.declare(typeRef[Long], nodeVar)
    Templates.handleExceptions(generator, fields.ro) { body =>
      body.assign(local, Expression.invoke(readOperations, Methods.nodeGetUniqueFromIndexLookup, generator.load(descriptorVar), value ))
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
    FieldReference.field(aux.typeReference(structure), GeneratedQueryStructure.lowerType(fieldType), fieldName)
  }
}

private object Methods {

  import GeneratedQueryStructure.{method, typeRef}

  val countingTablePut = method[PrimitiveLongIntMap, Int]("put", typeRef[Long], typeRef[Int])
  val countingTableCompositeKeyPut = method[util.HashMap[_, _], Int]("put", typeRef[CompositeKey], typeRef[Int])
  val countingTableGet = method[PrimitiveLongIntMap, Int]("get", typeRef[Long])
  val countingTableCompositeKeyGet = method[util.HashMap[_, _], Int]("get", typeRef[CompositeKey])
  val compositeKey = method[CompiledConversionUtils, CompositeKey]("compositeKey", typeRef[Array[Long]])
  val hasNext = method[PrimitiveLongIterator, Boolean]("hasNext")
  val arrayAsList = method[MapUtil, util.Map[String, Object]]("map", typeRef[Array[Object]])
  val relationshipVisit = method[RelationshipIterator, Boolean]("relationshipVisit", typeRef[Long], typeRef[RelationshipVisitor[RuntimeException]])
  val relationship = method[RelationshipDataExtractor, Long]("relationship", typeRef[Long])
  val startNode = method[RelationshipIterator, Long]("startNode")
  val endNode = method[RelationshipIterator, Long]("endNode")
  val nodeGetAllRelationships = method[ReadOperations, RelationshipIterator]("nodeGetRelationships", typeRef[Long], typeRef[Direction])
  val nodeGetRelationships = method[ReadOperations, RelationshipIterator]("nodeGetRelationships", typeRef[Long], typeRef[Direction], typeRef[Array[Int]])
  val allConnectingRelationships = method[CompiledExpandUtils, RelationshipIterator]("connectingRelationships", typeRef[ReadOperations], typeRef[Long], typeRef[Long], typeRef[Direction])
  val connectingRelationships = method[CompiledExpandUtils, RelationshipIterator]("connectingRelationships", typeRef[ReadOperations], typeRef[Long], typeRef[Long], typeRef[Direction], typeRef[Array[Int]])
  val mathAdd = method[CompiledMathHelper, Object]("add", typeRef[Object], typeRef[Object])
  val mathSub = method[CompiledMathHelper, Object]("subtract", typeRef[Object], typeRef[Object])
  val mathMul = method[CompiledMathHelper, Object]("multiply", typeRef[Object], typeRef[Object])
  val mathDiv = method[CompiledMathHelper, Object]("divide", typeRef[Object], typeRef[Object])
  val mathMod = method[CompiledMathHelper, Object]("modulo", typeRef[Object], typeRef[Object])
  val mathCastToInt = method[CompiledMathHelper, Int]("transformToInt", typeRef[Object])
  val mapGet = method[util.Map[String, Object], Object]("get", typeRef[String])
  val mapContains = method[util.Map[String, Object], Boolean]("containsKey", typeRef[String])
  val labelGetForName = method[ReadOperations, Int]("labelGetForName", typeRef[String])
  val propertyKeyGetForName = method[ReadOperations, Int]("propertyKeyGetForName", typeRef[String])
  val coerceToPredicate = method[CompiledConversionUtils, Boolean]("coerceToPredicate", typeRef[Object])
  val toCollection = method[CompiledConversionUtils, java.util.Collection[Object]]("toCollection", typeRef[Object])
  val ternaryEquals = method[CompiledConversionUtils, java.lang.Boolean]("equals", typeRef[Object], typeRef[Object])
  val equals = method[Object, Boolean]("equals", typeRef[Object])
  val or = method[CompiledConversionUtils, java.lang.Boolean]("or", typeRef[Object], typeRef[Object])
  val not = method[CompiledConversionUtils, java.lang.Boolean]("not", typeRef[Object])
  val loadParameter = method[CompiledConversionUtils, java.lang.Object]("loadParameter", typeRef[Object])
  val relationshipTypeGetForName = method[ReadOperations, Int]("relationshipTypeGetForName", typeRef[String])
  val nodesGetAll = method[ReadOperations, PrimitiveLongIterator]("nodesGetAll")
  val nodeGetProperty = method[ReadOperations, Object]("nodeGetProperty")
  val nodesGetFromIndexLookup = method[ReadOperations, PrimitiveLongIterator]("nodesGetFromIndexSeek", typeRef[IndexDescriptor], typeRef[Object])
  val nodeGetUniqueFromIndexLookup = method[ReadOperations, Long]("nodeGetFromUniqueIndexSeek", typeRef[IndexDescriptor], typeRef[Object])
  val relationshipGetProperty = method[ReadOperations, Object]("relationshipGetProperty")
  val nodesGetForLabel = method[ReadOperations, PrimitiveLongIterator]("nodesGetForLabel", typeRef[Int])
  val nodeHasLabel = method[ReadOperations, Boolean]("nodeHasLabel", typeRef[Long], typeRef[Int])
  val nextLong = method[PrimitiveLongIterator, Long]("next")
  val newNodeProxyById = method[NodeManager, Node]("newNodeProxyById")
  val nodeId = method[NodeIdWrapper, Long]("id")
  val relId = method[RelationshipIdWrapper, Long]("id")
  val newRelationshipProxyById = method[NodeManager, Relationship]("newRelationshipProxyById")
  val set = method[ResultRowImpl, Unit]("set", typeRef[String], typeRef[Object])
  val visit = method[ResultVisitor[_], Boolean]("visit", typeRef[ResultRow])
  val executeOperator = method[QueryExecutionTracer, QueryExecutionEvent]("executeOperator", typeRef[Id])
  val dbHit = method[QueryExecutionEvent, Unit]("dbHit")
  val row = method[QueryExecutionEvent, Unit]("row")
}

private object Templates {
  import GeneratedQueryStructure.{method, param, staticField, typeRef}

  def newInstance(valueType: TypeReference, args: Expression*): Expression = {
    Expression.invoke(Expression.newInstance(valueType), MethodReference.constructorReference(valueType), args:_*)
  }

  val newLongObjectMap = Expression.invoke(method[Primitive,PrimitiveLongObjectMap[_]]("longObjectMap"))
  val newCountingMap = Expression.invoke(method[Primitive,PrimitiveLongIntMap]("longIntMap"))

  def asList(values: Seq[Expression]): Expression = Expression.invoke(
    methodReference(typeRef[util.Arrays], typeRef[util.List[String]], "asList", typeRef[Array[String]]),
    values: _*)

  def handleExceptions[V](generate: CodeBlock, ro: FieldReference)(block: CodeBlock => V) = using(generate.tryBlock()) { body =>
    // the body of the try
    val result = block(body)
    // the catch block
    using(body.catchBlock(param[KernelException]("e"))) { handle =>
      handle.throwException(Expression.invoke(
        Expression.newInstance(typeRef[CypherExecutionException]),
        MethodReference.constructorReference(typeRef[CypherExecutionException], typeRef[String], typeRef[Throwable]),
        Expression.invoke(handle.load("e"), GeneratedQueryStructure.method[KernelException, String]("getUserMessage", typeRef[TokenNameLookup]),
          Expression.invoke(
            Expression.newInstance(typeRef[StatementTokenNameLookup]),
            MethodReference.constructorReference(typeRef[StatementTokenNameLookup], typeRef[ReadOperations]),
            Expression.get(handle.self(), ro))),
        handle.load("e")
      ))
    }
    result
  }

  val incoming = Expression.get(staticField[Direction, Direction](Direction.INCOMING.name()))
  val outgoing = Expression.get(staticField[Direction, Direction](Direction.OUTGOING.name()))
  val both = Expression.get(staticField[Direction, Direction](Direction.BOTH.name()))
  val newResultRow = Expression.invoke(Expression.newInstance(typeRef[ResultRowImpl]), MethodReference.constructorReference(typeRef[ResultRowImpl]))
  val newRelationshipDataExtractor = Expression.invoke(Expression.newInstance(typeRef[RelationshipDataExtractor]), MethodReference.constructorReference(typeRef[RelationshipDataExtractor]))

  val CONSTRUCTOR = MethodTemplate.constructor(
    param[TaskCloser]("closer"),
    param[Statement]("statement"),
    param[NodeManager]("nodeManager"),
    param[ExecutionMode]("executionMode"),
    param[java.util.function.Supplier[InternalPlanDescription]]("description"),
    param[QueryExecutionTracer]("tracer"),
    param[util.Map[String, Object]]("params")).
    put(self(), typeRef[TaskCloser], "closer", load("closer")).
    put(self(), typeRef[ReadOperations], "ro", invoke(load("statement"), method[Statement, ReadOperations]("readOperations"))).
    put(self(), typeRef[ExecutionMode], "executionMode", load("executionMode")).
    put(self(), typeRef[java.util.function.Supplier[InternalPlanDescription]], "description", load("description")).
    put(self(), typeRef[QueryExecutionTracer], "tracer", load("tracer")).
    put(self(), typeRef[util.Map[String, Object]], "params", load("params")).
    put(self(), typeRef[NodeManager], "nodeManager", load("nodeManager")).
    build()

  val SET_SUCCESSFUL_CLOSEABLE = MethodTemplate.method(typeRef[Unit], "setSuccessfulCloseable",
    param[SuccessfulCloseable]("closeable")).
    put(self(), typeRef[SuccessfulCloseable], "closeable", load("closeable")).
    build()
  val SUCCESS = MethodTemplate.method(typeRef[Unit], "success").
    expression(invoke(get(self(), typeRef[SuccessfulCloseable], "closeable"), method[SuccessfulCloseable, Unit]("success"))).
    build()
  val CLOSE = MethodTemplate.method(typeRef[Unit], "close").
    expression(invoke(get(self(), typeRef[SuccessfulCloseable], "closeable"), method[SuccessfulCloseable, Unit]("close"))).
    build()
  val EXECUTION_MODE = MethodTemplate.method(typeRef[ExecutionMode], "executionMode").
    returns(get(self(), typeRef[ExecutionMode], "executionMode")).
    build()
  val EXECUTION_PLAN_DESCRIPTION = MethodTemplate.method(typeRef[InternalPlanDescription], "executionPlanDescription").
    returns(invoke(get(self(), typeRef[java.util.function.Supplier[InternalPlanDescription]], "description"), method[java.util.function.Supplier[InternalPlanDescription], InternalPlanDescription]("get"))).
    build()
  val JAVA_COLUMNS = MethodTemplate.method(typeRef[util.List[String]], "javaColumns").
    returns(get(typeRef[util.List[String]], "COLUMNS")).
    build()
}
