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

package org.neo4j.cypher.internal.compiler.v2_3.codegen

import java.util

import org.neo4j.codegen
import org.neo4j.codegen.ExpressionTemplate._
import org.neo4j.codegen.MethodReference._
import org.neo4j.codegen.TypeReference.{extending, parameterizedType, typeParameter}
import org.neo4j.codegen._
import org.neo4j.collection.primitive.hopscotch.LongKeyIntValueTable
import org.neo4j.collection.primitive.{Primitive, PrimitiveLongIntMap, PrimitiveLongIterator, PrimitiveLongObjectMap}
import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{GeneratedQuery, GeneratedQueryExecution, SuccessfulCloseable}
import org.neo4j.cypher.internal.compiler.v2_3.helpers.using
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.{Id, InternalPlanDescription}
import org.neo4j.cypher.internal.compiler.v2_3.planner.CantCompileQueryException
import org.neo4j.cypher.internal.compiler.v2_3.symbols.CypherType
import org.neo4j.function.Supplier
import org.neo4j.graphdb.Result.{ResultRow, ResultVisitor}
import org.neo4j.graphdb.{Direction, GraphDatabaseService, Node, Relationship}
import org.neo4j.helpers.collection.MapUtil
import org.neo4j.kernel.api.exceptions.KernelException
import org.neo4j.kernel.api.properties.Property
import org.neo4j.kernel.api.{ReadOperations, Statement, StatementTokenNameLookup, TokenNameLookup}
import org.neo4j.kernel.impl.api.store.RelationshipIterator
import org.neo4j.kernel.impl.api.{RelationshipDataExtractor, RelationshipVisitor}

import scala.collection.mutable

// <SPI>
trait CodeStructure[T] {
  def generateQuery(packageName: String, className: String, columns: Seq[String], operatorIds: Map[String, Id])(block: MethodStructure[_] => Unit): T
}

sealed trait JoinTableType

case object LongToCountTable extends JoinTableType
case class LongToListTable(structure: Map[String, CypherType], localMap: Map[String, String]) extends JoinTableType

trait MethodStructure[E] {

  // misc
  def declareFlag(name: String, initialValue: Boolean)
  def updateFlag(name: String, newValue: Boolean)
  def declarePredicate(name: String): Unit
  def declareProperty(name: String): Unit
  def declareCounter(name: String, initialValue: E): Unit
  def putField(structure: Map[String, CypherType], value: E, fieldType: CypherType, fieldName: String, localVar: String): Unit
  def updateProbeTable(structure: Map[String, CypherType], tableVar: String, keyVar: String, element: E): Unit
  def probe(tableVar: String, tableType: JoinTableType, keyVar:String)(block: MethodStructure[E]=>Unit): Unit
  def updateProbeTableCount(tableVar: String, keyVar: String): Unit
  def allocateProbeTable(tableVar: String, tableType: JoinTableType): Unit
  def method(resultType: JoinTableType, resultVar: String, methodName: String)(block: MethodStructure[E]=>Unit): Unit

  // expressions
  def decreaseCounterAndCheckForZero(name: String): E
  def counterEqualsZero(variableName: String): E
  def newTableValue(targetVar: String, structure: Map[String, CypherType]): E
  def constant(value: Object): E
  def asMap(map: Map[String, E]): E
  def asList(values: Seq[E]): E
  def load(varName: String): E

  // arithmetic
  def add(lhs: E, rhs: E): E
  def sub(lhs: E, rhs: E): E

  // predicates
  def not(value: E): E

  // null handling
  def markAsNull(varName: String, cypherType: CypherType): Unit
  def nullable(varName: String, cypherType: CypherType, onSuccess: E): E

  // parameters
  def expectParameter(key: String): Unit
  def parameter(key: String): E

  // tracing
  def trace[V](planStepId: String)(block: MethodStructure[E] => V): V
  def incrementDbHits(): Unit
  def incrementRows(): Unit

  // db access
  def labelScan(iterVar: String, labelIdVar: String): Unit
  def hasLabel(nodeVar: String, labelVar: String, predVar: String): E
  def allNodesScan(iterVar: String): Unit
  def lookupLabelId(labelIdVar: String, labelName: String): Unit
  def lookupRelationshipTypeId(typeIdVar: String, typeName: String): Unit
  def nodeGetAllRelationships(iterVar: String, nodeVar: String, direction: Direction): Unit
  def nodeGetRelationships(iterVar: String, nodeVar: String, direction: Direction, typeVars: Seq[String]): Unit
  def nextNode(targetVar: String, iterVar: String): Unit
  def nextRelationshipNode(targetVar: String, iterVar: String, direction: Direction, nodeVar: String, relVar: String): Unit
  def hasNext(iterVar: String): E
  def nodeGetPropertyById(nodeIdVar: String, propId: Int, propValueVar: String): Unit
  def nodeGetPropertyForVar(nodeIdVar: String, propIdVar: String, propValueVar: String): Unit
  def relationshipGetPropertyById(nodeIdVar: String, propId: Int, propValueVar: String): Unit
  def relationshipGetPropertyForVar(nodeIdVar: String, propIdVar: String, propValueVar: String): Unit
  def lookupPropertyKey(propName: String, propVar: String)
  def propertyValueAsPredicate(propertyExpression: E): E

  // code structure
  def whileLoop(test: E)(block: MethodStructure[E] => Unit): Unit
  def ifStatement(test: E)(block: MethodStructure[E] => Unit): Unit
  def returnSuccessfully(): Unit

  // results
  def materializeNode(nodeIdVar: String): E
  def materializeRelationship(relIdVar: String): E
  def visitRow(): Unit
  def setInRow(column: String, value: E): Unit
}

// </SPI>

// TODO: the rest of this file is implementation of the SPI above, and should move to cypher/cypher

object CodeStructure {
  val __TODO__MOVE_IMPLEMENTATION = new CodeStructure[GeneratedQuery] {
    override def generateQuery(packageName: String, className: String, columns: Seq[String], operatorIds: Map[String, Id])(block: MethodStructure[_] => Unit) = {
      val generator = try {
        codegen.CodeGenerator.generateCode(CodeStructure.getClass.getClassLoader/*, SourceCode.PRINT_SOURCE*/)
      } catch {
        case e: Exception => throw new CantCompileQueryException(e.getMessage, e)
      }
      val execution = using(generator.generateClass(packageName, className+"Execution", typeRef[GeneratedQueryExecution], typeRef[SuccessfulCloseable])) { clazz =>
        // fields
        val fields = Fields(
          closer = clazz.field(typeRef[TaskCloser], "closer"),
          ro = clazz.field(typeRef[ReadOperations], "ro"),
          db = clazz.field(typeRef[GraphDatabaseService], "db"),
          executionMode = clazz.field(typeRef[ExecutionMode], "executionMode"),
          description = clazz.field(typeRef[Supplier[InternalPlanDescription]], "description"),
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
          param[GraphDatabaseService]("db"),
          param[ExecutionMode]("executionMode"),
          param[Supplier[InternalPlanDescription]]("description"),
          param[QueryExecutionTracer]("tracer"),
          param[util.Map[String, Object]]("params"))) { execute =>
          execute.returns(Expression.invoke(Expression.newInstance(execution), MethodReference.constructorReference(execution,
            typeRef[TaskCloser],
            typeRef[Statement],
            typeRef[GraphDatabaseService],
            typeRef[ExecutionMode],
            typeRef[Supplier[InternalPlanDescription]],
            typeRef[QueryExecutionTracer],
            typeRef[util.Map[String, Object]]),
            execute.load("closer"),
            execute.load("statement"),
            execute.load("db"),
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

  def lowerType(cType:CypherType):TypeReference = cType match {
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
                          db: FieldReference,
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
        case (fieldName, fieldType) => clazz.field(CodeStructure.lowerType(fieldType), fieldName)
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

private case class Method(fields: Fields, generator: CodeBlock, aux: AuxGenerator, tracing: Boolean = true,
                          event: Option[String] = None, var locals: Map[String, LocalVariable] = Map.empty)
  extends MethodStructure[Expression] {

  import CodeStructure.typeRef

  override def nextNode(targetVar: String, iterVar: String) =
    generator.assign(typeRef[Long], targetVar, Expression.invoke(generator.load(iterVar), Methods.nextLong))

  override def nextRelationshipNode(targetVar: String, iterVar: String, direction: Direction, nodeVar: String, relVar: String) = {
    val startNode = Expression.invoke(generator.load("rel"), Methods.startNode)
    val endNode = Expression.invoke(generator.load("rel"), Methods.endNode)
    generator.expression(Expression.invoke(generator.load(iterVar), Methods.relationshipVisit, Expression.invoke(generator.load(iterVar), Methods.nextLong), generator.load("rel")))
    generator.assign(typeRef[Long], targetVar, direction match {
      case Direction.INCOMING => startNode
      case Direction.OUTGOING => endNode
      case Direction.BOTH => Expression.ternary(Expression.eq(startNode, generator.load(nodeVar)), endNode, startNode)
    })
    generator.assign(typeRef[Long], relVar, Expression.invoke(generator.load("rel"), Methods.relationship))
  }

  override def allNodesScan(iterVar: String) =
    generator.assign(typeRef[PrimitiveLongIterator], iterVar, Expression.invoke(readOperations, Methods.nodesGetAll))

  override def labelScan(iterVar: String, labelIdVar: String) =
    generator.assign(typeRef[PrimitiveLongIterator], iterVar, Expression.invoke(readOperations, Methods.nodesGetForLabel, generator.load(labelIdVar)))


  override def lookupLabelId(labelIdVar: String, labelName: String) =
    generator.assign(typeRef[Int], labelIdVar, Expression.invoke(readOperations, Methods.labelGetForName, Expression.constant(labelName)))

  override def lookupRelationshipTypeId(typeIdVar: String, typeName: String) =
    generator.assign(typeRef[Int], typeIdVar, Expression.invoke(readOperations, Methods.relationshipTypeGetForName, Expression.constant(typeName)))

  override def hasNext(iterVar: String) =
    Expression.invoke(generator.load(iterVar), Methods.hasNext)

  override def whileLoop(test: Expression)(block: MethodStructure[Expression] => Unit) =
    using(generator.whileLoop(test)) { body =>
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

  override def visitRow() = using(generator.ifStatement(Expression.not(
    Expression.invoke(generator.load("visitor"), Methods.visit, generator.load("row"))))) { body =>
    // NOTE: we are in this if-block if the visitor decided to terminate early (by returning false)
    body.expression(Expression.invoke(body.self(), fields.success))
    body.returns()
  }

  override def materializeNode(nodeIdVar: String) = Expression.invoke(db, Methods.getNodeById, generator.load(nodeIdVar))

  override def nullable(varName: String, cypherType: CypherType, onSuccess: Expression) = {
    Expression.ternary(
      Expression.eq(CodeStructure.nullValue(cypherType), generator.load(varName)),
      Expression.constant(null),
      onSuccess)
  }

  override def materializeRelationship(relIdVar: String) = Expression.invoke(db, Methods.getRelationshipById, generator.load(relIdVar))

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

  override def expectParameter(key: String) =
    using(generator.ifStatement(Expression.not(Expression.invoke(params, Methods.mapContains, Expression.constant(key))))) { block =>
      block.throwException(parameterNotFoundException(key))
    }

  override def parameter(key: String) = Expression.invoke(params, Methods.mapGet, Expression.constant(key))

  override def constant(value: Object) = Expression.constant(value)

  override def not(value: Expression): Expression = Expression.not(value)

  override def markAsNull(varName: String, cypherType: CypherType) =
    generator.assign(CodeStructure.lowerType(cypherType), varName, CodeStructure.nullValue(cypherType))

  override def nodeGetAllRelationships(iterVar: String, nodeVar: String, direction: Direction) = {
    val local = generator.declare(typeRef[RelationshipIterator], iterVar)
    Templates.handleExceptions(generator, fields.ro) { body =>
      body.assign(local, Expression.invoke(readOperations, Methods.nodeGetAllRelationships, body.load(nodeVar), dir(direction)))
    }
  }

  override def nodeGetRelationships(iterVar: String, nodeVar: String, direction: Direction, typeVars: Seq[String]) = {
    val local = generator.declare(typeRef[RelationshipIterator], iterVar)
    Templates.handleExceptions(generator, fields.ro) { body =>
      val args = Seq(body.load(nodeVar), dir(direction)) ++ typeVars.map(body.load)
      body.assign(local, Expression.invoke(readOperations, Methods.nodeGetRelationships, args: _*))
    }
  }

  override def load(varName: String) = generator.load(varName)

  override def add(lhs: Expression, rhs: Expression) = math(Methods.mathAdd, lhs, rhs)

  override def sub(lhs: Expression, rhs: Expression) = math(Methods.mathSub, lhs, rhs)

  private def math(method: MethodReference, lhs: Expression, rhs: Expression): Expression =
    // TODO: generate specialized versions for specific types
    Expression.invoke(method, lhs, rhs)

  private def readOperations = Expression.get(generator.self(), fields.ro)

  private def db = Expression.get(generator.self(), fields.db)

  private def resultRow = generator.load("row")

  private def tracer = Expression.get(generator.self(), fields.tracer)

  private def params = Expression.get(generator.self(), fields.params)

  private def parameterNotFoundException(key: String) =
    Expression.invoke(Expression.newInstance(typeRef[ParameterNotFoundException]),
      MethodReference.constructorReference(typeRef[ParameterNotFoundException], typeRef[String]),
      Expression.constant(s"Expected a parameter named $key"))

  private def dir(dir: Direction): Expression = dir match {
    case Direction.INCOMING => Templates.incoming
    case Direction.OUTGOING => Templates.outgoing
    case Direction.BOTH => Templates.both
  }

  override def asList(values: Seq[Expression]) = Templates.asList(values)

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
      case LongToListTable(structure,_) => TypeReference.parameterizedType(classOf[PrimitiveLongObjectMap[_]],
        TypeReference.parameterizedType(classOf[util.ArrayList[_]], aux.typeReference(structure)))
    }
    returnType
  }

  private def allocate(resultType: JoinTableType): Expression = resultType match {
    case LongToCountTable => Templates.newCountingMap
    case LongToListTable(_,_) => Templates.newLongObjectMap
  }

  override def updateProbeTableCount(tableVar: String, keyVar: String) = {
    generator.assign(typeRef[Int], "count", Expression.invoke(generator.load(tableVar), Methods.countingTableGet, generator.load(keyVar)))
    generator.expression(Expression.invoke(generator.load(tableVar), Methods.countingTablePut, generator.load(keyVar), Expression.ternary(
      Expression.eq(generator.load("count"), Expression.get(CodeStructure.staticField[LongKeyIntValueTable, Int]("NULL"))),
      Expression.constant(1),
      Expression.add(generator.load("count"), Expression.constant(1)))))
  }

  override def probe(tableVar: String, tableType: JoinTableType, keyVar:String)(block: MethodStructure[Expression] => Unit) = tableType match {
    case LongToCountTable =>
      val times = generator.declare(typeRef[Int], "times") // TODO: use a namer here!
      generator.assign(times, Expression.invoke(generator.load(tableVar), Methods.countingTableGet, generator.load(keyVar)))
      using(generator.whileLoop(Expression.gt(times, Expression.constant(0)))) { body =>
        block(copy(generator=body))
        body.assign(times, Expression.sub(times, Expression.constant(1)))
      }
    case LongToListTable(structure,locals) =>
      // compute the participating types
      val valueType = aux.typeReference(structure)
      val listType = TypeReference.parameterizedType(classOf[util.ArrayList[_]], valueType)
      val tableType = TypeReference.parameterizedType(classOf[PrimitiveLongObjectMap[_]], valueType)
      // the methods we use on those types
      val get = MethodReference.methodReference(tableType, listType, "get", typeRef[Long])
      // generate the code
      val list = generator.declare(listType, "list")
      generator.assign(list, Expression.invoke(generator.load(tableVar), get, generator.load(keyVar)))
      using(generator.ifStatement(Expression.not(Expression.eq(list, Expression.constant(null))))) { onTrue =>
        using(onTrue.forEach(Parameter.param(valueType, "element"), list)) { forEach =>
          locals.foreach {
            case (local, field) =>
              val fieldType = CodeStructure.lowerType(structure(field))
              forEach.assign(fieldType, local, Expression.get(forEach.load("element"),FieldReference.field(valueType, fieldType, field)))
          }
          block(copy(generator=forEach))
        }
      }
  }

  override def putField(structure: Map[String, CypherType], value: Expression, fieldType: CypherType, fieldName: String, localVar: String) = {
    generator.put(value, field(structure, fieldType, fieldName), generator.load(localVar))
  }

  override def updateProbeTable(structure: Map[String, CypherType], tableVar: String, keyVar: String, element: Expression) = {
    // compute the participating types
    val valueType = aux.typeReference(structure)
    val listType = TypeReference.parameterizedType(classOf[util.ArrayList[_]], valueType)
    val tableType = TypeReference.parameterizedType(classOf[PrimitiveLongObjectMap[_]], valueType)
    // the methods we use on those types
    val get = MethodReference.methodReference(tableType, listType, "get", typeRef[Long])
    val put = MethodReference.methodReference(tableType, listType, "put", typeRef[Long], listType)
    val add = MethodReference.methodReference(listType, typeRef[Boolean], "add", valueType)
    // generate the code
    val list = generator.declare(listType, "list") // ProbeTable list;
    generator.assign(list, Expression.invoke(generator.load(tableVar), get, generator.load(keyVar))) // list = tableVar.get(keyVar);
    using(generator.ifStatement(Expression.eq(Expression.constant(null), generator.load("list")))) { onTrue =>  // if (null == list)
      onTrue.assign(list, Templates.newInstance(listType)) // list = new ListType();
      onTrue.expression(Expression.invoke(generator.load(tableVar), put, generator.load(keyVar), generator.load("list"))) // tableVar.put(keyVar, list);
    }
    generator.expression(Expression.invoke(list, add, element)) // list.add( element );
  }

  override def declareProperty(propertyVar: String) = {
    locals = locals + (propertyVar -> generator.declare(typeRef[Object], propertyVar))
  }

  override def hasLabel(nodeVar: String, labelVar: String, predVar: String) =  {
     val local = locals(predVar)
    Templates.handleExceptions(generator, fields.ro) { inner =>
      val invoke = Expression.invoke(readOperations, Methods.nodeHasLabel, inner.load(nodeVar), inner.load(labelVar))
      inner.assign(local, invoke)
      generator.load(predVar)
    }
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

      body.assign(local, Expression.invoke(
        Expression.invoke(readOperations, Methods.nodeGetProperty, body.load(nodeIdVar), body.load(propIdVar)),
        Methods.value, Expression.constant(null)))

    }
  }

  override def nodeGetPropertyById(nodeIdVar: String, propId: Int, propValueVar: String) = {
    val local = locals(propValueVar)
    Templates.handleExceptions(generator, fields.ro) { body =>
      body.assign(local, Expression.invoke(
        Expression.invoke(readOperations, Methods.nodeGetProperty, body.load(nodeIdVar), Expression.constant(propId)),
        Methods.value, Expression.constant(null)))
    }
  }

  override def relationshipGetPropertyForVar(relIdVar: String, propIdVar: String, propValueVar: String) = {
    val local = locals(propValueVar)
    Templates.handleExceptions(generator, fields.ro) { body =>
      body.assign(local, Expression.invoke(
        Expression.invoke(readOperations, Methods.relationshipGetProperty, body.load(relIdVar), body.load(propIdVar)),
        Methods.value, Expression.constant(null)))
    }
  }

  override def relationshipGetPropertyById(relIdVar: String, propId: Int, propValueVar: String) = {
    val local = locals(propValueVar)
    Templates.handleExceptions(generator, fields.ro) { body =>
      body.assign(local, Expression.invoke(
        Expression.invoke(readOperations, Methods.relationshipGetProperty, body.load(relIdVar), Expression.constant(propId)),
        Methods.value, Expression.constant(null)))
    }
  }

  override def lookupPropertyKey(propName: String, propIdVar: String) =
    generator.assign(typeRef[Int], propIdVar, Expression.invoke(readOperations, Methods.propertyKeyGetForName ,Expression.constant(propName)))

  override def propertyValueAsPredicate(propertyExpression: Expression): Expression =
    Expression.invoke(Methods.propertyAsPredicate, propertyExpression)

  override def newTableValue(targetVar: String, structure: Map[String, CypherType]) = {
    val valueType = aux.typeReference(structure)
    generator.assign(valueType, targetVar, Templates.newInstance(valueType))
    generator.load(targetVar)
  }

  private def field(structure: Map[String, CypherType], fieldType: CypherType, fieldName: String) = {
    FieldReference.field(aux.typeReference(structure), CodeStructure.lowerType(fieldType), fieldName)
  }
}

private object Methods {

  import CodeStructure.{method, typeRef}

  val countingTablePut = method[PrimitiveLongIntMap, Int]("put", typeRef[Long], typeRef[Int])
  val countingTableGet = method[PrimitiveLongIntMap, Int]("get", typeRef[Long])
  val hasNext = method[PrimitiveLongIterator, Boolean]("hasNext")
  val arrayAsList = method[MapUtil, util.Map[String, Object]]("map", typeRef[Array[Object]])
  val relationshipVisit = method[RelationshipIterator, Boolean]("relationshipVisit", typeRef[Long], typeRef[RelationshipVisitor[RuntimeException]])
  val relationship = method[RelationshipDataExtractor, Long]("relationship", typeRef[Long])
  val startNode = method[RelationshipIterator, Long]("startNode")
  val endNode = method[RelationshipIterator, Long]("endNode")
  val nodeGetAllRelationships = method[ReadOperations, RelationshipIterator]("nodeGetRelationships", typeRef[Long], typeRef[Direction])
  val nodeGetRelationships = method[ReadOperations, RelationshipIterator]("nodeGetRelationships", typeRef[Long], typeRef[Direction], typeRef[Array[Int]])
  val mathAdd = method[CompiledMathHelper, Object]("add", typeRef[Object], typeRef[Object])
  val mathSub = method[CompiledMathHelper, Object]("subtract", typeRef[Object], typeRef[Object])
  val mathCastToInt = method[CompiledMathHelper, Int]("transformToInt", typeRef[Object])
  val mapGet = method[util.Map[String, Object], Object]("get", typeRef[String])
  val mapContains = method[util.Map[String, Object], Boolean]("containsKey", typeRef[String])
  val labelGetForName = method[ReadOperations, Int]("labelGetForName", typeRef[String])
  val propertyKeyGetForName = method[ReadOperations, Int]("propertyKeyGetForName", typeRef[String])
  val propertyAsPredicate = method[CompiledPredicateHelper, Boolean]("isPropertyValueTrue", typeRef[Object])
  val relationshipTypeGetForName = method[ReadOperations, Int]("relationshipTypeGetForName", typeRef[String])
  val nodesGetAll = method[ReadOperations, PrimitiveLongIterator]("nodesGetAll")
  val nodeGetProperty = method[ReadOperations, Object]("nodeGetProperty")
  val relationshipGetProperty = method[ReadOperations, Object]("relationshipGetProperty")
  val nodesGetForLabel = method[ReadOperations, PrimitiveLongIterator]("nodesGetForLabel", typeRef[Int])
  val nodeHasLabel = method[ReadOperations, Boolean]("nodeHasLabel", typeRef[Long], typeRef[Int])
  val nextLong = method[PrimitiveLongIterator, Long]("next")
  val getNodeById = method[GraphDatabaseService, Node]("getNodeById")
  val getRelationshipById = method[GraphDatabaseService, Relationship]("getRelationshipById")
  val set = method[ResultRowImpl, Unit]("set", typeRef[String], typeRef[Object])
  val visit = method[ResultVisitor[_], Boolean]("visit", typeRef[ResultRow])
  val executeOperator = method[QueryExecutionTracer, QueryExecutionEvent]("executeOperator", typeRef[Id])
  val dbHit = method[QueryExecutionEvent, Unit]("dbHit")
  val row = method[QueryExecutionEvent, Unit]("row")
  val value = method[Property, Object]("value", typeRef[Object])
}

private object Templates {
  import CodeStructure.{method, param, staticField, typeRef}

  def newInstance(valueType: TypeReference): Expression = {
    Expression.invoke(Expression.newInstance(valueType), MethodReference.constructorReference(valueType))
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
        Expression.invoke(handle.load("e"), CodeStructure.method[KernelException, String]("getUserMessage", typeRef[TokenNameLookup]),
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
    param[GraphDatabaseService]("db"),
    param[ExecutionMode]("executionMode"),
    param[Supplier[InternalPlanDescription]]("description"),
    param[QueryExecutionTracer]("tracer"),
    param[util.Map[String, Object]]("params")).
    put(self(), typeRef[TaskCloser], "closer", load("closer")).
    put(self(), typeRef[ReadOperations], "ro", invoke(load("statement"), method[Statement, ReadOperations]("readOperations"))).
    put(self(), typeRef[GraphDatabaseService], "db", load("db")).
    put(self(), typeRef[ExecutionMode], "executionMode", load("executionMode")).
    put(self(), typeRef[Supplier[InternalPlanDescription]], "description", load("description")).
    put(self(), typeRef[QueryExecutionTracer], "tracer", load("tracer")).
    put(self(), typeRef[util.Map[String, Object]], "params", load("params")).
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
    returns(invoke(get(self(), typeRef[Supplier[InternalPlanDescription]], "description"), method[Supplier[InternalPlanDescription], InternalPlanDescription]("get"))).
    build()
  val JAVA_COLUMNS = MethodTemplate.method(typeRef[util.List[String]], "javaColumns").
    returns(get(typeRef[util.List[String]], "COLUMNS")).
    build()
}
