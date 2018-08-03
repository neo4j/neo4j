/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime.compiled.expressions

import java.util.regex

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.SlotConfiguration
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.ast._
import org.neo4j.cypher.internal.compiler.v3_5.helpers.PredicateHelper.isPredicate
import org.neo4j.cypher.internal.runtime.DbAccess
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateRepresentation.{load, method}
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.NestedPipeExpression
import org.neo4j.cypher.internal.v3_5.logical.plans.{CoerceToPredicate, NestedPlanExpression}
import org.neo4j.cypher.operations.{CypherBoolean, CypherCoercions, CypherFunctions, CypherMath}
import org.neo4j.internal.kernel.api.procs.Neo4jTypes
import org.neo4j.internal.kernel.api.procs.Neo4jTypes.AnyType
import org.neo4j.values.AnyValue
import org.neo4j.values.storable._
import org.neo4j.values.virtual._
import org.opencypher.v9_0.expressions
import org.opencypher.v9_0.expressions._
import org.opencypher.v9_0.util.symbols.{CTAny, CTBoolean, CTDate, CTDateTime, CTDuration, CTFloat, CTGeometry, CTInteger, CTLocalDateTime, CTLocalTime, CTMap, CTNode, CTNumber, CTPath, CTPoint, CTRelationship, CTString, CTTime, CypherType, ListType}
import org.opencypher.v9_0.util.{CypherTypeException, InternalException}

/**
  * Produces IntermediateRepresentation from a Cypher Expression
  */
class IntermediateCodeGeneration(slots: SlotConfiguration) {

  private var counter: Int = 0

  import IntermediateCodeGeneration._
  import IntermediateRepresentation._


  def compile(expression: Expression): Option[IntermediateExpression] = expression match {

    //functions
    case c: FunctionInvocation => compileFunction(c)

    //math
    case Multiply(lhs, rhs) =>
      for {l <- compile(lhs)
           r <- compile(rhs)
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherMath, AnyValue, AnyValue, AnyValue]("multiply"), l.ir, r.ir),
            l.fields ++ r.fields, l.variables ++ r.variables, l.nullCheck ++ r.nullCheck)
      }

    case Add(lhs, rhs) =>
      for {l <- compile(lhs)
           r <- compile(rhs)
      } yield {
        IntermediateExpression(
            invokeStatic(method[CypherMath, AnyValue, AnyValue, AnyValue]("add"), l.ir, r.ir),
            l.fields ++ r.fields, l.variables ++ r.variables,  l.nullCheck ++ r.nullCheck)
      }

    case Subtract(lhs, rhs) =>
      for {l <- compile(lhs)
           r <- compile(rhs)
      } yield {
        IntermediateExpression(
            invokeStatic(method[CypherMath, AnyValue, AnyValue, AnyValue]("subtract"), l.ir, r.ir),
            l.fields ++ r.fields, l.variables ++ r.variables,  l.nullCheck ++ r.nullCheck)
      }

    //literals
    case d: DoubleLiteral => Some(IntermediateExpression(
      invokeStatic(method[Values, DoubleValue, Double]("doubleValue"), constant(d.value)), Seq.empty, Set.empty, Set.empty))
    case i: IntegerLiteral => Some(IntermediateExpression(
      invokeStatic(method[Values, LongValue, Long]("longValue"), constant(i.value)), Seq.empty, Set.empty, Set.empty))
    case s: expressions.StringLiteral => Some(IntermediateExpression(
      invokeStatic(method[Values, TextValue, String]("stringValue"), constant(s.value)), Seq.empty, Set.empty, Set.empty) )
    case _: Null => Some(IntermediateExpression(noValue, Seq.empty, Set.empty, Set(constant(true))))
    case _: True => Some(IntermediateExpression(truthValue, Seq.empty, Set.empty, Set.empty))
    case _: False => Some(IntermediateExpression(falseValue, Seq.empty, Set.empty, Set.empty))
    case ListLiteral(args) =>
      val in = args.flatMap(compile)
      if (in.size < args.size) None
      else {
        val fields: Seq[Field] = in.foldLeft(Seq.empty[Field])((a, b) => a ++ b.fields)
        val variables: Set[LocalVariable] = in.foldLeft(Set.empty[LocalVariable])((a, b) => a ++ b.variables)
        Some(IntermediateExpression(
          invokeStatic(method[VirtualValues, ListValue, Array[AnyValue]]("list"), arrayOf(in.map(_.ir):_*)),
          fields, variables, Set.empty))
      }
    case Variable(name) =>
      val variableName = s"variable_${name.replaceAll("\\s", "_")}"
      val local = variable[AnyValue](variableName,
                                     invokeStatic(
                                       method[CompiledHelpers, AnyValue, ExecutionContext, String]("loadVariable"),
                                       load("context"),
                                                  constant(name)))
      Some(IntermediateExpression(load(variableName),Seq.empty, Set(local), Set(equal(load(variableName), noValue))))

    //boolean operators
    case Or(lhs, rhs) =>
      for {l <- compile(lhs)
           r <- compile(rhs)
      } yield {
        val left = if (isPredicate(lhs)) l else coerceToPredicate(l)
        val right = if (isPredicate(rhs)) r else coerceToPredicate(r)
        generateOrs(List(left, right))
      }

    case Ors(expressions) =>
      val compiled = expressions.foldLeft[Option[List[(IntermediateExpression, Boolean)]]](Some(List.empty)) { (acc, current) =>
        for {l <- acc
             e <- compile(current)} yield l :+ (e -> isPredicate(current))
      }

      for (e <- compiled) yield e match {
        case Nil => IntermediateExpression(truthValue, Seq.empty, Set.empty, Set.empty) //this will not really happen because of rewriters etc
        case (a, isPredicate) :: Nil  => if (isPredicate) a else coerceToPredicate(a)
        case list =>
          val coerced = list.map {
            case (p, true) => p
            case (p, false) => coerceToPredicate(p)
          }
          generateOrs(coerced)
      }

    case Xor(lhs, rhs) =>
      for {l <- compile(lhs)
           r <- compile(rhs)
      } yield {
        val left = if (isPredicate(lhs)) l else coerceToPredicate(l)
        val right = if (isPredicate(rhs)) r else coerceToPredicate(r)
        IntermediateExpression(
          invokeStatic(method[CypherBoolean, Value, AnyValue, AnyValue]("xor"), left.ir, right.ir),
          l.fields ++ r.fields, l.variables ++ r.variables, l.nullCheck ++ r.nullCheck)
      }

    case And(lhs, rhs) =>
      for {l <- compile(lhs)
           r <- compile(rhs)
      } yield {
        val left = if (isPredicate(lhs)) l else coerceToPredicate(l)
        val right = if (isPredicate(rhs)) r else coerceToPredicate(r)
        generateAnds(List(left, right))
      }

    case Ands(expressions) =>
      val compiled = expressions.foldLeft[Option[List[(IntermediateExpression, Boolean)]]](Some(List.empty)) { (acc, current) =>
          for {l <- acc
               e <- compile(current)} yield l :+ (e -> isPredicate(current))
        }

      for (e <- compiled) yield e match {
        case Nil => IntermediateExpression(truthValue, Seq.empty, Set.empty, Set.empty) //this will not really happen because of rewriters etc
        case (a, isPredicate) :: Nil  => if (isPredicate) a else coerceToPredicate(a)
        case list =>
          val coerced = list.map {
            case (p, true) => p
            case (p, false) => coerceToPredicate(p)
          }
          generateAnds(coerced)
      }

    case Not(arg) =>
      compile(arg).map(a => {
        val in = if (isPredicate(arg)) a else coerceToPredicate(a)
        IntermediateExpression(
          invokeStatic(method[CypherBoolean, Value, AnyValue]("not"), in.ir), in.fields, in.variables, in.nullCheck)
      })

    case Equals(lhs, rhs) =>
      for {l <- compile(lhs)
           r <- compile(rhs)
      } yield {
        val variableName = nextVariableName()
        val local = variable[Value](variableName, invokeStatic(method[CypherBoolean, Value, AnyValue, AnyValue]("equals"), l.ir, r.ir))
        IntermediateExpression(load(variableName), l.fields ++ r.fields, l.variables ++ r.variables + local, Set(equal(load(variableName), noValue)))
      }

    case NotEquals(lhs, rhs) =>
      for {l <- compile(lhs)
           r <- compile(rhs)
      } yield {
        val variableName = nextVariableName()
        val local = variable[Value](variableName, invokeStatic(method[CypherBoolean, Value, AnyValue, AnyValue]("notEquals"), l.ir, r.ir))
        IntermediateExpression(load(variableName), l.fields ++ r.fields, l.variables ++ r.variables + local, Set(equal(load(variableName), noValue)))
      }

    case CoerceToPredicate(inner) => compile(inner).map(coerceToPredicate)

    case RegexMatch(lhs, rhs) => rhs match {
      case expressions.StringLiteral(name) =>
        for ( e <- compile(lhs)) yield {
          val f = field[regex.Pattern](nextVariableName())
          val variableName = nextVariableName()
          val nullChecks = if (e.nullCheck.nonEmpty) Set(equal(load(variableName), noValue)) else Set.empty[IntermediateRepresentation]
          val local = variable[Value](variableName, nullCheck(e)(
            block(
              //if (f == null) { f = Pattern.compile(...) }
              condition(isNull(loadField(f)))(
                setField(f,invokeStatic(method[regex.Pattern, regex.Pattern, String]("compile"), constant(name)))),
            invokeStatic(method[CypherBoolean, Value, AnyValue, regex.Pattern]("regex"), e.ir, loadField(f)))))
          IntermediateExpression(load(variableName),
                                 e.fields :+ f, e.variables + local, nullChecks)
        }

      case _ =>
        for {l <- compile(lhs)
             r <- compile(rhs)
        } yield {
          val variableName = nextVariableName()
          val nullChecks = if (r.nullCheck.nonEmpty) Set(equal(load(variableName), noValue)) else Set.empty[IntermediateRepresentation]
          val local = variable[Value](variableName, nullCheck(r)(invokeStatic(method[CypherBoolean, Value, AnyValue, AnyValue]("regex"), l.ir, r.ir)))
          IntermediateExpression(
           load(variableName),
            l.fields ++ r.fields, l.variables ++ r.variables + local, nullChecks)
        }
    }

    case StartsWith(lhs, rhs) =>
      for {l <- compile(lhs)
            r <- compile(rhs)} yield {
        val variableName = nextVariableName()
        val local = variable[Value](variableName, invokeStatic(method[CypherBoolean, Value, AnyValue, AnyValue]("startsWith"), l.ir, r.ir))
        IntermediateExpression(
          load(variableName),
          l.fields ++ r.fields, l.variables ++ r.variables + local,  Set(equal(load(variableName), noValue)))
      }

    case EndsWith(lhs, rhs) =>
      for {l <- compile(lhs)
           r <- compile(rhs)} yield {
        val variableName = nextVariableName()
        val local = variable[Value](variableName, invokeStatic(method[CypherBoolean, Value, AnyValue, AnyValue]("endsWith"), l.ir, r.ir))
        IntermediateExpression(
          load(variableName),
          l.fields ++ r.fields, l.variables ++ r.variables + local,  Set(equal(load(variableName), noValue)))
      }

    case Contains(lhs, rhs) =>
      for {l <- compile(lhs)
           r <- compile(rhs)} yield {
        val variableName = nextVariableName()
        val local = variable[Value](variableName, invokeStatic(method[CypherBoolean, Value, AnyValue, AnyValue]("contains"), l.ir, r.ir))
        IntermediateExpression(
          load(variableName),
          l.fields ++ r.fields, l.variables ++ r.variables + local,  Set(equal(load(variableName), noValue)))
      }

    case expressions.IsNull(test) =>
      for (e <- compile(test)) yield {
        IntermediateExpression(
          ternary(equal(e.ir, noValue), truthValue, falseValue), e.fields, e.variables, Set.empty)
      }

    case expressions.IsNotNull(test) =>
      for (e <- compile(test)) yield {
        IntermediateExpression(
          ternary(notEqual(e.ir, noValue), truthValue, falseValue), e.fields, e.variables, Set.empty)
      }

    case LessThan(lhs, rhs) =>
      for {l <- compile(lhs)
           r <- compile(rhs)} yield {
        val variableName = nextVariableName()
        val nullChecks = if (l.nullCheck.nonEmpty || r.nullCheck.nonEmpty) Set(equal(load(variableName), noValue)) else Set.empty[IntermediateRepresentation]
        val local = variable[Value](variableName, nullCheck(l, r)(invokeStatic(method[CypherBoolean, Value, AnyValue, AnyValue]("lessThan"), l.ir, r.ir)))
        IntermediateExpression(
          load(variableName),
          l.fields ++ r.fields, l.variables ++ r.variables + local, nullChecks)
      }

    case LessThanOrEqual(lhs, rhs) =>
      for {l <- compile(lhs)
           r <- compile(rhs)} yield {
        val variableName = nextVariableName()
        val nullChecks = if (l.nullCheck.nonEmpty || r.nullCheck.nonEmpty) Set(equal(load(variableName), noValue)) else Set.empty[IntermediateRepresentation]
        val local = variable[Value](variableName,
                                    nullCheck(l, r)(invokeStatic(method[CypherBoolean, Value, AnyValue, AnyValue]("lessThanOrEqual"), l.ir, r.ir)))
        IntermediateExpression(
          load(variableName),
          l.fields ++ r.fields, l.variables ++ r.variables + local, nullChecks)
      }

    case GreaterThan(lhs, rhs) =>
      for {l <- compile(lhs)
           r <- compile(rhs)} yield {
        val variableName = nextVariableName()
        val nullChecks = if (l.nullCheck.nonEmpty || r.nullCheck.nonEmpty) Set(equal(load(variableName), noValue)) else Set.empty[IntermediateRepresentation]
        val local = variable[Value](variableName, nullCheck(l, r)(invokeStatic(method[CypherBoolean, Value, AnyValue, AnyValue]("greaterThan"), l.ir, r.ir)))
        IntermediateExpression(
          load(variableName),
          l.fields ++ r.fields, l.variables ++ r.variables + local, nullChecks)
      }

    case GreaterThanOrEqual(lhs, rhs) =>
      for {l <- compile(lhs)
           r <- compile(rhs)} yield {
        val variableName = nextVariableName()
        val nullChecks = if (l.nullCheck.nonEmpty || r.nullCheck.nonEmpty) Set(equal(load(variableName), noValue)) else Set.empty[IntermediateRepresentation]
        val local = variable[Value](variableName, nullCheck(l, r)(invokeStatic(method[CypherBoolean, Value, AnyValue, AnyValue]("greaterThanOrEqual"), l.ir, r.ir)))
        IntermediateExpression(
          load(variableName),
          l.fields ++ r.fields, l.variables ++ r.variables + local, nullChecks)
      }

    // misc
    case CoerceTo(expr, typ) =>
      for (e <- compile(expr)) yield {
        typ match {
          case CTAny => e
          case CTString =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, TextValue, AnyValue]("asTextValue"), e.ir),
               e.fields, e.variables, e.nullCheck)
          case CTNode =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, NodeValue, AnyValue]("asNodeValue"), e.ir),
              e.fields, e.variables, e.nullCheck)
          case CTRelationship =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, RelationshipValue, AnyValue]("asRelationshipValue"), e.ir),
              e.fields, e.variables, e.nullCheck)
          case CTPath =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, PathValue, AnyValue]("asPathValue"), e.ir),
              e.fields, e.variables, e.nullCheck)
          case CTInteger =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, IntegralValue, AnyValue]("asIntegralValue"), e.ir),
              e.fields, e.variables, e.nullCheck)
          case CTFloat =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, FloatingPointValue, AnyValue]("asFloatingPointValue"), e.ir),
              e.fields, e.variables, e.nullCheck)
          case CTMap =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, MapValue, AnyValue, DbAccess]("asMapValue"), e.ir, DB_ACCESS),
              e.fields, e.variables, e.nullCheck)

          case l: ListType  =>
            val typ = asNeoType(l.innerType)

            IntermediateExpression(
              invokeStatic(method[CypherCoercions, ListValue, AnyValue, AnyType, DbAccess]("asList"), e.ir, typ, DB_ACCESS),
              e.fields, e.variables, e.nullCheck)

          case CTBoolean =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, BooleanValue, AnyValue]("asBooleanValue"), e.ir),
              e.fields, e.variables, e.nullCheck)
          case CTNumber =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, NumberValue, AnyValue]("asNumberValue"), e.ir),
              e.fields, e.variables, e.nullCheck)
          case CTPoint =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, PointValue, AnyValue]("asPointValue"), e.ir),
              e.fields, e.variables, e.nullCheck)
          case CTGeometry =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, PointValue, AnyValue]("asPointValue"), e.ir),
              e.fields, e.variables, e.nullCheck)
          case CTDate =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, DateValue, AnyValue]("asDateValue"), e.ir),
              e.fields, e.variables, e.nullCheck)
          case CTLocalTime =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, LocalTimeValue, AnyValue]("asLocalTimeValue"), e.ir),
              e.fields, e.variables, e.nullCheck)
          case CTTime =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, TimeValue, AnyValue]("asTimeValue"), e.ir),
              e.fields, e.variables, e.nullCheck)
          case CTLocalDateTime =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, LocalDateTimeValue, AnyValue]("asLocalDateTimeValue"), e.ir),
              e.fields, e.variables, e.nullCheck)
          case CTDateTime =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, DateTimeValue, AnyValue]("asDateTimeValue"), e.ir),
              e.fields, e.variables, e.nullCheck)
          case CTDuration =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, DurationValue, AnyValue]("asDurationValue"), e.ir),
              e.fields, e.variables, e.nullCheck)
          case _ =>  throw new CypherTypeException(s"Can't coerce to $typ")
        }
      }

    //data access
    case ContainerIndex(container, index) =>
      for {c <- compile(container)
           idx <- compile(index)
      } yield {
        val variableName = nextVariableName()
        val nullChecks = if (c.nullCheck.nonEmpty || idx.nullCheck.nonEmpty) Set(equal(load(variableName), noValue)) else Set.empty[IntermediateRepresentation]
        val local = variable[AnyValue](variableName, nullCheck(c, idx)(invokeStatic(method[CypherFunctions, AnyValue, AnyValue, AnyValue, DbAccess]("containerIndex"),
                                                               c.ir, idx.ir, DB_ACCESS)))
        IntermediateExpression(
          load(variableName), c.fields ++ idx.fields, c.variables ++ idx.variables + local, nullChecks)
      }

    case Parameter(name, _) => //TODO parameters that are autogenerated from literals should have nullable = false
      val parameterVariable = s"p_${name.replaceAll("\\s", "_")}"
      val local = variable[AnyValue](parameterVariable,
                                     invoke(load("params"), method[MapValue, AnyValue, String]("get"),
                                            constant(name)))
      Some(IntermediateExpression(load(parameterVariable), Seq.empty, Set(local), Set(equal(load(parameterVariable), noValue))))

    case NodeProperty(offset, token, _) =>
      val variableName = nextVariableName()
      val local = variable[Value](variableName, invoke(DB_ACCESS, method[DbAccess, Value, Long, Int]("nodeProperty"),
                                                        getLongAt(offset), constant(token)))

        Some(IntermediateExpression(load(variableName), Seq.empty, Set(local), Set(equal(load(variableName), noValue))))

    case NodePropertyLate(offset, key, _) =>
      val f = field[Int](nextVariableName(), constant(-1))
      val variableName = nextVariableName()
      val local = variable[Value](variableName, block(
        condition(equal(loadField(f), constant(-1)))(
          setField(f, invoke(DB_ACCESS, method[DbAccess, Int, String]("propertyKey"), constant(key)))),
        invoke(DB_ACCESS, method[DbAccess, Value, Long, Int]("nodeProperty"),
               getLongAt(offset), loadField(f))))
      Some(IntermediateExpression(
        load(variableName), Seq(f), Set(local), Set(equal(load(variableName), noValue))))

    case NodePropertyExists(offset, token, _) =>
      Some(
        IntermediateExpression(
          ternary(
          invoke(DB_ACCESS, method[DbAccess, Boolean, Long, Int]("nodeHasProperty"),
                 getLongAt(offset), constant(token)), truthValue, falseValue), Seq.empty, Set.empty, Set.empty))

    case NodePropertyExistsLate(offset, key, _) =>
      val f = field[Int](nextVariableName(), constant(-1))
      Some(IntermediateExpression(
        block(
          condition(equal(loadField(f), constant(-1)))(
            setField(f, invoke(DB_ACCESS, method[DbAccess, Int, String]("propertyKey"), constant(key)))),
        ternary(
        invoke(DB_ACCESS, method[DbAccess, Boolean, Long, Int]("nodeHasProperty"),
               getLongAt(offset), loadField(f)), truthValue, falseValue)), Seq(f), Set.empty, Set.empty))

    case RelationshipProperty(offset, token, _) =>
      val variableName = nextVariableName()
      val local = variable[Value](variableName, invoke(DB_ACCESS, method[DbAccess, Value, Long, Int]("relationshipProperty"),
                                                       getLongAt(offset), constant(token)))

      Some(IntermediateExpression(load(variableName), Seq.empty, Set(local), Set(equal(load(variableName), noValue))))

    case RelationshipPropertyLate(offset, key, _) =>
      val f = field[Int](nextVariableName(), constant(-1))
      val variableName = nextVariableName()
      val local = variable[Value](variableName, block(
        condition(equal(loadField(f), constant(-1)))(
          setField(f, invoke(DB_ACCESS, method[DbAccess, Int, String]("propertyKey"), constant(key)))),
        invoke(DB_ACCESS, method[DbAccess, Value, Long, Int]("relationshipProperty"),
               getLongAt(offset), loadField(f))))
      Some(IntermediateExpression(
        load(variableName), Seq(f), Set(local), Set(equal(load(variableName), noValue))))

    case RelationshipPropertyExists(offset, token, _) =>
      Some(IntermediateExpression(
        ternary(
          invoke(DB_ACCESS, method[DbAccess, Boolean, Long, Int]("relationshipHasProperty"),
                 getLongAt(offset), constant(token)),
          truthValue,
          falseValue), Seq.empty, Set.empty, Set.empty)
      )

    case RelationshipPropertyExistsLate(offset, key, _) =>
      val f = field[Int](nextVariableName(), constant(-1))
      Some(IntermediateExpression(
        block(
          condition(equal(loadField(f), constant(-1)))(
            setField(f, invoke(DB_ACCESS, method[DbAccess, Int, String]("propertyKey"), constant(key)))),
        ternary(
        invoke(DB_ACCESS, method[DbAccess, Boolean, Long, Int]("relationshipHasProperty"),
               getLongAt(offset), loadField(f)),
        truthValue,
        falseValue)), Seq(f), Set.empty, Set.empty))

    case NodeFromSlot(offset, name) =>
      Some(IntermediateExpression(
        invoke(DB_ACCESS, method[DbAccess, NodeValue, Long]("nodeById"), getLongAt(offset)),
        Seq.empty, Set.empty,
        slots.get(name).filter(_.nullable).map(slot => equal(getLongAt(slot.offset), constant(-1L))).toSet))

    case RelationshipFromSlot(offset, name) =>
      Some(IntermediateExpression(
        invoke(DB_ACCESS, method[DbAccess, RelationshipValue, Long]("relationshipById"),
               getLongAt(offset)), Seq.empty, Set.empty,
        slots.get(name).filter(_.nullable).map(slot => equal(getLongAt(slot.offset), constant(-1L))).toSet))

    case GetDegreePrimitive(offset, typ, dir) =>
      val methodName = dir match {
        case SemanticDirection.OUTGOING => "nodeGetOutgoingDegree"
        case SemanticDirection.INCOMING => "nodeGetIncomingDegree"
        case SemanticDirection.BOTH => "nodeGetTotalDegree"
      }
      typ match {
        case None =>
          Some(
            IntermediateExpression(
              invokeStatic(method[Values, IntValue, Int]("intValue"),
                           invoke(DB_ACCESS, method[DbAccess, Int, Long](methodName), getLongAt(offset))),
              Seq.empty, Set.empty, Set.empty))

        case Some(t) =>
          val f = field[Int](nextVariableName(), constant(-1))
          Some(
            IntermediateExpression(
              block(
                condition(equal(loadField(f), constant(-1)))(
                  setField(f, invoke(DB_ACCESS, method[DbAccess, Int, String]("relationshipType"), constant(t)))),
                invokeStatic(method[Values, IntValue, Int]("intValue"),
                           invoke(DB_ACCESS, method[DbAccess, Int, Long, Int](methodName),
                                  getLongAt(offset), loadField(f)))), Seq(f), Set.empty, Set.empty))
      }

    //slotted operations
    case ReferenceFromSlot(offset, name) =>
      Some(IntermediateExpression(getRefAt(offset), Seq.empty, Set.empty,
                                  slots.get(name).filter(_.nullable).map(_ => equal(getRefAt(offset), noValue)).toSet))

    case IdFromSlot(offset) =>
      Some(IntermediateExpression(invokeStatic(method[Values, LongValue, Long]("longValue"), getLongAt(offset)),
                                  Seq.empty, Set.empty, Set.empty))

    case PrimitiveEquals(lhs, rhs) =>
      for {l <- compile(lhs)
           r <- compile(rhs)
      } yield
        IntermediateExpression(
          ternary(invoke(l.ir, method[AnyValue, Boolean, AnyRef]("equals"), r.ir), truthValue, falseValue),
          l.fields ++ r.fields, l.variables ++ r.variables, Set.empty)

    case NullCheck(offset, inner) =>
      compile(inner).map(i =>
        IntermediateExpression(
          ternary(equal(getLongAt(offset), constant(-1L)), noValue, i.ir),
          i.fields, i.variables, Set(equal(getLongAt(offset), constant(-1L))))
      )

    case NullCheckVariable(offset, inner) =>
      compile(inner).map(i => IntermediateExpression(ternary(equal(getLongAt(offset), constant(-1L)), noValue, i.ir),
                                                   i.fields, i.variables, Set(equal(getLongAt(offset), constant(-1L)))))

    case NullCheckProperty(offset, inner) =>
      compile(inner).map(i =>
                           IntermediateExpression(ternary(equal(getLongAt(offset), constant(-1L)), noValue, i.ir),
                                                  i.fields, i.variables, Set(equal(getLongAt(offset), constant(-1L)))))

    case IsPrimitiveNull(offset) =>
      Some(IntermediateExpression(ternary(equal(getLongAt(offset), constant(-1L)), truthValue, falseValue),
                                  Seq.empty, Set.empty, Set.empty))

    case _ => None
  }

  def compileFunction(c: FunctionInvocation): Option[IntermediateExpression] = c.function match {
    case functions.Acos =>
      compile(c.args.head).map(in => IntermediateExpression(
       invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("acos"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Cos =>
      compile(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("cos"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Cot =>
      compile(c.args.head).map(in => IntermediateExpression(
       invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("cot"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Asin =>
      compile(c.args.head).map(in => IntermediateExpression(
       invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("asin"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Haversin =>
      compile(c.args.head).map(in => IntermediateExpression(
       invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("haversin"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Sin =>
      compile(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("sin"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Atan =>
      compile(c.args.head).map(in => IntermediateExpression(
       invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("atan"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Atan2 =>
      for {y <- compile(c.args(0))
           x <- compile(c.args(1))
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, DoubleValue, AnyValue, AnyValue]("atan2"), y.ir, x.ir),
          y.fields ++ x.fields, y.variables ++ x.variables, y.nullCheck ++ x.nullCheck)
      }
    case functions.Tan =>
      compile(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("tan"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Round =>
      compile(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("round"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Rand =>
      Some(IntermediateExpression(invokeStatic(method[CypherFunctions, DoubleValue]("rand")),
                                  Seq.empty, Set.empty, Set.empty))
    case functions.Abs =>
      compile(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, NumberValue, AnyValue]("abs"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Ceil =>
      compile(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("ceil"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Floor =>
      compile(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("floor"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Degrees =>
      compile(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("toDegrees"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Exp =>
      compile(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("exp"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Log =>
      compile(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("log"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Log10 =>
      compile(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("log10"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Radians =>
      compile(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("toRadians"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Sign =>
      compile(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, LongValue, AnyValue]("signum"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Sqrt =>
      compile(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("sqrt"), in.ir), in.fields, in.variables, in.nullCheck))

    case functions.Range  if c.args.length == 2 =>
      for {start <- compile(c.args(0))
           end <- compile(c.args(1))
      } yield IntermediateExpression(invokeStatic(method[CypherFunctions, ListValue, AnyValue, AnyValue]("range"),
                                                  start.ir, end.ir),
                                     start.fields ++ end.fields,
                                     start.variables ++ end.variables, Set.empty)

    case functions.Range  if c.args.length == 3 =>
      for {start <- compile(c.args(0))
           end <- compile(c.args(1))
           step <- compile(c.args(2))
      } yield IntermediateExpression(invokeStatic(method[CypherFunctions, ListValue, AnyValue, AnyValue, AnyValue]("range"),
                                                  start.ir, end.ir, step.ir),
                                     start.fields ++ end.fields ++ step.fields,
                                     start.variables ++ end.variables ++ step.variables, Set.empty)

    case functions.Pi => Some(IntermediateExpression(getStatic[Values, DoubleValue]("PI"), Seq.empty, Set.empty, Set.empty))
    case functions.E => Some(IntermediateExpression(getStatic[Values, DoubleValue]("E"), Seq.empty, Set.empty, Set.empty))

    case functions.Coalesce =>
      val args = c.args.flatMap(compile)
      if (args.size < c.args.size) None
      else {
        val tempVariable = nextVariableName()
        val local = variable[AnyValue](tempVariable, noValue)
        // This loop will generate:
        // AnyValue tempVariable = arg0;
        //if (tempVariable == NO_VALUE) {
        //  tempVariable = arg1;
        //  if ( tempVariable == NO_VALUE) {
        //    tempVariable = arg2;
        //  ...
        //}
        def loop(expressions: List[IntermediateExpression]): IntermediateRepresentation = expressions match {
          case Nil => throw new InternalException("we should never exhaust this loop")
          case expression :: Nil => assign(tempVariable, expression.ir)
          case expression :: tail =>
            //tempVariable = hd; if (tempVariable == NO_VALUE){[continue with tail]}
            if (expression.nullCheck.nonEmpty) block(assign(tempVariable, expression.ir),
                                 condition(expression.nullCheck.reduceLeft((acc,current) => or(acc, current)))(loop(tail)))
            // WHOAH[Keanu Reeves voice] if not nullable we don't even need to generate code for the coming expressions,
            else assign(tempVariable, expression.ir)
        }
        val repr = block(loop(args.toList),
                          load(tempVariable))

        Some(IntermediateExpression(repr, args.foldLeft(Seq.empty[Field])((a,b) => a ++ b.fields),
                                    args.foldLeft(Set.empty[LocalVariable])((a,b) => a ++ b.variables) + local,
                                    Set(equal(load(tempVariable), noValue))))
      }

    case functions.Distance =>
      for {p1 <- compile(c.args(0))
           p2 <- compile(c.args(1))
      } yield {
        val variableName = nextVariableName()
        val local = variable[AnyValue](variableName,
                                       invokeStatic(method[CypherFunctions, Value, AnyValue, AnyValue]("distance"), p1.ir, p2.ir))
        IntermediateExpression(
          load(variableName),
          p1.fields ++ p2.fields,  p1.variables ++ p2.variables + local, Set(equal(load(variableName), noValue)))
      }

    case functions.StartNode =>
      compile(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, NodeValue, AnyValue, DbAccess]("startNode"), in.ir, DB_ACCESS), in.fields, in.variables, in.nullCheck))

    case functions.EndNode =>
      compile(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, NodeValue, AnyValue, DbAccess]("endNode"), in.ir,
                                      DB_ACCESS), in.fields, in.variables, in.nullCheck))

    case functions.Nodes =>
      compile(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, ListValue, AnyValue]("nodes"), in.ir), in.fields, in.variables, in.nullCheck))

    case functions.Relationships =>
      compile(c.args.head).map(in => IntermediateExpression(
       invokeStatic(method[CypherFunctions, ListValue, AnyValue]("relationships"), in.ir), in.fields, in.variables, in.nullCheck))

    case functions.Exists =>
      c.arguments.head match {
        case property: Property =>
          compile(property.map).map(in => IntermediateExpression(
              invokeStatic(method[CypherFunctions, BooleanValue, String, AnyValue, DbAccess]("propertyExists"),
                           constant(property.propertyKey.name),
                           in.ir, DB_ACCESS ), in.fields, in.variables, in.nullCheck))
        case _: PatternExpression => None//TODO
        case _: NestedPipeExpression => None//TODO?
        case _: NestedPlanExpression => None//TODO
        case _ => None
      }

    case functions.Head =>
      compile(c.args.head).map(in => {
        val variableName = nextVariableName()
        val nullChecks = if (in.nullCheck.nonEmpty) Set(equal(load(variableName), noValue)) else Set.empty[IntermediateRepresentation]

        val local = variable[AnyValue](variableName,
                                       nullCheck(in)(invokeStatic(method[CypherFunctions, AnyValue, AnyValue]("head"), in.ir)))
        IntermediateExpression(load(variableName),
         in.fields, in.variables + local, nullChecks)
      })

    case functions.Id =>
      compile(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, LongValue, AnyValue]("id"), in.ir),
        in.fields, in.variables, in.nullCheck))

    case functions.Labels =>
      compile(c.args.head).map(in => IntermediateExpression(
       invokeStatic(method[CypherFunctions, ListValue, AnyValue, DbAccess]("labels"), in.ir, DB_ACCESS),
       in.fields, in.variables, in.nullCheck))

    case functions.Type =>
      compile(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, TextValue, AnyValue]("type"), in.ir),
         in.fields, in.variables, in.nullCheck))

    case functions.Last =>
      compile(c.args.head).map(in => {
        val variableName = nextVariableName()
        val nullChecks = if (in.nullCheck.nonEmpty) Set(equal(load(variableName), noValue)) else Set.empty[IntermediateRepresentation]
        val local = variable[AnyValue](variableName, nullCheck(in)(invokeStatic(method[CypherFunctions, AnyValue, AnyValue]("last"), in.ir)))
        IntermediateExpression(load(variableName), in.fields, in.variables + local, nullChecks)
      })

    case functions.Left =>
      for {in <- compile(c.args(0))
           endPos <- compile(c.args(1))
      } yield {
        IntermediateExpression(
         invokeStatic(method[CypherFunctions, TextValue, AnyValue, AnyValue]("left"), in.ir, endPos.ir),
         in.fields ++ endPos.fields, in.variables ++ endPos.variables, in.nullCheck)
      }

    case functions.LTrim =>
      for (in <- compile(c.args.head)) yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, TextValue, AnyValue]("ltrim"), in.ir),
          in.fields, in.variables, in.nullCheck)
      }

    case functions.RTrim =>
      for (in <- compile(c.args.head)) yield {
        IntermediateExpression(
         invokeStatic(method[CypherFunctions, TextValue, AnyValue]("rtrim"), in.ir),
         in.fields, in.variables, in.nullCheck)
      }

    case functions.Trim =>
      for (in <- compile(c.args.head)) yield {
        IntermediateExpression(
         invokeStatic(method[CypherFunctions, TextValue, AnyValue]("trim"), in.ir),
         in.fields, in.variables, in.nullCheck)
      }

    case functions.Replace =>
      for {original <- compile(c.args(0))
           search <- compile(c.args(1))
           replaceWith <- compile(c.args(2))
      } yield {
        IntermediateExpression(
            invokeStatic(method[CypherFunctions, TextValue, AnyValue, AnyValue, AnyValue]("replace"),
                         original.ir, search.ir, replaceWith.ir),
          original.fields ++ search.fields ++ replaceWith.fields, original.variables ++ search.variables ++ replaceWith.variables,
          original.nullCheck ++ search.nullCheck ++ replaceWith.nullCheck)
      }

    case functions.Reverse =>
      for (in <- compile(c.args.head)) yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, AnyValue, AnyValue]("reverse"), in.ir), in.fields, in.variables, in.nullCheck)
      }

    case functions.Right =>
      for {in <- compile(c.args(0))
           len <- compile(c.args(1))
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, TextValue, AnyValue, AnyValue]("right"), in.ir, len.ir),
          in.fields ++ len.fields, in.variables ++ len.variables, in.nullCheck)
      }

    case functions.Split =>
      for {original <- compile(c.args(0))
           sep <- compile(c.args(1))
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, ListValue, AnyValue, AnyValue]("split"), original.ir, sep.ir),
          original.fields ++ sep.fields, original.variables ++ sep.variables,
        original.nullCheck ++ sep.nullCheck)
      }

    case functions.Substring if c.args.size == 2 =>
      for {original <- compile(c.args(0))
           start <- compile(c.args(1))
      } yield {
        IntermediateExpression(
         invokeStatic(method[CypherFunctions, TextValue, AnyValue, AnyValue]("substring"), original.ir, start.ir),
          original.fields ++ start.fields, original.variables ++ start.variables, original.nullCheck)
      }

    case functions.Substring  =>
      for {original <- compile(c.args(0))
           start <- compile(c.args(1))
           len <- compile(c.args(2))
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, TextValue, AnyValue, AnyValue, AnyValue]("substring"),
                                              original.ir, start.ir, len.ir),
          original.fields ++ start.fields ++ len.fields,
          original.variables ++ start.variables ++ len.variables, original.nullCheck)
      }

    case functions.ToLower =>
      for (in <- compile(c.args.head)) yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, TextValue, AnyValue]("toLower"), in.ir),
          in.fields, in.variables, in.nullCheck)
      }

    case functions.ToUpper =>
      for (in <- compile(c.args.head)) yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, TextValue, AnyValue]("toUpper"), in.ir),
          in.fields, in.variables, in.nullCheck)
      }

    case functions.Point =>
      for (in <- compile(c.args.head)) yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, Value, AnyValue, DbAccess]("point"), in.ir, DB_ACCESS),
          in.fields, in.variables, in.nullCheck)
      }

    case functions.Keys =>
      for (in <- compile(c.args.head)) yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, ListValue, AnyValue, DbAccess]("keys"), in.ir, DB_ACCESS),
          in.fields, in.variables, in.nullCheck)
      }

    case functions.Size =>
      for (in <- compile(c.args.head)) yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, IntegralValue, AnyValue]("size"), in.ir),
          in.fields, in.variables, in.nullCheck)
      }

    case functions.Length =>
      for (in <- compile(c.args.head)) yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, IntegralValue, AnyValue]("length"), in.ir),
          in.fields, in.variables, in.nullCheck)
      }

    case functions.Tail =>
      for (in <- compile(c.args.head)) yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, ListValue, AnyValue]("tail"), in.ir),
          in.fields, in.variables, in.nullCheck)
      }

    case functions.ToBoolean =>
      for (in <- compile(c.args.head)) yield {
        val variableName = nextVariableName()
        val nullChecks = if (in.nullCheck.nonEmpty) Set(equal(load(variableName), noValue)) else Set.empty[IntermediateRepresentation]
        val local = variable[AnyValue](variableName,
                                       nullCheck(in)(invokeStatic(method[CypherFunctions, Value, AnyValue]("toBoolean"), in.ir)))
        IntermediateExpression(load(variableName), in.fields, in.variables + local, nullChecks)
      }

    case functions.ToFloat =>
      for (in <- compile(c.args.head)) yield {
        val variableName = nextVariableName()
        val nullChecks = if (in.nullCheck.nonEmpty) Set(equal(load(variableName), noValue)) else Set.empty[IntermediateRepresentation]
        val local = variable[AnyValue](variableName,
                                       nullCheck(in)(invokeStatic(method[CypherFunctions, Value, AnyValue]("toFloat"), in.ir)))
        IntermediateExpression(load(variableName),
                               in.fields, in.variables + local, nullChecks)
      }

    case functions.ToInteger =>
      for (in <- compile(c.args.head)) yield {
        val variableName = nextVariableName()
        val nullChecks = if (in.nullCheck.nonEmpty) Set(equal(load(variableName), noValue)) else Set.empty[IntermediateRepresentation]
        val local = variable[AnyValue](variableName,
                                       nullCheck(in)(invokeStatic(method[CypherFunctions, Value, AnyValue]("toInteger"), in.ir)))
        IntermediateExpression(load(variableName), in.fields, in.variables + local, nullChecks)
      }

    case functions.ToString =>
      for (in <- compile(c.args.head)) yield {
        val variableName = nextVariableName()
        val nullChecks = if (in.nullCheck.nonEmpty) Set(equal(load(variableName), noValue)) else Set.empty[IntermediateRepresentation]
        val local = variable[AnyValue](variableName,
                                       nullCheck(in)(invokeStatic(method[CypherFunctions, TextValue, AnyValue]("toString"), in.ir)))
        IntermediateExpression(load(variableName), in.fields, in.variables + local, nullChecks)
      }

    case functions.Properties =>
      for (in <- compile(c.args.head)) yield {
        val variableName = nextVariableName()
        val nullChecks = if (in.nullCheck.nonEmpty) Set(equal(load(variableName), noValue)) else Set.empty[IntermediateRepresentation]
        val local = variable[AnyValue](variableName,
                                       nullCheck(in)(invokeStatic(method[CypherFunctions, MapValue, AnyValue, DbAccess]("properties"), in.ir, DB_ACCESS)))
        IntermediateExpression(load(variableName),
                               in.fields, in.variables + local, nullChecks)
      }

    case _ => None
  }

  private def getLongAt(offset: Int): IntermediateRepresentation =
    invoke(load("context"), method[ExecutionContext, Long, Int]("getLongAt"),
           constant(offset))

  private def getRefAt(offset: Int): IntermediateRepresentation =
    invoke(load("context"), method[ExecutionContext, AnyValue, Int]("getRefAt"),
           constant(offset))

  private def nextVariableName(): String = {
    val nextName = s"v$counter"
    counter += 1
    nextName
  }

  private def nullCheck(expressions: IntermediateExpression*)(onNotNull: IntermediateRepresentation): IntermediateRepresentation = {
    val checks = expressions.foldLeft(Set.empty[IntermediateRepresentation])((acc, current) => acc ++ current.nullCheck)
    if (checks.nonEmpty) ternary(checks.reduceLeft(or), noValue, onNotNull)
    else onNotNull
  }

  private def coerceToPredicate(e: IntermediateExpression) = IntermediateExpression(
    invokeStatic(method[CypherBoolean, Value, AnyValue]("coerceToBoolean"), e.ir), e.fields, e.variables, e.nullCheck)

  /**
    * Ok AND and ANDS are complicated.  At the core we try to find a single `FALSE` if we find one there is no need to look
    * at more predicates. If it doesn't find a `FALSE` it will either return `NULL` if any of the predicates has evaluated
    * to `NULL` or `TRUE` if all predicates evaluated to `TRUE`.
    *
    * For example:
    * - AND(FALSE, NULL) -> FALSE
    * - AND(NULL, FALSE) -> FALSE
    * - AND(TRUE, NULL) -> NULL
    * - AND(NULL, TRUE) -> NULL
    *
    * Errors are an extra complication here, errors are treated as `NULL` except that we will throw an error instead of
    * returning `NULL`, so for example:
    *
    * - AND(FALSE, 42) -> FALSE
    * - AND(42, FALSE) -> FALSE
    * - AND(TRUE, 42) -> throw type error
    * - AND(42, TRUE) -> throw type error
    *
    * The generated code below will look something like;
    *
    * RuntimeException error = null;
    * boolean seenNull = false;
    * Value returnValue = null;
    * try
    * {
    *   returnValue = [expressions.head];
    * }
    * catch( RuntimeException e)
    * {
    *   error = e;
    * }
    * seenNull = returnValue == NO_VALUE;
    * if ( returnValue != FALSE )
    * {
    *    try
    *    {
    *      returnValue = expressions.tail.head;
    *    }
    *    catch( RuntimeException e)
    *    {
    *      error = e;
    *    }
    *    seenValue = returnValue == FALSE ? false : (seenValue ? true : returnValue == NO_VALUE);
    *    if ( returnValue != FALSE )
    *    {
    *       try
    *       {
    *         returnValue = expressions.tail.tail.head;
    *       }
    *       catch( RuntimeException e)
    *       {
    *         error = e;
    *       }
    *       seenValue = returnValue == FALSE ? false : (seenValue ? true : returnValue == NO_VALUE);
    *       ...[continue unroll until we are at the end of expressions]
    *     }
    * }
    * if ( error != null && returnValue != FALSE )
    * {
    *   throw error;
    * }
    * return seenNull ? NO_VALUE : returnValue;
    */
  private def generateAnds(expressions: List[IntermediateExpression]) =
    generateCompositeBoolean(expressions, falseValue)

  /**
    * Ok OR and ORS are also complicated.  At the core we try to find a single `TRUE` if we find one there is no need to look
    * at more predicates. If it doesn't find a `TRUE` it will either return `NULL` if any of the predicates has evaluated
    * to `NULL` or `FALSE` if all predicates evaluated to `FALSE`.
    *
    * For example:
    * - OR(FALSE, NULL) -> NULL
    * - OR(NULL, FALSE) -> NULL
    * - OR(TRUE, NULL) -> TRUE
    * - OR(NULL, TRUE) -> TRUE
    *
    * Errors are an extra complication here, errors are treated as `NULL` except that we will throw an error instead of
    * returning `NULL`, so for example:
    *
    * - OR(TRUE, 42) -> TRUE
    * - OR(42, TRUE) -> TRUE
    * - OR(FALSE, 42) -> throw type error
    * - OR(42, FALSE) -> throw type error
    *
    * The generated code below will look something like;
    *
    * RuntimeException error = null;
    * boolean seenNull = false;
    * Value returnValue = null;
    * try
    * {
    *   returnValue = [expressions.head];
    * }
    * catch( RuntimeException e)
    * {
    *   error = e;
    * }
    * seenNull = returnValue == NO_VALUE;
    * if ( returnValue != TRUE )
    * {
    *    try
    *    {
    *      returnValue = expressions.tail.head;
    *    }
    *    catch( RuntimeException e)
    *    {
    *      error = e;
    *    }
    *    seenValue = returnValue == TRUE ? false : (seenValue ? true : returnValue == NO_VALUE);
    *    if ( returnValue != TRUE )
    *    {
    *       try
    *       {
    *         returnValue = expressions.tail.tail.head;
    *       }
    *       catch( RuntimeException e)
    *       {
    *         error = e;
    *       }
    *       seenValue = returnValue == TRUE ? false : (seenValue ? true : returnValue == NO_VALUE);
    *       ...[continue unroll until we are at the end of expressions]
    *     }
    * }
    * if ( error != null && returnValue != TRUE )
    * {
    *   throw error;
    * }
    * return seenNull ? NO_VALUE : returnValue;
    */
  private def generateOrs(expressions: List[IntermediateExpression]): IntermediateExpression =
    generateCompositeBoolean(expressions, truthValue)

  private def generateCompositeBoolean(expressions: List[IntermediateExpression], breakValue: IntermediateRepresentation): IntermediateExpression = {
    //do we need to do nullchecks
    val nullable = expressions.exists(_.nullCheck.nonEmpty)

    //these are the temp variables used
    val returnValue = nextVariableName()
    val local = variable[AnyValue](returnValue, constant(null))
    val seenNull = nextVariableName()
    val error = nextVariableName()
    val exceptionName = nextVariableName()
    //this is setting up  a `if (returnValue != breakValue)`
    val ifNotBreakValue: IntermediateRepresentation => IntermediateRepresentation = condition(notEqual(load(returnValue), breakValue))
    //this is the inner block of the condition
    val inner = (e: IntermediateExpression) => {
      val loadValue = tryCatch[RuntimeException](exceptionName)(assign(returnValue, nullCheck(e)(invokeStatic(ASSERT_PREDICATE, e.ir))))(
        assign(error, load(exceptionName)))

        if (nullable) {
          Seq(loadValue,
              assign(seenNull,
                     //returnValue == breakValue ? false :
                     ternary(equal(load(returnValue), breakValue), constant(false),
                             //seenNull ? true : (returnValue == NO_VALUE)
                             ternary(load(seenNull), constant(true), equal(load(returnValue), noValue)))))
        } else Seq(loadValue)
      }


    //this loop generates the nested expression:
    //if (returnValue != breakValue) {
    //  try {
    //    returnValue = ...;
    //  } catch ( RuntimeException e) { error = e}
    //  ...
    //  if (returnValue != breakValue ) {
    //    try {
    //        returnValue = ...;
    //    } catch ( RuntimeException e) { error = e}
    //    ...
    def loop(e: List[IntermediateExpression]): IntermediateRepresentation = e match {
      case Nil => throw new InternalException("we should never get here")
      case a :: Nil => ifNotBreakValue(block(inner(a):_*))
      case hd::tl => ifNotBreakValue(block(inner(hd) :+ loop(tl):_*))
    }

    val firstExpression = expressions.head
    val nullChecks = if (nullable) Seq(declare[Boolean](seenNull), assign(seenNull, constant(false))) else Seq.empty
    val nullCheckAssign = if (firstExpression.nullCheck.nonEmpty) Seq(assign(seenNull, equal(load(returnValue), noValue))) else Seq.empty
    val ir =
      block(
        //set up all temp variables
        nullChecks ++ Seq(
          declare[RuntimeException](error),
          assign(error, constant(null)),
          //assign returnValue to head of expressions
          tryCatch[RuntimeException](exceptionName)(
            assign(returnValue, nullCheck(firstExpression)(invokeStatic(ASSERT_PREDICATE, firstExpression.ir))))(
            assign(error, load(exceptionName)))) ++ nullCheckAssign ++ Seq(
          //generated unrolls tail of expression
          loop(expressions.tail),
          //checks if there was an error and that we never evaluated to breakValue, if so throw
          condition(and(notEqual(load(error), constant(null)), notEqual(load(returnValue), breakValue)))(
            fail(load(error))),
          //otherwise check if we have seen a null which implicitly also mean we never seen a FALSE
          //if we seen a null we should return null otherwise we return whatever currently
          //stored in returnValue
          if (nullable) ternary(load(seenNull), noValue, load(returnValue)) else load(returnValue)): _*)
    IntermediateExpression(ir,
                           expressions.foldLeft(Seq.empty[Field])((a,b) => a ++ b.fields),
                           expressions.foldLeft(Set.empty[LocalVariable])((a,b) => a ++ b.variables) + local,
                           Set(equal(load(returnValue), noValue)))
  }

  private def asNeoType(ct: CypherType): IntermediateRepresentation = ct match {
    case CTString => getStatic[Neo4jTypes, Neo4jTypes.TextType]("NTString")
    case CTInteger => getStatic[Neo4jTypes, Neo4jTypes.IntegerType]("NTInteger")
    case CTFloat => getStatic[Neo4jTypes, Neo4jTypes.FloatType]("NTFloat")
    case CTNumber =>  getStatic[Neo4jTypes, Neo4jTypes.NumberType]("NTNumber")
    case CTBoolean => getStatic[Neo4jTypes, Neo4jTypes.BooleanType]("NTBoolean")
    case l: ListType => invokeStatic(method[Neo4jTypes , Neo4jTypes.ListType, AnyType]("NTList"), asNeoType(l.innerType))
    case CTDateTime => getStatic[Neo4jTypes, Neo4jTypes.DateTimeType]("NTDateTime")
    case CTLocalDateTime => getStatic[Neo4jTypes, Neo4jTypes.LocalDateTimeType]("NTLocalDateTime")
    case CTDate => getStatic[Neo4jTypes, Neo4jTypes.DateType]("NTDate")
    case CTTime => getStatic[Neo4jTypes, Neo4jTypes.TimeType]("NTTime")
    case CTLocalTime => getStatic[Neo4jTypes, Neo4jTypes.LocalTimeType]("NTLocalTime")
    case CTDuration =>getStatic[Neo4jTypes, Neo4jTypes.DurationType]("NTDuration")
    case CTPoint => getStatic[Neo4jTypes, Neo4jTypes.PointType]("NTPoint")
    case CTNode =>getStatic[Neo4jTypes, Neo4jTypes.NodeType]("NTNode")
    case CTRelationship => getStatic[Neo4jTypes, Neo4jTypes.RelationshipType]("NTRelationship")
    case CTPath => getStatic[Neo4jTypes, Neo4jTypes.PathType]("NTPath")
    case CTGeometry => getStatic[Neo4jTypes, Neo4jTypes.GeometryType]("NTGeometry")
    case CTMap =>getStatic[Neo4jTypes, Neo4jTypes.MapType]("NTMap")
    case CTAny => getStatic[Neo4jTypes, Neo4jTypes.AnyType]("NTAny")
  }
}

object IntermediateCodeGeneration {
  private val ASSERT_PREDICATE = method[CompiledHelpers, Value, AnyValue]("assertBooleanOrNoValue")
  private val DB_ACCESS = load("dbAccess")
}
