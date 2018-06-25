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

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.SlotConfiguration
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.ast._
import org.neo4j.cypher.internal.compiler.v3_5.helpers.PredicateHelper.isPredicate
import org.neo4j.cypher.internal.runtime.DbAccess
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateRepresentation.method
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.v3_5.logical.plans.CoerceToPredicate
import org.neo4j.cypher.operations.{CypherBoolean, CypherFunctions, CypherMath}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable._
import org.neo4j.values.virtual.{ListValue, MapValue, NodeValue, RelationshipValue}
import org.opencypher.v9_0.expressions
import org.opencypher.v9_0.expressions._
import org.opencypher.v9_0.util.InternalException

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
          noValueCheck(l, r)(
            invokeStatic(method[CypherMath, AnyValue, AnyValue, AnyValue]("multiply"), l.ir, r.ir)
          ), l.nullable || r.nullable)
      }

    case Add(lhs, rhs) =>
      for {l <- compile(lhs)
           r <- compile(rhs)
      } yield {
        IntermediateExpression(
          noValueCheck(l, r)(
            invokeStatic(method[CypherMath, AnyValue, AnyValue, AnyValue]("add"), l.ir, r.ir)
          ), l.nullable || r.nullable)
      }

    case Subtract(lhs, rhs) =>
      for {l <- compile(lhs)
           r <- compile(rhs)
      } yield {
        IntermediateExpression(
          noValueCheck(l, r)(
            invokeStatic(method[CypherMath, AnyValue, AnyValue, AnyValue]("subtract"), l.ir, r.ir)
          ), l.nullable || r.nullable)

      }

    //literals
    case d: DoubleLiteral => Some(IntermediateExpression(
      invokeStatic(method[Values, DoubleValue, Double]("doubleValue"), constant(d.value)), nullable = false))
    case i: IntegerLiteral => Some(IntermediateExpression(
      invokeStatic(method[Values, LongValue, Long]("longValue"), constant(i.value)), nullable = false))
    case s: expressions.StringLiteral => Some(IntermediateExpression(
      invokeStatic(method[Values, TextValue, String]("stringValue"), constant(s.value)), nullable = false) )
    case _: Null => Some(IntermediateExpression(noValue, nullable = true))
    case _: True => Some(IntermediateExpression(truthValue, nullable = false))
    case _: False => Some(IntermediateExpression(falseValue, nullable = false))

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
        case Nil => IntermediateExpression(truthValue, nullable = false) //this will not really happen because of rewriters etc
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
          noValueCheck(left, right)(invokeStatic(method[CypherBoolean, Value, AnyValue, AnyValue]("xor"), left.ir, right.ir)),
                               left.nullable | right.nullable)
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
        case Nil => IntermediateExpression(truthValue, nullable = false) //this will not really happen because of rewriters etc
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
          noValueCheck(in)(invokeStatic(method[CypherBoolean, Value, AnyValue]("not"), in.ir)), in.nullable)
      })

    case Equals(lhs, rhs) =>
      for {l <- compile(lhs)
           r <- compile(rhs)
      } yield IntermediateExpression(invokeStatic(method[CypherBoolean, Value, AnyValue, AnyValue]("equals"), l.ir, r.ir),
                l.nullable | r.nullable)


    case NotEquals(lhs, rhs) =>
      for {l <- compile(lhs)
           r <- compile(rhs)
      } yield IntermediateExpression(invokeStatic(method[CypherBoolean, Value, AnyValue, AnyValue]("notEquals"), l.ir, r.ir),
        l.nullable | r.nullable)

    case CoerceToPredicate(inner) => compile(inner).map(coerceToPredicate)

    //data access
    case Parameter(name, _) => //TODO parameters that are autogenerated from literals should have nullable = false
      Some(IntermediateExpression(invoke(load("params"), method[MapValue, AnyValue, String]("get"),
                                         constant(name)), nullable = true))

    case NodeProperty(offset, token, _) =>
      Some(IntermediateExpression(
        invoke(load("dbAccess"), method[DbAccess, Value, Long, Int]("nodeProperty"),
                  getLongAt(offset), constant(token)), nullable = true))

    case NodePropertyLate(offset, key, _) =>
      Some(IntermediateExpression(
        invoke(load("dbAccess"), method[DbAccess, Value, Long, String]("nodeProperty"),
                  getLongAt(offset), constant(key)), nullable = true))

    case NodePropertyExists(offset, token, _) =>
      Some(
        IntermediateExpression(
          ternary(
          invoke(load("dbAccess"), method[DbAccess, Boolean, Long, Int]("nodeHasProperty"),
                 getLongAt(offset), constant(token)), truthValue, falseValue), nullable = false))

    case NodePropertyExistsLate(offset, key, _) =>
      Some(IntermediateExpression(
        ternary(
        invoke(load("dbAccess"), method[DbAccess, Boolean, Long, String]("nodeHasProperty"),
               getLongAt(offset), constant(key)), truthValue, falseValue), nullable = false))

    case RelationshipProperty(offset, token, _) =>
      Some(IntermediateExpression(
        invoke(load("dbAccess"), method[DbAccess, Value, Long, Int]("relationshipProperty"),
                  getLongAt(offset), constant(token)), nullable = true))

    case RelationshipPropertyLate(offset, key, _) =>
      Some(IntermediateExpression(
        invoke(load("dbAccess"), method[DbAccess, Value, Long, String]("relationshipProperty"),
                  getLongAt(offset), constant(key)), nullable = true))

    case RelationshipPropertyExists(offset, token, _) =>
      Some(IntermediateExpression(
        ternary(
          invoke(load("dbAccess"), method[DbAccess, Boolean, Long, Int]("relationshipHasProperty"),
                 getLongAt(offset), constant(token)),
          truthValue,
          falseValue), nullable = false)
      )

    case RelationshipPropertyExistsLate(offset, key, _) =>
      Some(IntermediateExpression(
        ternary(
        invoke(load("dbAccess"), method[DbAccess, Boolean, Long, String]("relationshipHasProperty"),
               getLongAt(offset), constant(key)),
        truthValue,
        falseValue), nullable = false))

    case NodeFromSlot(offset, name) =>
      Some(IntermediateExpression(
        invoke(load("dbAccess"), method[DbAccess, NodeValue, Long]("nodeById"), getLongAt(offset)),
        nullable = slots.get(name).forall(_.nullable)))

    case RelationshipFromSlot(offset, name) =>
      Some(IntermediateExpression(
        invoke(load("dbAccess"), method[DbAccess, RelationshipValue, Long]("relationshipById"),
                  getLongAt(offset)), nullable = slots.get(name).forall(_.nullable)))

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
                           invoke(load("dbAccess"), method[DbAccess, Int, Long](methodName), getLongAt(offset))),
              nullable = false))

        case Some(t) =>
          Some(
            IntermediateExpression(
              invokeStatic(method[Values, IntValue, Int]("intValue"),
                           invoke(load("dbAccess"), method[DbAccess, Int, Long, String](methodName),
                                  getLongAt(offset), constant(t))), nullable = false))
      }

    //slotted operations
    case ReferenceFromSlot(offset, name) =>
      Some(IntermediateExpression(getRefAt(offset), slots.get(name).forall(_.nullable) ))
    case IdFromSlot(offset) =>
      Some(IntermediateExpression(invokeStatic(method[Values, LongValue, Long]("longValue"), getLongAt(offset)),
                                  nullable = false))

    case PrimitiveEquals(lhs, rhs) =>
      for {l <- compile(lhs)
           r <- compile(rhs)
      } yield
        IntermediateExpression(
          ternary(invoke(l.ir, method[AnyValue, Boolean, AnyRef]("equals"), r.ir), truthValue, falseValue),
          nullable = false)

    case NullCheck(offset, inner) =>
      compile(inner).map(i =>
                           IntermediateExpression(
                             ternary(equal(getLongAt(offset), constant(-1L)), noValue, i.ir),
                             nullable = true))

    case NullCheckVariable(offset, inner) =>
      compile(inner).map(i => IntermediateExpression(ternary(equal(getLongAt(offset), constant(-1L)), noValue, i.ir),
                                                  nullable = true))

    case NullCheckProperty(offset, inner) =>
      compile(inner).map(i =>
                           IntermediateExpression(ternary(equal(getLongAt(offset), constant(-1L)), noValue, i.ir),
                                                      nullable = true))

    case IsPrimitiveNull(offset) =>
      Some(IntermediateExpression(ternary(equal(getLongAt(offset), constant(-1L)), truthValue, falseValue), nullable = false))

    case _ => None
  }

  def compileFunction(c: FunctionInvocation): Option[IntermediateExpression] = c.function match {
    case functions.Acos =>
      compile(c.args.head).map(in => IntermediateExpression(
        noValueCheck(in)(invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("acos"), in.ir)), in.nullable))
    case functions.Cos =>
      compile(c.args.head).map(in => IntermediateExpression(
        noValueCheck(in)(invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("cos"), in.ir)), in.nullable))
    case functions.Cot =>
      compile(c.args.head).map(in => IntermediateExpression(
        noValueCheck(in)(invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("cot"), in.ir)), in.nullable))
    case functions.Asin =>
      compile(c.args.head).map(in => IntermediateExpression(
        noValueCheck(in)(invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("asin"), in.ir)), in.nullable))
    case functions.Haversin =>
      compile(c.args.head).map(in => IntermediateExpression(
        noValueCheck(in)(invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("haversin"), in.ir)), in.nullable))
    case functions.Sin =>
      compile(c.args.head).map(in => IntermediateExpression(
        noValueCheck(in)(invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("sin"), in.ir)), in.nullable))
    case functions.Atan =>
      compile(c.args.head).map(in => IntermediateExpression(
        noValueCheck(in)(invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("atan"), in.ir)), in.nullable))
    case functions.Atan2 =>
      for {y <- compile(c.args(0))
           x <- compile(c.args(1))
      } yield {
        IntermediateExpression(
          noValueCheck(y, x)(invokeStatic(method[CypherFunctions, DoubleValue, AnyValue, AnyValue]("atan2"), y.ir, x.ir)),
          y.nullable || x.nullable)
      }
    case functions.Tan =>
      compile(c.args.head).map(in => IntermediateExpression(
        noValueCheck(in)(invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("tan"), in.ir)), in.nullable))
    case functions.Round =>
      compile(c.args.head).map(in => IntermediateExpression(
        noValueCheck(in)(invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("round"), in.ir)), in.nullable))
    case functions.Rand =>
      Some(IntermediateExpression(invokeStatic(method[CypherFunctions, DoubleValue]("rand")),
                                  nullable = false))
    case functions.Abs =>
      compile(c.args.head).map(in => IntermediateExpression(
        noValueCheck(in)(invokeStatic(method[CypherFunctions, NumberValue, AnyValue]("abs"), in.ir)), in.nullable))
    case functions.Ceil =>
      compile(c.args.head).map(in => IntermediateExpression(
        noValueCheck(in)(invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("ceil"), in.ir)), in.nullable))
    case functions.Floor =>
      compile(c.args.head).map(in => IntermediateExpression(
        noValueCheck(in)(invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("floor"), in.ir)), in.nullable))
    case functions.Degrees =>
      compile(c.args.head).map(in => IntermediateExpression(
        noValueCheck(in)(invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("toDegrees"), in.ir)), in.nullable))
    case functions.Exp =>
      compile(c.args.head).map(in => IntermediateExpression(
        noValueCheck(in)(invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("exp"), in.ir)), in.nullable))
    case functions.Log =>
      compile(c.args.head).map(in => IntermediateExpression(
        noValueCheck(in)(invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("log"), in.ir)), in.nullable))
    case functions.Log10 =>
      compile(c.args.head).map(in => IntermediateExpression(
        noValueCheck(in)(invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("log10"), in.ir)), in.nullable))
    case functions.Radians =>
      compile(c.args.head).map(in => IntermediateExpression(
        noValueCheck(in)(invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("toRadians"), in.ir)), in.nullable))
    case functions.Sign =>
      compile(c.args.head).map(in => IntermediateExpression(
        noValueCheck(in)(invokeStatic(method[CypherFunctions, LongValue, AnyValue]("signum"), in.ir)), in.nullable))
    case functions.Sqrt =>
      compile(c.args.head).map(in => IntermediateExpression(
        noValueCheck(in)(invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("sqrt"), in.ir)), in.nullable))
    case functions.Range =>
      for {start <- compile(c.args(0))
           end <- compile(c.args(1))
           step <- compile(c.args(2))
      } yield IntermediateExpression(invokeStatic(method[CypherFunctions, ListValue, AnyValue, AnyValue, AnyValue]("range"),
                                                  start.ir, end.ir, step.ir), nullable = false)

    case functions.Pi => Some(IntermediateExpression(getStatic[Values, DoubleValue]("PI"), nullable = false))
    case functions.E => Some(IntermediateExpression(getStatic[Values, DoubleValue]("E"), nullable = false))

    case functions.Coalesce =>
      val args = c.args.flatMap(compile)
      if (args.size < c.args.size) None
      else {
        val tempVariable = nextVariableName()

        //AnyValue tempVariable = arg0;
        //if (tempVariable == NO_VALUE) {
        //  tempVariable = arg1;
        //  if ( tempVariable == NO_VALUE) {
        //    tempVariable = arg2;
        //  ...
        //}
        def loop(expressions: List[IntermediateExpression]): IntermediateRepresentation = expressions match {
          case Nil => throw new InternalException("we should never exhaust this loop")
          case e :: Nil => assign(tempVariable, e.ir)
          case hd :: tl =>
            //tempVariable = hd; if (tempVariable == NO_VALUE){[continue with tail]}
            if (hd.nullable) block(assign(tempVariable, hd.ir),
                                 condition(equal(load(tempVariable), noValue))(loop(tl)))
            // WHOAH[Keanu Reeves voice] if not nullable we don't even need to generate code for the coming expressions,
            else assign(tempVariable, hd.ir)
        }
        val repr = block(declare[AnyValue](tempVariable),
                          assign(tempVariable, noValue),
                          loop(args.toList),
                          load(tempVariable))

        Some(IntermediateExpression(repr, args.exists(_.nullable)))
      }

    case functions.Distance =>
      for {p1 <- compile(c.args(0))
           p2 <- compile(c.args(1))
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, Value, AnyValue, AnyValue]("distance"), p1.ir, p2.ir), nullable = true)
      }

    case functions.StartNode =>
      compile(c.args.head).map(in => IntermediateExpression(
        noValueCheck(in)(invokeStatic(method[CypherFunctions, NodeValue, AnyValue, DbAccess]("startNode"), in.ir,
                                      load("dbAccess"))), in.nullable))

    case functions.EndNode =>
      compile(c.args.head).map(in => IntermediateExpression(
        noValueCheck(in)(invokeStatic(method[CypherFunctions, NodeValue, AnyValue, DbAccess]("endNode"), in.ir,
                                      load("dbAccess"))), in.nullable))

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

  private def coerceToPredicate(e: IntermediateExpression) = IntermediateExpression(
    invokeStatic(method[CypherBoolean, Value, AnyValue]("coerceToBoolean"), e.ir),
    nullable = e.nullable)

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
    val nullable = expressions.exists(_.nullable)

    //these are the temp variables used
    val returnValue = nextVariableName()
    val seenNull = nextVariableName()
    val error = nextVariableName()
    val exceptionName = nextVariableName()
    //this is setting up  a `if (returnValue != breakValue)`
    val ifNotBreakValue: IntermediateRepresentation => IntermediateRepresentation = condition(notEqual(load(returnValue), breakValue))
    //this is the inner block of the condition
    val inner = (e: IntermediateExpression) => {
      val loadValue = tryCatch[RuntimeException](exceptionName)(assign(returnValue, invokeStatic(ASSERT_PREDICATE, e.ir)))(
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
    val nullCheck = if (nullable) Seq(declare[Boolean](seenNull), assign(seenNull, constant(false))) else Seq.empty
    val nullCheckAssign = if (firstExpression.nullable) Seq(assign(seenNull, equal(load(returnValue), noValue))) else Seq.empty
    val ir =
      block(
        //set up all temp variables
        nullCheck ++ Seq(
          declare[RuntimeException](error),
          assign(error, constant(null)),
          declare[Value](returnValue),
          assign(returnValue, constant(null)),
          //assign returnValue to head of expressions
          tryCatch[RuntimeException](exceptionName)(
            assign(returnValue, invokeStatic(ASSERT_PREDICATE, firstExpression.ir)))(
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
    IntermediateExpression(ir, nullable)
  }
}

object IntermediateCodeGeneration {
  private val ASSERT_PREDICATE = method[CompiledHelpers, Value, AnyValue]("assertBooleanOrNoValue")

}
