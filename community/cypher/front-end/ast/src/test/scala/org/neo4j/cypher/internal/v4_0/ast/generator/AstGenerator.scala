/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.v4_0.ast.generator

import org.neo4j.cypher.internal.v4_0.ast._
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.util.symbols.AnyType
import org.neo4j.cypher.internal.v4_0.util.{ASTNode, InputPosition, Rewriter, bottomUp}
import org.scalacheck.Gen._
import org.scalacheck.util.Buildable
import org.scalacheck.{Arbitrary, Gen, Shrink}

case class AstGenerator(simpleStrings: Boolean = true) {

  // HELPERS
  // ==========================================================================

  val ? : InputPosition = InputPosition.NONE

  def boolean: Gen[Boolean] =
    Arbitrary.arbBool.arbitrary

  def char: Gen[Char] =
    Arbitrary.arbChar.arbitrary.suchThat(acceptedByParboiled)

  def acceptedByParboiled(c: Char): Boolean = {
    val DEL_ERROR = '\ufdea'
    val INS_ERROR = '\ufdeb'
    val RESYNC = '\ufdec'
    val RESYNC_START = '\ufded'
    val RESYNC_END = '\ufdee'
    val RESYNC_EOI = '\ufdef'
    val EOI = '\uffff'

    c match {
      case DEL_ERROR    => false
      case INS_ERROR    => false
      case RESYNC       => false
      case RESYNC_START => false
      case RESYNC_END   => false
      case RESYNC_EOI   => false
      case EOI          => false
      case _            => true
    }
  }

  def string: Gen[String] =
    if (simpleStrings) alphaLowerChar.map(_.toString)
    else listOf(char).map(_.mkString)

  def zeroOrMore[T](gen: Gen[T]): Gen[List[T]] =
    choose(0, 3).flatMap(listOfN(_, gen))

  def oneOrMore[T](gen: Gen[T]): Gen[List[T]] =
    choose(1, 3).flatMap(listOfN(_, gen))

  def tuple[A, B](ga: Gen[A], gb: Gen[B]): Gen[(A, B)] = for {
    a <- ga
    b <- gb
  } yield (a, b)

  // IDENTIFIERS
  // ==========================================================================

  def _identifier: Gen[String] =
    if (simpleStrings) alphaLowerChar.map(_.toString)
    else nonEmptyListOf(char).map(_.mkString)

  def _labelName: Gen[LabelName] =
    _identifier.map(LabelName(_)(?))

  def _relTypeName: Gen[RelTypeName] =
    _identifier.map(RelTypeName(_)(?))

  def _propertyKeyName: Gen[PropertyKeyName] =
    _identifier.map(PropertyKeyName(_)(?))

  // EXPRESSIONS
  // ==========================================================================

  // LEAFS
  // ----------------------------------

  def _nullLit: Gen[Null] =
    const(Null.NULL)

  def _stringLit: Gen[StringLiteral] =
    string.flatMap(StringLiteral(_)(?))

  def _booleanLit: Gen[BooleanLiteral] =
    oneOf(True()(?), False()(?))

  def _unsignedIntString(prefix: String, radix: Int): Gen[String] = for {
    num <- posNum[Int]
    str = Integer.toString(num, radix)
  } yield List(prefix, str).mkString

  def _signedIntString(prefix: String, radix: Int): Gen[String] = for {
    str <- _unsignedIntString(prefix, radix)
    neg <- boolean
    sig = if (neg) "-" else ""
  } yield List(sig, str).mkString

  def _unsignedDecIntLit: Gen[UnsignedDecimalIntegerLiteral] =
    _unsignedIntString("", 10).map(UnsignedDecimalIntegerLiteral(_)(?))

  def _signedDecIntLit: Gen[SignedDecimalIntegerLiteral] =
    _signedIntString("", 10).map(SignedDecimalIntegerLiteral(_)(?))

  def _signedHexIntLit: Gen[SignedHexIntegerLiteral] =
    _signedIntString("0x", 16).map(SignedHexIntegerLiteral(_)(?))

  def _signedOctIntLit: Gen[SignedOctalIntegerLiteral] =
    _signedIntString("0", 8).map(SignedOctalIntegerLiteral(_)(?))

  def _signedIntLit: Gen[SignedIntegerLiteral] = oneOf(
    _signedDecIntLit,
    _signedHexIntLit,
    _signedOctIntLit
  )

  def _doubleLit: Gen[DecimalDoubleLiteral] =
    Arbitrary.arbDouble.arbitrary.map(_.toString).map(DecimalDoubleLiteral(_)(?))

  def _parameter: Gen[Parameter] =
    _identifier.map(Parameter(_, AnyType.instance)(?))

  def _variable: Gen[Variable] = for {
    name <- _identifier
  } yield Variable(name)(?)

  // Predicates
  // ----------------------------------

  def _predicateComparisonPar(l: Expression, r: Expression): Gen[Expression] = oneOf(
    GreaterThanOrEqual(l, r)(?),
    GreaterThan(l, r)(?),
    LessThanOrEqual(l, r)(?),
    LessThan(l, r)(?),
    Equals(l, r)(?),
    Equivalent(l, r)(?),
    NotEquals(l, r)(?),
    InvalidNotEquals(l, r)(?)
  )

  def _predicateComparison: Gen[Expression] = for {
    l <- _expression
    r <- _expression
    res <- _predicateComparisonPar(l, r)
  } yield res

  def _predicateComparisonChain: Gen[Expression] = for {
    exprs <- listOfN(4, _expression)
    pairs = exprs.sliding(2)
    gens = pairs.map(p => _predicateComparisonPar(p.head, p.last)).toList
    chain <- sequence(gens)(Buildable.buildableCanBuildFrom)
  } yield Ands(chain.toSet)(?)

  def _predicateUnary: Gen[Expression] = for {
    r <- _expression
    res <- oneOf(
      Not(r)(?),
      IsNull(r)(?),
      IsNotNull(r)(?)
    )
  } yield res

  def _predicateBinary: Gen[Expression] = for {
    l <- _expression
    r <- _expression
    res <- oneOf(
      And(l, r)(?),
      Or(l, r)(?),
      Xor(l, r)(?),
      RegexMatch(l, r)(?),
      In(l, r)(?),
      StartsWith(l, r)(?),
      EndsWith(l, r)(?),
      Contains(l, r)(?)
    )
  } yield res

  def _hasLabels: Gen[HasLabels] = for {
    expression <- _expression
    labels <- oneOrMore(_labelName)
  } yield HasLabels(expression, labels)(?)

  // Collections
  // ----------------------------------

  def _map: Gen[MapExpression] = for {
    items <- zeroOrMore(tuple(_propertyKeyName, _expression))
  } yield MapExpression(items)(?)

  def _property: Gen[Property] = for {
    map <- _expression
    key <- _propertyKeyName
  } yield Property(map, key)(?)

  def _mapProjectionElement: Gen[MapProjectionElement] =
    oneOf(
      for {key <- _propertyKeyName; exp <- _expression} yield LiteralEntry(key, exp)(?),
      for {id <- _variable} yield VariableSelector(id)(?),
      for {id <- _variable} yield PropertySelector(id)(?),
      const(AllPropertiesSelector()(?))
    )

  def _mapProjection: Gen[MapProjection] = for {
    name <- _variable
    items <- oneOrMore(_mapProjectionElement)
  } yield MapProjection(name, items)(?, None)

  def _list: Gen[ListLiteral] = for {
    parts <- zeroOrMore(_expression)
  } yield ListLiteral(parts)(?)

  def _listSlice: Gen[ListSlice] = for {
    list <- _expression
    from <- option(_expression)
    to <- option(_expression)
  } yield ListSlice(list, from, to)(?)

  def _containerIndex: Gen[ContainerIndex] = for {
    expr <- _expression
    idx <- _expression
  } yield ContainerIndex(expr, idx)(?)

  def _filterScope: Gen[FilterScope] = for {
    variable <- _variable
    innerPredicate <- option(_expression)
  } yield FilterScope(variable, innerPredicate)(?)

  def _filter: Gen[FilterExpression] = for {
    scope <- _filterScope
    expression <- _expression
  } yield FilterExpression(scope, expression)(?)

  def _extractScope: Gen[ExtractScope] = for {
    variable <- _variable
    innerPredicate <- option(_expression)
    extractExpression <- option(_expression)
  } yield ExtractScope(variable, innerPredicate, extractExpression)(?)

  def _extract: Gen[ExtractExpression] = for {
    scope <- _extractScope
    expression <- _expression
  } yield ExtractExpression(scope, expression)(?)

  def _listComprehension: Gen[ListComprehension] = for {
    scope <- _extractScope
    expression <- _expression
  } yield ListComprehension(scope, expression)(?)

  def _iterablePredicate: Gen[IterablePredicateExpression] = for {
    scope <- _filterScope
    expression <- _expression
    predicate <- oneOf(
      AllIterablePredicate(scope, expression)(?),
      AnyIterablePredicate(scope, expression)(?),
      NoneIterablePredicate(scope, expression)(?),
      SingleIterablePredicate(scope, expression)(?)
    )
  } yield predicate

  def _reduceScope: Gen[ReduceScope] = for {
    accumulator <- _variable
    variable <- _variable
    expression <- _expression
  } yield ReduceScope(accumulator, variable, expression)(?)

  def _reduceExpr: Gen[ReduceExpression] = for {
    scope <- _reduceScope
    init <- _expression
    list <- _expression
  } yield ReduceExpression(scope, init, list)(?)

  // Arithmetic
  // ----------------------------------

  def _arithmeticUnary: Gen[Expression] = for {
    r <- _expression
    exp <- oneOf(
      UnaryAdd(r)(?),
      UnarySubtract(r)(?)
    )
  } yield exp

  def _arithmeticBinary: Gen[Expression] = for {
    l <- _expression
    r <- _expression
    exp <- oneOf(
      Add(l, r)(?),
      Multiply(l, r)(?),
      Divide(l, r)(?),
      Pow(l, r)(?),
      Modulo(l, r)(?),
      Subtract(l, r)(?)
    )
  } yield exp

  def _case: Gen[CaseExpression] = for {
    expression <- option(_expression)
    alternatives <- oneOrMore(tuple(_expression, _expression))
    default <- option(_expression)
  } yield CaseExpression(expression, alternatives, default)(?)

  // Functions
  // ----------------------------------

  def _namespace: Gen[Namespace] = for {
    parts <- zeroOrMore(_identifier)
  } yield Namespace(parts)(?)

  def _functionName: Gen[FunctionName] = for {
    name <- _identifier
  } yield FunctionName(name)(?)

  def _functionInvocation: Gen[FunctionInvocation] = for {
    namespace <- _namespace
    functionName <- _functionName
    distinct <- boolean
    args <- zeroOrMore(_expression)
  } yield FunctionInvocation(namespace, functionName, distinct, args.toIndexedSeq)(?)

  def _countStar: Gen[CountStar] =
    const(CountStar()(?))

  // Patterns
  // ----------------------------------

  def _relationshipsPattern: Gen[RelationshipsPattern] = for {
    chain <- _relationshipChain
  } yield RelationshipsPattern(chain)(?)

  def _patternExpr: Gen[PatternExpression] = for {
    pattern <- _relationshipsPattern
  } yield PatternExpression(pattern)

  def _shortestPaths: Gen[ShortestPaths] = for {
    element <- _patternElement
    single <- boolean
  } yield ShortestPaths(element, single)(?)

  def _shortestPathExpr: Gen[ShortestPathExpression] = for {
    pattern <- _shortestPaths
  } yield ShortestPathExpression(pattern)

  def _existsSubClause: Gen[ExistsSubClause] = for {
    pattern <- _pattern
    where <- option(_expression)
    outerScope <- zeroOrMore(_variable)
  } yield ExistsSubClause(pattern, where)(?, outerScope.toSet)

  def _patternComprehension: Gen[PatternComprehension] = for {
    namedPath <- option(_variable)
    pattern <- _relationshipsPattern
    predicate <- option(_expression)
    projection <- _expression
    outerScope <- zeroOrMore(_variable)
  } yield PatternComprehension(namedPath, pattern, predicate, projection)(?, outerScope.toSet)

  // Expression
  // ----------------------------------

  def _expression: Gen[Expression] =
    frequency(
      5 -> oneOf(
        lzy(_nullLit),
        lzy(_stringLit),
        lzy(_booleanLit),
        lzy(_signedDecIntLit),
        lzy(_signedHexIntLit),
        lzy(_signedOctIntLit),
        lzy(_doubleLit),
        lzy(_variable),
        lzy(_parameter)
      ),
      1 -> oneOf(
        lzy(_predicateComparison),
        lzy(_predicateUnary),
        lzy(_predicateBinary),
        lzy(_predicateComparisonChain),
        lzy(_iterablePredicate),
        lzy(_hasLabels),
        lzy(_arithmeticUnary),
        lzy(_arithmeticBinary),
        lzy(_case),
        lzy(_functionInvocation),
        lzy(_countStar),
        lzy(_reduceExpr),
        lzy(_shortestPathExpr),
        lzy(_patternExpr),
        lzy(_map),
        lzy(_mapProjection),
        lzy(_property),
        lzy(_list),
        lzy(_listSlice),
        lzy(_listComprehension),
        lzy(_containerIndex),
        lzy(_extract),
        lzy(_filter),
        lzy(_existsSubClause),
        lzy(_patternComprehension)
      )
    )

  // PATTERNS
  // ==========================================================================

  def _nodePattern: Gen[NodePattern] = for {
    variable <- option(_variable)
    labels <- zeroOrMore(_labelName)
    properties <- option(oneOf(_map, _parameter))
    baseNode <- option(_variable)
  } yield NodePattern(variable, labels, properties, baseNode)(?)

  def _range: Gen[Range] = for {
    lower <- option(_unsignedDecIntLit)
    upper <- option(_unsignedDecIntLit)
  } yield Range(lower, upper)(?)

  def _semanticDirection: Gen[SemanticDirection] =
    oneOf(
      SemanticDirection.OUTGOING,
      SemanticDirection.INCOMING,
      SemanticDirection.BOTH
    )

  def _relationshipPattern: Gen[RelationshipPattern] = for {
    variable <- option(_variable)
    types <- zeroOrMore(_relTypeName)
    length <- option(option(_range))
    properties <- option(oneOf(_map, _parameter))
    direction <- _semanticDirection
    baseRel <- option(_variable)
  } yield RelationshipPattern(variable, types, length, properties, direction, false, baseRel)(?)

  def _relationshipChain: Gen[RelationshipChain] = for {
    element <- _patternElement
    relationship <- _relationshipPattern
    rightNode <- _nodePattern
  } yield RelationshipChain(element, relationship, rightNode)(?)

  def _patternElement: Gen[PatternElement] = oneOf(
    _nodePattern,
    lzy(_relationshipChain)
  )

  def _anonPatternPart: Gen[AnonymousPatternPart] = for {
    element <- _patternElement
    single <- boolean
    part <- oneOf(
      EveryPath(element),
      ShortestPaths(element, single)(?)
    )
  } yield part

  def _namedPatternPart: Gen[NamedPatternPart] = for {
    variable <- _variable
    part <- _anonPatternPart
  } yield NamedPatternPart(variable, part)(?)

  def _patternPart: Gen[PatternPart] =
    oneOf(
      _anonPatternPart,
      _namedPatternPart
    )

  def _pattern: Gen[Pattern] = for {
    parts <- oneOrMore(_patternPart)
  } yield Pattern(parts)(?)

  def _patternSingle: Gen[Pattern] = for {
    part <- _patternPart
  } yield Pattern(Seq(part))(?)

  // CLAUSES
  // ==========================================================================

  def _returnItem: Gen[ReturnItem] = for {
    expr <- _expression
    variable <- _variable
    item <- oneOf(
      UnaliasedReturnItem(expr, "")(?),
      AliasedReturnItem(expr, variable)(?)
    )
  } yield item

  def _sortItem: Gen[SortItem] = for {
    expr <- _expression
    item <- oneOf(
      AscSortItem(expr)(?),
      DescSortItem(expr)(?)
    )
  } yield item

  def _orderBy: Gen[OrderBy] = for {
    items <- oneOrMore(_sortItem)
  } yield OrderBy(items)(?)

  def _skip: Gen[Skip] =
    _expression.map(Skip(_)(?))

  def _limit: Gen[Limit] =
    _expression.map(Limit(_)(?))

  def _where: Gen[Where] =
    _expression.map(Where(_)(?))

  def _returnItems1: Gen[ReturnItems] = for {
    retItems <- oneOrMore(_returnItem)
  } yield ReturnItems(includeExisting = false, retItems)(?)

  def _returnItems2: Gen[ReturnItems] = for {
    retItems <- zeroOrMore(_returnItem)
  } yield ReturnItems(includeExisting = true, retItems)(?)

  def _returnItems: Gen[ReturnItems] =
    oneOf(_returnItems1, _returnItems2)

  def _with: Gen[With] = for {
    distinct <- boolean
    inclExisting <- boolean
    retItems <- oneOrMore(_returnItem)
    orderBy <- option(_orderBy)
    skip <- option(_skip)
    limit <- option(_limit)
    where <- option(_where)
  } yield With(distinct, ReturnItems(inclExisting, retItems)(?), orderBy, skip, limit, where)(?)

  def _return: Gen[Return] = for {
    distinct <- boolean
    inclExisting <- boolean
    retItems <- oneOrMore(_returnItem)
    orderBy <- option(_orderBy)
    skip <- option(_skip)
    limit <- option(_limit)
  } yield Return(distinct, ReturnItems(inclExisting, retItems)(?), orderBy, skip, limit)(?)

  def _match: Gen[Match] = for {
    optional <- boolean
    pattern <- _pattern
    hints <- zeroOrMore(_hint)
    where <- option(_where)
  } yield Match(optional, pattern, hints, where)(?)

  def _create: Gen[Create] = for {
    pattern <- _pattern
  } yield Create(pattern)(?)

  def _createUnique: Gen[CreateUnique] = for {
    pattern <- _pattern
  } yield CreateUnique(pattern)(?)

  def _unwind: Gen[Unwind] = for {
    expression <- _expression
    variable <- _variable
  } yield Unwind(expression, variable)(?)

  def _setItem: Gen[SetItem] = for {
    variable <- _variable
    labels <- oneOrMore(_labelName)
    property <- _property
    expression <- _expression
    item <- oneOf(
      SetLabelItem(variable, labels)(?),
      SetPropertyItem(property, expression)(?),
      SetExactPropertiesFromMapItem(variable, expression)(?),
      SetIncludingPropertiesFromMapItem(variable, expression)(?)
    )
  } yield item

  def _set: Gen[SetClause] = for {
    items <- oneOrMore(_setItem)
  } yield SetClause(items)(?)

  def _delete: Gen[Delete] = for {
    expressions <- oneOrMore(_expression)
    forced <- boolean
  } yield Delete(expressions, forced)(?)


  def _mergeAction: Gen[MergeAction] = for {
    set <- _set
    action <- oneOf(
      OnCreate(set)(?),
      OnMatch(set)(?)
    )
  } yield action

  def _merge: Gen[Merge] = for {
    pattern <- _patternSingle
    actions <- oneOrMore(_mergeAction)
  } yield Merge(pattern, actions)(?)

  def _procedureName: Gen[ProcedureName] = for {
    name <- _identifier
  } yield ProcedureName(name)(?)

  def _procedureOutput: Gen[ProcedureOutput] = for {
    name <- _identifier
  } yield ProcedureOutput(name)(?)

  def _procedureResultItem: Gen[ProcedureResultItem] = for {
    output <- option(_procedureOutput)
    variable <- _variable
  } yield ProcedureResultItem(output, variable)(?)

  def _procedureResult: Gen[ProcedureResult] = for {
    items <- oneOrMore(_procedureResultItem)
    where <- option(_where)
  } yield ProcedureResult(items.toIndexedSeq, where)(?)

  def _call: Gen[UnresolvedCall] = for {
    procedureNamespace <- _namespace
    procedureName <- _procedureName
    declaredArguments <- option(zeroOrMore(_expression))
    declaredResult <- option(_procedureResult)
  } yield UnresolvedCall(procedureNamespace, procedureName, declaredArguments, declaredResult)(?)

  def _foreach: Gen[Foreach] = for {
    variable <- _variable
    expression <- _expression
    updates <- oneOrMore(_clause)
  } yield Foreach(variable, expression, updates)(?)

  def _loadCsv: Gen[LoadCSV] = for {
    withHeaders <- boolean
    urlString <- _expression
    variable <- _variable
    fieldTerminator <- option(_stringLit)
  } yield LoadCSV(withHeaders, urlString, variable, fieldTerminator)(?)

  def _startItem: Gen[StartItem] = for {
    variable <- _variable
    parameter <- _parameter
    ids <- oneOrMore(_unsignedDecIntLit)
    item <- oneOf(
      NodeByParameter(variable, parameter)(?),
      AllNodes(variable)(?),
      NodeByIds(variable, ids)(?),
      RelationshipByIds(variable, ids)(?),
      RelationshipByParameter(variable, parameter)(?),
      AllRelationships(variable)(?)
    )
  } yield item

  def _start: Gen[Start] = for {
    items <- oneOrMore(_startItem)
    where <- option(_where)
  } yield Start(items, where)(?)

  // Hints
  // ----------------------------------

  def _usingIndexHint: Gen[UsingIndexHint] = for {
    variable <- _variable
    label <- _labelName
    properties <- oneOrMore(_propertyKeyName)
    spec <- oneOf(SeekOnly, SeekOrScan)
  } yield UsingIndexHint(variable, label, properties, spec)(?)

  def _usingJoinHint: Gen[UsingJoinHint] = for {
    variables <- oneOrMore(_variable)
  } yield UsingJoinHint(variables)(?)

  def _usingScanHint: Gen[UsingScanHint] = for {
    variable <- _variable
    label <- _labelName
  } yield UsingScanHint(variable, label)(?)

  def _hint: Gen[UsingHint] = oneOf(
    _usingIndexHint,
    _usingJoinHint,
    _usingScanHint
  )

  def _clause: Gen[Clause] = oneOf(
    lzy(_with),
    lzy(_return),
    lzy(_match),
    lzy(_create),
    lzy(_createUnique),
    lzy(_unwind),
    lzy(_set),
    lzy(_delete),
    lzy(_merge),
    lzy(_call),
    lzy(_foreach),
    lzy(_loadCsv),
    lzy(_start)
  )

  def _singleQuery: Gen[SingleQuery] = for {
    s <- choose(1, 1)
    clauses <- listOfN(s, _clause)
  } yield SingleQuery(clauses)(?)

  def _union: Gen[Union] = for {
    part <- _queryPart
    single <- _singleQuery
    union <- oneOf(
      UnionDistinct(part, single)(?),
      UnionAll(part, single)(?)
    )
  } yield union

  def _queryPart: Gen[QueryPart] = frequency(
    5 -> lzy(_singleQuery),
    1 -> lzy(_union)
  )

  def _regularQuery: Gen[Query] = for {
    part <- _queryPart
  } yield Query(None, part)(?)

  def _periodicCommitHint: Gen[PeriodicCommitHint] = for {
    size <- option(_signedIntLit)
  } yield PeriodicCommitHint(size)(?)

  def _bulkImportQuery: Gen[Query] = for {
    periodicCommitHint <- option(_periodicCommitHint)
    load <- _loadCsv
  } yield Query(periodicCommitHint, SingleQuery(Seq(load))(?))(?)

  def _query: Gen[Query] = frequency(
    10 -> _regularQuery,
    1 -> _bulkImportQuery
  )

  object Shrinker {

    import scala.util.Random

    implicit val IntAddMonoid: Monoid[Int] = Monoid.create(0)(_ + _)

    def shrinkOnce(q: Query): Option[Query] = {
      var splitPoints = 0
      q.rewritten.bottomUp {
        case l: List[_] if l.size > 1    =>
          splitPoints += 1
          l
        case o: Option[_] if o.isDefined =>
          splitPoints += 1
          o
      }
      if (splitPoints == 0) {
        None
      } else {
        var point = Random.nextInt(splitPoints)

        def onPoint[T, R >: T](i: T)(f: => R): R = if (point == 0) {
          point -= 1
          f
        } else {
          point -= 1
          i
        }

        Some(
          q.rewritten.bottomUp {
            case l: List[_] if l.size > 1    => onPoint(l)(List(l.head))
            case o: Option[_] if o.isDefined => onPoint(o)(Option.empty)
          })
      }
    }

    implicit val shrinkQuery: Shrink[Query] = Shrink[Query] { q =>
      Stream.iterate(shrinkOnce(q))(i => i.flatMap(shrinkOnce))
        .takeWhile(_.isDefined)
        .map(_.get)
        .take(100)
    }
  }

}
