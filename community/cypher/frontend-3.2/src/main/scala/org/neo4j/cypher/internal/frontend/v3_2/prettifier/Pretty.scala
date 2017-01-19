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
package org.neo4j.cypher.internal.frontend.v3_2.prettifier

import org.bitbucket.inkytonik.kiama.output.PrettyPrinter
import org.bitbucket.inkytonik.kiama.output.PrettyPrinterTypes.Width
import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.neo4j.cypher.internal.frontend.v3_2.parser.CypherParser
import org.neo4j.cypher.internal.frontend.v3_2.{SemanticDirection, ast}

case class Pretty(functionNameNormaliser: String => String = _.toUpperCase) extends PrettyPrinter {

  private val parser = new CypherParser

  override val defaultIndent: PPosition = 2

  def reformat(query: String, width: Width = defaultWidth): String = {
    val ast: Statement = parser.parse(query)
    super.pretty(show(ast), width).layout
  }

  private def show(name: SymbolicName): Doc = name match {
    case LabelName(label) => colon <> label
    case RelTypeName(label) => colon <> label
    case PropertyKeyName(prop) => prop
  }

  def show(t: Statement): Doc = t match {
    case Query(_, part) =>
      show(part)

    case CreateIndex(l, p) =>
      "CREATE INDEX ON" <+> group(show(l) <> parens(show(p)))

    case CreateUniquePropertyConstraint(v, l, p) =>
      "CREATE CONSTRAINT ON" <+> parens(expr(v) <> show(l)) <+> group("ASSERT" <+> expr(p) <+> "IS UNIQUE")

    case DropIndex(l, p) =>
      "DROP INDEX ON" <+> group(show(l) <> parens(show(p)))

    case DropUniquePropertyConstraint(v, l, p) =>
      "DROP CONSTRAINT ON" <+> parens(expr(v) <> show(l)) <+> "ASSERT" <+> expr(p) <+> "IS UNIQUE"
  }

  private def show(t: QueryPart): Doc = t match {
    case SingleQuery(clauses) =>
      vlist(clauses.map(show))

    case UnionDistinct(p, q) =>
      show(p) <@> "UNION DISTINCT" <@> show(q)

    case UnionAll(p, q) =>
      show(p) <@> "UNION" <@> show(q)
  }

  private def ifTrue(opt: Boolean, f: Doc) =
    if (opt) f else emptyDoc

  private def show(action: MergeAction): Doc = action match {
    case OnCreate(setClause) => "ON CREATE" <+> show(setClause)
    case OnMatch(setClause) => "ON MATCH" <+> show(setClause)
  }

  private def showWhere(w: Option[Where]) =
    maybe(w, (w: Where) => nest(line <> "WHERE" <+> nest(group(expr(w.expression)))))

  private def show(p: Pattern): Doc = {
    val paths = p.patternParts.map(show).map(group).toList
    nest(group(softline <> folddoc(paths, _ <> comma <> softline <> _)))
  }

  private def show(astNode: Clause): Doc = astNode match {
    case Create(pattern) =>
      "CREATE" <+> show(pattern)

    case CreateUnique(pattern) =>
      "CREATE UNIQUE" <+> show(pattern)

    case Delete(expressions, detach) =>
      val DETACH: Doc = if (detach) "DETACH" <> space else emptyDoc
      DETACH <> "DELETE" <+> hlist(expressions.map(expr), comma)

    case Foreach(variable, collExp, updates) =>
      group("FOREACH" <+> lparen <> expr(variable) <+> "IN" <+> expr(collExp) <+> "|" <> group(nest(line <> vlist(updates.map(show)))) <@@> rparen)

    case Match(optional, pattern, hints, where) =>
      val OPTIONAL: Doc = ifTrue(optional, "OPTIONAL" <> line)
      val HINTS = ifTrue(hints.nonEmpty, line <> vlist(hints.map(show)))
      group(OPTIONAL <> "MATCH") <> show(pattern) <> HINTS <> showWhere(where)

    case LoadCSV(withHeaders, url, variable, fieldTerminator) =>
      val HEADERS: Doc = if (withHeaders) space <> "WITH HEADERS" else emptyDoc
      val TERMINATOR: Doc = maybe(fieldTerminator, space <> "FIELDTERMINATOR" <+> expr(_: Expression))
      "LOAD CSV" <> HEADERS <+> "FROM" <+> expr(url) <+> "AS" <+> expr(variable) <> TERMINATOR

    case Merge(pattern, actions) =>
      val mergeActions = if (actions.isEmpty)
        emptyDoc else
        nest(line <> vlist(actions.map(show)))
      "MERGE" <+> show(pattern) <> mergeActions

    case PragmaWithout(vars) =>
      "_PRAGMA WITHOUT" <> hlist(vars.map(expr))

    case With(distinct, items, orderBy, skip, limit, where) =>
      "WITH" <> projection(distinct, items, orderBy, skip, limit) <> showWhere(where)

    case Return(distinct, items, orderBy, skip, limit, _) =>
      "RETURN" <> projection(distinct, items, orderBy, skip, limit)

    case Remove(rs) =>
      "REMOVE" <+> vlist(rs.map(show), comma)

    case SetClause(items) =>
      val setItems = items.map(show)
      "SET" <+> hlist(setItems, comma)

    case ast.Start(items, where) =>
      "START" <+> vlist(items.map(startItem), comma) <> showWhere(where)

    case UnresolvedCall(ns, name, args, pr) =>
      val YIELD = maybe(pr, (result: ProcedureResult) =>
        space <> "YIELD" <+> vlist(result.items.map(show), comma)
      )
      val ARGS = maybe(args, (es: Seq[Expression]) => space <> parens(vlist(es.map(expr), comma <> linebreak)))
      val NAME = name.name
      val FQN = fqn(ns, name.name)

      "CALL" <+> FQN <> ARGS <> YIELD

    case Unwind(e, v) =>
      "UNWIND" <+> expr(e) <+> "AS" <+> expr(v)
  }

  private def show(i: SetItem): Doc = i match {
    case SetPropertyItem(p, e) =>
      showbin(p, "=", e)
    case SetLabelItem(v, ls) =>
      expr(v) <> folddoc(ls.map(show).toList, _ <> _)
    case SetExactPropertiesFromMapItem(v, e) =>
      showbin(v, "=", e)
    case SetIncludingPropertiesFromMapItem(v, e) =>
      showbin(v, "=", e)
  }

  private def fqn(ns: Namespace, name: String) = {
    val elements = ns.parts.map(string) :+ string(name)
    folddoc(elements, _ <> dot <> _)
  }

  private def show(items: ReturnItems): Doc = {
    val itemsDocs = items.items.map {
      case UnaliasedReturnItem(e, t) => expr(e)
      case AliasedReturnItem(e, alias) => expr(e) <+> "AS" <+> expr(alias)
    }

    val returnItems = if (items.includeExisting) asterisk +: itemsDocs else itemsDocs
    nest(group(fillsep(returnItems.toList, comma)))
  }

  private def projection(distinct: Boolean,
                         items: ReturnItems,
                         orderBy: Option[OrderBy],
                         skip: Option[Skip],
                         limit: Option[Limit]): Doc = {
    val DISTINCT: Doc = if (distinct) space <> "DISTINCT" else emptyDoc
    val ORDER_BY: Doc = maybe(orderBy, space <> order(_: OrderBy))
    DISTINCT <+> show(items) <> ORDER_BY
  }

  private def curlies(d: Doc) = enclose("{", d, "}")

  private def vlist(docs: Traversable[Doc], sep: Doc = emptyDoc): Doc = vsep(docs.toList, sep)

  private def hlist(docs: Traversable[Doc], sep: Doc = emptyDoc): Doc = hsep(docs.toList, sep)

  private def expr(expression: Expression): Doc = expression match {
    case Parenthesis(inner) => parens(expr(inner))
    case MapExpression(elements) if elements.isEmpty => "{}"
    case MapProjection(v, items, _) =>
      val firstLine = if (items.size == 1) emptyDoc else line
      expr(v) <> curlies(nest(firstLine <> vlist(items.map(show), comma)))
    case ListLiteral(elements) if elements.isEmpty => "[]"
    case ListLiteral(elements) => brackets(hlist(elements.map(expr), comma))
    case Parameter(name, _) => "$" <> string(name)
    case HasLabels(e, ls) => expr(e) <> vlist(ls.map(show))

    case StringLiteral(lit) =>
      val sep = if (lit.indexOf('\'') >= 0) "\"" else "'"
      sep <> lit <> sep

    case lit: Literal => value(lit.value)
    case Variable(name) => if (isValidIdentifier(name)) name else surround(name, "`")
    case Property(e, prop) => expr(e) <> dot <> show(prop)
    case PatternExpression(pattern) => show(pattern.element)
    case CountStar() => "COUNT(*)"
    case Not(e) => "NOT" <+> expr(e)
    case ContainerIndex(lst, exp) => expr(lst) <> brackets(expr(exp))

    case AllIterablePredicate(FilterScope(v, pred), e) =>
      "ALL" <> parens(expr(v) <+> "IN" <+> expr(e) <+> "WHERE" <+> expr(pred.get))

    case AnyIterablePredicate(FilterScope(v, pred), e) =>
      "ANY" <> parens(expr(v) <+> "IN" <+> expr(e) <+> "WHERE" <+> expr(pred.get))

    case SingleIterablePredicate(FilterScope(v, pred), e) =>
      "SINGLE" <> parens(expr(v) <+> "IN" <+> expr(e) <+> "WHERE" <+> expr(pred.get))

    case IsNull(e) =>
      expr(e) <+> "IS NULL"

    case IsNotNull(e) =>
      expr(e) <+> "IS NOT NULL"

    case Ands(predicates) =>
      folddoc(predicates.map(expr).toList, _ <+> "AND" <+> _)

    case CaseExpression(e, alts, default) =>
      val EXPRESSION = maybe(e, expr)
      val DEFAULT = maybe(default, space <> "ELSE" <+> expr(_: Expression))
      val ALTERNATIVES = alts.map {
        case (a, b) => "WHEN" <+> expr(a) <+> "THEN" <+> expr(b)
      }
      "CASE" <+> EXPRESSION <+> vlist(ALTERNATIVES) <> DEFAULT <+> "END"

    case ReduceExpression(ReduceScope(accumulator, variable, e), init, list) =>
      val INIT = expr(accumulator) <+> equal <+> expr(init)
      val IN = expr(variable) <+> "IN" <+> expr(list)
      "REDUCE" <> parens(INIT <> comma <+> IN <+> "|" <+> expr(e))

    case NoneIterablePredicate(FilterScope(v, pred), e) =>
      "NONE" <> parens(expr(v) <+> "IN" <+> expr(e) <+> "WHERE" <+> expr(pred.get))

    case FilterExpression(FilterScope(v, pred), e) =>
      "FILTER" <> parens(expr(v) <+> "IN" <+> expr(e) <+> "WHERE" <+> expr(pred.get))

    case ExtractExpression(ExtractScope(variable, pred, extract), e) =>
      val PRED = maybe(pred, (p: Expression) => space <> "WHERE" <+> expr(p))
      val PROJ = maybe(extract, (e: Expression) => space <> "|" <+> expr(e))
      val IN = expr(variable) <+> "IN" <+> expr(e)
      "EXTRACT" <> parens(IN <> PRED <> PROJ)

    case o: BinaryOperatorExpression =>
      val op: String = o match {
        case _: Add => "+"
        case _: And => "AND"
        case _: Contains => "CONTAINS"
        case _: Divide => "/"
        case _: EndsWith => "ENDS WITH"
        case _: Equals => "="
        case _: GreaterThan => ">"
        case _: GreaterThanOrEqual => ">="
        case _: In => "IN"
        case _: InvalidNotEquals => "!="
        case _: LessThan => "<"
        case _: LessThanOrEqual => "<="
        case _: Modulo => "%"
        case _: Multiply => "*"
        case _: NotEquals => "<>"
        case _: Or => "OR"
        case _: Pow => "^"
        case _: RegexMatch => "~="
        case _: StartsWith => "STARTS WITH"
        case _: Subtract => "-"
        case _: Xor => "XOR"
      }
      val break = op == "AND" || op == "OR" || op == "XOR"
      showbin(o.lhs, op, o.rhs, break)

    case StringLiteral(lit) =>
      val sep = if (lit.indexOf('\'') >= 0) "\"" else "'"
      sep <> lit <> sep


    case MapExpression(elements) =>
      val els = elements map {
        case (prop, e) => show(prop) <> colon <+> expr(e)
      }
      curlies(hlist(els, comma))

    case ListComprehension(ExtractScope(variable, innerPredicate, extractExpression), e) =>
      val where = maybe(innerPredicate, space <> "WHERE" <+> expr(_: Expression))
      val extract = maybe(extractExpression, space <> "|" <+> expr(_: Expression))
      brackets(expr(variable) <+> "IN" <+> expr(e) <> where <> extract)

    case FunctionInvocation(ns, funcName, _, args) =>
      val NAME = funcName.name.toUpperCase
      val QUALIFIED_NAME = fqn(ns, functionNameNormaliser(funcName.name))
      val ARGUMENTS = hlist(args.map(expr), comma)

      QUALIFIED_NAME <> parens(ARGUMENTS)
  }

  private def startItem(si: StartItem): Doc = si match {
    case NodeByIds(variable, ids) =>
      expr(variable) <+> equal <+> "node" <> parens(vlist(ids.map(expr), comma))
    case NodeByIdentifiedIndex(v, i, k, e) =>
      expr(v) <+> equal <+> "node" <> parens(k <> equal <> expr(e))
    case NodeByIndexQuery(v, i, q) =>
      expr(v) <+> equal <+> "node" <> parens(expr(q))
    case RelationshipByIdentifiedIndex(v, i, k, e) =>
      expr(v) <+> equal <+> "relationship" <> parens(k <> equal <> expr(e))
    case RelationshipByIndexQuery(v, i, q) =>
      expr(v) <+> equal <+> "relationship" <> parens(expr(q))
  }

  private def show(ri: ProcedureResultItem) = expr(ri.variable)

  private def maybe[T](v: Option[T], f: T => Doc): Doc =
    v.map(f(_)).getOrElse(emptyDoc)

  private def si(sortItem: SortItem): Doc = sortItem match {
    case AscSortItem(e) => expr(e) <+> "ASC"
    case DescSortItem(e) => expr(e) <+> "ASC"
  }

  private def order(orderBy: OrderBy): Doc = {
    val sortItems: Doc = hlist(orderBy.sortItems.map(si))
    group("ORDER BY" <+> sortItems)
  }

  private def show(v: Option[Variable]): Doc = {
    val x = v.map(expr)
    x.getOrElse(emptyDoc)
  }

  private def show(ri: RemoveItem): Doc = ri match {
    case RemoveLabelItem(v, ls) => expr(v) <> vlist(ls.map(show))
    case RemovePropertyItem(prop) => expr(prop)
  }

  private def show(mpe: MapProjectionElement): Doc = mpe match {
    case LiteralEntry(k, e) => show(k) <> colon <+> expr(e)
    case VariableSelector(id) => expr(id)
    case PropertySelector(id) => dot <> expr(id)
    case AllPropertiesSelector() => dot <> asterisk
  }

  private def show(hint: UsingHint): Doc = hint match {
    case UsingIndexHint(variable, label, property) =>
      "USING INDEX" <+> expr(variable) <> show(label) <> parens(show(property))
    case UsingJoinHint(variables) =>
      "USING JOIN ON" <+> vlist(variables.map(expr).toIndexedSeq, comma)
    case UsingScanHint(variable, label) =>
      "USING SCAN" <+> expr(variable) <> show(label)
  }

  private def show(p: PatternPart): Doc = p match {
    case EveryPath(patternElement) => show(patternElement)
    case ShortestPaths(patternElement, single) =>
      val name = if (single) "shortestPath" else "allShortestPaths"
      name <> parens(show(patternElement))
    case NamedPatternPart(pathName, patternElement) => expr(pathName) <+> equal <+> show(patternElement)
  }

  private def show(p: RelationshipPattern): Doc = {
    val (lDoc, rDoc) = p.direction match {
      case SemanticDirection.INCOMING => "<-" -> "-"
      case SemanticDirection.OUTGOING => "-" -> "->"
      case SemanticDirection.BOTH => "-" -> "-"
    }

    val relInfo = {
      val NAME = maybe(p.variable, expr)
      val LENGTH: Doc = p.length match {
        case None => emptyDoc
        case Some(None) => "*"
        case Some(Some(Range(None, None))) => "*"
        case Some(Some(Range(min, max))) =>
          val from = maybe(min, expr)
          val to = maybe(max, expr)
          if (min == max)
            "*" <> from
          else
            "*" <> from <> ".." <> to
      }
      val TYPES = if (p.types.isEmpty) emptyDoc else
        vlist(p.types.map(show))

      val PROPS = maybe(p.properties, space <> expr(_: Expression))

      if (p.variable.isEmpty && p.length.isEmpty && p.types.isEmpty)
        emptyDoc
      else
        group("[" <> NAME <> TYPES <> LENGTH <> PROPS <> "]")
    }

    group(lDoc <> relInfo <> rDoc)
  }

  private def isValidIdentifier(name: String): Boolean = {
    if (!Character.isJavaIdentifierStart(name.head))
      false
    else
      name.tail.toCharArray.forall(Character.isJavaIdentifierPart)
  }

  private def show(p: PatternElement): Doc = p match {
    case NodePattern(name, labels, props) =>
      parens(show(name) <> hlist(labels.map(show)) <> maybe(props, space <> expr(_: Expression)))

    case RelationshipChain(el, pat, node) =>
      show(el) <> show(pat) <> show(node)
  }

  private def showbin(l: Expression, op: String, r: Expression, allowBreaks: Boolean = false): Doc = {
    val SEP = if(allowBreaks) line else space
    expr(l) <> SEP <> op <> space <> expr(r)
  }

}
