/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.v4_0.ast.prettifier

import org.neo4j.cypher.internal.v4_0.ast.Union.UnionMapping
import org.neo4j.cypher.internal.v4_0.ast.{Skip, Statement, _}
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.util.InputPosition

case class Prettifier(expr: ExpressionStringifier) {

  private val NL = System.lineSeparator()
  private val INDENT = "  "

  private def indentedNewLine(l: String) = NL + INDENT + l
  private def indentedLine(l: String) = INDENT + l

  def asString(statement: Statement): String = statement match {
    case Query(maybePeriodicCommit, part) =>
      val hint = maybePeriodicCommit.map("USING PERIODIC COMMIT" + _.size.map(" " + expr(_)).getOrElse("") + NL).getOrElse("")
      val query = queryPart(part)
      s"$hint$query"

    case CreateIndex(LabelName(label), properties) =>
      s"CREATE INDEX ON :$label${properties.map(_.name).mkString("(", ", ", ")")}"

    case CreateIndexNewSyntax(variable, LabelName(label), properties, None) =>
      val propString = properties.map(p => s"${p.map.asInstanceOf[Variable].name}.${p.propertyKey.name}").mkString("(", ", ", ")")
      s"CREATE INDEX FOR (${variable.name}:$label) ON $propString"

    case CreateIndexNewSyntax(variable, LabelName(label), properties, Some(name)) =>
      val propString = properties.map(p => s"${p.map.asInstanceOf[Variable].name}.${p.propertyKey.name}").mkString("(", ", ", ")")
      s"CREATE INDEX ${Prettifier.escapeName(name)} FOR (${variable.name}:$label) ON $propString"

    case DropIndex(LabelName(label), properties) =>
      s"DROP INDEX ON :$label${properties.map(_.name).mkString("(", ", ", ")")}"

    case DropIndexOnName(name) =>
      s"DROP INDEX ${Prettifier.escapeName(name)}"

    case CreateNodeKeyConstraint(Variable(variable), LabelName(label), properties, None) =>
      s"CREATE CONSTRAINT ON ($variable:$label) ASSERT ${asString(properties)} IS NODE KEY"

    case CreateNodeKeyConstraint(Variable(variable), LabelName(label), properties, Some(name)) =>
      s"CREATE CONSTRAINT ${Prettifier.escapeName(name)} ON ($variable:$label) ASSERT ${asString(properties)} IS NODE KEY"

    case DropNodeKeyConstraint(Variable(variable), LabelName(label), properties) =>
      s"DROP CONSTRAINT ON ($variable:$label) ASSERT ${properties.map(_.asCanonicalStringVal).mkString("(", ", ", ")")} IS NODE KEY"

    case CreateUniquePropertyConstraint(Variable(variable), LabelName(label), properties, None) =>
      s"CREATE CONSTRAINT ON ($variable:$label) ASSERT ${properties.map(_.asCanonicalStringVal).mkString("(", ", ", ")")} IS UNIQUE"

    case CreateUniquePropertyConstraint(Variable(variable), LabelName(label), properties, Some(name)) =>
      s"CREATE CONSTRAINT ${Prettifier.escapeName(name)} ON ($variable:$label) ASSERT ${properties.map(_.asCanonicalStringVal).mkString("(", ", ", ")")} IS UNIQUE"

    case DropUniquePropertyConstraint(Variable(variable), LabelName(label), properties) =>
      s"DROP CONSTRAINT ON ($variable:$label) ASSERT ${properties.map(_.asCanonicalStringVal).mkString("(", ", ", ")")} IS UNIQUE"

    case CreateNodePropertyExistenceConstraint(Variable(variable), LabelName(label), property, None) =>
      s"CREATE CONSTRAINT ON ($variable:$label) ASSERT exists(${property.asCanonicalStringVal})"

    case CreateNodePropertyExistenceConstraint(Variable(variable), LabelName(label), property, Some(name)) =>
      s"CREATE CONSTRAINT ${Prettifier.escapeName(name)} ON ($variable:$label) ASSERT exists(${property.asCanonicalStringVal})"

    case DropNodePropertyExistenceConstraint(Variable(variable), LabelName(label), property) =>
      s"DROP CONSTRAINT ON ($variable:$label) ASSERT exists(${property.asCanonicalStringVal})"

    case CreateRelationshipPropertyExistenceConstraint(Variable(variable), RelTypeName(relType), property, None) =>
      s"CREATE CONSTRAINT ON ()-[$variable:$relType]-() ASSERT exists(${property.asCanonicalStringVal})"

    case CreateRelationshipPropertyExistenceConstraint(Variable(variable), RelTypeName(relType), property, Some(name)) =>
      s"CREATE CONSTRAINT ${Prettifier.escapeName(name)} ON ()-[$variable:$relType]-() ASSERT exists(${property.asCanonicalStringVal})"

    case DropRelationshipPropertyExistenceConstraint(Variable(variable), RelTypeName(relType), property) =>
      s"DROP CONSTRAINT ON ()-[$variable:$relType]-() ASSERT exists(${property.asCanonicalStringVal})"

    case DropConstraintOnName(name) =>
      s"DROP CONSTRAINT ${Prettifier.escapeName(name)}"

    case x: ShowUsers =>
      s"${x.name}"

    case x @ CreateUser(userName, _, initialParameterPassword, requirePasswordChange, suspended, ifExistsDo) =>
      val userNameString = Prettifier.escapeName(userName)
      val ifNotExists = ifExistsDo match {
        case _: IfExistsDoNothing => " IF NOT EXISTS"
        case _ => ""
      }
      val password = if (initialParameterPassword.isDefined)
        s"$$${initialParameterPassword.get.name}"
      else "'******'"
      val passwordString = s"SET PASSWORD $password CHANGE ${if (!requirePasswordChange) "NOT " else ""}REQUIRED"
      val statusString = if(suspended.isDefined) s" SET STATUS ${if (suspended.get) "SUSPENDED" else "ACTIVE"}"
                         else ""
      s"${x.name} $userNameString$ifNotExists $passwordString$statusString"

    case x @ DropUser(userName, ifExists) =>
      if (ifExists) s"${x.name} ${Prettifier.escapeName(userName)} IF EXISTS"
      else s"${x.name} ${Prettifier.escapeName(userName)}"

    case x @ AlterUser(userName, initialStringPassword, initialParameterPassword, requirePasswordChange, suspended) =>
      val userNameString = Prettifier.escapeName(userName)
      val passwordString = if (initialStringPassword.isDefined) s" '******'"
      else if (initialParameterPassword.isDefined)
        s" $$${initialParameterPassword.get.name}"
      else ""
      val passwordModeString = if (requirePasswordChange.isDefined)
        s" CHANGE ${if (!requirePasswordChange.get) "NOT " else ""}REQUIRED"
      else
        ""
      val passwordPrefix = if (passwordString.nonEmpty || passwordModeString.nonEmpty) " SET PASSWORD" else ""
      val statusString = if (suspended.isDefined) s" SET STATUS ${if (suspended.get) "SUSPENDED" else "ACTIVE"}" else ""
      s"${x.name} $userNameString$passwordPrefix$passwordString$passwordModeString$statusString"

    case x @ SetOwnPassword(_, newParameterPassword, _, currentParameterPassword) =>
      val newPassword = if (newParameterPassword.isDefined)
        s"$$${newParameterPassword.get.name}"
      else "'******'"
      val currentPassword = if (currentParameterPassword.isDefined)
        s"$$${currentParameterPassword.get.name}"
      else "'******'"
      s"${x.name} FROM $currentPassword TO $newPassword"

    case x @ ShowRoles(withUsers, _) =>
      s"${x.name}${if (withUsers) " WITH USERS" else ""}"

    case x @ CreateRole(roleName, None, ifExistsDo) =>
      ifExistsDo match {
        case _: IfExistsDoNothing => s"${x.name} ${Prettifier.escapeName(roleName)} IF NOT EXISTS"
        case _ => s"${x.name} ${Prettifier.escapeName(roleName)}"
      }

    case x @ CreateRole(roleName, Some(fromRole), ifExistsDo) =>
      ifExistsDo match {
        case _: IfExistsDoNothing => s"${x.name} ${Prettifier.escapeName(roleName)} IF NOT EXISTS AS COPY OF ${Prettifier.escapeName(fromRole)}"
        case _ => s"${x.name} ${Prettifier.escapeName(roleName)} AS COPY OF ${Prettifier.escapeName(fromRole)}"
      }

    case x @ DropRole(roleName, ifExists) =>
      if (ifExists) s"${x.name} ${Prettifier.escapeName(roleName)} IF EXISTS"
      else s"${x.name} ${Prettifier.escapeName(roleName)}"

    case x @ GrantRolesToUsers(roleNames, userNames) if roleNames.length > 1 =>
      s"${x.name}S ${roleNames.map(Prettifier.escapeName).mkString(", " )} TO ${userNames.map(Prettifier.escapeName).mkString(", ")}"

    case x @ GrantRolesToUsers(roleNames, userNames) =>
      s"${x.name} ${roleNames.map(Prettifier.escapeName).mkString(", " )} TO ${userNames.map(Prettifier.escapeName).mkString(", ")}"

    case x @ RevokeRolesFromUsers(roleNames, userNames) if roleNames.length > 1 =>
      s"${x.name}S ${roleNames.map(Prettifier.escapeName).mkString(", " )} FROM ${userNames.map(Prettifier.escapeName).mkString(", ")}"

    case x @ RevokeRolesFromUsers(roleNames, userNames) =>
      s"${x.name} ${roleNames.map(Prettifier.escapeName).mkString(", " )} FROM ${userNames.map(Prettifier.escapeName).mkString(", ")}"

    case x @ GrantPrivilege(DbmsPrivilege(_), _, _, _, roleNames) =>
      s"${x.name} ON DBMS TO ${Prettifier.escapeNames(roleNames)}"

    case x @ DenyPrivilege(DbmsPrivilege(_), _, _, _, roleNames) =>
      s"${x.name} ON DBMS TO ${Prettifier.escapeNames(roleNames)}"

    case x @ RevokePrivilege(DbmsPrivilege(_), _, _, _, roleNames, _) =>
      s"${x.name} ON DBMS FROM ${Prettifier.escapeNames(roleNames)}"

    case x @ GrantPrivilege(DatabasePrivilege(_), _, dbScope, _, roleNames) =>
      val dbName = Prettifier.extractDbScope(dbScope)
      s"${x.name} ON DATABASE $dbName TO ${Prettifier.escapeNames(roleNames)}"

    case x @ DenyPrivilege(DatabasePrivilege(_), _, dbScope, _, roleNames) =>
      val dbName = Prettifier.extractDbScope(dbScope)
      s"${x.name} ON DATABASE $dbName TO ${Prettifier.escapeNames(roleNames)}"

    case x @ RevokePrivilege(DatabasePrivilege(_), _, dbScope, _, roleNames, _) =>
      val dbName = Prettifier.extractDbScope(dbScope)
      s"${x.name} ON DATABASE $dbName FROM ${Prettifier.escapeNames(roleNames)}"

    case x @ GrantPrivilege(TraversePrivilege(), _, dbScope, qualifier, roleNames) =>
      val (dbName, segment) = Prettifier.extractScope(dbScope, qualifier)
      s"${x.name} ON GRAPH $dbName $segment (*) TO ${Prettifier.escapeNames(roleNames)}"

    case x @ DenyPrivilege(TraversePrivilege(), _, dbScope, qualifier, roleNames) =>
      val (dbName, segment) = Prettifier.extractScope(dbScope, qualifier)
      s"${x.name} ON GRAPH $dbName $segment (*) TO ${Prettifier.escapeNames(roleNames)}"

    case x @ RevokePrivilege(TraversePrivilege(), _, dbScope, qualifier, roleNames, _) =>
      val (dbName, segment) = Prettifier.extractScope(dbScope, qualifier)
      s"${x.name} ON GRAPH $dbName $segment (*) FROM ${Prettifier.escapeNames(roleNames)}"

    case x @ GrantPrivilege(WritePrivilege(), _, dbScope, qualifier, roleNames) =>
      val (dbName, segment) = Prettifier.extractScope(dbScope, qualifier)
      s"${x.name} ON GRAPH $dbName $segment (*) TO ${Prettifier.escapeNames(roleNames)}"

    case x @ DenyPrivilege(WritePrivilege(), _, dbScope, qualifier, roleNames) =>
      val (dbName, segment) = Prettifier.extractScope(dbScope, qualifier)
      s"${x.name} ON GRAPH $dbName $segment (*) TO ${Prettifier.escapeNames(roleNames)}"

    case x @ RevokePrivilege(WritePrivilege(), _, dbScope, qualifier, roleNames, _) =>
      val (dbName, segment) = Prettifier.extractScope(dbScope, qualifier)
      s"${x.name} ON GRAPH $dbName $segment (*) FROM ${Prettifier.escapeNames(roleNames)}"

    case x @ GrantPrivilege(_, resource, dbScope, qualifier, roleNames) =>
      val (resourceName, dbName, segment) = Prettifier.extractScope(resource, dbScope, qualifier)
      s"${x.name} {$resourceName} ON GRAPH $dbName $segment (*) TO ${Prettifier.escapeNames(roleNames)}"

    case x @ DenyPrivilege(_, resource, dbScope, qualifier, roleNames) =>
      val (resourceName, dbName, segment) = Prettifier.extractScope(resource, dbScope, qualifier)
      s"${x.name} {$resourceName} ON GRAPH $dbName $segment (*) TO ${Prettifier.escapeNames(roleNames)}"

    case x @ RevokePrivilege(_, resource, dbScope, qualifier, roleNames, _) =>
      val (resourceName, dbName, segment) = Prettifier.extractScope(resource, dbScope, qualifier)
      s"${x.name} {$resourceName} ON GRAPH $dbName $segment (*) FROM ${Prettifier.escapeNames(roleNames)}"

    case x @ ShowPrivileges(scope) =>
      s"SHOW ${Prettifier.extractScope(scope)} PRIVILEGES"

    case x: ShowDatabases =>
      s"${x.name}"

    case x: ShowDefaultDatabase =>
      s"${x.name}"

    case x @ ShowDatabase(dbName) =>
      s"${x.name} ${Prettifier.escapeName(dbName)}"

    case x @ CreateDatabase(dbName, ifExistsDo) =>
      ifExistsDo match {
        case _: IfExistsDoNothing => s"${x.name} ${Prettifier.escapeName(dbName)} IF NOT EXISTS"
        case _ => s"${x.name} ${Prettifier.escapeName(dbName)}"
      }

    case x @ DropDatabase(dbName, ifExists) =>
      if (ifExists) s"${x.name} ${Prettifier.escapeName(dbName)} IF EXISTS"
      else s"${x.name} ${Prettifier.escapeName(dbName)}"

    case x @ StartDatabase(dbName) =>
      s"${x.name} ${Prettifier.escapeName(dbName)}"

    case x @ StopDatabase(dbName) =>
      s"${x.name} ${Prettifier.escapeName(dbName)}"

    case x @ CreateGraph(catalogName, query) =>
      val graphName = catalogName.parts.mkString(".")
      s"${x.name} $graphName {$NL${queryPart(query)}$NL}"

    case x @ DropGraph(catalogName) =>
      val graphName = catalogName.parts.mkString(".")
      s"${x.name} $graphName"

    case x @ CreateView(catalogName, params, query, innerQuery) =>
      val graphName = catalogName.parts.mkString(".")
      val paramString = params.map(p => "$" + p.name).mkString("(", ", ", ")")
      s"CATALOG CREATE VIEW $graphName$paramString {$NL${queryPart(query)}$NL}"

    case x @ DropView(catalogName) =>
      val graphName = catalogName.parts.mkString(".")
      s"CATALOG DROP VIEW $graphName"
  }

  private def queryPart(part: QueryPart): String =
    part match {
      case SingleQuery(clauses) =>
        clauses.map(dispatch).mkString(NL)

      case UnionAll(partA, partB) =>
        s"${queryPart(partA)}${NL}UNION ALL$NL${queryPart(partB)}"

      case UnionDistinct(partA, partB) =>
        s"${queryPart(partA)}${NL}UNION$NL${queryPart(partB)}"

      case ProjectingUnionAll(partA, partB, mappings) =>
        s"${queryPart(partA)}${NL}UNION ALL mappings: (${mappings.map(asString).mkString(", ")})$NL${queryPart(partB)}"

      case ProjectingUnionDistinct(partA, partB, mappings) =>
        s"${queryPart(partA)}${NL}UNION mappings: (${mappings.map(asString).mkString(", ")})$NL${queryPart(partB)}"
    }

  private def asString(u: UnionMapping): String = {
    s"${u.unionVariable.name}: [${u.variableInPart.name}, ${u.variableInQuery.name}]"
  }

  private def dispatch(clause: Clause) = clause match {
    case e: Return => asString(e)
    case m: Match => asString(m)
    case c: SubQuery => asString(c)
    case w: With => asString(w)
    case c: Create => asString(c)
    case u: Unwind => asString(u)
    case u: UnresolvedCall => asString(u)
    case s: SetClause => asString(s)
    case r: Remove => asString(r)
    case d: Delete => asString(d)
    case m: Merge => asString(m)
    case l: LoadCSV => asString(l)
    case f: Foreach => asString(f)
    case s: Start => asString(s)
    case _ => clause.asCanonicalStringVal // TODO
  }

  def asString(m: Match): String = {
    val o = if(m.optional) "OPTIONAL " else ""
    val p = expr.patterns.apply(m.pattern)
    val w = m.where.map(asString).map(indentedNewLine).getOrElse("")
    val h = m.hints.map(asString).map(indentedNewLine).mkString
    s"${o}MATCH $p$h$w"
  }

  def asString(c: SubQuery): String = {
    val q = queryPart(c.part)
    val qWithIndent = q.split(NL).map(indentedLine).mkString(NL)
    s"""CALL {
       |$qWithIndent
       |}""".stripMargin
  }

  private def asString(w: Where): String =
    "WHERE " + expr(w.expression)

  private def asString(m: UsingHint): String = {
    m match {
      case UsingIndexHint(v, l, ps, s) => Seq(
        "USING INDEX ", if (s == SeekOnly) "SEEK " else "",
        expr(v), ":", expr(l),
        ps.map(expr(_)).mkString("(", ",", ")")
      ).mkString

      case UsingScanHint(v, l) => Seq(
        "USING SCAN ", expr(v), ":", expr(l)
      ).mkString

      case UsingJoinHint(vs) => Seq(
        "USING JOIN ON ", vs.map(expr(_)).toIterable.mkString(", ")
      ).mkString
    }
  }

  private def asString(ma: MergeAction): String = ma match {
    case OnMatch(set) => s"ON MATCH ${asString(set)}"
    case OnCreate(set) => s"ON CREATE ${asString(set)}"
  }

  private def asString(m: Merge): String = {
    val p = expr.patterns.apply(m.pattern)
    val a = m.actions.map(asString).map(indentedNewLine).mkString
    s"MERGE $p$a"
  }

  private def asString(o: Skip): String = "SKIP " + expr(o.expression)
  private def asString(o: Limit): String = "LIMIT " + expr(o.expression)

  private def asString(o: OrderBy): String = "ORDER BY " + {
    o.sortItems.map {
      case AscSortItem(expression) => expr(expression) + " ASCENDING"
      case DescSortItem(expression) => expr(expression) + " DESCENDING"
    }.mkString(", ")
  }

  private def asString(r: ReturnItem): String = r match {
    case AliasedReturnItem(e, v) => expr(e) + " AS " + expr(v)
    case UnaliasedReturnItem(e, _) => expr(e)
  }

  private def asString(r: ReturnItemsDef): String = {
    val as = if (r.includeExisting) Seq("*") else Seq()
    val is = r.items.map(asString)
    (as ++ is).mkString(", ")
  }

  private def asString(r: Return): String = {
    val d = if (r.distinct) " DISTINCT" else ""
    val i = asString(r.returnItems)
    val o = r.orderBy.map(asString).map(indentedNewLine).getOrElse("")
    val l = r.limit.map(asString).map(indentedNewLine).getOrElse("")
    val s = r.skip.map(asString).map(indentedNewLine).getOrElse("")
    s"RETURN$d $i$o$s$l"
  }

  private def asString(w: With): String = {
    val d = if (w.distinct) " DISTINCT" else ""
    val i = asString(w.returnItems)
    val o = w.orderBy.map(asString).map(indentedNewLine).getOrElse("")
    val l = w.limit.map(asString).map(indentedNewLine).getOrElse("")
    val s = w.skip.map(asString).map(indentedNewLine).getOrElse("")
    val wh = w.where.map(asString).map(indentedNewLine).getOrElse("")
    s"WITH$d $i$o$s$l$wh"
  }

  private def asString(c: Create): String = {
    val p = expr.patterns.apply(c.pattern)
    s"CREATE $p"
  }

  private def asString(u: Unwind): String = {
    s"UNWIND ${expr(u.expression)} AS ${expr(u.variable)}"
  }

  private def asString(u: UnresolvedCall): String = {
    val namespace = expr(u.procedureNamespace)
    val prefix = if (namespace.isEmpty) "" else namespace + "."
    val arguments = u.declaredArguments.map(list => list.map(expr(_)).mkString("(", ", ", ")")).getOrElse("")
    def item(i: ProcedureResultItem) = i.output.map(expr(_) + " AS ").getOrElse("") + expr(i.variable)
    def result(r: ProcedureResult) = "YIELD " + r.items.map(item).mkString(", ") + r.where.map(asString).map(indentedNewLine).getOrElse("")
    val yields = u.declaredResult.map(result).map(indentedNewLine).getOrElse("")
    s"CALL $prefix${expr(u.procedureName)}$arguments$yields"
  }

  private def asString(s: SetClause): String = {
    val items = s.items.map {
      case SetPropertyItem(prop, exp) => s"${expr(prop)} = ${expr(exp)}"
      case SetLabelItem(variable, labels) => expr(variable) + labels.map(l => s":${expr(l)}").mkString("")
      case SetIncludingPropertiesFromMapItem(variable, exp) => s"${expr(variable)} += ${expr(exp)}"
      case SetExactPropertiesFromMapItem(variable, exp) => s"${expr(variable)} = ${expr(exp)}"
      case _ => s.asCanonicalStringVal
    }
    s"SET ${items.mkString(", ")}"
  }

  private def asString(r: Remove): String = {
    val items = r.items.map {
      case RemovePropertyItem(prop) => s"${expr(prop)}"
      case RemoveLabelItem(variable, labels) => expr(variable) + labels.map(l => s":${expr(l)}").mkString("")
      case _ => r.asCanonicalStringVal
    }
    s"REMOVE ${items.mkString(", ")}"
  }

  private def asString(v: LoadCSV): String = {
    val withHeaders = if (v.withHeaders) " WITH HEADERS" else ""
    val url = expr(v.urlString)
    val varName = expr(v.variable)
    val fieldTerminator = v.fieldTerminator.map(x => " FIELDTERMINATOR " + expr(x)).getOrElse("")
    s"LOAD CSV$withHeaders FROM $url AS $varName$fieldTerminator"
  }

  private def asString(delete: Delete): String = {
    val detach = if (delete.forced) "DETACH " else ""
    s"${detach}DELETE ${delete.expressions.map(expr(_)).mkString(", ")}"
  }

  private def asString(foreach: Foreach): String = {
    val varName = expr(foreach.variable)
    val list = expr(foreach.expression)
    val updates = foreach.updates.map(dispatch).mkString(s"$NL  ", s"$NL  ", NL)
    s"FOREACH ( $varName IN $list |$updates)"
  }

  private def asString(start: Start): String = {
    val startItems =
      start.items.map {
        case AllNodes(v) => s"${expr(v)} = NODE( * )"
        case NodeByIds(v, ids) => s"${expr(v)} = NODE( ${ids.map(expr(_)).mkString(", ")} )"
        case NodeByParameter(v, param :Parameter) => s"${expr(v)} = NODE( ${expr(param)} )"
        case NodeByParameter(v, param :ParameterWithOldSyntax) => s"${expr(v)} = NODE( ${expr(param)} )"
        case AllRelationships(v) => s"${expr(v)} = RELATIONSHIP( * )"
        case RelationshipByIds(v, ids) => s"${expr(v)} = RELATIONSHIP( ${ids.map(expr(_)).mkString(", ")} )"
        case RelationshipByParameter(v, param: Parameter) => s"${expr(v)} = RELATIONSHIP( ${expr(param)} )"
        case RelationshipByParameter(v, param: ParameterWithOldSyntax) => s"${expr(v)} = RELATIONSHIP( ${expr(param)} )"
      }

    val where = start.where.map(asString).map(indentedNewLine).getOrElse("")
    s"START ${startItems.mkString(s",$NL      ")}$where"
  }

  private def asString(properties: Seq[Property]): String =
    properties.map(_.asCanonicalStringVal).mkString("(", ", ", ")")
}

object Prettifier {

  def extractScope(scope: ShowPrivilegeScope): String = {
    scope match {
      case ShowUserPrivileges(name) => s"USER ${escapeName(name)}"
      case ShowRolePrivileges(name) => s"ROLE ${escapeName(name)}"
      case ShowAllPrivileges() => "ALL"
      case _ => "<unknown>"
    }
  }

  def extractScope(dbScope: GraphScope, qualifier: PrivilegeQualifier): (String, String) = {
    val (r, d, q) = extractScope(AllResource()(InputPosition.NONE), dbScope, qualifier)
    (d, q)
  }

  def extractScope(resource: ActionResource, dbScope: GraphScope, qualifier: PrivilegeQualifier): (String, String, String) = {
    val resourceName = resource match {
      case PropertyResource(name) => escapeName(name)
      case PropertiesResource(names) => names.map(escapeName).mkString(", ")
      case NoResource() => ""
      case AllResource() => "*"
      case _ => "<unknown>"
    }
    val segment = qualifier match {
      case LabelQualifier(name) => "NODE " + escapeName(name)
      case LabelsQualifier(names) => "NODES " + names.map(escapeName).mkString(", ")
      case LabelAllQualifier() => "NODES *"
      case RelationshipQualifier(name) => "RELATIONSHIP " + escapeName(name)
      case RelationshipsQualifier(names) => "RELATIONSHIPS " + names.map(escapeName).mkString(", ")
      case RelationshipAllQualifier() => "RELATIONSHIPS *"
      case AllQualifier() => "ELEMENTS *"
      case _ => "<unknown>"
    }
    (resourceName, extractDbScope(dbScope), segment)
  }

  def revokeOperation(operation: String, revokeType: String) = s"$operation($revokeType)"

  def extractDbScope(dbScope: GraphScope): String = dbScope match {
    case NamedGraphScope(name) => escapeName(name)
    case AllGraphsScope() => "*"
    case _ => "<unknown>"
  }

  /*
   * Some strings (identifiers) were escaped with back-ticks to allow non-identifier characters
   * When printing these again, the knowledge of the back-ticks is lost, but the same test for
   * non-identifier characters can be used to recover that knowledge.
   */
  def escapeName(name: String): String = {
    if (name.isEmpty)
      name
    else {
      val c = name.chars().toArray.toSeq
      if (Character.isJavaIdentifierStart(c.head) && Character.getType(c.head) != Character.CURRENCY_SYMBOL &&
        (c.tail.isEmpty || c.tail.forall(Character.isJavaIdentifierPart)))
        name
      else
        s"`$name`"
    }
  }

  def escapeNames(names: Seq[String]): String = names.map(escapeName).mkString(", ")
}
