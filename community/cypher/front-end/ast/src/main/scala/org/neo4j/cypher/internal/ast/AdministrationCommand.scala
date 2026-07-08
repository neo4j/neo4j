/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.AdministrationCommand.NATIVE_AUTH
import org.neo4j.cypher.internal.ast.AdministrationCommand.authRuleAllowListedFunctions
import org.neo4j.cypher.internal.ast.AdministrationCommand.checkIsStringLiteralOrParameter
import org.neo4j.cypher.internal.ast.AdministrationCommand.checkIsStringOrStringListOrParameter
import org.neo4j.cypher.internal.ast.AdministrationCommand.propertyRuleAllowedTemporalFunctions
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.ast.semantics.SemanticAnalysisTooling
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck.success
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck.when
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckResult
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckable
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticExpressionCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.ast.semantics._
import org.neo4j.cypher.internal.ast.semantics.iterableOnceSemanticChecking
import org.neo4j.cypher.internal.ast.semantics.optionSemanticChecking
import org.neo4j.cypher.internal.expressions.BooleanExpression
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Expression.SemanticContext
import org.neo4j.cypher.internal.expressions.FunctionInvocationLike
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.GreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.In
import org.neo4j.cypher.internal.expressions.IsNotNull
import org.neo4j.cypher.internal.expressions.IsNull
import org.neo4j.cypher.internal.expressions.LessThan
import org.neo4j.cypher.internal.expressions.LessThanOrEqual
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.Literal
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.NaN
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.NotEquals
import org.neo4j.cypher.internal.expressions.Null
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.StringDecimalInteger
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.SubqueryExpression
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.invariantTypeSpec
import org.neo4j.gqlstatus.ErrorGqlStatusObject
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation
import org.neo4j.gqlstatus.GqlHelper
import org.neo4j.gqlstatus.GqlParams
import org.neo4j.gqlstatus.GqlStatusInfoCodes

import scala.jdk.CollectionConverters.SeqHasAsJava

sealed trait AdministrationCommand extends StatementWithGraph with SemanticAnalysisTooling {

  def name: String

  def commandDescription: String = name

  // We parse USE to give a nice error message, but it's not considered to be a part of the AST
  private var useGraphVar: Option[UseGraph] = None
  def useGraph: Option[UseGraph] = useGraphVar

  override def withGraph(useGraph: Option[UseGraph]): AdministrationCommand = {
    this.useGraphVar = useGraph
    this
  }

  def isReadOnly: Boolean

  override def containsUpdates: Boolean = !isReadOnly

  override def semanticCheck: SemanticCheck =
    requireFeatureSupport(s"The `$name` clause", SemanticFeature.MultipleDatabases, position) chain
      when(useGraphVar.isDefined)(SemanticError.useClauseWithAdministrationCommand(position))

  override def dup(children: Seq[AnyRef]): this.type =
    super.dup(children).withGraph(useGraph).asInstanceOf[this.type]
}

object AdministrationCommand extends SemanticAnalysisTooling {
  val NATIVE_AUTH = "native"

  val propertyRuleAllowedTemporalFunctions: Seq[String] =
    Seq("date", "datetime", "localdatetime", "localtime", "time", "duration", "point")

  val authRuleAllowListedFunctions: Seq[String] = Seq(
    // ABAC oidc user attributes function
    "abac.oidc.user_attribute",
    // ABAC native user tags function
    "abac.native.user_tags",
    // List functions
    "range",
    "reduce",
    "reverse",
    "tail",
    "toBooleanList",
    "toFloatList",
    "toIntegerList",
    "toStringList",
    // Numeric functions
    "abs",
    "ceil",
    "floor",
    "isNaN",
    "round",
    "sign",
    // Predicate functions,
    "all",
    "any",
    "isEmpty",
    "none",
    "single",
    // Scalar functions
    "char_length",
    "character_length",
    "coalesce",
    "head",
    "last",
    "nullIf",
    "size",
    "toBoolean",
    "toBooleanOrNull",
    "toFloat",
    "toFloatOrNull",
    "toInteger",
    "toIntegerOrNull",
    // String Functions
    "btrim",
    "left",
    "lower",
    "ltrim",
    "replace",
    "right",
    "rtrim",
    "split",
    "substring",
    "toLower",
    "toString",
    "toStringOrNull",
    "toUpper",
    "trim",
    "upper",
    // Temporal duration functions
    "duration",
    "duration.between",
    "duration.inDays",
    "duration.inMonths",
    "duration.inSeconds",
    // Temporal instant functions
    "date",
    "date.transaction",
    "date.truncate",
    "datetime",
    "datetime.transaction",
    "datetime.fromEpoch",
    "datetime.fromEpochMillis",
    "datetime.truncate",
    "localdatetime",
    "localdatetime.transaction",
    "localdatetime.truncate",
    "localtime",
    "localtime.transaction",
    "localtime.truncate",
    "time",
    "time.transaction",
    "time.truncate"
  ).map(_.toLowerCase)

  private[ast] def checkIsStringLiteralOrParameter(value: String, expression: Expression): SemanticCheck =
    expression match {
      case _: StringLiteral                            => success
      case p: Parameter if p.parameterType == CTString => success
      case exp => SemanticCheck.error(SemanticError.invalidEntityType(
          ExpressionStringifier()(exp),
          value,
          Seq("STRING NOT NULL"),
          s"$value must be a String, or a String parameter.",
          exp.position
        ))
    }

  /**
   * Validate that {@code expr} is a non-empty StringLiteral, a ListLiteral of non-empty
   * StringLiterals, or a Parameter. On failure produces a {@code 22N04} error tagged with the
   * given {@code context} (e.g. "REMOVE AUTH", "SET TAGS").
   *
   * When {@code allowEmptyList} is true an empty ListLiteral is also accepted; the tag clauses use
   * this to let {@code SET TAGS []} clear all tags (and {@code ADD}/{@code REMOVE TAGS []} be no-ops).
   */
  private[ast] def checkIsStringOrStringListOrParameter(
    context: String,
    expr: Expression,
    allowEmptyList: Boolean = false
  ): SemanticCheck =
    expr match {
      case s: StringLiteral if s.value.nonEmpty => success
      case _: Parameter                         => success
      case list: ListLiteral
        if (allowEmptyList || list.expressions.nonEmpty) &&
          list.expressions.forall(e =>
            e.isInstanceOf[StringLiteral] && e.asInstanceOf[StringLiteral].value.nonEmpty
          ) =>
        success
      case _ =>
        val stringifier = ExpressionStringifier()
        val listForm = if (allowEmptyList) "List of non-empty Strings" else "non-empty List of non-empty Strings"
        val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N04)
          .atPosition(expr.position.offset, expr.position.line, expr.position.column)
          .withParam(GqlParams.StringParam.input, stringifier(expr))
          .withParam(GqlParams.StringParam.context, context)
          .withParam(
            GqlParams.ListParam.inputList,
            List(
              "non-empty String",
              listForm,
              "Parameter"
            ).asJava
          )
          .build()
        error(gql, s"Expected a non-empty String, $listForm, or Parameter.", expr.position)
    }
}

sealed trait ReadAdministrationCommand extends AdministrationCommand {

  val isReadOnly: Boolean = true

  private[ast] val defaultColumnSet: List[ShowColumn]

  def returnColumnNames: List[String] = (yields, returns) match {
    case (_, Some(r))                => r.returnItems.items.map(ri => ri.alias.get.name).toList
    case (Some(resultColumns), None) => resultColumns.returnItems.items.map(ri => ri.alias.get.name).toList
    case (None, None)                => defaultColumnNames
  }

  def defaultColumnNames: List[String] = defaultColumnSet.map(_.name)

  // Needed for scoping information
  def defaultColumnVariables: List[LogicalVariable] = defaultColumnSet.map(_.variable)

  def yieldOrWhere: YieldOrWhere = None
  def yields: Option[Yield] = yieldOrWhere.flatMap(yw => yw.left.toOption.map { case (y, _) => y })
  def returns: Option[Return] = yieldOrWhere.flatMap(yw => yw.left.toOption.flatMap { case (_, r) => r })
  def withYieldOrWhere(newYieldOrWhere: YieldOrWhere): ReadAdministrationCommand

  override def returnColumns: List[LogicalVariable] =
    returnColumnNames.map(name => Variable(name)(position, Variable.isIsolatedDefault))

  override def semanticCheck: SemanticCheck = SemanticCheck.nestedCheck {

    def checkForSubquery(astNode: ASTNode): SemanticCheck = {
      val invalid: Option[Expression] = astNode.folder.treeFind[Expression] {
        case _: SubqueryExpression => true
      }
      invalid.map {
        case exp: ExistsExpression =>
          AdministrationCommandSemanticAnalysis.unsupportedRequestErrorOnSystemDatabase(
            "EXISTS expression",
            "The EXISTS expression is not valid on SHOW commands.",
            exp.position
          )
        case exp: CollectExpression =>
          AdministrationCommandSemanticAnalysis.unsupportedRequestErrorOnSystemDatabase(
            "COLLECT expression",
            "The COLLECT expression is not valid on SHOW commands.",
            exp.position
          )
        case exp: CountExpression =>
          AdministrationCommandSemanticAnalysis.unsupportedRequestErrorOnSystemDatabase(
            "COUNT expression",
            "The COUNT expression is not valid on SHOW commands.",
            exp.position
          )
        case exp: PatternExpression =>
          AdministrationCommandSemanticAnalysis.unsupportedRequestErrorOnSystemDatabase(
            "Pattern expression",
            "Pattern expressions are not valid on SHOW commands.",
            exp.position
          )
        case exp: PatternComprehension =>
          AdministrationCommandSemanticAnalysis.unsupportedRequestErrorOnSystemDatabase(
            "Pattern comprehension",
            "Pattern comprehensions are not valid on SHOW commands.",
            exp.position
          )
        case exp =>
          AdministrationCommandSemanticAnalysis.unsupportedRequestErrorOnSystemDatabase(
            "Subquery expression",
            "Subquery expressions are not valid on SHOW commands.",
            exp.position
          )
      }.getOrElse(success)
    }

    def checkProjection(r: ProjectionClause): SemanticCheck = {
      val check = r.semanticCheck
      for {
        closingResult <- check
        continuationResult <- r.semanticCheckContinuation(closingResult.state.currentScope.scope)
      } yield {
        semantics.SemanticCheckResult(continuationResult.state, closingResult.errors ++ continuationResult.errors)
      }
    }

    val initialChecks: SemanticCheck = super.semanticCheck
      .chain((state: SemanticState) => SemanticCheckResult.success(state.newChildScope))
      .chain(
        // Create variables for the columns generated by the command
        semanticCheckFold(defaultColumnSet)(sc => declareVariable(sc.variable, sc.cypherType))
      )
      .chain(checkForSubquery(this))

    val projectionChecks = Seq(yields, returns).foldSemanticCheck {
      maybeClause =>
        maybeClause.foldSemanticCheck(r =>
          checkProjection(r).chain(recordCurrentScope(r))
        )
    }

    initialChecks chain projectionChecks
  }
}

sealed trait WriteAdministrationCommand extends AdministrationCommand {
  val isReadOnly: Boolean = false
  override def returnColumns: List[LogicalVariable] = List.empty

  protected def defaultLanguageVersionCheck(defaultVersion: Option[CypherVersion], command: String): SemanticCheck = {
    defaultVersion.map(version =>
      if (version.experimental) {
        requireFeatureSupport(
          s"`${version.description}` as default language for `$command`",
          SemanticFeature.ExperimentalCypherVersions,
          position
        )
      } else {
        SemanticCheck.success
      }
    ).getOrElse(SemanticCheck.success)
  }

  protected def checkDefaultLanguageAndComposite(
    aliasName: DatabaseName,
    defaultLanguage: Option[CypherVersion]
  ): SemanticCheck = {
    defaultLanguage.map(_ => {
      aliasName match {
        case NamespacedName(_, Some(_)) =>
          SemanticCheck.error(SemanticError.defaultLanguageForConstituentAliases(position))
        case _ => SemanticCheck.success
      }
    }).getOrElse(SemanticCheck.success)
  }
}

sealed trait TopologyCheck extends SemanticAnalysisTooling {

  protected def topologyCheck(
    topology: Option[Topology],
    command: String,
    action: String,
    position: InputPosition
  ): SemanticCheck = {

    def numPrimaryGreaterThanZero(topology: Topology, position: InputPosition): SemanticCheck =
      if (topology.primaries.flatMap(_.left.toOption).exists(_ < 1)) {
        val count = topology.primaries.flatMap(_.left.toOption).get
        val topologyString = Prettifier.extractTopology(topology).trim
        error(SemanticError.numPrimariesOutOfRange(count, command, action, topologyString, position))
      } else {
        SemanticCheck.success
      }

    def numSecondaryPositive(topology: Topology, position: InputPosition): SemanticCheck =
      if (topology.secondaries.flatMap(_.left.toOption).exists(_ < 0)) {
        val count = topology.secondaries.flatMap(_.left.toOption).get
        val topologyString = Prettifier.extractTopology(topology).trim
        error(SemanticError.numSecondariesOutOfRange(count, command, action, topologyString, position))
      } else {
        SemanticCheck.success
      }

    topology.map(topology => {
      numPrimaryGreaterThanZero(topology, position) chain
        numSecondaryPositive(topology, position)
    }).getOrElse(SemanticCheck.success)
  }

}

// User commands

final case class ShowUsers(
  override val yieldOrWhere: YieldOrWhere,
  withAuth: Boolean,
  override val defaultColumnSet: List[ShowColumn]
)(val position: InputPosition) extends ReadAdministrationCommand {

  override def name: String = "SHOW USERS"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)

  override def withYieldOrWhere(newYieldOrWhere: YieldOrWhere): ShowUsers =
    this.copy(yieldOrWhere = newYieldOrWhere)(position)
}

object ShowUsers {

  def apply(yieldOrWhere: YieldOrWhere, withAuth: Boolean)(position: InputPosition): ShowUsers = {
    val baseColumns: List[(ShowColumn, DefaultOrAllShowColumns.ShowByDefault)] = List(
      (ShowColumn("user")(position), true),
      (ShowColumn("roles", CTList(CTString))(position), true),
      (ShowColumn("passwordChangeRequired", CTBoolean)(position), true),
      (ShowColumn("suspended", CTBoolean)(position), true),
      (ShowColumn("home")(position), true)
    )
    val withAuthColumns =
      if (withAuth)
        baseColumns ++ List(
          (ShowColumn("provider")(position), true),
          (ShowColumn("auth", CTMap)(position), true)
        )
      else baseColumns
    // The `tags` column is exposed regardless of the `SemanticFeature.UserTags` flag.
    val allColumns = withAuthColumns :+ (ShowColumn("tags", CTList(CTString))(position), false)
    val columns = DefaultOrAllShowColumns(allColumns, yieldOrWhere).columns
    ShowUsers(
      yieldOrWhere,
      withAuth,
      columns
    )(position)
  }
}

final case class ShowCurrentUser(
  override val yieldOrWhere: YieldOrWhere,
  override val defaultColumnSet: List[ShowColumn]
)(val position: InputPosition) extends ReadAdministrationCommand {

  override def name: String = "SHOW CURRENT USER"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck

  override def withYieldOrWhere(newYieldOrWhere: YieldOrWhere): ShowCurrentUser =
    this.copy(yieldOrWhere = newYieldOrWhere)(position)
}

object ShowCurrentUser {

  def apply(yieldOrWhere: YieldOrWhere)(position: InputPosition): ShowCurrentUser = {
    val allColumns: List[(ShowColumn, DefaultOrAllShowColumns.ShowByDefault)] = List(
      (ShowColumn("user")(position), true),
      (ShowColumn("roles", CTList(CTString))(position), true),
      (ShowColumn("passwordChangeRequired", CTBoolean)(position), true),
      (ShowColumn("suspended", CTBoolean)(position), true),
      (ShowColumn("home")(position), true),
      // The `tags` column is exposed regardless of the `SemanticFeature.UserTags` flag.
      (ShowColumn("tags", CTList(CTString))(position), false)
    )
    val columns = DefaultOrAllShowColumns(allColumns, yieldOrWhere).columns
    ShowCurrentUser(yieldOrWhere, columns)(position)
  }
}

sealed trait UserAuth extends SemanticAnalysisTooling {
  protected def newStyleAuth: List[Auth]
  protected def oldStyleAuth: Option[Auth]

  protected[ast] val externalAuths: List[ExternalAuth] =
    newStyleAuth.filter(_.provider != NATIVE_AUTH).map(a => ExternalAuth(a.provider, a.authAttributes)(a.position))

  private val allNativeAuths: List[NativeAuth] =
    (newStyleAuth.filter(_.provider == NATIVE_AUTH) ++ oldStyleAuth).map(a => NativeAuth(a.authAttributes)(a.position))

  // semantic check makes sure at most one exists
  protected[ast] val nativeAuth: Option[NativeAuth] = allNativeAuths.headOption

  protected val allAuths: Seq[AuthImpl] = externalAuths ++ allNativeAuths

  protected def checkDuplicateAuth: SemanticCheck = newStyleAuth.groupBy(_.provider).collectFirst {
    case (_, List(_, duplicate, _*)) =>
      AdministrationCommandSemanticAnalysis.duplicateClauseError(
        s"SET AUTH '${duplicate.provider}'",
        s"Duplicate `SET AUTH '${duplicate.provider}'` clause.",
        duplicate.position
      )
  }.getOrElse(success)

  protected def checkOldAndNewStyleCombination: SemanticCheck = newStyleAuth.filter(_.provider == NATIVE_AUTH) match {
    case Seq(_, _*) if oldStyleAuth.nonEmpty =>
      val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N92)
        .atPosition(
          oldStyleAuth.head.position.offset,
          oldStyleAuth.head.position.line,
          oldStyleAuth.head.position.column
        )
        .build()
      error(
        gql,
        "Cannot combine old and new auth syntax for the same auth provider.",
        oldStyleAuth.head.authAttributes.head.position
      )
    case _ => success
  }

  val usesOldStyleNativeAuth: Boolean = oldStyleAuth.nonEmpty
}

sealed trait UserTagsAction extends ASTNode

final case class SetTags(tags: Expression)(val position: InputPosition) extends UserTagsAction
final case class AddTags(tags: Expression)(val position: InputPosition) extends UserTagsAction
final case class RemoveTags(tags: Expression)(val position: InputPosition) extends UserTagsAction
final case class RemoveAllTags()(val position: InputPosition) extends UserTagsAction

object UserTagsAction extends SemanticAnalysisTooling {

  def checkFeature(tags: Seq[UserTagsAction]): SemanticCheck =
    tags.headOption.map { t =>
      val desc = t match {
        case _: SetTags       => "The SET TAGS clause"
        case _: AddTags       => "The ADD TAGS clause"
        case _: RemoveTags    => "The REMOVE TAGS clause"
        case _: RemoveAllTags => "The REMOVE ALL TAGS clause"
      }
      requireFeatureSupport(desc, SemanticFeature.UserTags, t.position)
    }.getOrElse(success)

  def checkConsistency(tags: Seq[UserTagsAction]): SemanticCheck =
    if (
      tags.exists(_.isInstanceOf[SetTags]) &&
      tags.exists(t => t.isInstanceOf[AddTags] || t.isInstanceOf[RemoveTags] || t.isInstanceOf[RemoveAllTags])
    ) {
      val setPos = tags.collectFirst { case t: SetTags => t.position }.get
      val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N92)
        .atPosition(setPos.offset, setPos.line, setPos.column)
        .build()
      error(gql, "SET TAGS cannot be combined with ADD TAGS or REMOVE TAGS.", setPos)
    } else success

  /**
   * Validate the expressions carried by SET/ADD/REMOVE TAGS clauses: each must be a non-empty
   * StringLiteral, a ListLiteral of non-empty StringLiterals, or a Parameter.
   */
  def checkValues(tags: Seq[UserTagsAction]): SemanticCheck =
    tags.foldSemanticCheck {
      case _: RemoveAllTags => success
      case SetTags(expr)    => checkIsStringOrStringListOrParameter("SET TAGS", expr, allowEmptyList = true)
      case AddTags(expr)    => checkIsStringOrStringListOrParameter("ADD TAGS", expr, allowEmptyList = true)
      case RemoveTags(expr) => checkIsStringOrStringListOrParameter("REMOVE TAGS", expr, allowEmptyList = true)
    }
}

final case class CreateUser(
  userName: Expression,
  userOptions: UserOptions,
  ifExistsDo: IfExistsDo,
  protected val newStyleAuth: List[Auth],
  protected val oldStyleAuth: Option[Auth],
  tags: Option[SetTags] = None
)(val position: InputPosition) extends WriteAdministrationCommand with UserAuth {

  override def name: String = ifExistsDo match {
    case IfExistsReplace | IfExistsInvalidSyntax => "CREATE OR REPLACE USER"
    case _                                       => "CREATE USER"
  }

  private def checkAtLeastOneAuth: SemanticCheck = if (allAuths.isEmpty) {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N06)
      .atPosition(position.offset, position.line, position.column)
      .withParam(GqlParams.ListParam.inputList, List("Auth provider").asJava)
      .build()
    error(gql, "No auth given for user.", position)
  } else success

  override def semanticCheck: SemanticCheck = ifExistsDo match {
    case IfExistsInvalidSyntax =>
      SemanticCheck.error(SemanticError.bothOrReplaceAndIfNotExists("user", userAsString, position))
    case _ =>
      UserTagsAction.checkFeature(tags.toSeq) chain
        UserTagsAction.checkValues(tags.toSeq) chain
        checkAtLeastOneAuth chain
        checkDuplicateAuth chain
        checkOldAndNewStyleCombination chain
        allAuths.foldSemanticCheck(auth =>
          auth.checkDuplicates chain
            auth.checkRequiredAttributes chain // for external checks that ID exists and is string or parameter
            auth.checkNoUnsupportedAttributes chain
            auth.checkProviderName
        ) chain
        super.semanticCheck chain
        checkIsStringLiteralOrParameter("username", userName) chain
        SemanticState.recordCurrentScope(this)
  }

  private val userAsString: String = Prettifier.escapeName(userName)
}

object CreateUser {

  def unapply(c: CreateUser)
    : Some[(Expression, UserOptions, IfExistsDo, List[ExternalAuth], Option[NativeAuth], Option[SetTags])] =
    Some((c.userName, c.userOptions, c.ifExistsDo, c.externalAuths, c.nativeAuth, c.tags))
}

final case class DropUser(userName: Expression, ifExists: Boolean)(val position: InputPosition)
    extends WriteAdministrationCommand {

  override def name = "DROP USER"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      checkIsStringLiteralOrParameter("username", userName) chain
      SemanticState.recordCurrentScope(this)
}

final case class RenameUser(
  fromUserName: Expression,
  toUserName: Expression,
  ifExists: Boolean
)(val position: InputPosition) extends WriteAdministrationCommand {

  override def name: String = "RENAME USER"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      checkIsStringLiteralOrParameter("from username", fromUserName) chain
      checkIsStringLiteralOrParameter("to username", toUserName) chain
      SemanticState.recordCurrentScope(this)
}

final case class AlterUser(
  userName: Expression,
  userOptions: UserOptions,
  ifExists: Boolean,
  protected val newStyleAuth: List[Auth],
  protected val oldStyleAuth: Option[Auth],
  removeAuth: RemoveAuth,
  tags: Seq[UserTagsAction] = Seq.empty
)(val position: InputPosition) extends WriteAdministrationCommand with UserAuth {

  override def name = "ALTER USER"

  private def checkAtLeastOneClause: SemanticCheck =
    if (userOptions.isEmpty && allAuths.isEmpty && removeAuth.isEmpty && tags.isEmpty) {
      val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N94)
        .atPosition(position.offset, position.line, position.column)
        .build()
      error(gql, "`ALTER USER` requires at least one clause.", position)
    } else {
      success
    }

  private def checkRemoveAuth: SemanticCheck =
    removeAuth.auths.foldSemanticCheck(checkIsStringOrStringListOrParameter("REMOVE AUTH", _))

  override def semanticCheck: SemanticCheck =
    UserTagsAction.checkFeature(tags) chain
      checkAtLeastOneClause chain
      UserTagsAction.checkConsistency(tags) chain
      checkDuplicateAuth chain
      checkOldAndNewStyleCombination chain
      allAuths.foldSemanticCheck(auth =>
        auth.checkDuplicates chain
          auth.checkNoUnsupportedAttributes chain
          auth.checkProviderName
      ) chain
      externalAuths.foldSemanticCheck(_.checkIdIsStringLiteralOrParameter) chain
      checkRemoveAuth chain
      UserTagsAction.checkValues(tags) chain
      super.semanticCheck chain
      checkIsStringLiteralOrParameter("username", userName) chain
      SemanticState.recordCurrentScope(this)
}

object AlterUser {

  def unapply(a: AlterUser)
    : Some[(
      Expression,
      UserOptions,
      Boolean,
      List[ExternalAuth],
      Option[NativeAuth],
      RemoveAuth,
      Seq[UserTagsAction]
    )] =
    Some((a.userName, a.userOptions, a.ifExists, a.externalAuths, a.nativeAuth, a.removeAuth, a.tags))
}

final case class AlterUsers(
  userNames: Seq[Expression],
  ifExists: Boolean,
  tags: Seq[UserTagsAction]
)(val position: InputPosition) extends WriteAdministrationCommand {
  override def name: String = "ALTER USERS"

  private def checkTagsFeature: SemanticCheck =
    requireFeatureSupport("The ALTER USERS command", SemanticFeature.UserTags, position)

  private def checkAtLeastOneClause: SemanticCheck =
    if (tags.isEmpty) {
      val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N94)
        .atPosition(position.offset, position.line, position.column)
        .build()
      error(gql, "`ALTER USERS` requires at least one tag clause.", position)
    } else success

  override def semanticCheck: SemanticCheck =
    checkTagsFeature chain
      checkAtLeastOneClause chain
      UserTagsAction.checkConsistency(tags) chain
      UserTagsAction.checkValues(tags) chain
      userNames.foldSemanticCheck(checkIsStringLiteralOrParameter("username", _)) chain
      super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class SetOwnPassword(newPassword: Expression, currentPassword: Expression)(val position: InputPosition)
    extends WriteAdministrationCommand {

  override def name = "ALTER CURRENT USER SET PASSWORD"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

case class RemoveAuth(all: Boolean, auths: List[Expression]) {
  def isEmpty: Boolean = auths.isEmpty && !all
  def nonEmpty: Boolean = !isEmpty
}

// Only used during parsing
case class Auth(provider: String, authAttributes: List[AuthAttribute])(val position: InputPosition) extends ASTNode

object allAuthAttr {

  val allAuthsAttributes: List[AuthAttribute] =
    List(Password(null, false)(null), PasswordChange(false)(null), AuthId(null)(null))
}

sealed trait AuthImpl extends ASTNode with SemanticAnalysisTooling {
  def authAttributes: List[AuthAttribute]
  def provider: String

  def checkRequiredAttributes: SemanticCheck
  def checkNoUnsupportedAttributes: SemanticCheck

  def checkDuplicates: SemanticCheck = authAttributes.groupBy(_.name).collectFirst {
    case (_, List(_, duplicate, _*)) =>
      AdministrationCommandSemanticAnalysis.duplicateClauseError(
        duplicate.name,
        s"Duplicate `${duplicate.name}` clause.",
        duplicate.position
      )
  }.getOrElse(success)

  def checkProviderName: SemanticCheck = {
    if (provider.isEmpty) {
      val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NB6)
        .atPosition(position.offset, position.line, position.column)
        .withParam(GqlParams.StringParam.item, "Auth provider")
        .build()
      error(gql, "Invalid input. Auth provider is not allowed to be an empty string.", position)
    } else success
  }

  protected def requiredAttributes(func: AuthAttribute => Boolean, name: String): SemanticCheck =
    authAttributes.find(func) match {
      case Some(_) => success
      case None => AdministrationCommandSemanticAnalysis.missingMandatoryAuthClauseError(
          name,
          provider,
          missingRequiredClauseErrorMessage(name),
          position
        )
    }

  protected def noUnsupportedAttributes(func: AuthAttribute => Boolean): SemanticCheck = {
    authAttributes.find(func) match {
      case Some(unsupported) => {
        val supportedFunc: AuthAttribute => Boolean = x => !func(x)

        val supported =
          allAuthAttr.allAuthsAttributes.filter(supportedFunc).map(attr =>
            attr.name
          ) // TODO do you agree that this filtering should be inside the "case"? A little inefficient but only occurs when we have unsupported, better than doing outside. Also do we even want to give the allowed commands like this if they are passed?

        val pos = unsupported.position
        val expected = if (supported.isEmpty) java.util.List.of("no clause")
        else
          supported.map(
            String.valueOf(_)
          ).asJava
        SemanticCheck.error(SemanticError.authForbidsClauseError(provider, unsupported.name, expected, pos))
      }
      case None => success
    }
  }

  protected def missingRequiredClauseErrorMessage(name: String): String =
    s"Clause `$name` is mandatory for auth provider `$provider`."
}

final case class NativeAuth(authAttributes: List[AuthAttribute])(val position: InputPosition) extends AuthImpl {

  val provider: String = NATIVE_AUTH

  override def checkRequiredAttributes: SemanticCheck =
    requiredAttributes(attr => attr.isInstanceOf[Password], "SET PASSWORD")

  override def checkNoUnsupportedAttributes: SemanticCheck =
    noUnsupportedAttributes(attr => !attr.isInstanceOf[NativeAuthAttribute])

  def password: Option[Password] = authAttributes.collectFirst { case p: Password => p }

  def changeRequired: Option[Boolean] = authAttributes.collectFirst { case PasswordChange(change) => change }
}

final case class ExternalAuth(provider: String, authAttributes: List[AuthAttribute])(val position: InputPosition)
    extends AuthImpl {
  private val maybeId = authAttributes.collectFirst { case AuthId(id) => id }

  override def checkRequiredAttributes: SemanticCheck =
    requiredAttributes(attr => attr.isInstanceOf[AuthId], "SET ID") ifOkChain
      checkIdIsStringLiteralOrParameter

  override def checkNoUnsupportedAttributes: SemanticCheck =
    noUnsupportedAttributes(attr => !attr.isInstanceOf[ExternalAuthAttribute])

  def checkIdIsStringLiteralOrParameter: SemanticCheck =
    maybeId.map(id => checkIsStringLiteralOrParameter("id", id))
      .getOrElse(AdministrationCommandSemanticAnalysis.missingMandatoryAuthClauseError(
        "SET ID",
        provider,
        missingRequiredClauseErrorMessage("SET ID"),
        position
      ))

  // this is expected to only be called after checkRequiredAttributes has been called
  def id: Expression = maybeId.get
}

sealed trait AuthAttribute extends ASTNode {
  def position: InputPosition
  def name: String
}

sealed trait NativeAuthAttribute extends AuthAttribute
sealed trait ExternalAuthAttribute extends AuthAttribute

final case class Password(
  password: Expression,
  isEncrypted: Boolean
)(val position: InputPosition) extends NativeAuthAttribute {
  override val name: String = "SET PASSWORD"
}

final case class PasswordChange(
  requireChange: Boolean
)(val position: InputPosition) extends NativeAuthAttribute {
  override val name: String = "SET PASSWORD CHANGE [NOT] REQUIRED"
}

final case class AuthId(
  id: Expression
)(val position: InputPosition) extends ExternalAuthAttribute {
  override val name: String = "SET ID"
}

sealed trait HomeDatabaseAction
case object RemoveHomeDatabaseAction extends HomeDatabaseAction
final case class SetHomeDatabaseAction(name: DatabaseName) extends HomeDatabaseAction

final case class UserOptions(
  suspended: Option[Boolean],
  homeDatabase: Option[HomeDatabaseAction]
) {
  def isEmpty: Boolean = suspended.isEmpty && homeDatabase.isEmpty
}

// Role commands

final case class ShowRoles(
  withUsers: Boolean,
  withAuthRules: Boolean,
  showAll: Boolean,
  asCommands: Boolean,
  override val yieldOrWhere: YieldOrWhere,
  override val defaultColumnSet: List[ShowColumn]
)(val position: InputPosition) extends ReadAdministrationCommand {

  override def name: String = if (showAll) "SHOW ALL ROLES" else "SHOW POPULATED ROLES"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)

  override def withYieldOrWhere(newYieldOrWhere: YieldOrWhere): ShowRoles =
    this.copy(yieldOrWhere = newYieldOrWhere)(position)
}

object ShowRoles {

  def apply(
    withUsers: Boolean,
    withAuthRules: Boolean,
    showAll: Boolean,
    asCommands: Boolean,
    yieldOrWhere: YieldOrWhere
  )(position: InputPosition): ShowRoles = {
    val commandColumn = if (asCommands)
      List((ShowColumn(Variable("command")(position, Variable.isIsolatedDefault), CTString, "command"), true))
    else List.empty
    val roleColumn =
      List((ShowColumn(Variable("role")(position, Variable.isIsolatedDefault), CTString, "role"), !asCommands))
    val extraColumn = {
      if (withUsers) List(
        (ShowColumn(Variable("member")(position, Variable.isIsolatedDefault), CTString, "member"), true)
      )
      else if (withAuthRules) List(
        (ShowColumn(Variable("authRule")(position, Variable.isIsolatedDefault), CTString, "authRule"), true)
      )
      else List.empty
    }
    val allColumns = commandColumn ++ roleColumn ++ extraColumn ++ List((
      ShowColumn(Variable("immutable")(position, Variable.isIsolatedDefault), CTBoolean, "immutable"),
      false
    ))
    val columns = DefaultOrAllShowColumns(allColumns, yieldOrWhere).columns
    ShowRoles(withUsers, withAuthRules, showAll, asCommands, yieldOrWhere, columns)(position)
  }
}

final case class CreateRole(
  roleName: Expression,
  immutable: Boolean,
  from: Option[Expression],
  ifExistsDo: IfExistsDo
)(val position: InputPosition) extends WriteAdministrationCommand {

  override def name: String = ifExistsDo match {
    case IfExistsReplace | IfExistsInvalidSyntax => s"CREATE OR REPLACE${Prettifier.maybeImmutable(immutable)} ROLE"
    case _                                       => s"CREATE${Prettifier.maybeImmutable(immutable)} ROLE"
  }

  override def semanticCheck: SemanticCheck =
    ifExistsDo match {
      case IfExistsInvalidSyntax =>
        val name = Prettifier.escapeName(roleName)
        SemanticCheck.error(SemanticError.bothOrReplaceAndIfNotExists("role", name, position))
      case _ =>
        super.semanticCheck chain
          checkIsStringLiteralOrParameter("rolename", roleName) chain
          semanticCheckFold(from)(roleName => checkIsStringLiteralOrParameter("from rolename", roleName)) chain
          SemanticState.recordCurrentScope(this)
    }
}

final case class DropRole(roleName: Expression, ifExists: Boolean)(val position: InputPosition)
    extends WriteAdministrationCommand {

  override def name = "DROP ROLE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      checkIsStringLiteralOrParameter("rolename", roleName) chain
      SemanticState.recordCurrentScope(this)
}

final case class RenameRole(
  fromRoleName: Expression,
  toRoleName: Expression,
  ifExists: Boolean
)(val position: InputPosition) extends WriteAdministrationCommand {

  override def name: String = "RENAME ROLE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      checkIsStringLiteralOrParameter("from rolename", fromRoleName) chain
      checkIsStringLiteralOrParameter("to rolename", toRoleName) chain
      SemanticState.recordCurrentScope(this)
}

final case class GrantRolesToUsers(
  roleNames: Seq[Expression],
  userNames: Seq[Expression]
)(val position: InputPosition) extends WriteAdministrationCommand {

  override def name = "GRANT ROLE"

  override def semanticCheck: SemanticCheck = {
    super.semanticCheck chain
      semanticCheckFold(roleNames)(roleName => checkIsStringLiteralOrParameter("rolename", roleName)) chain
      semanticCheckFold(userNames)(username => checkIsStringLiteralOrParameter("username", username)) chain
      SemanticState.recordCurrentScope(this)
  }
}

final case class GrantRolesToAuthRules(
  roleNames: Seq[Expression],
  ruleNames: Seq[Expression]
)(val position: InputPosition) extends WriteAdministrationCommand {

  override def name = "GRANT ROLE TO AUTH RULE"

  private val featureCheck =
    requireFeatureSupport(
      s"The `$name` clause",
      SemanticFeature.AttributeBasedAccessControl,
      position
    )

  override def semanticCheck: SemanticCheck = {
    super.semanticCheck chain
      featureCheck chain
      semanticCheckFold(roleNames)(roleName => checkIsStringLiteralOrParameter("rolename", roleName)) chain
      semanticCheckFold(ruleNames)(ruleName => checkIsStringLiteralOrParameter("rulename", ruleName)) chain
      SemanticState.recordCurrentScope(this)
  }
}

final case class RevokeRolesFromUsers(
  roleNames: Seq[Expression],
  userNames: Seq[Expression]
)(val position: InputPosition) extends WriteAdministrationCommand {

  override def name = "REVOKE ROLE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      semanticCheckFold(roleNames)(roleName => checkIsStringLiteralOrParameter("rolename", roleName)) chain
      semanticCheckFold(userNames)(username => checkIsStringLiteralOrParameter("username", username)) chain
      SemanticState.recordCurrentScope(this)
}

final case class RevokeRolesFromAuthRules(
  roleNames: Seq[Expression],
  ruleNames: Seq[Expression]
)(val position: InputPosition) extends WriteAdministrationCommand {

  override def name = "REVOKE ROLE FROM AUTH RULE"

  private val featureCheck =
    requireFeatureSupport(
      s"The `$name` clause",
      SemanticFeature.AttributeBasedAccessControl,
      position
    )

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      featureCheck chain
      semanticCheckFold(roleNames)(roleName => checkIsStringLiteralOrParameter("rolename", roleName)) chain
      semanticCheckFold(ruleNames)(ruleName => checkIsStringLiteralOrParameter("rulename", ruleName)) chain
      SemanticState.recordCurrentScope(this)
}

// AuthRule commands

sealed trait AuthRules extends SemanticAnalysisTooling {

  protected def featureCheck(name: String, position: InputPosition): SemanticCheck =
    requireFeatureSupport(
      s"The `$name` clause",
      SemanticFeature.AttributeBasedAccessControl,
      position
    )

  protected def checkExpression(condition: Option[AuthRuleCondition]): SemanticCheck = {
    condition.map(_.expression).toSeq
      .flatMap(e => Seq(e) ++ e.subExpressions)
      .foldSemanticCheck {
        case subqueryExpression: SubqueryExpression => SemanticCheck.error(
            SemanticError.authRuleConditionCannotSubqueryExpression(subqueryExpression.position)
          )
        case parameter: Parameter => SemanticCheck.error(
            SemanticError.authRuleConditionCannotContainParameter(parameter)
          )
        case _ => SemanticCheck.success
      } chain checkFunctions(condition)
  }

  protected def checkFunctions(condition: Option[AuthRuleCondition]): SemanticCheck = {
    condition.map(_.expression).toSeq
      .flatMap(e => Seq(e) ++ e.subExpressions)
      .flatMap {
        case f: FunctionInvocationLike => Some(f)
        case _                         => None
      }
      .foldSemanticCheck(f => {
        checkAllowlist(f) chain
          checkTemporalFunctionsArguments(f) chain
          checkAbacOidcUserAttributeFunction(f)
      })
  }

  protected def checkAllowlist(functionInvocation: FunctionInvocationLike): SemanticCheck = {

    val name = functionInvocation.functionName.fullName
    // Reject an allow-listed name only when it resolved to a user-defined function (a UDF
    // shadowing the name). Compiler built-ins, the genuine resolved abac functions, and abac
    // names whose provider is not enabled (so they don't resolve to a signature) are all
    // accepted here; a disabled-but-genuine abac function is then rejected at evaluation time.
    if (authRuleAllowListedFunctions.contains(name.toLowerCase) && !functionInvocation.isUserDefined)
      SemanticCheck.success
    else
      SemanticCheck.error(SemanticError.authRuleConditionHaveInvalidFunctionInCondition(
        name,
        functionInvocation.position
      ))
  }

  protected def checkTemporalFunctionsArguments(functionInvocation: FunctionInvocationLike): SemanticCheck = {
    val temporalFunctionsThatRequireArgs = Seq(
      "date",
      "datetime",
      "localdatetime",
      "localtime",
      "time"
    )

    val name = functionInvocation.functionName.fullName
    if (temporalFunctionsThatRequireArgs.contains(name.toLowerCase)) {
      functionInvocation.callArguments match {
        case Seq() =>
          SemanticCheck.error(SemanticError.authRuleConditionHaveInvalidFunctionInCondition(
            name,
            functionInvocation.position
          ))
        case Seq(MapExpression(items), _*) if items.size == 1 && items.head._1.name.equalsIgnoreCase("timezone") =>
          val (key, valueExpr) = items.head
          val (callStr, suggestion) = valueExpr match {
            case s: StringLiteral =>
              (s"$name({${key.name}: '${s.value}'})", s"$name.transaction('${s.value}')")
            case other =>
              (s"$name({${key.name}: ${other.asCanonicalStringVal}})", s"$name.transaction()")
          }
          SemanticCheck.error(SemanticError.authRuleConditionTemporalFunctionRetrievesCurrentTime(
            callStr,
            suggestion,
            functionInvocation.position
          ))
        case _ => SemanticCheck.success
      }
    } else {
      SemanticCheck.success
    }
  }

  protected def checkAbacOidcUserAttributeFunction(functionInvocation: FunctionInvocationLike): SemanticCheck = {
    val name = functionInvocation.functionName.fullName
    val args = functionInvocation.callArguments
    if (name == "abac.oidc.user_attribute") {
      lazy val argHeadOption = args.headOption
        .flatMap {
          case _: Parameter             => None // cannot evaluate parameter so this will pass semantic check
          case literal: Literal         => Some(literal)
          case listLiteral: ListLiteral => Some(listLiteral)
          case _                        => None // might want to evaluate the inner expression to fail more cases
        }
      if (args.size != 1) {
        // This will probably never be more than 1 since the parser gives one ListLiteral argument
        SemanticError.functionCallWrongNumberOfArguments(
          1,
          args.size,
          name,
          "abac.oidc.user_attribute(attributeKey :: STRING) :: ANY",
          args.map(_.asCanonicalStringVal).mkString(", "),
          functionInvocation.position
        )
      } else if (argHeadOption.exists(_.isInstanceOf[ListLiteral])) {
        SemanticError.functionCallWrongNumberOfArguments(
          1,
          args.head.asInstanceOf[ListLiteral].expressions.size,
          name,
          "abac.oidc.user_attribute(attributeKey :: STRING) :: ANY",
          args.map(_.asCanonicalStringVal).mkString(", "),
          functionInvocation.position
        )
      } else if (argHeadOption.nonEmpty && !argHeadOption.exists(_.isInstanceOf[StringLiteral])) {
        val argLiteral = argHeadOption.get.asInstanceOf[Literal]
        SemanticExpressionCheck.simple(argLiteral) chain
          expectType(CTString.covariant, argLiteral)
      } else {
        SemanticCheck.success
      }
    } else {
      SemanticCheck.success
    }
  }
}

final case class ShowAuthRules(
  override val yieldOrWhere: YieldOrWhere,
  override val defaultColumnSet: List[ShowColumn],
  asCommands: Boolean
)(val position: InputPosition) extends ReadAdministrationCommand with AuthRules {

  override def name: String = "SHOW AUTH RULES"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      featureCheck(name, position) chain
      SemanticState.recordCurrentScope(this)

  override def withYieldOrWhere(newYieldOrWhere: YieldOrWhere): ShowAuthRules =
    this.copy(yieldOrWhere = newYieldOrWhere)(position)
}

object ShowAuthRules {

  def apply(yieldOrWhere: YieldOrWhere, asCommands: Boolean)(position: InputPosition): ShowAuthRules = {
    val columns =
      if (asCommands)
        List(
          (ShowColumn("command")(position), true),
          (ShowColumn("roles", CTList(CTString))(position), false)
        )
      else
        List(
          ShowColumn("name")(position),
          ShowColumn("condition", CTString)(position),
          ShowColumn("enabled", CTBoolean)(position),
          ShowColumn("roles", CTList(CTString))(position)
        ).map(column => (column, true))

    ShowAuthRules(
      yieldOrWhere,
      DefaultOrAllShowColumns(columns, yieldOrWhere).columns,
      asCommands
    )(position)
  }
}

final case class CreateAuthRule(
  authRuleName: Expression,
  ifExistsDo: IfExistsDo,
  setClauses: List[AuthRuleSetClause]
)(val position: InputPosition) extends WriteAdministrationCommand with AuthRules {

  override def name: String = ifExistsDo match {
    case IfExistsReplace | IfExistsInvalidSyntax => s"CREATE OR REPLACE AUTH RULE"
    case _                                       => s"CREATE AUTH RULE"
  }

  override def semanticCheck: SemanticCheck =
    ifExistsDo match {
      case IfExistsInvalidSyntax =>
        val name = Prettifier.escapeName(authRuleName)
        SemanticCheck.error(SemanticError.bothOrReplaceAndIfNotExists("auth rule", name, position))
      case _ =>
        super.semanticCheck chain
          featureCheck(name, position) chain
          checkIsStringLiteralOrParameter("auth rule", authRuleName) chain
          checkMustHaveCondition chain
          checkExpression(condition) chain
          SemanticState.recordCurrentScope(this)
    }

  def condition: Option[AuthRuleCondition] = setClauses.collectFirst { case condition: AuthRuleCondition => condition }

  def enabled: Option[AuthRuleEnabled] = setClauses.collectFirst { case enabled: AuthRuleEnabled => enabled }

  private def checkMustHaveCondition: SemanticCheck =
    if (condition.isEmpty) SemanticCheck.error(SemanticError.authRuleMustHaveACondition(position))
    else SemanticCheck.success
}

sealed trait AuthRuleSetClause extends ASTNode {
  def position: InputPosition
  def name: String
}

final case class AuthRuleCondition(
  expression: Expression
)(val position: InputPosition) extends AuthRuleSetClause {
  override val name: String = "SET CONDITION"
}

final case class AuthRuleEnabled(
  enabled: Boolean
)(val position: InputPosition) extends AuthRuleSetClause {
  override val name: String = "SET ENABLED"
}

final case class DropAuthRule(authRuleName: Expression, ifExists: Boolean)(val position: InputPosition)
    extends WriteAdministrationCommand with AuthRules {

  override def name = "DROP AUTH RULE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      featureCheck(name, position) chain
      checkIsStringLiteralOrParameter("auth rule", authRuleName) chain
      SemanticState.recordCurrentScope(this)
}

final case class RenameAuthRule(
  fromAuthRuleName: Expression,
  toAuthRuleName: Expression,
  ifExists: Boolean
)(val position: InputPosition) extends WriteAdministrationCommand with AuthRules {

  override def name: String = "RENAME AUTH RULE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      featureCheck(name, position) chain
      checkIsStringLiteralOrParameter("from auth rule name", fromAuthRuleName) chain
      checkIsStringLiteralOrParameter("to auth rule name", fromAuthRuleName) chain
      SemanticState.recordCurrentScope(this)
}

final case class AlterAuthRule(
  authRuleName: Expression,
  ifExists: Boolean,
  setClauses: List[AuthRuleSetClause]
)(val position: InputPosition) extends WriteAdministrationCommand with AuthRules {

  override def name: String = "ALTER AUTH RULE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      featureCheck(name, position) chain
      checkIsStringLiteralOrParameter("auth rule", authRuleName) chain
      checkExpression(condition) chain
      SemanticState.recordCurrentScope(this)

  def condition: Option[AuthRuleCondition] = setClauses.collectFirst { case condition: AuthRuleCondition => condition }

  def enabled: Option[AuthRuleEnabled] = setClauses.collectFirst { case enabled: AuthRuleEnabled => enabled }
}

// Privilege commands

final case class ShowPrivileges(
  scope: ShowPrivilegeScope,
  override val yieldOrWhere: YieldOrWhere,
  override val defaultColumnSet: List[ShowColumn]
)(val position: InputPosition) extends ReadAdministrationCommand {
  override def name = "SHOW PRIVILEGE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)

  override def withYieldOrWhere(newYieldOrWhere: YieldOrWhere): ShowPrivileges =
    this.copy(yieldOrWhere = newYieldOrWhere)(position)
}

object ShowPrivileges {

  def apply(scope: ShowPrivilegeScope, yieldOrWhere: YieldOrWhere)(position: InputPosition): ShowPrivileges = {
    val columns = List(
      ShowColumn("access")(position),
      ShowColumn("action")(position),
      ShowColumn("resource")(position),
      ShowColumn("graph")(position),
      ShowColumn("segment")(position),
      ShowColumn("role")(position),
      ShowColumn("immutable", CTBoolean)(position)
    ) ++ (scope match {
      case _: ShowUserPrivileges | _: ShowUsersPrivileges => List(ShowColumn("user")(position))
      case _                                              => List.empty
    })
    ShowPrivileges(scope, yieldOrWhere, columns)(position)
  }
}

final case class ShowSupportedPrivilegeCommand(
  override val yieldOrWhere: YieldOrWhere,
  override val defaultColumnSet: List[ShowColumn]
)(val position: InputPosition) extends ReadAdministrationCommand {
  override def name = "SHOW SUPPORTED PRIVILEGES"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)

  override def withYieldOrWhere(newYieldOrWhere: YieldOrWhere): ShowSupportedPrivilegeCommand =
    this.copy(yieldOrWhere = newYieldOrWhere)(position)
}

object ShowSupportedPrivilegeCommand {
  val ACTION: String = "action"
  val QUALIFIER: String = "qualifier"
  val TARGET: String = "target"
  val SCOPE: String = "scope"
  val DESCRIPTION: String = "description"

  def apply(yieldOrWhere: YieldOrWhere)(position: InputPosition): ShowSupportedPrivilegeCommand = {
    val columns =
      List(
        ShowColumn(ACTION)(position),
        ShowColumn(QUALIFIER)(position),
        ShowColumn(TARGET)(position),
        ShowColumn(SCOPE, CTList(CTString))(position),
        ShowColumn(DESCRIPTION)(position)
      )
    ShowSupportedPrivilegeCommand(yieldOrWhere, columns)(position)
  }
}

final case class ShowPrivilegeCommands(
  scope: ShowPrivilegeScope,
  asRevoke: Boolean,
  override val yieldOrWhere: YieldOrWhere,
  override val defaultColumnSet: List[ShowColumn]
)(val position: InputPosition) extends ReadAdministrationCommand {
  override def name = "SHOW PRIVILEGE COMMANDS"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)

  override def withYieldOrWhere(newYieldOrWhere: YieldOrWhere): ShowPrivilegeCommands =
    this.copy(yieldOrWhere = newYieldOrWhere)(position)
}

object ShowPrivilegeCommands {

  def apply(
    scope: ShowPrivilegeScope,
    asRevoke: Boolean,
    yieldOrWhere: YieldOrWhere
  )(position: InputPosition): ShowPrivilegeCommands = {
    val allColumns =
      List((ShowColumn("command")(position), true), (ShowColumn("immutable", CTBoolean)(position), false))
    val columns = DefaultOrAllShowColumns(allColumns, yieldOrWhere).columns
    ShowPrivilegeCommands(scope, asRevoke, yieldOrWhere, columns)(position)
  }
}

//noinspection ScalaUnusedSymbol
sealed abstract class PrivilegeCommand(
  privilege: PrivilegeType,
  val qualifier: List[PrivilegeQualifier],
  position: InputPosition
) extends WriteAdministrationCommand {

  private val FAILED_PROPERTY_RULE = "Failed to administer property rule."

  private def nanError(l: NaN) = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NA0)
      .atPosition(l.position.offset, l.position.line, l.position.column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NA3)
        .atPosition(l.position.offset, l.position.line, l.position.column)
        .build())
      .build()
    error(gql, s"$FAILED_PROPERTY_RULE `NaN` is not supported for property-based access control.", l.position)
  }

  private def propertyAlwaysNullError(
    gqlBuilder: String => ErrorGqlStatusObject,
    predicate: String,
    pos: InputPosition,
    hint: String = ""
  ) = {
    error(
      gqlBuilder(predicate),
      s"$FAILED_PROPERTY_RULE The property value access rule pattern `$predicate` always evaluates to `NULL`.$hint",
      pos
    )
  }

  private def propertyPositionError(p: Property, operator: String) =
    error(
      ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NA0)
        .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NA1)
          .withParam(GqlParams.StringParam.propKey, p.propertyKey.name)
          .withParam(GqlParams.StringParam.operation, operator)
          .build())
        .build(),
      s"$FAILED_PROPERTY_RULE The property `${p.propertyKey.name}` must appear on the left hand side of the `$operator` operator.",
      p.position
    )

  private val featureCheck =
    requireFeatureSupport(
      s"The `$name` clause on relationship patterns",
      SemanticFeature.RelationshipPropertyValueAccessRules,
      position
    )

  private def checkActionTypeForPropertyRules(privilegeType: PrivilegeType): SemanticCheck = {
    privilegeType match {
      case GraphPrivilege(action, _) => action match {
          case ReadAction | TraverseAction | MatchAction => SemanticCheck.success
          case _ =>
            SemanticCheck.error(SemanticError.unsupportedActionAccess(
              action.name,
              java.util.List.of(ReadAction.name, TraverseAction.name, MatchAction.name),
              position
            ))
        }
      case _ => SemanticCheck.error(SemanticError.notSupported(position)) // We should never end up here
    }
  }

  private def privilegeQualifierCheckForPropertyRules(qualifiers: List[PrivilegeQualifier]): SemanticCheck =
    SemanticCheck.fromContext { context =>
      qualifiers.foldLeft(SemanticCheck.success)((acc, qualifier) => {
        acc.chain(qualifier match {
          case PatternQualifier(_, v, e, Relationship) =>
            featureCheck chain
              v.foldSemanticCheck(declareVariable(_, CTRelationship)) chain
              SemanticExpressionCheck.check(SemanticContext.Results, e) chain
              checkActionTypeForPropertyRules(privilege) chain
              checkExpression(e, context.cypherVersion)
          case PatternQualifier(_, v, e, Node) =>
            v.foldSemanticCheck(declareVariable(_, CTNode)) chain
              SemanticExpressionCheck.check(SemanticContext.Results, e) chain
              checkActionTypeForPropertyRules(privilege) chain
              checkExpression(e, context.cypherVersion)
          case _ => SemanticCheck.success
        })
      })
    }

  private def checkExpression(expression: Expression, cypherVersion: CypherVersion) = {

    def stringifyExpression = {
      ExpressionStringifier.apply(_.asCanonicalStringVal).apply(expression)
    }

    def unsupportedExpression = s"$FAILED_PROPERTY_RULE The expression: `$stringifyExpression` is not supported. " +
      s"Only single, literal-based predicate expressions are allowed for property-based access control."

    def checkScalarExpression(value: Expression): SemanticCheck = {
      value match {
        case _: Literal | _: ExplicitParameter => SemanticCheck.success
        case f: FunctionInvocationLike
          if f.isBuiltIn && propertyRuleAllowedTemporalFunctions.contains(f.functionName.fullName.toLowerCase) =>
          SemanticCheck.success
        case _ =>
          AdministrationCommandSemanticAnalysis.invalidPropertyBasedAccessControlRuleInvolvingNontrivialPredicatesError(
            value.asCanonicalStringVal,
            unsupportedExpression,
            expression.position
          )
      }
    }

    def checkListExpression(value: Expression): SemanticCheck = {
      value match {
        case ll: ListLiteral          => checkTypesInList(ll)
        case param: ExplicitParameter => SemanticCheck.success
        case _ =>
          AdministrationCommandSemanticAnalysis.invalidPropertyBasedAccessControlRuleInvolvingNontrivialPredicatesError(
            value.asCanonicalStringVal,
            unsupportedExpression,
            expression.position
          )
      }
    }

    def checkTypesInList(listLiteral: ListLiteral): SemanticCheck =
      if (
        listLiteral.expressions.forall {
          checkScalarExpression(_) == SemanticCheck.success
        }
      ) SemanticCheck.success
      else error(SemanticError.mixedListInPBAC(stringifyExpression, expression.position))

    val unwrappedExpression = expression match {
      case Not(e: BooleanExpression) => e
      case e                         => e
    }

    def valueInListPropertyFeatureSupport: SemanticCheck =
      requireFeatureSupport(
        s"The `$name` clause using a `<value> IN <property>` predicate",
        SemanticFeature.ValueInListProperty,
        expression.position
      )

    def valueInListPropertyFeatureCheck: SemanticCheck = unwrappedExpression match {
      case In(lhs, _: Property) if !lhs.isInstanceOf[Property] && cypherVersion.isAfter(CypherVersion.Cypher5) =>
        valueInListPropertyFeatureSupport
      case Not(In(lhs, _: Property)) if !lhs.isInstanceOf[Property] && cypherVersion.isAfter(CypherVersion.Cypher5) =>
        valueInListPropertyFeatureSupport
      case _ => SemanticCheck.success
    }

    valueInListPropertyFeatureCheck chain (unwrappedExpression match {
      case Equals(_: Property, l: NaN)             => nanError(l)
      case NotEquals(_: Property, l: NaN)          => nanError(l)
      case GreaterThan(_: Property, l: NaN)        => nanError(l)
      case GreaterThanOrEqual(_: Property, l: NaN) => nanError(l)
      case LessThan(_: Property, l: NaN)           => nanError(l)
      case LessThanOrEqual(_: Property, l: NaN)    => nanError(l)
      case Equals(l: NaN, _: Property)             => nanError(l)
      case NotEquals(l: NaN, _: Property)          => nanError(l)
      case GreaterThan(l: NaN, _: Property)        => nanError(l)
      case GreaterThanOrEqual(l: NaN, _: Property) => nanError(l)
      case LessThan(l: NaN, _: Property)           => nanError(l)
      case LessThanOrEqual(l: NaN, _: Property)    => nanError(l)
      case Equals(p: Property, l: Null) =>
        propertyAlwaysNullError(
          GqlHelper.getGql22NA0_22NA5,
          s"${p.propertyKey.name} = NULL",
          l.position,
          " Use `IS NULL` instead."
        )
      case NotEquals(p: Property, l: Null) =>
        propertyAlwaysNullError(
          GqlHelper.getGql22NA0_22NA6,
          s"${p.propertyKey.name} <> NULL",
          l.position,
          " Use `IS NOT NULL` instead."
        )
      case GreaterThan(p: Property, l: Null) =>
        propertyAlwaysNullError(GqlHelper.getGql22NA0_22NA4, s"${p.propertyKey.name} > NULL", l.position)
      case GreaterThanOrEqual(p: Property, l: Null) =>
        propertyAlwaysNullError(GqlHelper.getGql22NA0_22NA4, s"${p.propertyKey.name} >= NULL", l.position)
      case LessThan(p: Property, l: Null) =>
        propertyAlwaysNullError(GqlHelper.getGql22NA0_22NA4, s"${p.propertyKey.name} < NULL", l.position)
      case LessThanOrEqual(p: Property, l: Null) =>
        propertyAlwaysNullError(GqlHelper.getGql22NA0_22NA4, s"${p.propertyKey.name} <= NULL", l.position)
      case Equals(l: Null, p: Property) =>
        propertyAlwaysNullError(
          GqlHelper.getGql22NA0_22NA5,
          s"NULL = ${p.propertyKey.name}",
          l.position,
          " Use `IS NULL` instead."
        )
      case NotEquals(l: Null, p: Property) =>
        propertyAlwaysNullError(
          GqlHelper.getGql22NA0_22NA6,
          s"NULL <> ${p.propertyKey.name}",
          l.position,
          " Use `IS NOT NULL` instead."
        )
      case GreaterThan(l: Null, p: Property) =>
        propertyAlwaysNullError(GqlHelper.getGql22NA0_22NA4, s"NULL > ${p.propertyKey.name}", l.position)
      case GreaterThanOrEqual(l: Null, p: Property) =>
        propertyAlwaysNullError(GqlHelper.getGql22NA0_22NA4, s"NULL >= ${p.propertyKey.name}", l.position)
      case LessThan(l: Null, p: Property) =>
        propertyAlwaysNullError(GqlHelper.getGql22NA0_22NA4, s"NULL < ${p.propertyKey.name}", l.position)
      case LessThanOrEqual(l: Null, p: Property) =>
        propertyAlwaysNullError(GqlHelper.getGql22NA0_22NA4, s"NULL <= ${p.propertyKey.name}", l.position)
      case Equals(_, p: Property)             => propertyPositionError(p, "=")
      case NotEquals(_, p: Property)          => propertyPositionError(p, "<>")
      case GreaterThan(_, p: Property)        => propertyPositionError(p, ">")
      case GreaterThanOrEqual(_, p: Property) => propertyPositionError(p, ">=")
      case LessThan(_, p: Property)           => propertyPositionError(p, "<")
      case LessThanOrEqual(_, p: Property)    => propertyPositionError(p, "<=")
      case map @ MapExpression(items) if items.size > 1 =>
        error(
          ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NA0)
            .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NA2)
              .withParam(GqlParams.StringParam.expr, stringifyExpression)
              .build())
            .build(),
          s"$FAILED_PROPERTY_RULE The expression: `$stringifyExpression` is not supported. Property rules can only contain one property.",
          map.position
        )
      case MapExpression(Seq((pk: PropertyKeyName, l: Null))) =>
        propertyAlwaysNullError(
          GqlHelper.getGql22NA0_22NB0,
          s"{${pk.name}:NULL}",
          l.position,
          " Use `WHERE` syntax in combination with `IS NULL` instead."
        )
      case Equals(_: Property, e: Expression)                                      => checkScalarExpression(e)
      case NotEquals(_: Property, e: Expression)                                   => checkScalarExpression(e)
      case In(_: Property, e: Expression)                                          => checkListExpression(e)
      case Not(In(_: Property, e: Expression))                                     => checkListExpression(e)
      case In(l: NaN, _: Property) if cypherVersion.isAfter(CypherVersion.Cypher5) => nanError(l)
      case In(l: Null, p: Property) if cypherVersion.isAfter(CypherVersion.Cypher5) =>
        propertyAlwaysNullError(GqlHelper.getGql22NA0_22NA4, s"NULL IN ${p.propertyKey.name}", l.position)
      case In(e: Expression, _: Property) if cypherVersion.isAfter(CypherVersion.Cypher5) => checkScalarExpression(e)
      case Not(In(e: Expression, _: Property)) if cypherVersion.isAfter(CypherVersion.Cypher5) =>
        checkScalarExpression(e)
      case IsNull(_: Property) | IsNotNull(_: Property)            => SemanticCheck.success
      case MapExpression(Seq((_: PropertyKeyName, e: Expression))) => checkScalarExpression(e)
      case GreaterThan(_: Property, e: Expression)                 => checkScalarExpression(e)
      case GreaterThanOrEqual(_: Property, e: Expression)          => checkScalarExpression(e)
      case LessThan(_: Property, e: Expression)                    => checkScalarExpression(e)
      case LessThanOrEqual(_: Property, e: Expression)             => checkScalarExpression(e)
      case _ =>
        AdministrationCommandSemanticAnalysis.invalidPropertyBasedAccessControlRuleInvolvingNontrivialPredicatesError(
          expression.asCanonicalStringVal,
          unsupportedExpression,
          expression.position
        )
    })
  }

  override def semanticCheck: SemanticCheck = {
    val showSettingFeatureCheck = privilege match {
      case DbmsPrivilege(ShowSettingAction) =>
        requireFeatureSupport(s"The `$name` clause", SemanticFeature.ShowSetting, position)
      case _ => SemanticCheck.success
    }

    val secretsManagerFeatureCheck = privilege match {
      case DbmsPrivilege(_: SecretManagementAction) =>
        requireFeatureSupport(s"The `$name` clause", SemanticFeature.SecretsManager, position)
      case _ => SemanticCheck.success
    }

    (privilege match {
      case _: LoadPrivilege =>
        qualifier match {
          case LoadUrlQualifier(_) :: _ =>
            val gql =
              GqlHelper.get51N31("URL pattern", "LOAD privileges", position.offset, position.line, position.column)
            error(gql, "LOAD privileges with a URL pattern are not currently supported", position)
          case _ => super.semanticCheck chain SemanticState.recordCurrentScope(this)
        }
      case _ => showSettingFeatureCheck chain secretsManagerFeatureCheck chain
          super.semanticCheck chain SemanticState.recordCurrentScope(this)
    }) chain privilegeQualifierCheckForPropertyRules(qualifier)
  }
}

final case class GrantPrivilege(
  privilege: PrivilegeType,
  immutable: Boolean,
  resource: Option[ActionResourceBase],
  override val qualifier: List[PrivilegeQualifier],
  roleNames: Seq[Expression]
)(val position: InputPosition) extends PrivilegeCommand(privilege, qualifier, position) {
  override def name = s"GRANT${Prettifier.maybeImmutable(immutable)} ${privilege.name}"

  override def semanticCheck: SemanticCheck = super.semanticCheck chain semanticCheckFold(roleNames)(roleName =>
    checkIsStringLiteralOrParameter("rolename", roleName)
  )
}

final case class DenyPrivilege(
  privilege: PrivilegeType,
  immutable: Boolean,
  resource: Option[ActionResourceBase],
  override val qualifier: List[PrivilegeQualifier],
  roleNames: Seq[Expression]
)(val position: InputPosition) extends PrivilegeCommand(privilege, qualifier, position) {

  override def name = s"DENY${Prettifier.maybeImmutable(immutable)} ${privilege.name}"

  override def semanticCheck: SemanticCheck = {
    privilege match {
      case GraphPrivilege(MergeAdminAction, _) =>
        SemanticCheck.error(SemanticError.denyMergeUnsupported(position))
      case _ => super.semanticCheck chain semanticCheckFold(roleNames)(roleName =>
          checkIsStringLiteralOrParameter("rolename", roleName)
        )
    }
  }
}

final case class RevokePrivilege(
  privilege: PrivilegeType,
  immutableOnly: Boolean,
  resource: Option[ActionResourceBase],
  override val qualifier: List[PrivilegeQualifier],
  roleNames: Seq[Expression],
  revokeType: RevokeType
)(val position: InputPosition) extends PrivilegeCommand(privilege, qualifier, position) {

  override def name: String = {
    val revokeTypeOrEmptyString = if (revokeType.name.nonEmpty) s" ${revokeType.name}" else ""
    s"REVOKE$revokeTypeOrEmptyString${Prettifier.maybeImmutable(immutableOnly)} ${privilege.name}"
  }

  override def semanticCheck: SemanticCheck = {
    (privilege, revokeType) match {
      case (GraphPrivilege(MergeAdminAction, _), RevokeDenyType()) =>
        SemanticCheck.error(SemanticError.denyMergeUnsupported(position))
      case _ => super.semanticCheck chain semanticCheckFold(roleNames)(roleName =>
          checkIsStringLiteralOrParameter("rolename", roleName)
        )
    }
  }

}

// Server commands

final case class EnableServer(serverName: Either[String, Parameter], optionsMap: Options)(
  val position: InputPosition
) extends WriteAdministrationCommand {
  override def name: String = "ENABLE SERVER"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class AlterServer(serverName: Either[String, Parameter], optionsMap: Options)(
  val position: InputPosition
) extends WriteAdministrationCommand {
  override def name: String = "ALTER SERVER"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class RenameServer(serverName: Either[String, Parameter], newName: Either[String, Parameter])(
  val position: InputPosition
) extends WriteAdministrationCommand {
  override def name: String = "RENAME SERVER"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class DropServer(serverName: Either[String, Parameter])(
  val position: InputPosition
) extends WriteAdministrationCommand {
  override def name: String = "DROP SERVER"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class ShowServers(override val yieldOrWhere: YieldOrWhere, defaultColumns: DefaultOrAllShowColumns)(
  val position: InputPosition
) extends ReadAdministrationCommand {
  override val defaultColumnSet: List[ShowColumn] = defaultColumns.columns

  override def name: String = "SHOW SERVERS"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)

  override def withYieldOrWhere(newYieldOrWhere: YieldOrWhere): ShowServers =
    this.copy(yieldOrWhere = newYieldOrWhere)(position)
}

object ShowServers {

  def apply(yieldOrWhere: YieldOrWhere)(position: InputPosition): ShowServers = {
    val showColumns = List(
      (ShowColumn("serverId")(position), false),
      (ShowColumn("name")(position), true),
      (ShowColumn("address")(position), true),
      (ShowColumn("httpAddress")(position), false),
      (ShowColumn("httpsAddress")(position), false),
      (ShowColumn("state")(position), true),
      (ShowColumn("health")(position), true),
      (ShowColumn("hosting", CTList(CTString))(position), true),
      (ShowColumn("requestedHosting", CTList(CTString))(position), false),
      (ShowColumn("tags", CTList(CTString))(position), false),
      (ShowColumn("allowedDatabases", CTList(CTString))(position), false),
      (ShowColumn("deniedDatabases", CTList(CTString))(position), false),
      (ShowColumn("modeConstraint")(position), false),
      (ShowColumn("version")(position), false)
    )
    val briefShowColumns = showColumns.filter(_._2).map(_._1)
    val allShowColumns = showColumns.map(_._1)

    val allColumns = yieldOrWhere match {
      case Some(Left(_)) => true
      case _             => false
    }
    val columns = DefaultOrAllShowColumns(allColumns, briefShowColumns, allShowColumns)
    ShowServers(yieldOrWhere, columns)(position)
  }
}

final case class DeallocateServers(dryRun: Boolean, serverNames: Seq[Either[String, Parameter]])(
  val position: InputPosition
) extends WriteAdministrationCommand {
  override def name: String = "DEALLOCATE DATABASES FROM SERVER"

  override val isReadOnly: Boolean = dryRun

  override def returnColumns: List[LogicalVariable] =
    if (dryRun) {
      List(
        Variable("database")(position, Variable.isIsolatedDefault),
        Variable("fromServerName")(position, Variable.isIsolatedDefault),
        Variable("fromServerId")(position, Variable.isIsolatedDefault),
        Variable("toServerName")(position, Variable.isIsolatedDefault),
        Variable("toServerId")(position, Variable.isIsolatedDefault),
        Variable("mode")(position, Variable.isIsolatedDefault)
      )
    } else List.empty

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class ReallocateDatabases(dryRun: Boolean)(
  val position: InputPosition
) extends WriteAdministrationCommand {
  override def name: String = "REALLOCATE DATABASES"

  override val isReadOnly: Boolean = dryRun

  override def returnColumns: List[LogicalVariable] =
    if (dryRun) {
      List(
        Variable("database")(position, Variable.isIsolatedDefault),
        Variable("fromServerName")(position, Variable.isIsolatedDefault),
        Variable("fromServerId")(position, Variable.isIsolatedDefault),
        Variable("toServerName")(position, Variable.isIsolatedDefault),
        Variable("toServerId")(position, Variable.isIsolatedDefault),
        Variable("mode")(position, Variable.isIsolatedDefault)
      )
    } else List.empty

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

// Database commands

final case class CreateDatabase(
  dbName: DatabaseName,
  ifExistsDo: IfExistsDo,
  options: Options,
  waitUntilComplete: WaitUntilComplete,
  topology: Option[Topology],
  defaultLanguage: Option[CypherVersion],
  shards: Option[ShardDefinition]
)(val position: InputPosition)
    extends WaitableAdministrationCommand with TopologyCheck {

  override def name: String = ifExistsDo match {
    case IfExistsReplace | IfExistsInvalidSyntax => "CREATE OR REPLACE DATABASE"
    case _                                       => "CREATE DATABASE"
  }

  override def semanticCheck: SemanticCheck =
    (ifExistsDo match {
      case IfExistsInvalidSyntax =>
        val name = Prettifier.escapeDatabaseName(dbName)
        SemanticCheck.error(SemanticError.bothOrReplaceAndIfNotExists("database", name, position))
      case _ =>
        super.semanticCheck chain
          SemanticState.recordCurrentScope(this)
    }) chain topologyCheck(topology, name, "create", position) chain
      defaultLanguageVersionCheck(defaultLanguage, name) chain
      shards.map(_.semanticCheck(name, "create", position, expectShard = true)).getOrElse(success)
}

final case class CreateReplicaDatabase(
  dbName: DatabaseName,
  ifExistsDo: IfExistsDo,
  options: Options,
  waitUntilComplete: WaitUntilComplete,
  topology: Option[Topology],
  defaultLanguage: Option[CypherVersion]
)(val position: InputPosition)
    extends WaitableAdministrationCommand with TopologyCheck {

  override def name: String = ifExistsDo match {
    case IfExistsReplace | IfExistsInvalidSyntax => "CREATE OR REPLACE REPLICA DATABASE"
    case _                                       => "CREATE REPLICA DATABASE"
  }

  override def semanticCheck: SemanticCheck =
    (ifExistsDo match {
      case IfExistsInvalidSyntax =>
        val name = Prettifier.escapeDatabaseName(dbName)
        SemanticCheck.error(SemanticError.bothOrReplaceAndIfNotExists("database", name, position))
      case _ =>
        super.semanticCheck chain
          SemanticState.recordCurrentScope(this)
    }) chain topologyCheck(topology, name, "create", position) chain
      defaultLanguageVersionCheck(defaultLanguage, name)
}

case class Topology(primaries: Option[Either[Int, Parameter]], secondaries: Option[Either[Int, Parameter]])

case class ShardDefinition(
  propertyShardCount: Int,
  graphShardTopology: Option[Topology],
  propertyShardReplicaCount: Option[Either[Int, Parameter]]
) extends TopologyCheck {

  def semanticCheck(command: String, action: String, position: InputPosition, expectShard: Boolean): SemanticCheck = {

    def numShardsInRange(shardCount: Int): SemanticCheck = {
      if ((shardCount < 1 && expectShard) || shardCount > 1000) {
        error(SemanticError.numShardsOutOfRange(shardCount, command, s"COUNT $shardCount", position))
      } else success
    }

    def numReplicasGreaterThanZero(replicas: Option[Either[Int, Parameter]]): SemanticCheck =
      if (replicas.flatMap(_.left.toOption).exists(c => c < 1 || c > 20)) {
        val count = replicas.flatMap(_.left.toOption).get
        val topologyString = Prettifier.extractShardTopology(replicas).trim
        error(SemanticError.numReplicasOutOfRange(count, command, topologyString, position))
      } else {
        SemanticCheck.success
      }

    topologyCheck(graphShardTopology, command, action, position) chain
      numShardsInRange(propertyShardCount) chain
      numReplicasGreaterThanZero(propertyShardReplicaCount)
  }

}

final case class CreateCompositeDatabase(
  databaseName: DatabaseName,
  ifExistsDo: IfExistsDo,
  options: Options,
  waitUntilComplete: WaitUntilComplete,
  defaultLanguage: Option[CypherVersion]
)(
  val position: InputPosition
) extends WaitableAdministrationCommand {

  override def name: String = ifExistsDo match {
    case IfExistsReplace | IfExistsInvalidSyntax => "CREATE OR REPLACE COMPOSITE DATABASE"
    case _                                       => "CREATE COMPOSITE DATABASE"
  }

  override def semanticCheck: SemanticCheck = SemanticCheck.fromContext { context =>
    ifExistsDo match {
      case IfExistsInvalidSyntax =>
        val name = Prettifier.escapeDatabaseName(databaseName)
        SemanticCheck.error(SemanticError.bothOrReplaceAndIfNotExists("composite database", name, position))
      case _ =>
        databaseName match {
          case nsn @ NamespacedName(_, Some(_)) if context.cypherVersion == CypherVersion.Cypher5 =>
            AdministrationCommandSemanticAnalysis.inputContainsInvalidCharactersError(
              nsn.toString,
              "composite database name",
              s"Failed to create the specified composite database '${nsn.toString}': COMPOSITE DATABASE names cannot contain \".\". " +
                "COMPOSITE DATABASE names using '.' must be quoted with backticks e.g. `composite.database`.",
              nsn.position
            )
          case _ => super.semanticCheck.chain(defaultLanguageVersionCheck(defaultLanguage, name))
        }
    }
  }
}

final case class DropDatabase(
  dbName: DatabaseName,
  ifExists: Boolean,
  composite: Boolean,
  aliasAction: DropDatabaseAliasAction,
  additionalAction: DropDatabaseAdditionalAction,
  waitUntilComplete: WaitUntilComplete
)(val position: InputPosition) extends WaitableAdministrationCommand {

  override def name: String = if (composite) "DROP COMPOSITE DATABASE" else "DROP DATABASE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class AlterDatabase(
  dbName: DatabaseName,
  ifExists: Boolean,
  access: Option[Access],
  topology: Option[Topology],
  options: Options,
  optionsToRemove: Set[String],
  waitUntilComplete: WaitUntilComplete,
  defaultLanguage: Option[CypherVersion],
  shardDefinition: Option[ShardDefinition],
  replicas: Option[Either[Int, Parameter]]
)(
  val position: InputPosition
) extends WaitableAdministrationCommand with TopologyCheck {

  override def name = "ALTER DATABASE"

  override def commandDescription: String =
    Seq(
      Some(name),
      access.map(_ => "SET ACCESS"),
      topology.map(_ => "SET TOPOLOGY"),
      replicas.map(_ => "SET TOPOLOGY"),
      defaultLanguage.map(_ => "SET DEFAULT LANGUAGE"),
      if (options != NoOptions) Some("SET OPTION") else None,
      if (optionsToRemove.nonEmpty) Some("REMOVE OPTION") else None,
      waitUntilComplete match {
        case _: NoWait => None
        case _         => Some("WAIT")
      },
      shardDefinition match {
        case Some(ShardDefinition(_, Some(_), _)) => Some("SET GRAPH SHARD")
        case Some(_)                              => Some("SET PROPERTY SHARD")
        case _                                    => None
      }
    ).flatten.mkString(" ")

  private def isValidReplicaCount(replicas: Option[Either[Int, Parameter]]): SemanticCheck =
    if (replicas.flatMap(_.left.toOption).exists(replicaCount => replicaCount < 1 || replicaCount > 20)) {
      val count = replicas.flatMap(_.left.toOption).get
      val topologyString = Prettifier.extractShardTopology(replicas).trim
      error(SemanticError.numReplicasOutOfRange(count, name, topologyString, position))
    } else {
      SemanticCheck.success
    }

  private def checkTopologyOrShards: SemanticCheck =
    shardDefinition match {
      case Some(ShardDefinition(_, Some(_), _)) if topology.nonEmpty || replicas.nonEmpty =>
        SemanticCheck.error(SemanticError.invalidClauseCombination("SET TOPOLOGY", "SET GRAPH SHARD", position))
      case Some(ShardDefinition(_, None, Some(_))) if topology.nonEmpty || replicas.nonEmpty =>
        SemanticCheck.error(SemanticError.invalidClauseCombination("SET TOPOLOGY", "SET PROPERTY SHARD", position))
      case _ => success
    }

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this) chain
      topologyCheck(topology, name, "alter", position) chain
      defaultLanguageVersionCheck(defaultLanguage, name) chain
      shardDefinition.map(_.semanticCheck(name, "alter", position, expectShard = false)).getOrElse(success) chain
      isValidReplicaCount(replicas) chain checkTopologyOrShards
}

final case class StartDatabase(dbName: DatabaseName, waitUntilComplete: WaitUntilComplete)(
  val position: InputPosition
) extends WaitableAdministrationCommand {

  override def name = "START DATABASE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class StopDatabase(dbName: DatabaseName, waitUntilComplete: WaitUntilComplete)(
  val position: InputPosition
) extends WaitableAdministrationCommand {

  override def name = "STOP DATABASE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

sealed trait WaitableAdministrationCommand extends WriteAdministrationCommand {
  val waitUntilComplete: WaitUntilComplete

  override def returnColumns: List[LogicalVariable] = waitUntilComplete match {
    case NoWait() => List.empty
    case _ => List("address", "state", "message", "success").map(Variable(_)(position, Variable.isIsolatedDefault))
  }

  override def semanticCheck: SemanticCheck = super.semanticCheck chain waitUntilComplete.semanticCheck
}

sealed trait WaitUntilComplete extends ASTNode with SemanticCheckable {
  val DEFAULT_TIMEOUT = 300L
  val name: String
  def timeout: Long = DEFAULT_TIMEOUT

  override def semanticCheck: SemanticCheck = success
}

case class NoWait()(val position: InputPosition) extends WaitUntilComplete {
  override val name: String = ""
}

case class IndefiniteWait()(val position: InputPosition) extends WaitUntilComplete {
  override val name: String = " WAIT"
}

case class TimeoutAfter(stringVal: String)(val position: InputPosition) extends Expression with StringDecimalInteger
    with WaitUntilComplete {
  override val name: String = s" WAIT $stringVal SECONDS"
  override def timeout: Long = value

  override def semanticCheck: SemanticCheck = {
    super.semanticCheck chain SemanticExpressionCheck.simple(this)
  }

  override def isConstantForQuery: Boolean = true
}

sealed trait Access
case object ReadOnlyAccess extends Access
case object ReadWriteAccess extends Access

sealed abstract class DropDatabaseAdditionalAction(val name: String)
case object DumpData extends DropDatabaseAdditionalAction("DUMP DATA")
case object DestroyData extends DropDatabaseAdditionalAction("DESTROY DATA")

sealed abstract class DropDatabaseAliasAction(val name: String)
case object Restrict extends DropDatabaseAliasAction("RESTRICT")
case object CascadeAliases extends DropDatabaseAliasAction("CASCADE ALIASES")

// Alias commands

final case class ShowAliases(
  aliasName: Option[DatabaseName],
  override val yieldOrWhere: YieldOrWhere,
  defaultColumns: DefaultOrAllShowColumns
)(
  val position: InputPosition
) extends ReadAdministrationCommand {
  override val defaultColumnSet: List[ShowColumn] = defaultColumns.columns

  override def name: String = aliasName match {
    case None    => "SHOW ALIASES"
    case Some(_) => "SHOW ALIAS"
  }

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)

  override def withYieldOrWhere(newYieldOrWhere: YieldOrWhere): ShowAliases =
    this.copy(yieldOrWhere = newYieldOrWhere)(position)
}

object ShowAliases {

  val OIDC_CREDENTIAL_FORWARDING = "OIDC CREDENTIAL FORWARDING"
  val STORED_NATIVE_CREDENTIALS = "STORED NATIVE CREDENTIALS"

  def apply(
    yieldOrWhere: YieldOrWhere,
    cypher5ColumnsOnly: Boolean,
    oidcCredentialForwardingEnabled: Boolean
  )(position: InputPosition): ShowAliases =
    apply(None, yieldOrWhere, cypher5ColumnsOnly, oidcCredentialForwardingEnabled)(position)

  def apply(
    aliasName: Option[DatabaseName],
    yieldOrWhere: YieldOrWhere,
    cypher5ColumnsOnly: Boolean,
    oidcCredentialForwardingEnabled: Boolean
  )(position: InputPosition): ShowAliases = {
    val columns = List(
      // (column, brief, includedInCypher5)
      (ShowColumn("name")(position), true, true),
      (ShowColumn("composite")(position), true, true),
      (ShowColumn("database")(position), true, true),
      (ShowColumn("location")(position), true, true),
      (ShowColumn("url")(position), true, true),
      (ShowColumn("credentials")(position), true, false),
      (ShowColumn("user")(position), true, true),
      (ShowColumn("driver", CTMap)(position), false, true),
      (ShowColumn("defaultLanguage")(position), false, true),
      (ShowColumn("properties", CTMap)(position), false, true)
    )

    val showColumns =
      columns.filter { case (column, _, includedInCypher5) =>
        (!cypher5ColumnsOnly || includedInCypher5) &&
        // the credential column is only available if oidcCredentialForwardingEnabled is true
        (oidcCredentialForwardingEnabled || !(column.name == "credentials"))
      }.map { case (showColumn, brief, _) => (showColumn, brief) }

    ShowAliases(aliasName, yieldOrWhere, DefaultOrAllShowColumns(showColumns, yieldOrWhere))(position)
  }
}

object AliasDriverSettingsCheck {

  def findInvalidDriverSettings(driverSettings: Option[Either[Map[String, Expression], Parameter]])
    : Option[Expression] = {
    driverSettings match {
      case Some(Left(settings)) =>
        settings.values.flatMap(s =>
          s.folder.treeFind[Expression] {
            case _: SubqueryExpression => true
          }
        ).headOption
      case _ => None
    }
  }
}

final case class CreateLocalDatabaseAlias(
  aliasName: DatabaseName,
  targetName: DatabaseName,
  ifExistsDo: IfExistsDo,
  properties: Option[Either[Map[String, Expression], Parameter]] = None
)(val position: InputPosition) extends WriteAdministrationCommand {

  override def name: String = ifExistsDo match {
    case IfExistsReplace | IfExistsInvalidSyntax => "CREATE OR REPLACE ALIAS"
    case _                                       => "CREATE ALIAS"
  }

  override def semanticCheck: SemanticCheck = ifExistsDo match {
    case IfExistsInvalidSyntax =>
      SemanticCheck.error(SemanticError.bothOrReplaceAndIfNotExists(
        "alias",
        Prettifier.escapeDatabaseName(aliasName),
        position
      ))
    case _ => super.semanticCheck chain
        namespacedNameHasNoDots chain
        SemanticState.recordCurrentScope(this)
  }

  private def namespacedNameHasNoDots: SemanticCheck = aliasName match {
    case nsn @ NamespacedName(nameComponents, Some(_)) =>
      if (nameComponents.length > 1) AdministrationCommandSemanticAnalysis.inputContainsInvalidCharactersError(
        nsn.toString,
        "local alias name",
        s"'.' is not a valid character in the local alias name '${nsn.toString}'. " +
          "Local alias names using '.' must be quoted with backticks when adding a local alias to a composite database e.g. `local.alias`.",
        nsn.position
      )
      else success
    case _ => success
  }
}

sealed trait RemoteAliasCredentials extends ASTNode

case class RemoteAliasStoredCredentials(username: Expression, password: Expression)(val position: InputPosition)
    extends RemoteAliasCredentials

case class OidcCredentialForwarding()(val position: InputPosition) extends RemoteAliasCredentials

final case class CreateRemoteDatabaseAlias(
  aliasName: DatabaseName,
  targetName: DatabaseName,
  ifExistsDo: IfExistsDo,
  url: Either[String, Parameter],
  remoteAliasCredentials: RemoteAliasCredentials,
  driverSettings: Option[Either[Map[String, Expression], Parameter]] = None,
  properties: Option[Either[Map[String, Expression], Parameter]] = None,
  defaultLanguage: Option[CypherVersion] = None
)(val position: InputPosition) extends WriteAdministrationCommand {

  override def name: String = ifExistsDo match {
    case IfExistsReplace | IfExistsInvalidSyntax => "CREATE OR REPLACE ALIAS"
    case _                                       => "CREATE ALIAS"
  }

  def remoteAliasCredentialSemanticCheck(remoteAliasCredentials: RemoteAliasCredentials): SemanticCheck =
    remoteAliasCredentials match {
      case RemoteAliasStoredCredentials(username, _) => checkIsStringLiteralOrParameter("username", username)
      case OidcCredentialForwarding() => requireFeatureSupport(
          "`OIDC CREDENTIAL FORWARDING`",
          SemanticFeature.OidcCredentialForwarding,
          position
        )
    }

  override def semanticCheck: SemanticCheck = ifExistsDo match {
    case IfExistsInvalidSyntax =>
      SemanticCheck.error(SemanticError.bothOrReplaceAndIfNotExists(
        "alias",
        Prettifier.escapeDatabaseName(aliasName),
        position
      ))
    case _ => AliasDriverSettingsCheck.findInvalidDriverSettings(driverSettings) match {
        case Some(expr: ExistsExpression) =>
          SemanticCheck.error(SemanticError.existsInDriverSettings(expr.position))
        case Some(expr: CountExpression) =>
          SemanticCheck.error(SemanticError.countInDriverSettings(expr.position))
        case Some(expr: CollectExpression) =>
          SemanticCheck.error(SemanticError.collectInDriverSettings(expr.position))
        case Some(expr: PatternExpression) =>
          SemanticCheck.error(SemanticError.patternExpressionInDriverSettings(expr.position))
        case Some(expr: PatternComprehension) =>
          SemanticCheck.error(SemanticError.patternComprehensionInDriverSettings(expr.position))
        case Some(expr) =>
          SemanticCheck.error(SemanticError.genericDriverSettingsFail(expr.position))
        case _ => super.semanticCheck chain
            remoteAliasCredentialSemanticCheck(remoteAliasCredentials) chain
            defaultLanguageVersionCheck(defaultLanguage, name) chain
            checkDefaultLanguageAndComposite(aliasName, defaultLanguage) chain
            SemanticState.recordCurrentScope(this)
      }
  }
}

final case class AlterLocalDatabaseAlias(
  aliasName: DatabaseName,
  targetName: Option[DatabaseName],
  ifExists: Boolean = false,
  properties: Option[Either[Map[String, Expression], Parameter]] = None
)(val position: InputPosition) extends WriteAdministrationCommand {

  override def name = "ALTER ALIAS"

  override def semanticCheck: SemanticCheck = super.semanticCheck chain SemanticState.recordCurrentScope(this)
}

final case class AlterRemoteDatabaseAlias(
  aliasName: DatabaseName,
  targetName: Option[DatabaseName] = None,
  ifExists: Boolean = false,
  url: Option[Either[String, Parameter]] = None,
  username: Option[Expression] = None,
  password: Option[Expression] = None,
  driverSettings: Option[Either[Map[String, Expression], Parameter]] = None,
  properties: Option[Either[Map[String, Expression], Parameter]] = None,
  defaultLanguage: Option[CypherVersion] = None
)(val position: InputPosition) extends WriteAdministrationCommand {

  override def name = "ALTER ALIAS"

  override def semanticCheck: SemanticCheck =
    AliasDriverSettingsCheck.findInvalidDriverSettings(driverSettings) match {
      case Some(expr) =>
        expr match {
          case _: ExistsExpression =>
            SemanticCheck.error(SemanticError.existsInDriverSettings(expr.position))
          case _: CountExpression =>
            SemanticCheck.error(SemanticError.countInDriverSettings(expr.position))
          case _: CollectExpression =>
            SemanticCheck.error(SemanticError.collectInDriverSettings(expr.position))
          case _: PatternExpression =>
            SemanticCheck.error(SemanticError.patternExpressionInDriverSettings(expr.position))
          case _: PatternComprehension =>
            SemanticCheck.error(SemanticError.patternComprehensionInDriverSettings(expr.position))
          case _ =>
            SemanticCheck.error(SemanticError.genericDriverSettingsFail(expr.position))
        }
      case _ =>
        val isLocalAlias = targetName.isDefined && url.isEmpty
        val isRemoteAlias =
          url.isDefined || username.isDefined || password.isDefined || driverSettings.isDefined || defaultLanguage.isDefined
        if (isLocalAlias && isRemoteAlias) {
          AdministrationCommandSemanticAnalysis.invalidInputError(
            Prettifier.escapeDatabaseName(aliasName),
            "database alias",
            List("url of a remote alias target"),
            s"Failed to alter the specified database alias '${Prettifier.escapeDatabaseName(aliasName)}': url needs to be defined to alter a remote alias target.",
            position
          )
        } else {
          super.semanticCheck chain
            semanticCheckFold(username)(un => checkIsStringLiteralOrParameter("username", un)) chain
            defaultLanguageVersionCheck(defaultLanguage, name) chain
            checkDefaultLanguageAndComposite(aliasName, defaultLanguage) chain
            SemanticState.recordCurrentScope(this)
        }
    }
}

final case class DropDatabaseAlias(aliasName: DatabaseName, ifExists: Boolean)(
  val position: InputPosition
) extends WriteAdministrationCommand {

  override def name = "DROP ALIAS"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}
