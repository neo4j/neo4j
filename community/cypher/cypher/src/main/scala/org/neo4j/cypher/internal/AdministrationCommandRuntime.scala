/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal

import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.ast.DatabaseName
import org.neo4j.cypher.internal.ast.HomeDatabaseAction
import org.neo4j.cypher.internal.ast.NamespacedName
import org.neo4j.cypher.internal.ast.ParameterName
import org.neo4j.cypher.internal.ast.RemoveHomeDatabaseAction
import org.neo4j.cypher.internal.ast.SetHomeDatabaseAction
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NameValidator
import org.neo4j.cypher.internal.options.CypherRuntimeOption
import org.neo4j.cypher.internal.procs.Continue
import org.neo4j.cypher.internal.procs.InitAndFinallyFunctions
import org.neo4j.cypher.internal.procs.ParameterTransformer
import org.neo4j.cypher.internal.procs.QueryHandler
import org.neo4j.cypher.internal.procs.ThrowException
import org.neo4j.cypher.internal.procs.UpdatingSystemCommandExecutionPlan
import org.neo4j.cypher.internal.security.SecureHasher
import org.neo4j.cypher.internal.security.SystemGraphCredential
import org.neo4j.cypher.internal.util.DeprecatedDatabaseNameNotification
import org.neo4j.cypher.internal.util.HomeDatabaseNotPresent
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.StringType
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.COMPOSITE_DATABASE_LABEL
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_NAME_LABEL
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_NAME_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DEFAULT_NAMESPACE
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DISPLAY_NAME_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.NAMESPACE_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.NAME_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.TARGETS_RELATIONSHIP
import org.neo4j.exceptions.CypherExecutionException
import org.neo4j.exceptions.DatabaseAdministrationOnFollowerException
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.exceptions.ParameterNotFoundException
import org.neo4j.exceptions.ParameterWrongTypeException
import org.neo4j.graphdb.Direction
import org.neo4j.graphdb.Transaction
import org.neo4j.internal.helpers.collection.Iterators
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.api.exceptions.Status.HasStatus
import org.neo4j.kernel.api.exceptions.schema.UniquePropertyValueValidationException
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.neo4j.string.UTF8
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.ByteArray
import org.neo4j.values.storable.StringValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.Locale
import java.util.UUID

import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.util.Using

trait AdministrationCommandRuntime extends CypherRuntime[RuntimeContext] {
  override def correspondingRuntimeOption: Option[CypherRuntimeOption] = None

  def isApplicableAdministrationCommand(logicalPlan: LogicalPlan): Boolean
}

object AdministrationCommandRuntime {

  private[internal] val followerError = "Administration commands must be executed on the LEADER server."
  private val secureHasher = new SecureHasher
  private val internalPrefix: String = "__internal_"

  private[internal] def internalKey(name: String): String = internalPrefix + name

  private[internal] def validatePassword(password: Array[Byte])(config: Config): Array[Byte] = {
    if (password == null || password.length == 0) throw new InvalidArgumentException("A password cannot be empty.")

    val minimumPasswordLength = config.get(GraphDatabaseSettings.auth_minimum_password_length)
    val cb = StandardCharsets.UTF_8.decode(ByteBuffer.wrap(password))
    try {
      if (cb.codePoints.count < minimumPasswordLength)
        throw new InvalidArgumentException(s"A password must be at least $minimumPasswordLength characters.")
      password
    } finally {
      for (i <- 0 until cb.length) cb.put(i, '0')
    }
  }

  protected def hashPassword(initialPassword: Array[Byte]): TextValue = {
    try {
      Values.utf8Value(SystemGraphCredential.createCredentialForPassword(initialPassword, secureHasher).serialize())
    } finally {
      // TODO: Make this work again (we have places we need this un-zero'd later for checking if password is duplicated)
      // if (initialPassword != null) java.util.Arrays.fill(initialPassword, 0.toByte)
    }
  }

  protected def validateAndFormatEncryptedPassword(password: Array[Byte]): TextValue =
    try {
      Values.utf8Value(SystemGraphCredential.serialize(password))
    } catch {
      case e: IllegalArgumentException => throw new InvalidArgumentException(e.getMessage, e)
    }

  protected case class PasswordExpression(
    key: String,
    value: Value,
    bytesKey: String,
    bytesValue: Value,
    mapValueConverter: (Transaction, MapValue) => MapValue
  )

  private[internal] def getPasswordExpression(
    password: expressions.Expression,
    isEncryptedPassword: Boolean,
    otherParams: Array[String]
  )(config: Config): PasswordExpression =
    password match {
      case parameterPassword: Parameter =>
        validateStringParameterType(parameterPassword)

        // make sure we get a unique parameter name so we don't overwrite other parameters
        val hashedPwKey = ensureUniqueParamName(internalKey(parameterPassword.name) + "_hashed", otherParams)
        val passwordByteKey = ensureUniqueParamName(internalKey(parameterPassword.name) + "_bytes", otherParams)

        def convertPasswordParameters(params: MapValue): MapValue = {
          val encodedPassword = getValidPasswordParameter(params, parameterPassword.name)
          val hashedPassword =
            if (isEncryptedPassword) validateAndFormatEncryptedPassword(encodedPassword)
            else hashPassword(validatePassword(encodedPassword)(config))
          params.updatedWith(hashedPwKey, hashedPassword).updatedWith(
            passwordByteKey,
            Values.byteArray(encodedPassword)
          )
        }
        PasswordExpression(
          hashedPwKey,
          Values.NO_VALUE,
          passwordByteKey,
          Values.NO_VALUE,
          (_, params) => convertPasswordParameters(params)
        )

      case _ => throw new IllegalStateException(s"Internal error when processing password.")
    }

  private[internal] def getValidPasswordParameter(params: MapValue, passwordParameter: String): Array[Byte] = {
    params.get(passwordParameter) match {
      case bytes: ByteArray =>
        bytes.asObject() // Have as few copies of the password in memory as possible
      case s: StringValue =>
        UTF8.encode(s.stringValue()) // User parameters have String type
      case Values.NO_VALUE =>
        throw new ParameterNotFoundException(s"Expected parameter(s): $passwordParameter")
      case other =>
        throw new ParameterWrongTypeException(
          s"Expected password parameter $$$passwordParameter to have type String but was ${other.getTypeName}"
        )
    }
  }

  private[internal] def ensureUniqueParamName(originalName: String, otherParams: Array[String]): String = {
    var uniqueName = originalName
    val params: Seq[String] = otherParams.sorted
    for (otherParamName <- params) {
      if (otherParamName.equals(uniqueName))
        uniqueName = uniqueName + "_"
    }
    uniqueName
  }

  private[internal] def validateStringParameterType(param: Parameter): Unit = {
    param.parameterType match {
      case _: StringType =>
      case _ => throw new ParameterWrongTypeException(
          s"Only $CTString values are accepted as password, got: " + param.parameterType
        )
    }
  }

  private[internal] def makeCreateUserExecutionPlan(
    userName: Either[String, Parameter],
    isEncryptedPassword: Boolean,
    password: expressions.Expression,
    requirePasswordChange: Boolean,
    suspended: Boolean,
    defaultDatabase: Option[HomeDatabaseAction] = None
  )(
    sourcePlan: Option[ExecutionPlan],
    normalExecutionEngine: ExecutionEngine,
    securityAuthorizationHandler: SecurityAuthorizationHandler,
    config: Config
  ): ExecutionPlan = {
    val passwordChangeRequiredKey = internalKey("passwordChangeRequired")
    val suspendedKey = internalKey("suspended")
    val uuidKey = internalKey("uuid")
    val homeDatabaseFields = defaultDatabase.map {
      case RemoveHomeDatabaseAction => NameFields(s"${internalPrefix}homeDatabase", Values.NO_VALUE, IdentityConverter)
      case SetHomeDatabaseAction(name) =>
        getNameFields("homeDatabase", name.asLegacyName, s => new NormalizedDatabaseName(s).name())
    }
    val userNameFields = getNameFields("username", userName)
    val nonPasswordParameterNames = Array(
      userNameFields.nameKey,
      uuidKey,
      passwordChangeRequiredKey,
      suspendedKey
    ) ++ homeDatabaseFields.map(_.nameKey)
    val credentials = getPasswordExpression(password, isEncryptedPassword, nonPasswordParameterNames)(config)
    val homeDatabaseCypher = homeDatabaseFields.map(ddf => s", homeDatabase: $$`${ddf.nameKey}`").getOrElse("")
    val parameterTransformer = ParameterTransformer()
      .convert(userNameFields.nameConverter)
      .optionallyConvert(homeDatabaseFields.map(_.nameConverter))
      .convert(credentials.mapValueConverter)
      .validate(isHomeDatabasePresent(homeDatabaseFields))
    UpdatingSystemCommandExecutionPlan(
      "CreateUser",
      normalExecutionEngine,
      securityAuthorizationHandler,
      // NOTE: If username already exists we will violate a constraint
      s"""CREATE (u:User {name: $$`${userNameFields.nameKey}`, id: $$`$uuidKey`, credentials: $$`${credentials.key}`,
         |passwordChangeRequired: $$`$passwordChangeRequiredKey`, suspended: $$`$suspendedKey`
         |$homeDatabaseCypher })
         |RETURN u.name""".stripMargin,
      VirtualValues.map(
        Array(credentials.key, credentials.bytesKey) ++ nonPasswordParameterNames,
        Array[AnyValue](
          credentials.value,
          credentials.bytesValue,
          userNameFields.nameValue,
          Values.utf8Value(UUID.randomUUID().toString),
          Values.booleanValue(requirePasswordChange),
          Values.booleanValue(suspended)
        ) ++ homeDatabaseFields.map(_.nameValue)
      ),
      QueryHandler
        .handleNoResult(params =>
          Some(ThrowException(
            new CypherExecutionException(
              s"Failed to create the specified user '${runtimeStringValue(userName, params)}'."
            )
          ))
        )
        .handleError((error, params) =>
          (error, error.getCause) match {
            case (_, _: UniquePropertyValueValidationException) =>
              new InvalidArgumentException(
                s"Failed to create the specified user '${runtimeStringValue(userName, params)}': User already exists.",
                error
              )
            case (e: HasStatus, _) if e.status() == Status.Cluster.NotALeader =>
              new DatabaseAdministrationOnFollowerException(
                s"Failed to create the specified user '${runtimeStringValue(userName, params)}': $followerError",
                error
              )
            case _ => new CypherExecutionException(
                s"Failed to create the specified user '${runtimeStringValue(userName, params)}'.",
                error
              )
          }
        ),
      sourcePlan,
      initAndFinally = InitAndFinallyFunctions(
        initFunction = params => NameValidator.assertValidUsername(runtimeStringValue(userName, params)),
        finallyFunction = p => p.get(credentials.bytesKey).asInstanceOf[ByteArray].zero()
      ),
      parameterTransformer = parameterTransformer
    )
  }

  private[internal] def makeAlterUserExecutionPlan(
    userName: Either[String, Parameter],
    isEncryptedPassword: Option[Boolean],
    password: Option[expressions.Expression],
    requirePasswordChange: Option[Boolean],
    suspended: Option[Boolean],
    homeDatabase: Option[HomeDatabaseAction] = None
  )(
    sourcePlan: Option[ExecutionPlan],
    normalExecutionEngine: ExecutionEngine,
    securityAuthorizationHandler: SecurityAuthorizationHandler,
    config: Config
  ): ExecutionPlan = {
    val userNameFields = getNameFields("username", userName)
    val homeDatabaseFields = homeDatabase.map {
      case RemoveHomeDatabaseAction => NameFields(s"${internalPrefix}homeDatabase", Values.NO_VALUE, IdentityConverter)
      case SetHomeDatabaseAction(name) =>
        getNameFields("homeDatabase", name.asLegacyName, s => new NormalizedDatabaseName(s).name())
    }
    val nonPasswordParameterNames = Array(userNameFields.nameKey) ++ homeDatabaseFields.map(_.nameKey)
    val maybePw =
      password.map(p =>
        getPasswordExpression(p, isEncryptedPassword.getOrElse(false), nonPasswordParameterNames)(config)
      )
    val params = Seq(
      maybePw -> "credentials",
      requirePasswordChange -> "passwordChangeRequired",
      suspended -> "suspended",
      homeDatabaseFields -> "homeDatabase"
    ).flatMap { param =>
      param._1 match {
        case None                    => Seq.empty
        case Some(boolExpr: Boolean) => Seq((param._2, internalKey(param._2), Values.booleanValue(boolExpr)))
        case Some(passwordExpression: PasswordExpression) =>
          Seq((param._2, passwordExpression.key, passwordExpression.value))
        case Some(nameFields: NameFields) => Seq((param._2, nameFields.nameKey, nameFields.nameValue))
        case Some(p) => throw new InvalidArgumentException(
            s"Invalid option type for ALTER USER, expected PasswordExpression, Boolean, String or Parameter but got: ${p.getClass.getSimpleName}"
          )
      }
    }
    val (query, keys, values) = params.foldLeft((
      s"MATCH (user:User {name: $$`${userNameFields.nameKey}`}) WITH user, user.credentials AS oldCredentials",
      Seq.empty[String],
      Seq.empty[Value]
    )) { (acc, param) =>
      val propertyName: String = param._1
      val key: String = param._2
      val value: Value = param._3
      (acc._1 + s" SET user.$propertyName = $$`$key`", acc._2 :+ key, acc._3 :+ value)
    }
    val parameterKeys: Seq[String] = (keys ++ maybePw.map(_.bytesKey).toSeq) :+ userNameFields.nameKey
    val parameterValues: Seq[Value] = (values ++ maybePw.map(_.bytesValue).toSeq) :+ userNameFields.nameValue
    val parameterTransformer = ParameterTransformer()
      .convert(userNameFields.nameConverter)
      .optionallyConvert(homeDatabaseFields.map(_.nameConverter))
      .optionallyConvert(maybePw.map(_.mapValueConverter))
      .validate(isHomeDatabasePresent(homeDatabaseFields))
    UpdatingSystemCommandExecutionPlan(
      "AlterUser",
      normalExecutionEngine,
      securityAuthorizationHandler,
      s"$query RETURN oldCredentials",
      VirtualValues.map(parameterKeys.toArray, parameterValues.toArray),
      QueryHandler
        .handleNoResult(p =>
          Some(ThrowException(new InvalidArgumentException(
            s"Failed to alter the specified user '${runtimeStringValue(userName, p)}': User does not exist."
          )))
        )
        .handleError {
          case (error: HasStatus, p) if error.status() == Status.Cluster.NotALeader =>
            new DatabaseAdministrationOnFollowerException(
              s"Failed to alter the specified user '${runtimeStringValue(userName, p)}': $followerError",
              error
            )
          case (error, p) => new CypherExecutionException(
              s"Failed to alter the specified user '${runtimeStringValue(userName, p)}'.",
              error
            )
        }
        .handleResult((_, value, p) =>
          maybePw.map { newPw =>
            val oldCredentials =
              SystemGraphCredential.deserialize(value.asInstanceOf[TextValue].stringValue(), secureHasher)
            val newValue = p.get(newPw.bytesKey).asInstanceOf[ByteArray].asObject()
            if (oldCredentials.matchesPassword(newValue))
              ThrowException(new InvalidArgumentException(
                s"Failed to alter the specified user '${runtimeStringValue(userName, p)}': Old password and new password cannot be the same."
              ))
            else
              Continue
          }.getOrElse(Continue)
        ),
      sourcePlan,
      initAndFinally = InitAndFinallyFunctions(finallyFunction =
        p => maybePw.foreach(newPw => p.get(newPw.bytesKey).asInstanceOf[ByteArray].zero())
      ),
      parameterTransformer =
        parameterTransformer
    )
  }

  private def isHomeDatabasePresent(homeDatabaseFields: Option[NameFields])(
    tx: Transaction,
    params: MapValue
  ): (MapValue, Set[InternalNotification]) =
    homeDatabaseFields.map(ddf => {
      params.get(ddf.nameKey) match {
        case tv: TextValue =>
          val notifications: Set[InternalNotification] =
            if (Iterators.asList(tx.findNodes(DATABASE_NAME_LABEL, DISPLAY_NAME_PROPERTY, tv.stringValue())).isEmpty) {
              Set(HomeDatabaseNotPresent(tv.stringValue()))
            } else {
              Set.empty
            }
          (params, notifications)
        case _ => (params, Set.empty[InternalNotification])
      }
    }).getOrElse((params, Set.empty))

  private[internal] def makeRenameExecutionPlan(
    entity: String,
    fromName: Either[String, Parameter],
    toName: Either[String, Parameter],
    initFunction: MapValue => Boolean
  )(
    sourcePlan: Option[ExecutionPlan],
    normalExecutionEngine: ExecutionEngine,
    securityAuthorizationHandler: SecurityAuthorizationHandler
  ): ExecutionPlan = {
    val fromNameFields = getNameFields("fromName", fromName)
    val toNameFields = getNameFields("toName", toName)

    val parameterTransformer = ParameterTransformer()
      .convert(fromNameFields.nameConverter)
      .convert(toNameFields.nameConverter)
    UpdatingSystemCommandExecutionPlan(
      s"Create$entity",
      normalExecutionEngine,
      securityAuthorizationHandler,
      s"""MATCH (old:$entity {name: $$`${fromNameFields.nameKey}`})
         |SET old.name = $$`${toNameFields.nameKey}`
         |RETURN old.name
        """.stripMargin,
      VirtualValues.map(
        Array(fromNameFields.nameKey, toNameFields.nameKey),
        Array(fromNameFields.nameValue, toNameFields.nameValue)
      ),
      QueryHandler
        .handleNoResult(p =>
          Some(ThrowException(new InvalidArgumentException(
            s"Failed to rename the specified ${entity.toLowerCase(Locale.ROOT)} '${runtimeStringValue(fromName, p)}' to " +
              s"'${runtimeStringValue(toName, p)}': The ${entity.toLowerCase(Locale.ROOT)} '${runtimeStringValue(fromName, p)}' does not exist."
          )))
        )
        .handleError((error, p) =>
          (error, error.getCause) match {
            case (_, _: UniquePropertyValueValidationException) =>
              new InvalidArgumentException(
                s"Failed to rename the specified ${entity.toLowerCase(Locale.ROOT)} '${runtimeStringValue(fromName, p)}' to " +
                  s"'${runtimeStringValue(toName, p)}': " + s"$entity '${runtimeStringValue(toName, p)}' already exists.",
                error
              )
            case (e: HasStatus, _) if e.status() == Status.Cluster.NotALeader =>
              new DatabaseAdministrationOnFollowerException(
                s"Failed to rename the specified ${entity.toLowerCase(Locale.ROOT)} '${runtimeStringValue(fromName, p)}': $followerError",
                error
              )
            case _ =>
              new CypherExecutionException(
                s"Failed to rename the specified ${entity.toLowerCase(Locale.ROOT)} '${runtimeStringValue(fromName, p)}' to '${runtimeStringValue(toName, p)}'.",
                error
              )
          }
        ),
      sourcePlan,
      initAndFinally = InitAndFinallyFunctions(initFunction = initFunction),
      parameterTransformer = parameterTransformer
    )
  }

  /**
   *
   * @param key parameter key used in the "inner" cypher
   * @param name the literal or parameter
   * @param valueMapper function to apply to the value
   * @return
   */
  private[internal] def getNameFields(
    key: String,
    name: Either[String, Parameter],
    valueMapper: String => String = identity
  ): NameFields = name match {
    case Left(u) =>
      NameFields(internalKey(key), Values.utf8Value(valueMapper(u)), IdentityConverter)
    case Right(parameter) =>
      def rename: String => String = paramName => internalKey(paramName)
      NameFields(
        rename(parameter.name),
        Values.NO_VALUE,
        RenamingStringParameterConverter(
          parameter.name,
          rename,
          { v => Values.utf8Value(valueMapper(v.stringValue())) }
        )
      )
  }

  /**
   *
   * @param nameKey parameter key used in the "inner" cypher
   * @param name the namespaced name or parameter
   * @return
   */
  private[internal] def getDatabaseNameFields(
    nameKey: String,
    name: DatabaseName
  ): DatabaseNameFields = {
    val valueMapper: String => String = new NormalizedDatabaseName(_).name()
    name match {
      case name @ NamespacedName(_, None) =>
        DatabaseNameFields(
          s"$internalPrefix$nameKey",
          Values.utf8Value(valueMapper(name.name)),
          s"$internalPrefix${nameKey}_namespace",
          Values.utf8Value(DEFAULT_NAMESPACE),
          s"$internalPrefix${nameKey}_displayName",
          Values.utf8Value(valueMapper(name.name)),
          wasParameter = false,
          IdentityConverter
        )
      case name @ NamespacedName(_, Some(namespace)) =>
        DatabaseNameFields(
          s"$internalPrefix$nameKey",
          Values.utf8Value(valueMapper(name.name)),
          s"$internalPrefix${nameKey}_namespace",
          Values.utf8Value(valueMapper(namespace)),
          s"$internalPrefix${nameKey}_displayName",
          Values.utf8Value(
            if (namespace == DEFAULT_NAMESPACE) valueMapper(name.name)
            else valueMapper(namespace) + "." + valueMapper(name.name)
          ),
          wasParameter = false,
          IdentityConverter
        )
      case pn @ ParameterName(parameter) =>
        def rename: String => String = paramName => internalKey(paramName)
        val displayNameKey = internalKey(parameter.name + "_displayName")
        DatabaseNameFields(
          rename(parameter.name),
          Values.NO_VALUE,
          internalKey(parameter.name + "_namespace"),
          Values.utf8Value(DEFAULT_NAMESPACE),
          displayNameKey,
          Values.NO_VALUE,
          wasParameter = true,
          (_, params) => {
            val (namespace, name, displayName) = pn.getNameParts(params, DEFAULT_NAMESPACE)
            params.updatedWith(
              internalKey(parameter.name + "_namespace"),
              Values.utf8Value(valueMapper(namespace.getOrElse(DEFAULT_NAMESPACE)))
            )
              .updatedWith(internalKey(parameter.name), Values.utf8Value(valueMapper(name)))
              .updatedWith(displayNameKey, Values.utf8Value(valueMapper(displayName)))
          }
        )
    }
  }

  private[internal] def runtimeStringValue(field: DatabaseName, params: MapValue): String = field match {
    case n: NamespacedName => n.toString
    case ParameterName(p)  => runtimeStringValue(p.name, params)
  }

  private[internal] def runtimeStringValue(field: Either[String, Parameter], params: MapValue): String = field match {
    case Left(s)  => s
    case Right(p) => runtimeStringValue(p.name, params)
  }

  private[internal] def runtimeStringValue(field: Expression, params: MapValue): String = ({
    case StringLiteral(s) => s
    case p: Parameter     => runtimeStringValue(p.name, params)
  }: PartialFunction[Expression, String]).apply(field)

  private[internal] def runtimeStringValue(parameter: String, params: MapValue): String = {
    val value: AnyValue =
      if (params.containsKey(parameter))
        params.get(parameter)
      else
        params.get(internalKey(parameter))
    value match {
      case tv: TextValue => tv.stringValue()
      case _ =>
        throw new ParameterWrongTypeException(s"Expected parameter $$$parameter to have type String but was $value")
    }
  }

  case class RenamingStringParameterConverter(
    parameter: String,
    rename: String => String = identity,
    valueMapper: TextValue => TextValue = identity
  ) extends ((Transaction, MapValue) => MapValue) {

    def apply(transaction: Transaction, params: MapValue): MapValue = {
      val paramValue = params.get(parameter)
      // Check the parameter is actually the expected type
      if (!paramValue.isInstanceOf[TextValue]) {
        throw new ParameterWrongTypeException(
          s"Expected parameter $$$parameter to have type String but was $paramValue"
        )
      } else params.updatedWith(rename(parameter), valueMapper(params.get(parameter).asInstanceOf[TextValue]))
    }
  }

  case object IdentityConverter extends ((Transaction, MapValue) => MapValue) {
    def apply(transaction: Transaction, map: MapValue): MapValue = map
  }

  trait NameConverter {
    val nameConverter: (Transaction, MapValue) => MapValue
  }

  case class NameFields(
    nameKey: String,
    nameValue: Value,
    override val nameConverter: (Transaction, MapValue) => MapValue
  ) extends NameConverter

  case class DatabaseNameFields(
    nameKey: String,
    nameValue: Value,
    namespaceKey: String,
    namespaceValue: Value,
    displayNameKey: String,
    displayNameValue: Value,
    wasParameter: Boolean,
    override val nameConverter: (Transaction, MapValue) => MapValue
  ) extends NameConverter {

    val keys: Array[String] = Array(nameKey, namespaceKey, displayNameKey)
    val values: Array[AnyValue] = Array(nameValue, namespaceValue, displayNameValue)

    def asNodeFilter: String = s"{$NAME_PROPERTY: $$`$nameKey`, $NAMESPACE_PROPERTY: $$`$namespaceKey`}"

  }

  type Show[T] = (T, MapValue) => String

  object Show {
    implicit val showDatabaseName: Show[DatabaseName] = (databaseName, p) => runtimeStringValue(databaseName, p)
    implicit val showString: Show[Either[String, Parameter]] = (s, p) => runtimeStringValue(s, p)
  }

  /*
   * This is a bit of a kludge to get around database names being ambiguous in 5.0 for backward
   * compatibility. We assume that 'db.name' means 'name' in composite 'db' but in case db does
   * not exist we need to rewrite the parameters to mean 'db.name' in the default namespace. Also flag
   * this usage as deprecated.
   */
  def checkNamespaceExists(aliasNameFields: DatabaseNameFields)(
    tx: Transaction,
    params: MapValue
  ): (MapValue, Set[InternalNotification]) = {

    def paramString(key: String) = params.get(key).asInstanceOf[StringValue].stringValue()

    if (paramString(aliasNameFields.namespaceKey) != DEFAULT_NAMESPACE) {
      // Check to see if there is a composite database node for this alias
      // MATCH (dbname:DatabaseName{name: name})-[:TARGETS]->(:CompositeDatabase) WHERE dbname.namespace = namespace
      val compositeDatabaseExists = Using.resource(tx.findNodes(
        DATABASE_NAME_LABEL,
        DATABASE_NAME_PROPERTY,
        paramString(aliasNameFields.namespaceKey)
      )) { nodes =>
        nodes.asScala.exists(n =>
          n.getProperty(NAMESPACE_PROPERTY).equals(DEFAULT_NAMESPACE) && n.getSingleRelationship(
            TARGETS_RELATIONSHIP,
            Direction.OUTGOING
          ).getEndNode.hasLabel(COMPOSITE_DATABASE_LABEL)
        )
      }
      if (!compositeDatabaseExists) {
        val aliasName = paramString(aliasNameFields.namespaceKey) + "." + paramString(aliasNameFields.nameKey)
        // This is just a regular local alias with . in the name, so use the default namespace
        (
          params.updatedWith(
            aliasNameFields.nameKey,
            Values.utf8Value(aliasName)
          )
            .updatedWith(aliasNameFields.namespaceKey, Values.utf8Value(DEFAULT_NAMESPACE)),
          if (aliasNameFields.wasParameter) Set.empty else Set(DeprecatedDatabaseNameNotification(aliasName, None))
        )
      } else {
        (params, Set.empty)
      }
    } else {
      (params, Set.empty)
    }
  }
}
