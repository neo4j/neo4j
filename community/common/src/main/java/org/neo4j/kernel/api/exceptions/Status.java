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
package org.neo4j.kernel.api.exceptions;

import static java.lang.String.format;
import static org.neo4j.kernel.api.exceptions.Status.Classification.ClientError;
import static org.neo4j.kernel.api.exceptions.Status.Classification.ClientNotification;
import static org.neo4j.kernel.api.exceptions.Status.Classification.DatabaseError;
import static org.neo4j.kernel.api.exceptions.Status.Classification.TransientError;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import org.neo4j.annotations.api.PublicApi;

/**
 * This is the codification of all available surface-api status codes. If you are throwing an error to a user through
 * one of the key APIs, you should opt for using or adding an error code here.
 * <br>
 * Each {@link Status} has an associated category, represented by the inner enums in this class.
 * Each {@link Status} also has an associated {@link Classification} which defines meta-data about the code, such
 * as if the error was caused by a user or the database (and later on if the code denotes an error or merely a warning).
 * <br>
 * This class is not part of the public Neo4j API, and backwards compatibility for using it as a Java class is not
 * guaranteed. Instead, the automatically generated documentation derived from this class and available in the Neo4j
 * manual should be considered a user-level API.
 */
@PublicApi
public interface Status {
    /*

    On naming...

    These are public status codes and users' error handling will rely on the precise names. Therefore, please take
    care over categorisation and classification and make sure not to introduce duplicates.

    Broadly, the naming convention here uses one of three types of name:

    1. For unexpected events, name with a leading noun followed by a short problem term in the past tense. For example,
       EntityNotFound or LeaseExpired. As a variant, names may omit the leading noun; in this case, the current
       ongoing operation is implied.

    2. For conditions that prevent the current ongoing operation from being performed (or being performed correctly),
       start with a leading noun (as above) and follow with an adjective. For example, DatabaseUnavailable. The
       leading noun may again be omitted and additionally a clarifying suffix may be added. For example,
       ForbiddenOnReadOnlyDatabase.

    3. For more general errors which have a well-understood or generic term available, the form XxxError may be used.
       For example, SyntaxError or TokenNameError.

    Where possible, evaluate naming decisions based on the order of the items above. Therefore, if it is possible to
    provide a type (1) name, do so, otherwise fall back to type (2) or type (3)

    Side note about HTTP: where possible, borrow words or terms from HTTP status codes. Be careful to make sure these
    use a similar meaning however as a major benefit of doing this is to ease communication of concepts to users.

    If you are unsure, please contact the Driver Team.

    */

    // TODO: rework the names and uses of Invalid and InvalidFormat and reconsider their categorisation (ClientError
    // TODO: MUST be resolvable by the user, do we need ProtocolError/DriverError?)
    enum Request implements Status {
        // client
        Invalid(
                ClientError, // TODO: see above
                "The client provided an invalid request."),
        InvalidFormat(
                ClientError, // TODO: see above
                "The client provided a request that was missing required fields, or had values that are not allowed."),
        InvalidUsage(
                ClientError, // TODO: see above
                "The client made a request but did not consume outgoing buffers in a timely fashion."),
        DeprecatedFormat(
                ClientNotification,
                "The client made a request for a format which has been deprecated.",
                SeverityLevel.WARNING,
                NotificationCategory.DEPRECATION),
        NoThreadsAvailable(
                TransientError, // TODO: see above
                "There are no available threads to serve this request at the moment. You can retry at a later time "
                        + "or consider increasing max thread pool size for bolt connector(s)."),
        ResourceExhaustion(
                TransientError,
                "The server has rejected this request as a resource is exhausted at the moment. "
                        + "You can retry at a later time. For further details see server logs.");

        private final Code code;

        @Override
        public Code code() {
            return code;
        }

        Request(Classification classification, String description) {
            this.code = new Code(classification, this, description);
        }

        Request(
                Classification classification,
                String description,
                SeverityLevel severity,
                NotificationCategory category) {
            this.code = new NotificationCode(classification, this, description, severity, category);
        }
    }

    enum Transaction implements Status {
        // client errors
        TransactionNotFound(ClientError, "The request referred to a transaction that does not exist."),
        TransactionAccessedConcurrently(
                ClientError, "There were concurrent requests accessing the same transaction, which is not allowed."),
        ForbiddenDueToTransactionType(
                ClientError,
                "The transaction is of the wrong type to service the request. For instance, a transaction that has "
                        + "had schema modifications performed in it cannot be used to subsequently perform data operations, "
                        + "and vice versa."),
        TransactionValidationFailed(ClientError, "Transaction changes did not pass validation checks"),
        TransactionHookFailed(ClientError, "Transaction hook failure."),
        TransactionMarkedAsFailed(
                ClientError,
                "Transaction was marked as both successful and failed. Failure takes precedence and so this "
                        + "transaction was rolled back although it may have looked like it was going to be committed"),
        TransactionTimedOut(
                ClientError,
                "The transaction has not completed within the specified timeout (db.transaction.timeout). You may want to retry with a longer "
                        + "timeout."),
        TransactionTimedOutClientConfiguration(
                ClientError,
                "The transaction has not completed within the timeout specified at its start by the client. You may want to retry with a longer "
                        + "timeout."),
        InvalidBookmark(
                ClientError,
                "Supplied bookmark cannot be interpreted. You should only supply a bookmark that was "
                        + "previously generated by Neo4j. Maybe you have generated your own bookmark, "
                        + "or modified a bookmark since it was generated by Neo4j."),
        InvalidBookmarkMixture(
                ClientError,
                "Mixing bookmarks generated by different databases is forbidden."
                        + "You should only chain bookmarks that are generated from the same database. "
                        + "You may however chain bookmarks generated from system database with bookmarks from another database."),
        Terminated(ClientError, "Explicitly terminated by the user."),
        LockAcquisitionTimeout(
                TransientError, "Unable to acquire lock within configured timeout (db.lock.acquisition.timeout)."),

        // database errors
        TransactionStartFailed(DatabaseError, "The database was unable to start the transaction."),
        TransactionTerminationFailed(DatabaseError, "The database was unable to terminate the transaction."),
        TransactionRollbackFailed(DatabaseError, "The database was unable to roll back the transaction."),
        TransactionCommitFailed(DatabaseError, "The database was unable to commit the transaction."),
        TransactionLogError(DatabaseError, "The database was unable to write transaction to log."),
        LinkedTransactionError(
                DatabaseError,
                "The transaction was terminated because another transaction executing the same query encountered an error."),

        // transient errors
        BookmarkTimeout(TransientError, "Bookmark wait timed out. Database has not reached the specified version"),
        LeaseExpired(TransientError, "The lease under which this transaction was started is no longer valid."),
        DeadlockDetected(
                TransientError,
                "This transaction, and at least one more transaction, has acquired locks in a way that it will wait "
                        + "indefinitely, and the database has aborted it. Retrying this transaction will most likely be "
                        + "successful."),
        ConstraintsChanged(TransientError, "Database constraints changed since the start of this transaction"),
        Outdated(
                TransientError,
                "Transaction has seen state which has been invalidated by applied updates while "
                        + "transaction was active. Transaction may succeed if retried."),
        LockClientStopped(
                ClientError,
                "The transaction has been terminated, so no more locks can be acquired. This can occur because the "
                        + "transaction ran longer than the configured transaction timeout, or because a human operator manually "
                        + "terminated the transaction, or because the database is shutting down."),
        MaximumTransactionLimitReached(
                TransientError,
                "Unable to start new transaction since the maximum number of concurrently executing transactions is "
                        + "reached (db.transaction.concurrent.maximum). "
                        + "You can retry at a later time or consider increasing allowed maximum of concurrent transactions."),
        Interrupted(TransientError, "Interrupted while waiting."),
        LeaderSwitch(TransientError, "The request could not be completed due to cluster leader switch"),
        QueryExecutionFailedOnTransaction(
                TransientError, "The transaction was marked as failed because a query failed.");

        private final Code code;

        @Override
        public Code code() {
            return code;
        }

        Transaction(Classification classification, String description) {
            this.code = new Code(classification, this, description);
        }
    }

    enum Statement implements Status {
        // client errors
        SyntaxError(ClientError, "The statement contains invalid or unsupported syntax."),
        SemanticError(
                ClientError,
                "The statement is syntactically valid, but expresses something that the database cannot do."),
        ParameterMissing(ClientError, "The statement refers to a parameter that was not provided in the request."),
        ParameterNotProvided(
                ClientNotification,
                "The statement refers to a parameter that was not provided in the request.",
                SeverityLevel.WARNING,
                NotificationCategory.GENERIC),
        ConstraintVerificationFailed(
                ClientError, "A constraint imposed by the statement is violated by the data in the database."),
        EntityNotFound(ClientError, "The statement refers to a non-existent entity."),
        PropertyNotFound(ClientError, "The statement refers to a non-existent property."),
        TypeError(
                ClientError,
                "The statement is attempting to perform operations on values with types that are not supported by "
                        + "the operation."),
        ArgumentError(ClientError, "The statement is attempting to perform operations using invalid arguments"),
        ArithmeticError(ClientError, "Invalid use of arithmetic, such as dividing by zero."),
        RuntimeUnsupportedError(ClientError, "This query is not supported by the chosen runtime."),
        NotSystemDatabaseError(
                ClientError,
                "This is an administration command and it should be executed against the system database."),
        InvalidTargetDatabaseError(ClientError, "The specified database is not a valid target for this command."),
        AccessMode(ClientError, "The request could not be completed due to access mode violation"),
        UnsupportedOperationError(
                ClientError, "This query performed an operation that is not supported in this context."),
        ExternalResourceFailed(ClientError, "Access to an external resource failed"),
        UnsatisfiableRelationshipTypeExpression(
                ClientNotification,
                "The query contains a relationship type expression that cannot be satisfied.",
                SeverityLevel.WARNING,
                NotificationCategory.GENERIC),
        RepeatedRelationshipReference(
                ClientNotification,
                "The query returns no results due to repeated references to a relationship.",
                SeverityLevel.WARNING,
                NotificationCategory.GENERIC),
        RemoteExecutionClientError(
                ClientError,
                "The database was unable to execute a remote part of the statement due to a client error."),

        // database errors
        ExecutionFailed(DatabaseError, "The database was unable to execute the statement."),
        CodeGenerationFailed(
                ClientNotification,
                "The database was unable to generate code for the query. A stacktrace can be found in the debug.log.",
                SeverityLevel.INFORMATION,
                NotificationCategory.PERFORMANCE),
        RemoteExecutionFailed(DatabaseError, "The database was unable to execute a remote part of the statement."),

        // transient errors
        ExecutionTimeout(TransientError, "The database was unable to execute the statement in a timely fashion."),
        RemoteExecutionTransientError(
                TransientError,
                "The database was unable to execute a remote part of the statement due to a transient failure."),

        // client notifications (performance)
        CartesianProduct(
                ClientNotification,
                "This query builds a cartesian product between disconnected patterns.",
                SeverityLevel.INFORMATION,
                NotificationCategory.PERFORMANCE),
        DynamicProperty(
                ClientNotification,
                "Queries using dynamic properties will use neither index seeks nor index scans for those properties",
                SeverityLevel.INFORMATION,
                NotificationCategory.PERFORMANCE),
        EagerOperator(
                ClientNotification,
                "The execution plan for this query contains the Eager operator, which forces all dependent data to "
                        + "be materialized in main memory before proceeding",
                SeverityLevel.INFORMATION,
                NotificationCategory.PERFORMANCE),
        JoinHintUnfulfillableWarning(
                ClientNotification,
                "The database was unable to plan a hinted join.",
                SeverityLevel.WARNING,
                NotificationCategory.HINT),
        NoApplicableIndex(
                ClientNotification,
                "Adding a schema index may speed up this query.",
                SeverityLevel.INFORMATION,
                NotificationCategory.PERFORMANCE),
        @Deprecated
        SuboptimalIndexForWildcardQuery(
                ClientNotification,
                "Index cannot execute wildcard query efficiently",
                SeverityLevel.INFORMATION,
                NotificationCategory.PERFORMANCE),
        UnboundedVariableLengthPattern(
                ClientNotification,
                "The provided pattern is unbounded, consider adding an upper limit to the number of node hops.",
                SeverityLevel.INFORMATION,
                NotificationCategory.PERFORMANCE),
        ExhaustiveShortestPath(
                ClientNotification,
                "Exhaustive shortest path has been planned for your query that means that shortest path graph "
                        + "algorithm might not be used to find the shortest path. Hence an exhaustive enumeration of all paths "
                        + "might be used in order to find the requested shortest path.",
                SeverityLevel.INFORMATION,
                NotificationCategory.PERFORMANCE),

        @Deprecated
        SideEffectVisibility(
                ClientNotification,
                "Using a subquery expression within a mutating statement has implications for its side-effect visibility",
                SeverityLevel.WARNING,
                NotificationCategory.DEPRECATION),

        // client notifications (not supported/deprecated)
        RuntimeUnsupportedWarning(
                ClientNotification,
                "This query is not supported by the chosen runtime.",
                SeverityLevel.WARNING,
                NotificationCategory.UNSUPPORTED),
        FeatureDeprecationWarning(
                ClientNotification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                NotificationCategory.DEPRECATION),
        @Deprecated
        RuntimeExperimental(
                ClientNotification,
                "This feature is experimental and should not be used in production systems.",
                SeverityLevel.WARNING,
                NotificationCategory.UNSUPPORTED),
        UnsupportedAdministrationCommand(ClientError, "This administration command is not supported."),

        // client notifications (unknown tokens)
        UnknownLabelWarning(
                ClientNotification,
                "The provided label is not in the database.",
                SeverityLevel.WARNING,
                NotificationCategory.UNRECOGNIZED),
        UnknownRelationshipTypeWarning(
                ClientNotification,
                "The provided relationship type is not in the database.",
                SeverityLevel.WARNING,
                NotificationCategory.UNRECOGNIZED),
        UnknownPropertyKeyWarning(
                ClientNotification,
                "The provided property key is not in the database",
                SeverityLevel.WARNING,
                NotificationCategory.UNRECOGNIZED),

        SubqueryVariableShadowing(
                ClientNotification,
                "Variable in subquery is shadowing a variable with the same name from the outer scope.",
                SeverityLevel.INFORMATION,
                NotificationCategory.GENERIC),
        // client notifications (runtime)
        AggregationSkippedNull(
                ClientNotification,
                "The query contains an aggregation function that skips null values.",
                SeverityLevel.WARNING,
                NotificationCategory.UNRECOGNIZED),
        ;

        private final Code code;

        @Override
        public Code code() {
            return code;
        }

        Statement(Classification classification, String description) {
            this.code = new Code(classification, this, description);
        }

        Statement(
                Classification classification,
                String description,
                SeverityLevel severity,
                NotificationCategory category) {
            this.code = new NotificationCode(classification, this, description, severity, category);
        }
    }

    enum Routing implements Status {
        DbmsInPanic(ClientError, "Server is in panic"),
        RoutingFailed(ClientError, "Failed to Route");

        private final Code code;

        Routing(Classification classification, String description) {
            this.code = new Code(classification, this, description);
        }

        @Override
        public Code code() {
            return code;
        }
    }

    enum Schema implements Status {
        // client errors
        RepeatedPropertyInCompositeSchema(
                ClientError, "Unable to create index or constraint because schema had a repeated property."),
        RepeatedLabelInSchema(ClientError, "Unable to create index or constraint because schema had a repeated label."),
        RepeatedRelationshipTypeInSchema(
                ClientError, "Unable to create index or constraint because schema had a repeated relationship type."),
        EquivalentSchemaRuleAlreadyExists(
                ClientError, "Unable to perform operation because an equivalent schema rule already exists."),
        ConstraintAlreadyExists(
                ClientError, "Unable to perform operation because it would clash with a pre-existing constraint."),
        ConstraintNotFound(
                ClientError, "The request (directly or indirectly) referred to a constraint that does not exist."),
        ConstraintValidationFailed(ClientError, "A constraint imposed by the database was violated."),
        ConstraintViolation(ClientError, "Added or changed index entry would violate constraint"),
        IndexAlreadyExists(
                ClientError, "Unable to perform operation because it would clash with a pre-existing index."),
        IndexNotFound(ClientError, "The request (directly or indirectly) referred to an index that does not exist."),
        IndexMultipleFound(
                ClientError,
                "The request referenced an index by its schema, and multiple matching indexes were found."),
        IndexNotApplicable(ClientError, "The request did not contain the properties required by the index."),
        IndexWithNameAlreadyExists(
                ClientError, "Unable to perform operation because index with given name already exists."),
        ConstraintWithNameAlreadyExists(
                ClientError, "Unable to perform operation because constraint with given name already exists."),
        ForbiddenOnConstraintIndex(
                ClientError,
                "A requested operation can not be performed on the specified index because the index is part of a "
                        + "constraint. If you want to drop the index, for instance, you must drop the constraint."),
        TokenNameError(
                ClientError,
                "A token name, such as a label, relationship type or property key, used is not valid. Tokens cannot "
                        + "be empty strings and cannot be null."),

        // client notifications
        HintedIndexNotFound(
                ClientNotification,
                "The request (directly or indirectly) referred to an index that does not exist.",
                SeverityLevel.WARNING,
                NotificationCategory.HINT),
        IndexOrConstraintAlreadyExists(
                ClientNotification, "`%s` has no effect.", SeverityLevel.INFORMATION, NotificationCategory.SCHEMA),
        IndexOrConstraintDoesNotExist(
                ClientNotification, "`%s` has no effect.", SeverityLevel.INFORMATION, NotificationCategory.SCHEMA),

        // database errors
        ConstraintCreationFailed(DatabaseError, "Creating a requested constraint failed."),
        ConstraintDropFailed(DatabaseError, "The database failed to drop a requested constraint."),
        IndexCreationFailed(DatabaseError, "Failed to create an index."),
        IndexDropFailed(DatabaseError, "The database failed to drop a requested index."),
        LabelAccessFailed(DatabaseError, "The request accessed a label that did not exist."),
        TokenLimitReached(
                DatabaseError,
                "The maximum number of tokens of this type has been reached, no more tokens of this type can be created."),
        PropertyKeyAccessFailed(DatabaseError, "The request accessed a property that does not exist."),
        RelationshipTypeAccessFailed(DatabaseError, "The request accessed a relationship type that does not exist."),
        SchemaRuleAccessFailed(DatabaseError, "The request referred to a schema rule that does not exist."),
        SchemaRuleDuplicateFound(
                DatabaseError, "The request referred to a schema rule that is defined multiple times.");

        private final Code code;

        @Override
        public Code code() {
            return code;
        }

        Schema(Classification classification, String description) {
            this.code = new Code(classification, this, description);
        }

        Schema(
                Classification classification,
                String description,
                SeverityLevel severity,
                NotificationCategory category) {
            this.code = new NotificationCode(classification, this, description, severity, category);
        }
    }

    enum Procedure implements Status {
        ProcedureRegistrationFailed(
                ClientError,
                "The database failed to register a procedure, refer to the associated error message for details."),
        ProcedureNotFound(
                ClientError,
                "A request referred to a procedure that is not registered with this database instance. If you are "
                        + "deploying custom procedures in a cluster setup, ensure all instances in the cluster have the "
                        + "procedure jar file deployed."),
        ProcedureCallFailed(
                ClientError, "Failed to invoke a procedure. See the detailed error description for exact cause."),
        TypeError(ClientError, "A procedure is using or receiving a value of an invalid type."),
        ProcedureTimedOut(
                ClientError,
                "The procedure has not completed within the specified timeout. You may want to retry with a longer "
                        + "timeout."),
        ProcedureWarning(
                ClientNotification,
                "The query used a procedure that generated a warning.",
                SeverityLevel.WARNING,
                NotificationCategory.GENERIC);

        private final Code code;

        @Override
        public Code code() {
            return code;
        }

        Procedure(Classification classification, String description) {
            this.code = new Code(classification, this, description);
        }

        Procedure(
                Classification classification,
                String description,
                SeverityLevel severity,
                NotificationCategory category) {
            this.code = new NotificationCode(classification, this, description, severity, category);
        }
    }

    enum Security implements Status {
        // client
        CredentialsExpired(ClientError, "The credentials have expired and need to be updated."),
        Unauthorized(ClientError, "The client is unauthorized due to authentication failure."),
        AuthenticationRateLimit(
                ClientError, "The client has provided incorrect authentication details too many times in a row."),
        ModifiedConcurrently(TransientError, "The user was modified concurrently to this request."),
        Forbidden(ClientError, "An attempt was made to perform an unauthorized action."),
        AuthorizationExpired(ClientError, "The stored authorization info has expired. Please reconnect."),
        AuthProviderTimeout(TransientError, "An auth provider request timed out."),
        AuthProviderFailed(TransientError, "An auth provider request failed."),
        TokenExpired(ClientError, "The auth provider token has expired"),

        // Administration command
        AuthProviderNotDefined(
                ClientNotification,
                "The auth provider is not defined.",
                SeverityLevel.INFORMATION,
                NotificationCategory.SECURITY),
        ExternalAuthNotEnabled(
                ClientNotification,
                "External auth for user is not enabled.",
                SeverityLevel.WARNING,
                NotificationCategory.SECURITY),
        CommandHasNoEffect(
                ClientNotification, "`%s` has no effect.", SeverityLevel.INFORMATION, NotificationCategory.SECURITY),
        ImpossibleRevokeCommand(
                ClientNotification, "`%s` has no effect.", SeverityLevel.WARNING, NotificationCategory.SECURITY);

        private final Code code;

        @Override
        public Code code() {
            return code;
        }

        Security(Classification classification, String description) {
            this.code = new Code(classification, this, description);
        }

        Security(
                Classification classification,
                String description,
                SeverityLevel severity,
                NotificationCategory category) {
            this.code = new NotificationCode(classification, this, description, severity, category);
        }
    }

    enum General implements Status {
        // client errors
        InvalidArguments(ClientError, "The request contained fields that were empty or are not allowed."),
        ForbiddenOnReadOnlyDatabase(
                ClientError, "This is a read only database, writing or modifying the database is not allowed."),
        WriteOnReadOnlyAccessDatabase(
                ClientError, "This database is in read-only mode, writing or modifying the database is not allowed."),
        TransactionOutOfMemoryError(
                ClientError,
                "The transaction used more memory than was allowed. The maximum allowed size for a "
                        + "transaction can be configured with 'db.memory.transaction.max' in the neo4j configuration "
                        + "(normally in 'conf/neo4j.conf' or, if you are using Neo4j Desktop, found through the user interface)."),
        UpgradeRequired(
                ClientError,
                "This transaction required database to be of a higher kernel version than it is. "
                        + "Make sure that dbms has been correctly upgraded before retrying this operation."),

        // database errors
        IndexCorruptionDetected(
                DatabaseError,
                "The request (directly or indirectly) referred to an index that is in a failed state. The index "
                        + "needs to be dropped and recreated manually."),
        SchemaCorruptionDetected(
                DatabaseError, "A malformed schema rule was encountered. Please contact your support representative."),
        StorageDamageDetected(DatabaseError, "Expected set of files not found on disk. Please restore from backup."),
        UnknownError(DatabaseError, "An unknown error occurred."),

        // transient errors

        // Off heap allocation limit exceeded
        TransactionMemoryLimit(
                TransientError,
                "There is not enough memory to perform the current task. Please try increasing "
                        + "'server.memory.off_heap.transaction_max_size' in the neo4j configuration (normally in 'conf/neo4j.conf' or, if "
                        + "you are using Neo4j Desktop, found through the user interface), and then restart the database."),
        OutOfMemoryError(
                TransientError,
                "There is not enough memory to perform the current task. Please try increasing "
                        + "'server.memory.heap.max_size' in the neo4j configuration (normally in 'conf/neo4j.conf' or, if "
                        + "you are using Neo4j Desktop, found through the user interface) or if you are running an embedded "
                        + "installation increase the heap by using '-Xmx' command line flag, and then restart the database."),
        StackOverFlowError(
                TransientError,
                "There is not enough stack size to perform the current task. This is generally considered to be a "
                        + "database error, so please contact Neo4j support. You could try increasing the stack size: "
                        + "for example to set the stack size to 2M, add `server.jvm.additional=-Xss2M' to "
                        + "in the neo4j configuration (normally in 'conf/neo4j.conf' or, if you are using "
                        + "Neo4j Desktop, found through the user interface) or if you are running an embedded installation "
                        + "just add -Xss2M as command line flag."),
        MemoryPoolOutOfMemoryError(
                TransientError,
                "The memory pool limit was exceeded. The corresponding setting can be found in the error message"),
        // Do not move the DatabaseUnavailable status to its more natural namespace of `Database` as downstream clients
        // depend on the string representation being `Neo.TransientError.General.DatabaseUnavailable`
        DatabaseUnavailable(
                TransientError,
                "The database is not currently available to serve your request, refer to the database logs for more "
                        + "details. Retrying your request at a later time may succeed.");

        private final Code code;

        @Override
        public Code code() {
            return code;
        }

        General(Classification classification, String description) {
            this.code = new Code(classification, this, description);
        }
    }

    enum Database implements Status {
        DatabaseNotFound(ClientError, "The request referred to a database that does not exist."),
        HomeDatabaseNotFound(
                ClientNotification,
                "The request referred to a home database that does not exist.",
                SeverityLevel.INFORMATION,
                NotificationCategory.UNRECOGNIZED),
        ExistingAliasFound(ClientError, "The request referred to a database with an alias."),
        ExistingDatabaseFound(ClientError, "The request referred to a database that already exists."),
        IllegalAliasChain(
                ClientError, "An illegal chain of aliases has been detected. This request cannot be executed."),
        DatabaseLimitReached(DatabaseError, "The limit to number of databases has been reached."),
        UnableToStartDatabase(DatabaseError, "Unable to start database."),
        Unknown(DatabaseError, "Unknown database management error");

        private final Code code;

        @Override
        public Code code() {
            return code;
        }

        Database(Classification classification, String description) {
            this.code = new Code(classification, this, description);
        }

        Database(
                Classification classification,
                String description,
                SeverityLevel severity,
                NotificationCategory category) {
            this.code = new NotificationCode(classification, this, description, severity, category);
        }
    }

    enum Cluster implements Status {
        // transient errors
        ReplicationFailure(TransientError, "Replication failure."),

        NotALeader(
                ClientError,
                "The request cannot be processed by this server. Write requests can only be processed by the leader."),

        Routing(ClientError, "Unable to route the request to the appropriate server"),

        ServerAlreadyEnabled(
                ClientNotification,
                "`ENABLE SERVER` has no effect.",
                SeverityLevel.INFORMATION,
                NotificationCategory.TOPOLOGY),

        ServerAlreadyCordoned(
                ClientNotification,
                "`CORDON SERVER` has no effect.",
                SeverityLevel.INFORMATION,
                NotificationCategory.TOPOLOGY),

        NoDatabasesReallocated(
                ClientNotification,
                "`REALLOCATE DATABASES` has no effect.",
                SeverityLevel.INFORMATION,
                NotificationCategory.TOPOLOGY),

        CordonedServersExistedDuringAllocation(
                ClientNotification,
                "Cordoned servers existed when making an allocation decision.",
                SeverityLevel.INFORMATION,
                NotificationCategory.TOPOLOGY),

        RequestedTopologyMatchedCurrentTopology(
                ClientNotification,
                "`ALTER DATABASE` has no effect.",
                SeverityLevel.INFORMATION,
                NotificationCategory.TOPOLOGY);

        private final Code code;

        @Override
        public Code code() {
            return code;
        }

        Cluster(Classification classification, String description) {
            this.code = new Code(classification, this, description);
        }

        Cluster(
                Classification classification,
                String description,
                SeverityLevel severity,
                NotificationCategory category) {
            this.code = new NotificationCode(classification, this, description, severity, category);
        }
    }

    enum Fabric implements Status {
        @Deprecated
        RemoteExecutionFailed(DatabaseError, "The database was unable to execute a remote part of the statement."),
        @Deprecated
        AccessMode(ClientError, "The request could not be completed due to access mode violation");

        private final Code code;

        @Override
        public Code code() {
            return code;
        }

        Fabric(Classification classification, String description) {
            this.code = new Code(classification, this, description);
        }
    }

    enum ChangeDataCapture implements Status {
        Disabled(DatabaseError, "Change Data Capture is not currently enabled for this database"),
        ScanFailure(DatabaseError, "Unable to read the Change Data Capture data for this database"),
        InvalidIdentifier(ClientError, "Invalid change identifier"),
        FutureIdentifier(
                TransientError,
                "Change identifier points to a future transaction that has not yet happened on this database instance");

        private final Code code;

        @Override
        public Code code() {
            return code;
        }

        ChangeDataCapture(Classification classification, String description) {
            this.code = new Code(classification, this, description);
        }
    }

    Code code();

    class Code {
        public static Collection<Status> all() {
            Collection<Status> result = new ArrayList<>();
            for (Class<?> child : Status.class.getDeclaredClasses()) {
                if (child.isEnum() && Status.class.isAssignableFrom(child)) {
                    @SuppressWarnings("unchecked")
                    Class<? extends Status> statusType = (Class<? extends Status>) child;
                    Collections.addAll(result, statusType.getEnumConstants());
                }
            }
            return result;
        }

        private final Classification classification;
        private final String description;
        private final String category;
        private final String title;

        <C extends Enum<C> & Status> Code(Classification classification, C categoryAndTitle, String description) {
            this.classification = classification;
            this.category = categoryAndTitle.getDeclaringClass().getSimpleName();
            this.title = categoryAndTitle.name();

            this.description = description;
        }

        @Override
        public String toString() {
            return "Status.Code[" + serialize() + "]";
        }

        /**
         * The portable, serialized status code. This will always be in the format:
         *
         * <pre>
         * Neo.[Classification].[Category].[Title]
         * </pre>
         */
        public final String serialize() {
            return format("Neo.%s.%s.%s", classification, category, title);
        }

        public final String description() {
            return description;
        }

        public Classification classification() {
            return classification;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Code code = (Code) o;

            return category.equals(code.category) && classification == code.classification && title.equals(code.title);
        }

        @Override
        public int hashCode() {
            int result = classification.hashCode();
            result = 31 * result + category.hashCode();
            result = 31 * result + title.hashCode();
            return result;
        }
    }

    class NotificationCode extends Code {

        private final SeverityLevel severity;
        private final NotificationCategory notificationCategory;
        private final Classification classification;
        private final String category;
        private final String title;

        <C extends Enum<C> & Status> NotificationCode(
                Classification classification,
                C categoryAndTitle,
                String description,
                SeverityLevel severity,
                NotificationCategory notificationCategory) {
            super(classification, categoryAndTitle, description);
            this.severity = severity;
            this.classification = classification;
            this.category = categoryAndTitle.getDeclaringClass().getSimpleName();
            this.notificationCategory = notificationCategory;
            this.title = categoryAndTitle.name();
        }

        public String getSeverity() {
            return severity.name();
        }

        public String getNotificationCategory() {
            return notificationCategory.name();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            NotificationCode code = (NotificationCode) o;

            return category.equals(code.category)
                    && classification == code.classification
                    && title.equals(code.title)
                    && severity == code.severity
                    && notificationCategory == code.notificationCategory;
        }

        @Override
        public int hashCode() {
            int result = classification.hashCode();
            result = 31 * result + category.hashCode();
            result = 31 * result + title.hashCode();
            result = 31 * result + severity.hashCode();
            result = 31 * result + notificationCategory.hashCode();
            return result;
        }
    }

    enum Classification {
        /**
         * The Client sent a bad request - changing the request might yield a successful outcome.
         */
        ClientError(
                TransactionEffect.ROLLBACK,
                "The Client sent a bad request - changing the request might yield a successful outcome."),
        /**
         * There are notifications about the request sent by the client.
         */
        ClientNotification(TransactionEffect.NONE, "There are notifications about the request sent by the client."),

        /**
         * The database cannot service the request right now, retrying later might yield a successful outcome.
         */
        TransientError(
                TransactionEffect.ROLLBACK,
                "The database cannot service the request right now, retrying later might yield a successful outcome. "),

        // Implementation note: These are a sharp tool, database error signals
        // that something is *seriously* wrong, and will prompt the user to send
        // an error report back to us. Only use this if the code path you are
        // at would truly indicate the database is in a broken or bug-induced state.
        /**
         * The database failed to service the request.
         */
        DatabaseError(TransactionEffect.ROLLBACK, "The database failed to service the request. ");

        private enum TransactionEffect {
            ROLLBACK,
            NONE,
        }

        private final boolean rollbackTransaction;
        private final String description;

        Classification(TransactionEffect transactionEffect, String description) {
            this.description = description;
            this.rollbackTransaction = transactionEffect == TransactionEffect.ROLLBACK;
        }

        public boolean rollbackTransaction() {
            return rollbackTransaction;
        }

        public String description() {
            return description;
        }
    }

    interface HasStatus {
        Status status();
    }

    static Status statusCodeOf(Throwable e) {
        do {
            if (e instanceof Status.HasStatus) {
                return ((Status.HasStatus) e).status();
            }
            e = e.getCause();
        } while (e != null);

        return null;
    }
}
