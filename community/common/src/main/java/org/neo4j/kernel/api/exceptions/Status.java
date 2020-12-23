/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.api.exceptions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.neo4j.annotations.api.PublicApi;

import static java.lang.String.format;
import static org.neo4j.kernel.api.exceptions.Status.Classification.ClientError;
import static org.neo4j.kernel.api.exceptions.Status.Classification.ClientNotification;
import static org.neo4j.kernel.api.exceptions.Status.Classification.DatabaseError;
import static org.neo4j.kernel.api.exceptions.Status.Classification.TransientError;

/**
 * This is the codification of all available surface-api status codes. If you are throwing an error to a user through
 * one of the key APIs, you should opt for using or adding an error code here.
 *
 * Each {@link Status} has an associated category, represented by the inner enums in this class.
 * Each {@link Status} also has an associated {@link Classification} which defines meta-data about the code, such
 * as if the error was caused by a user or the database (and later on if the code denotes an error or merely a warning).
 *
 * This class is not part of the public Neo4j API, and backwards compatibility for using it as a Java class is not
 * guaranteed. Instead, the automatically generated documentation derived from this class and available in the Neo4j
 * manual should be considered a user-level API.
 */
@PublicApi
public interface Status
{
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
    enum Request implements Status
    {
        // client
        Invalid( ClientError,  // TODO: see above
                "The client provided an invalid request." ),
        InvalidFormat( ClientError,  // TODO: see above
                "The client provided a request that was missing required fields, or had values that are not allowed." ),
        InvalidUsage( ClientError,  // TODO: see above
                "The client made a request but did not consume outgoing buffers in a timely fashion." ),
        NoThreadsAvailable( TransientError,  // TODO: see above
                "There are no available threads to serve this request at the moment. You can retry at a later time " +
                        "or consider increasing max thread pool size for bolt connector(s)." );
        private final Code code;

        @Override
        public Code code()
        {
            return code;
        }

        Request( Classification classification, String description )
        {
            this.code = new Code( classification, this, description );
        }
    }

    enum Transaction implements Status
    {
        // client errors
        TransactionNotFound( ClientError,
                "The request referred to a transaction that does not exist." ),
        TransactionAccessedConcurrently( ClientError,
                "There were concurrent requests accessing the same transaction, which is not allowed." ),
        ForbiddenDueToTransactionType( ClientError,
                "The transaction is of the wrong type to service the request. For instance, a transaction that has " +
                "had schema modifications performed in it cannot be used to subsequently perform data operations, " +
                "and vice versa." ),
        TransactionValidationFailed( ClientError,
                "Transaction changes did not pass validation checks" ),
        TransactionHookFailed( ClientError,
                "Transaction hook failure." ),
        TransactionMarkedAsFailed( ClientError,
                "Transaction was marked as both successful and failed. Failure takes precedence and so this " +
                "transaction was rolled back although it may have looked like it was going to be committed" ),
        TransactionTimedOut( ClientError,
                "The transaction has not completed within the specified timeout (dbms.transaction.timeout). You may want to retry with a longer " +
                "timeout." ),
        InvalidBookmark( ClientError,
                "Supplied bookmark cannot be interpreted. You should only supply a bookmark that was " +
                "previously generated by Neo4j. Maybe you have generated your own bookmark, " +
                "or modified a bookmark since it was generated by Neo4j." ),
        InvalidBookmarkMixture( ClientError,
                "Mixing bookmarks generated by different databases is forbidden." +
                "You should only chain bookmarks that are generated from the same database. " +
                "You may however chain bookmarks generated from system database with bookmarks from another database." ),

        // database errors
        TransactionStartFailed( DatabaseError,
                "The database was unable to start the transaction." ),
        TransactionRollbackFailed( DatabaseError,
                "The database was unable to roll back the transaction." ),
        TransactionCommitFailed( DatabaseError,
                "The database was unable to commit the transaction." ),
        TransactionLogError( DatabaseError,
                "The database was unable to write transaction to log." ),

        // transient errors
        BookmarkTimeout( TransientError,
                "Bookmark wait timed out. Database has not reached the specified version" ),
        LeaseExpired( TransientError,
                "The lease under which this transaction was started is no longer valid." ),
        DeadlockDetected( TransientError,
                "This transaction, and at least one more transaction, has acquired locks in a way that it will wait " +
                "indefinitely, and the database has aborted it. Retrying this transaction will most likely be " +
                "successful." ),
        ConstraintsChanged( TransientError,
                "Database constraints changed since the start of this transaction" ),
        Outdated( TransientError,
                "Transaction has seen state which has been invalidated by applied updates while " +
                "transaction was active. Transaction may succeed if retried." ),
        LockClientStopped( TransientError,
                "The transaction has been terminated, so no more locks can be acquired. This can occur because the " +
                "transaction ran longer than the configured transaction timeout, or because a human operator manually " +
                "terminated the transaction, or because the database is shutting down." ),
        LockAcquisitionTimeout( TransientError,
                "Unable to acquire lock within configured timeout (dbms.lock.acquisition.timeout)." ),
        MaximumTransactionLimitReached( TransientError,
                "Unable to start new transaction since the maximum number of concurrently executing transactions is " +
                        "reached (dbms.transaction.concurrent.maximum). " +
                        "You can retry at a later time or consider increasing allowed maximum of concurrent transactions." ),
        Terminated( TransientError,
                "Explicitly terminated by the user." ),
        Interrupted( TransientError,
                "Interrupted while waiting." ),
        @Deprecated // Non-specific Status for migrating legacy exceptions.
        TransientTransactionFailure( TransientError,
                                     "Unable to complete transaction." );

        private final Code code;

        @Override
        public Code code()
        {
            return code;
        }

        Transaction( Classification classification, String description )
        {
            this.code = new Code( classification, this, description );
        }
    }

    enum Statement implements Status
    {
        // client errors
        SyntaxError( ClientError,
                "The statement contains invalid or unsupported syntax." ),
        SemanticError( ClientError,
                "The statement is syntactically valid, but expresses something that the database cannot do." ),
        ParameterMissing( ClientError,
                "The statement refers to a parameter that was not provided in the request." ),
        ConstraintVerificationFailed( ClientError,
                "A constraint imposed by the statement is violated by the data in the database." ),
        EntityNotFound( ClientError,
                "The statement refers to a non-existent entity." ),
        PropertyNotFound( ClientError,
                "The statement refers to a non-existent property." ),
        TypeError( ClientError,
                "The statement is attempting to perform operations on values with types that are not supported by " +
                "the operation." ),
        ArgumentError( ClientError,
                "The statement is attempting to perform operations using invalid arguments" ),
        ArithmeticError( ClientError,
                "Invalid use of arithmetic, such as dividing by zero." ),
        RuntimeUnsupportedError( ClientError,
                "This query is not supported by the chosen runtime." ),
        NotSystemDatabaseError( ClientError,
                "This is an administration command and it should be executed against the system database." ),
        AccessMode( ClientError, "The request could not be completed due to access mode violation" ),

        // database errors
        ExecutionFailed( DatabaseError,
                "The database was unable to execute the statement." ),
        CodeGenerationFailed( DatabaseError,
                              "The database was unable to generate code for the query. A stacktrace can be found in the debug.log." ),
        RemoteExecutionFailed( DatabaseError, "The database was unable to execute a remote part of the statement." ),

        // transient errors
        ExternalResourceFailed( ClientError,
                "Access to an external resource failed" ),

        // client notifications (performance)
        CartesianProductWarning( ClientNotification,
                "This query builds a cartesian product between disconnected patterns." ),
        DynamicPropertyWarning( ClientNotification,
                "Queries using dynamic properties will use neither index seeks nor index scans for those properties" ),
        EagerOperatorWarning( ClientNotification,
                "The execution plan for this query contains the Eager operator, which forces all dependent data to " +
                 "be materialized in main memory before proceeding" ),
        JoinHintUnfulfillableWarning( ClientNotification,
                "The database was unable to plan a hinted join." ),
        NoApplicableIndexWarning( ClientNotification,
                "Adding a schema index may speed up this query." ),
        SuboptimalIndexForWildcardQuery( ClientNotification,
                "Index cannot execute wildcard query efficiently" ),
        UnboundedVariableLengthPatternWarning( ClientNotification,
                "The provided pattern is unbounded, consider adding an upper limit to the number of node hops." ),
        ExhaustiveShortestPathWarning( ClientNotification,
                "Exhaustive shortest path has been planned for your query that means that shortest path graph " +
                "algorithm might not be used to find the shortest path. Hence an exhaustive enumeration of all paths " +
                "might be used in order to find the requested shortest path." ),

        // client notifications (not supported/deprecated)
        RuntimeUnsupportedWarning( ClientNotification,
                "This query is not supported by the chosen runtime." ),
        FeatureDeprecationWarning( ClientNotification,
                "This feature is deprecated and will be removed in future versions." ),
        ExperimentalFeature( ClientNotification,
                "This feature is experimental and should not be used in production systems." ),

        // client notifications (unknown tokens)
        UnknownLabelWarning( ClientNotification,
                "The provided label is not in the database." ),
        UnknownRelationshipTypeWarning( ClientNotification,
                "The provided relationship type is not in the database." ),
        UnknownPropertyKeyWarning( ClientNotification,
                "The provided property key is not in the database" ),

        SubqueryVariableShadowingWarning( ClientNotification,
                "Variable in subquery is shadowing a variable with the same name from the outer scope." ),
        MissingAlias( ClientNotification,
                      "Missing alias in a RETURN clause in a CALL subquery." );

        private final Code code;

        @Override
        public Code code()
        {
            return code;
        }

        Statement( Classification classification, String description )
        {
            this.code = new Code( classification, this, description );
        }
    }

    enum Schema implements Status
    {
        // client errors
        RepeatedPropertyInCompositeSchema( ClientError,
                "Unable to create index or constraint because schema had a repeated property." ),
        RepeatedLabelInSchema( ClientError,
                "Unable to create index or constraint because schema had a repeated label." ),
        RepeatedRelationshipTypeInSchema( ClientError,
                "Unable to create index or constraint because schema had a repeated relationship type." ),
        EquivalentSchemaRuleAlreadyExists( ClientError,
                "Unable to perform operation because an equivalent schema rule already exists." ),
        ConstraintAlreadyExists( ClientError,
                "Unable to perform operation because it would clash with a pre-existing constraint." ),
        ConstraintNotFound( ClientError,
                "The request (directly or indirectly) referred to a constraint that does not exist." ),
        ConstraintValidationFailed( ClientError,
                "A constraint imposed by the database was violated." ),
        ConstraintViolation( ClientError,
                "Added or changed index entry would violate constraint" ),
        IndexAlreadyExists( ClientError,
                "Unable to perform operation because it would clash with a pre-existing index." ),
        IndexNotFound( ClientError,
                "The request (directly or indirectly) referred to an index that does not exist." ),
        IndexMultipleFound( ClientError,
                "The request referenced an index by its schema, and multiple matching indexes were found." ),
        IndexNotApplicable( ClientError,
                "The request did not contain the properties required by the index." ),
        IndexWithNameAlreadyExists( ClientError,
                "Unable to perform operation because index with given name already exists." ),
        ConstraintWithNameAlreadyExists( ClientError,
                "Unable to perform operation because constraint with given name already exists." ),
        ForbiddenOnConstraintIndex( ClientError,
                "A requested operation can not be performed on the specified index because the index is part of a " +
                "constraint. If you want to drop the index, for instance, you must drop the constraint." ),
        TokenNameError( ClientError,
                "A token name, such as a label, relationship type or property key, used is not valid. Tokens cannot " +
                        "be empty strings and cannot be null." ),

        // database errors
        ConstraintCreationFailed( DatabaseError,
                "Creating a requested constraint failed." ),
        ConstraintDropFailed( DatabaseError,
                "The database failed to drop a requested constraint." ),
        IndexCreationFailed( DatabaseError,
                "Failed to create an index." ),
        IndexDropFailed( DatabaseError,
                "The database failed to drop a requested index." ),
        LabelAccessFailed( DatabaseError,
                "The request accessed a label that did not exist." ),
        TokenLimitReached( DatabaseError,
                "The maximum number of tokens of this type has been reached, no more tokens of this type can be created." ),
        PropertyKeyAccessFailed( DatabaseError,
                "The request accessed a property that does not exist." ),
        RelationshipTypeAccessFailed( DatabaseError,
                "The request accessed a relationship type that does not exist." ),
        SchemaRuleAccessFailed( DatabaseError,
                "The request referred to a schema rule that does not exist." ),
        SchemaRuleDuplicateFound( DatabaseError,
                "The request referred to a schema rule that is defined multiple times." )
        ;

        private final Code code;

        @Override
        public Code code()
        {
            return code;
        }

        Schema( Classification classification, String description )
        {
            this.code = new Code( classification, this, description );
        }
    }

    enum Procedure implements Status
    {
        ProcedureRegistrationFailed( ClientError,
                "The database failed to register a procedure, refer to the associated error message for details." ),
        ProcedureNotFound( ClientError,
                "A request referred to a procedure that is not registered with this database instance. If you are " +
                "deploying custom procedures in a cluster setup, ensure all instances in the cluster have the " +
                "procedure jar file deployed." ),
        ProcedureCallFailed( ClientError,
                "Failed to invoke a procedure. See the detailed error description for exact cause." ),
        TypeError( ClientError,
                "A procedure is using or receiving a value of an invalid type." ),
        ProcedureTimedOut( ClientError,
                "The procedure has not completed within the specified timeout. You may want to retry with a longer " +
                "timeout." ),
        ProcedureWarning( ClientNotification, "The query used a procedure that generated a warning." );

        private final Code code;

        @Override
        public Code code()
        {
            return code;
        }

        Procedure( Classification classification, String description )
        {
            this.code = new Code( classification, this, description );
        }
    }

    enum Security implements Status
    {
        // client
        CredentialsExpired( ClientError, "The credentials have expired and need to be updated." ),
        Unauthorized( ClientError, "The client is unauthorized due to authentication failure." ),
        AuthenticationRateLimit( ClientError, "The client has provided incorrect authentication details too many times in a row." ),
        ModifiedConcurrently( TransientError, "The user was modified concurrently to this request." ),
        Forbidden( ClientError, "An attempt was made to perform an unauthorized action." ),
        AuthorizationExpired( ClientError, "The stored authorization info has expired. Please reconnect." ),
        AuthProviderTimeout( TransientError, "An auth provider request timed out." ),
        AuthProviderFailed( TransientError, "An auth provider request failed." );

        private final Code code;

        @Override
        public Code code()
        {
            return code;
        }

        Security( Classification classification, String description )
        {
            this.code = new Code( classification, this, description );
        }
    }

    enum General implements Status
    {
        // client errors
        InvalidArguments( ClientError, "The request contained fields that were empty or are not allowed." ),
        ForbiddenOnReadOnlyDatabase( ClientError,
                "This is a read only database, writing or modifying the database is not allowed." ),
        TransactionOutOfMemoryError( ClientError,
                "The transaction used more memory than was allowed. The maximum allowed size for a " +
                "transaction can be configured with 'dbms.memory.transaction.max_size' in the neo4j configuration " +
                "(normally in 'conf/neo4j.conf' or, if you are using Neo4j Desktop, found through the user interface)." ),

        // database errors
        IndexCorruptionDetected( DatabaseError,
                "The request (directly or indirectly) referred to an index that is in a failed state. The index " +
                "needs to be dropped and recreated manually." ),
        SchemaCorruptionDetected( DatabaseError,
                "A malformed schema rule was encountered. Please contact your support representative." ),
        StorageDamageDetected( DatabaseError,
                "Expected set of files not found on disk. Please restore from backup." ),
        UnknownError( DatabaseError,
                "An unknown error occurred." ),

        // transient errors

        // Off heap allocation limit exceeded
        TransactionMemoryLimit( TransientError,
                "There is not enough memory to perform the current task. Please try increasing " +
                        "'dbms.memory.off_heap.max_size' in the neo4j configuration (normally in 'conf/neo4j.conf' or, if " +
                        "you are using Neo4j Desktop, found through the user interface), and then restart the database." ),
        OutOfMemoryError( TransientError,
                "There is not enough memory to perform the current task. Please try increasing " +
                "'dbms.memory.heap.max_size' in the neo4j configuration (normally in 'conf/neo4j.conf' or, if " +
                "you are using Neo4j Desktop, found through the user interface) or if you are running an embedded " +
                "installation increase the heap by using '-Xmx' command line flag, and then restart the database." ),
        StackOverFlowError( TransientError,
                "There is not enough stack size to perform the current task. This is generally considered to be a " +
                "database error, so please contact Neo4j support. You could try increasing the stack size: " +
                "for example to set the stack size to 2M, add `dbms.jvm.additional=-Xss2M' to " +
                "in the neo4j configuration (normally in 'conf/neo4j.conf' or, if you are using " +
                "Neo4j Desktop, found through the user interface) or if you are running an embedded installation " +
                "just add -Xss2M as command line flag." ),
        MemoryPoolOutOfMemoryError( TransientError,
                "The memory pool limit was exceeded. The corresponding setting can be found in the error message" )
        ;

        private final Code code;

        @Override
        public Code code()
        {
            return code;
        }

        General( Classification classification, String description )
        {
            this.code = new Code( classification, this, description );
        }
    }

    enum Database implements Status
    {
        DatabaseUnavailable( TransientError,
                "The database is not currently available to serve your request, refer to the database logs for more " +
                "details. Retrying your request at a later time may succeed." ),
        DatabaseNotFound( ClientError, "The request referred to a database that does not exist." ),
        ExistingDatabaseFound( ClientError, "The request referred to a database that already exists." ),
        DatabaseLimitReached( DatabaseError, "The limit to number of databases has been reached." ),
        UnableToStartDatabase( DatabaseError, "Unable to start database." ),
        Unknown( DatabaseError, "Unknown database management error" );

        private final Code code;

        @Override
        public Code code()
        {
            return code;
        }

        Database( Classification classification, String description )
        {
            this.code = new Code( classification, this, description );
        }
    }

    enum Cluster implements Status
    {
        // transient errors
        ReplicationFailure( TransientError,
                "Replication failure." ),

        NotALeader( ClientError,
                "The request cannot be processed by this server. Write requests can only be processed by the leader." ),

        Routing( TransientError, "Unable to route the request to the appropriate server" )
        ;

        private final Code code;

        @Override
        public Code code()
        {
            return code;
        }

        Cluster( Classification classification, String description )
        {
            this.code = new Code( classification, this, description );
        }
    }

    enum Fabric implements Status
    {
        @Deprecated
        RemoteExecutionFailed( DatabaseError, "The database was unable to execute a remote part of the statement." ),
        @Deprecated
        AccessMode( ClientError, "The request could not be completed due to access mode violation" );

        private final Code code;

        @Override
        public Code code()
        {
            return code;
        }

        Fabric( Classification classification, String description )
        {
            this.code = new Code( classification, this, description );
        }
    }

    Code code();

    class Code
    {
        public static Collection<Status> all()
        {
            Collection<Status> result = new ArrayList<>();
            for ( Class<?> child : Status.class.getDeclaredClasses() )
            {
                if ( child.isEnum() && Status.class.isAssignableFrom( child ) )
                {
                    @SuppressWarnings( "unchecked" )
                    Class<? extends Status> statusType = (Class<? extends Status>) child;
                    Collections.addAll( result, statusType.getEnumConstants() );
                }
            }
            return result;
        }

        private final Classification classification;
        private final String description;
        private final String category;
        private final String title;

        <C extends Enum<C> & Status> Code( Classification classification, C categoryAndTitle, String description )
        {
            this.classification = classification;
            this.category = categoryAndTitle.getDeclaringClass().getSimpleName();
            this.title = categoryAndTitle.name();

            this.description = description;
        }

        @Override
        public String toString()
        {
            return "Status.Code[" + serialize() + "]";
        }

        /**
         * The portable, serialized status code. This will always be in the format:
         *
         * <pre>
         * Neo.[Classification].[Category].[Title]
         * </pre>
         */
        public final String serialize()
        {
            return format( "Neo.%s.%s.%s", classification, category, title );
        }

        public final String description()
        {
            return description;
        }

        public Classification classification()
        {
            return classification;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }

            Code code = (Code) o;

            return category.equals( code.category ) && classification == code.classification &&
                   title.equals( code.title );
        }

        @Override
        public int hashCode()
        {
            int result = classification.hashCode();
            result = 31 * result + category.hashCode();
            result = 31 * result + title.hashCode();
            return result;
        }
    }

    enum Classification
    {
        /** The Client sent a bad request - changing the request might yield a successful outcome. */
        ClientError( TransactionEffect.ROLLBACK,
                "The Client sent a bad request - changing the request might yield a successful outcome." ),
        /** There are notifications about the request sent by the client.*/
        ClientNotification( TransactionEffect.NONE,
                "There are notifications about the request sent by the client." ),

        /** The database cannot service the request right now, retrying later might yield a successful outcome. */
        TransientError( TransactionEffect.ROLLBACK,
                "The database cannot service the request right now, retrying later might yield a successful outcome. " ),

        // Implementation note: These are a sharp tool, database error signals
        // that something is *seriously* wrong, and will prompt the user to send
        // an error report back to us. Only use this if the code path you are
        // at would truly indicate the database is in a broken or bug-induced state.
        /** The database failed to service the request. */
        DatabaseError( TransactionEffect.ROLLBACK,
                "The database failed to service the request. " );

        private enum TransactionEffect
        {
            ROLLBACK, NONE,
        }

        private final boolean rollbackTransaction;
        private final String description;

        Classification( TransactionEffect transactionEffect, String description )
        {
            this.description = description;
            this.rollbackTransaction = transactionEffect == TransactionEffect.ROLLBACK;
        }

        public boolean rollbackTransaction()
        {
            return rollbackTransaction;
        }

        public String description()
        {
            return description;
        }
    }

    interface HasStatus
    {
        Status status();
    }

    static Status statusCodeOf( Throwable e )
    {
        do
        {
            if ( e instanceof Status.HasStatus )
            {
                return ((Status.HasStatus) e).status();
            }
            e = e.getCause();
        }
        while ( e != null );

        return null;
    }
}
