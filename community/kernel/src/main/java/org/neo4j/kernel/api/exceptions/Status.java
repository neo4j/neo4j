/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.api.exceptions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

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
 *
 * Currently, only the transactional http endpoint is dedicated to using these status codes.
 */
public interface Status
{
    /*
     * A note on naming: Since these are public status codes and users will base error handling on them, please take
     * care to place them in correct categories and assign them correct classifications. Also make sure you are not
     * introducing duplicates.
     *
     * If you are unsure, contact Jake or Tobias before making modifications.
     */

    enum Network implements Status
    {
        // transient
        UnknownFailure( TransientError, "An unknown network failure occurred, a retry may resolve the issue." );
        private final Code code;

        @Override
        public Code code()
        {
            return code;
        }

        Network( Classification classification, String description )
        {
            this.code = new Code( classification, this, description );
        }
    }

    enum Request implements Status
    {
        // client
        Invalid( ClientError, "The client provided an invalid request." ),
        InvalidFormat( ClientError, "The client provided a request that was missing required fields, or had values " +
                "that are not allowed." );
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
        UnknownId( ClientError, "The request referred to a transaction that does not exist."),
        ConcurrentRequest( ClientError, "There were concurrent requests accessing the same transaction, which is not " +
                "allowed." ),
        CouldNotBegin( DatabaseError,    "The database was unable to start the transaction." ),
        CouldNotRollback( DatabaseError, "The database was unable to roll back the transaction." ),
        CouldNotCommit( DatabaseError,   "The database was unable to commit the transaction." ),
        CouldNotWriteToLog( DatabaseError, "The database was unable to write transaction to log." ),

        InvalidType( ClientError, "The transaction is of the wrong type to service the request. For instance, a " +
                "transaction that has had schema modifications performed in it cannot be used to subsequently " +
                "perform data operations, and vice versa." ),

        ReleaseLocksFailed( DatabaseError, "The transaction was unable to release one or more of its locks." ),
        AcquireLockTimeout( TransientError, "The transaction was unable to acquire a lock, for instance due to a " +
                "timeout or the transaction thread being interrupted." ),
        DeadlockDetected( TransientError, "This transaction, and at least one more transaction, has acquired locks " +
        "in a way that it will wait indefinitely, and the database has aborted it. Retrying this transaction " +
        "will most likely be successful."),

        EventHandlerThrewException( ClientError, "A transaction event handler threw an exception. The transaction " +
        "will be rolled back." ),

        ValidationFailed( ClientError, "Transaction changes did not pass validation checks" ),
        ConstraintsChanged( TransientError, "Database constraints changed since the start of this transaction" ),
        HookFailed( ClientError, "Transaction hook failure." ),
        MarkedAsFailed( ClientError, "Transaction was marked as both successful and failed. Failure takes precedence" +
                " and so this transaction was rolled back although it may have looked like it was going to be " +
                "committed" ),
        Outdated( TransientError, "Transaction has seen state which has been invalidated by applied updates while " +
                "transaction was active. Transaction may succeed if retried." ),
        LockClientStopped( TransientError, "Transaction terminated, no more locks can be acquired." ),
        Terminated( TransientError, "Explicitly terminated by the user." )
        ;


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
        // client
        InvalidSyntax( ClientError, "The statement contains invalid or unsupported syntax." ),
        InvalidSemantics( ClientError, "The statement is syntactically valid, but expresses something that the " +
                "database cannot do." ),
        ParameterMissing( ClientError, "The statement is referring to a parameter that was not provided in the " +
                "request." ),
        ConstraintViolation( ClientError, "A constraint imposed by the statement is violated by the data in the " +
                "database." ),
        EntityNotFound( ClientError,      "The statement is directly referring to an entity that does not exist." ),
        NoSuchProperty( ClientError, "The statement is referring to a property that does not exist." ),
        NoSuchLabel( ClientError, "The statement is referring to a label that does not exist."),
        InvalidType( ClientError,         "The statement is attempting to perform operations on values with types that " +
                "are not supported by the operation." ),
        InvalidArguments( ClientError, "The statement is attempting to perform operations using invalid arguments"),
        ArithmeticError( ClientError,     "Invalid use of arithmetic, such as dividing by zero." ),
        // database
        ExecutionFailure( DatabaseError, "The database was unable to execute the statement." ),
        ExternalResourceFailure( TransientError, "The external resource is not available"),
        CartesianProduct( ClientNotification, "This query builds a cartesian product between disconnected patterns." ),
        PlannerUnsupportedWarning( ClientNotification, "This query is not supported by the COST planner." ),
        RuntimeUnsupportedWarning( ClientNotification, "This query is not supported by the compiled runtime." ),
        DeprecationWarning( ClientNotification, "This feature is deprecated and will be removed in future versions." ),
        JoinHintUnfulfillableWarning( ClientNotification, "The database was unable to plan a hinted join." ),
        JoinHintUnsupportedWarning( ClientNotification, "Queries with join hints are not supported by the RULE planner." ),
        DynamicPropertyWarning( ClientNotification, "Queries using dynamic properties will use neither index seeks " +
                                                    "nor index scans for those properties" ),
        EagerWarning(ClientNotification, "The execution plan for this query contains the Eager operator, " +
                                         "which forces all dependent data to be materialized in main memory " +
                                         "before proceeding"),
        IndexMissingWarning( ClientNotification, "Adding a schema index may speed up this query." ),
        LabelMissingWarning( ClientNotification, "The provided label is not in the database." ),
        RelTypeMissingWarning( ClientNotification, "The provided relationship type is not in the database." ),
        PropertyNameMissingWarning( ClientNotification, "The provided property name is not in the database" ),
        UnboundedPatternWarning( ClientNotification, "The provided pattern is unbounded, consider adding an upper limit to the number of node hops."  );

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
        /** A constraint in the database was violated by the query. */
        ConstraintViolation( ClientError, "A constraint imposed by the database was violated." ),
        NoSuchIndex( ClientError, "The request (directly or indirectly) referred to an index that does not exist." ),
        NoSuchConstraint( ClientError, "The request (directly or indirectly) referred to a constraint that does " +
                "not exist." ),
        IndexCreationFailure( DatabaseError, "Failed to create an index."),
        ConstraintAlreadyExists( ClientError, "Unable to perform operation because it would clash with a pre-existing" +
                " constraint." ),
        IndexAlreadyExists( ClientError, "Unable to perform operation because it would clash with a pre-existing " +
                "index." ),
        IndexDropFailure( DatabaseError, "The database failed to drop a requested index." ),

        ConstraintVerificationFailure( ClientError, "Unable to create constraint because data that exists in the " +
        "database violates it." ),
        ConstraintCreationFailure( DatabaseError, "Creating a requested constraint failed." ),
        ConstraintDropFailure( DatabaseError, "The database failed to drop a requested constraint." ),

        IllegalTokenName( ClientError, "A token name, such as a label, relationship type or property key, used is " +
                "not valid. Tokens cannot be empty strings and cannot be null." ),

        IndexBelongsToConstraint(
            ClientError, "A requested operation can not be performed on the specified index because the index is " +
            "part of a constraint. If you want to drop the index, for instance, you must drop the constraint." ),

        NoSuchLabel( DatabaseError, "The request accessed a label that did not exist." ),
        NoSuchPropertyKey( DatabaseError, "The request accessed a property that does not exist." ),
        NoSuchRelationshipType( DatabaseError, "The request accessed a relationship type that does not exist." ),
        NoSuchSchemaRule( DatabaseError, "The request referred to a schema rule that does not exist." ),
        DuplicateSchemaRule( DatabaseError, "The request referred to a schema rule that defined multiple times." ),

        LabelLimitReached( ClientError, "The maximum number of labels supported has been reached, no more labels can be created." ),
        IndexLimitReached( ClientError, "The maximum number of index entries supported has been reached, no more entities can be indexed." ),

        ModifiedConcurrently( TransientError, "The database schema was modified while this transaction was running, the transaction should be retried." ),

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

    enum LegacyIndex implements Status
    {
        NoSuchIndex( ClientError, "The request (directly or indirectly) referred to a index that does not exist." )

        ;

        private final Code code;

        @Override
        public Code code()
        {
            return code;
        }

        LegacyIndex( Classification classification, String description )
        {
            this.code = new Code( classification, this, description );
        }
    }

    enum Security implements Status
    {
        // client
        AuthenticationFailed( ClientError, "The client provided an incorrect username and/or password." ),
        AuthorizationFailed( ClientError, "The client does not have privileges to perform the operation requested." ),
        AuthenticationRateLimit( ClientError, "The client has provided incorrect authentication details too many times in a row." ),
        ModifiedConcurrently( TransientError, "The user was modified concurrently to this request." );

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
        ReadOnly( ClientError, "This is a read only database, writing or modifying the database is not allowed." ),
        // database
        FailedIndex( DatabaseError, "The request (directly or indirectly) referred to an index that is in a failed " +
        "state. The index needs to be dropped and recreated manually." ),
        UnknownFailure( DatabaseError, "An unknown failure occurred." ),
        DatabaseUnavailable( TransientError, "The database is not currently available to serve your request, refer to the database logs for more details. Retrying your request at a later time may succeed." ),

        CorruptSchemaRule( DatabaseError, "A malformed schema rule was encountered. Please contact your support representative." ),

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
                    @SuppressWarnings("unchecked")
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
         * @return
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

            if ( !category.equals( code.category ) )
            {
                return false;
            }
            if ( classification != code.classification )
            {
                return false;
            }
            if ( !title.equals( code.title ) )
            {
                return false;
            }

            return true;
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
        ClientError( TransactionEffect.ROLLBACK, PublishingPolicy.PUBLISHABLE,
                "The Client sent a bad request - changing the request might yield a successful outcome."),
        /** There are notifications about the request sent by the client.*/
        ClientNotification( TransactionEffect.NONE, PublishingPolicy.PUBLISHABLE,
                "There are notifications about the request sent by the client." ),
        /** The database failed to service the request. */
        DatabaseError( TransactionEffect.ROLLBACK, PublishingPolicy.INTERNAL,
                "The database failed to service the request. " ),
        /** The database cannot service the request right now, retrying later might yield a successful outcome. */
        TransientError( TransactionEffect.ROLLBACK, PublishingPolicy.PUBLISHABLE,
                "The database cannot service the request right now, retrying later might yield a successful outcome. "),
        ;

        private enum TransactionEffect
        {
            ROLLBACK, NONE,
        }

        /**
         * A PUBLISHABLE error/warning is one which allows sending a meaningful error message to
         * the client with the expectation that the user will be able to take any necessary action
         * to resolve the error. INTERNAL errors are more sensitive and may not be resolvable by
         * the user - these will be logged and a only a reference will be forwarded to the user.
         * This is a security feature to avoid leaking potentially sensitive information to end
         * users.
         */
        private enum PublishingPolicy
        {
            PUBLISHABLE, INTERNAL,
        }

        private final boolean rollbackTransaction;
        private final boolean publishable;
        private final String description;

        Classification( TransactionEffect transactionEffect, PublishingPolicy publishingPolicy, String description )
        {
            this.description = description;
            this.publishable = publishingPolicy == PublishingPolicy.PUBLISHABLE;
            this.rollbackTransaction = transactionEffect == TransactionEffect.ROLLBACK;
        }

        public boolean rollbackTransaction()
        {
            return rollbackTransaction;
        }

        public boolean publishable()
        {
            return publishable;
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
}
