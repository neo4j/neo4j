/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.server.rest.transactional.error;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import static java.lang.String.format;
import static org.neo4j.server.rest.transactional.error.Status.Classification.ClientError;
import static org.neo4j.server.rest.transactional.error.Status.Classification.DatabaseError;
import static org.neo4j.server.rest.transactional.error.Status.Classification.TransientError;

/*
 * Put in place as an enum to enforce all error codes remaining collected in one location.
 * Note: These codes will be exposed to the user through our API, although for now they will
 * remain undocumented. There is a discussion to be had about these codes and how we should
 * categorize and pick them.
 *
 * The categories below are an initial proposal, we should have a real discussion about this before
 * anything is documented.
 */
public interface Status
{
    enum Network implements Status
    {
        // transient
        UnknownFailure( TransientError, "" );
        private final Code code;

        @Override
        public Code code()
        {
            return code;
        }

        private Network( Classification classification, String description )
        {
            this.code = new Code( classification, this, description );
        }
    }

    enum Request implements Status
    {
        // client
        Invalid( ClientError, "" ),
        InvalidFormat( ClientError, "" );
        private final Code code;

        @Override
        public Code code()
        {
            return code;
        }

        private Request( Classification classification, String description )
        {
            this.code = new Code( classification, this, description );
        }
    }

    enum Transaction implements Status
    {
        // database
        UnknownId( ClientError, ""),
        ConcurrentRequest( ClientError, "" ),
        // client
        CouldNotBegin( DatabaseError, "" ),
        CouldNotRollback( DatabaseError, "" ),
        CouldNotCommit( DatabaseError, "" );
        private final Code code;

        @Override
        public Code code()
        {
            return code;
        }

        private Transaction( Classification classification, String description )
        {
            this.code = new Code( classification, this, description );
        }
    }

    enum Statement implements Status
    {
        // client
        InvalidSyntax( ClientError, "" ),
        /** The syntax is valid, but the query is not semantically valid. */
        InvalidSemantics( ClientError, "" ),
        ParameterMissing( ClientError, "" ),
        /** A constraint that should hold in the query was violated by the data. */
        ConstraintViolation( ClientError, "" ),
        EntityNotFound( ClientError, "" ),
        InvalidType( ClientError, "" ),
        ArithmeticError( ClientError, "" ),
        // database
        ExecutionFailure( DatabaseError, "" ),
        ;
        private final Code code;

        @Override
        public Code code()
        {
            return code;
        }

        private Statement( Classification classification, String description )
        {
            this.code = new Code( classification, this, description );
        }
    }

    enum Schema implements Status
    {
        /** A constraint in the database was violated by the query. */
        ConstraintViolation( ClientError, "" ),
        NoSuchIndex( ClientError, "" ),
        NoSuchConstraint( ClientError, "" ),
        ;
        private final Code code;

        @Override
        public Code code()
        {
            return code;
        }

        private Schema( Classification classification, String description )
        {
            this.code = new Code( classification, this, description );
        }
    }

    enum General implements Status
    {
        // database
        FailedIndex( DatabaseError, "" ),
        UnknownFailure( DatabaseError, "" );
        private final Code code;

        @Override
        public Code code()
        {
            return code;
        }

        private General( Classification classification, String description )
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

        public final boolean includeStackTrace()
        {
            return classification.includeStackTrace;
        }

        public static boolean shouldRollBackOn( Collection<Neo4jError> errors )
        {
            if ( errors.isEmpty() )
            {
                return false;
            }
            for ( Neo4jError error : errors )
            {
                if ( error.status().code().classification.rollbackTransaction )
                {
                    return true;
                }
            }
            return false;
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

    public enum Classification
    {
        /** The Client sent a bad request - changing the request might yield a successful outcome. */
        ClientError( StackTraceStrategy.SWALLOW, TransactionEffect.NONE, "" ),
        /** The database failed to service the request. */
        DatabaseError( StackTraceStrategy.SEND_TO_CLIENT, TransactionEffect.ROLLBACK, "" ),
        /** The database cannot service the request right now, retrying later might yield a successful outcome. */
        TransientError( StackTraceStrategy.SEND_TO_CLIENT, TransactionEffect.NONE, "" ),;

        private enum StackTraceStrategy
        {
            SWALLOW, SEND_TO_CLIENT,
        }

        private enum TransactionEffect
        {
            ROLLBACK, NONE,
        }

        final boolean includeStackTrace;
        final boolean rollbackTransaction; // TODO: make use of this!!!

        private final String description;

        private Classification( StackTraceStrategy stackTraceStrategy, TransactionEffect transactionEffect,
                                String description )
        {
            this.description = description;
            this.includeStackTrace = stackTraceStrategy == StackTraceStrategy.SEND_TO_CLIENT;
            this.rollbackTransaction = transactionEffect == TransactionEffect.ROLLBACK;
        }

        public String description()
        {
            return description;
        }

        public boolean rollbackTransaction()
        {
            return rollbackTransaction;
        }
    }
}

