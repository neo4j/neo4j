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
package org.neo4j.internal.kernel.api.security;

import java.util.Collections;
import java.util.Set;
import java.util.function.Supplier;

import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.kernel.api.RelTypeSupplier;
import org.neo4j.internal.kernel.api.TokenSet;
import org.neo4j.kernel.api.exceptions.Status;

/** Controls the capabilities of a KernelTransaction. */
public interface AccessMode
{
    enum Static implements AccessMode
    {
        /** No reading or writing allowed. */
        ACCESS( false, false, false, false, false ),
        /** No reading or writing allowed because of expired credentials. */
        CREDENTIALS_EXPIRED( false, false, false, false, false )
                {
                    @Override
                    public AuthorizationViolationException onViolation( String msg )
                    {
                        return new AuthorizationViolationException( String.format(
                                msg + "%n%nThe credentials you provided were valid, but must be " +
                                "changed before you can " +
                                "use this instance. If this is the first time you are using Neo4j, this is to " +
                                "ensure you are not using the default credentials in production. If you are not " +
                                "using default credentials, you are getting this message because an administrator " +
                                "requires a password change.%n" +
                                "Changing your password is easy to do via the Neo4j Browser.%n" +
                                "If you are connecting via a shell or programmatically via a driver, " +
                                "just issue a `ALTER CURRENT USER SET PASSWORD FROM 'current password' TO 'new password'` " +
                                "statement against the system database in the current " +
                                "session, and then restart your driver with the new password configured." ),
                                Status.Security.CredentialsExpired );
                    }
                },

        /** Allows reading data and schema, but not writing. */
        READ( true, false, false, false, false ),
        /** Allows writing data */
        WRITE_ONLY( false, true, false, false, false ),
        /** Allows reading and writing data, but not schema. */
        WRITE( true, true, false, false, false ),
        /** Allows reading and writing data and creating new tokens, but not schema. */
        TOKEN_WRITE( true, true, true, false, false ),
        /** Allows reading and writing data and creating new tokens and changing schema. */
        SCHEMA( true, true, true, true, false ),
        /** Allows all operations. */
        FULL( true, true, true, true, true );

        private final boolean read;
        private final boolean write;
        private final boolean token;
        private final boolean schema;
        private final boolean procedureBoost;

        Static( boolean read, boolean write, boolean token, boolean schema, boolean procedureBoost )
        {
            this.read = read;
            this.write = write;
            this.token = token;
            this.schema = schema;
            this.procedureBoost = procedureBoost;
        }

        @Override
        public boolean allowsWrites()
        {
            return write;
        }

        @Override
        public boolean allowsTokenCreates( PrivilegeAction action )
        {
            return token;
        }

        @Override
        public boolean allowsSchemaWrites()
        {
            return schema;
        }

        @Override
        public boolean allowsSchemaWrites( PrivilegeAction action )
        {
            return schema;
        }

        @Override
        public boolean allowsShowIndex()
        {
            return schema;
        }

        @Override
        public boolean allowsShowConstraint()
        {
            return schema;
        }

        @Override
        public boolean allowsTraverseAllLabels()
        {
            return read;
        }

        @Override
        public boolean allowsTraverseAllNodesWithLabel( long label )
        {
            return read;
        }

        @Override
        public boolean disallowsTraverseLabel( long label )
        {
            return false;
        }

        @Override
        public boolean allowsTraverseNode( long... labels )
        {
            return read;
        }

        @Override
        public boolean allowsTraverseAllRelTypes()
        {
            return read;
        }

        @Override
        public boolean allowsTraverseRelType( int relType )
        {
            return read;
        }

        @Override
        public boolean disallowsTraverseRelType( int relType )
        {
            return false;
        }

        @Override
        public boolean allowsReadPropertyAllLabels( int propertyKey )
        {
            return read;
        }

        @Override
        public boolean disallowsReadPropertyForSomeLabel( int propertyKey )
        {
            return false;
        }

        @Override
        public boolean allowsReadNodeProperty( Supplier<TokenSet> labels, int propertyKey )
        {
            return read;
        }

        @Override
        public boolean allowsReadPropertyAllRelTypes( int propertyKey )
        {
            return read;
        }

        @Override
        public boolean allowsReadRelationshipProperty( RelTypeSupplier relType, int propertyKey )
        {
            return read;
        }

        @Override
        public boolean allowsSeePropertyKeyToken( int propertyKey )
        {
            return read;
        }

        @Override
        public boolean shouldBoostAccessForProcedureWith( String[] allowed )
        {
            return procedureBoost;
        }

        @Override
        public boolean allowsExecuteProcedure( int procedureId )
        {
            return true;
        }

        @Override
        public boolean shouldBoostProcedure( int procedureId )
        {
            return procedureBoost;
        }

        @Override
        public boolean allowsExecuteFunction( int id )
        {
            return true;
        }

        @Override
        public boolean shouldBoostFunction( int id )
        {
            return procedureBoost;
        }

        @Override
        public boolean allowsExecuteAggregatingFunction( int id )
        {
            return true;
        }

        @Override
        public boolean shouldBoostAggregatingFunction( int id )
        {
            return procedureBoost;
        }

        @Override
        public boolean allowsSetLabel( long labelId )
        {
            return write;
        }

        @Override
        public boolean allowsRemoveLabel( long labelId )
        {
            return write;
        }

        @Override
        public boolean allowsCreateNode( int[] labelIds )
        {
            return write;
        }

        @Override
        public boolean allowsDeleteNode( Supplier<TokenSet> labelSupplier )
        {
            return write;
        }

        @Override
        public boolean allowsCreateRelationship( int relType )
        {
            return write;
        }

        @Override
        public boolean allowsDeleteRelationship( int relType )
        {
            return write;
        }

        @Override
        public boolean allowsSetProperty( Supplier<TokenSet> labels, int propertyKey )
        {
            return write;
        }

        @Override
        public boolean allowsSetProperty( RelTypeSupplier relType, int propertyKey )
        {
            return write;
        }

        @Override
        public AuthorizationViolationException onViolation( String msg )
        {
            return new AuthorizationViolationException( msg );
        }
    }

    boolean allowsWrites();
    boolean allowsTokenCreates( PrivilegeAction action );
    boolean allowsSchemaWrites();
    boolean allowsSchemaWrites( PrivilegeAction action );
    boolean allowsShowIndex();
    boolean allowsShowConstraint();

    /** true if all nodes can be traversed */
    boolean allowsTraverseAllLabels();

    /** true if all nodes with this label always can be traversed */
    boolean allowsTraverseAllNodesWithLabel( long label );

    /** true if this label is blacklisted for traversal */
    boolean disallowsTraverseLabel( long label );

    /** true if a particular node with exactly these labels can be traversed.
     * @param labels the labels on the node to be checked. If labels only contains {@link org.neo4j.token.api.TokenConstants#ANY_LABEL} it will work
     *               the same as {@link #allowsTraverseAllLabels}
     */
    boolean allowsTraverseNode( long... labels );

    /** true if all relationships can be traversed */
    boolean allowsTraverseAllRelTypes();

    /**
     * true if the relType can be traversed.
     * @param relType the relationship type to check access for. If relType is {@link org.neo4j.token.api.TokenConstants#ANY_RELATIONSHIP_TYPE} it will work
     *               the same as {@link #allowsTraverseAllRelTypes}
     */
    boolean allowsTraverseRelType( int relType );

    /**
     * true if the relType is blacklisted for traversal.
     * @param relType the relationship type to check access for.
     */
    boolean disallowsTraverseRelType( int relType );

    boolean allowsReadPropertyAllLabels( int propertyKey );
    boolean disallowsReadPropertyForSomeLabel( int propertyKey );
    boolean allowsReadNodeProperty( Supplier<TokenSet> labels, int propertyKey );

    boolean allowsReadPropertyAllRelTypes( int propertyKey );

    boolean allowsReadRelationshipProperty( RelTypeSupplier relType, int propertyKey );

    boolean allowsSeePropertyKeyToken( int propertyKey );

    /**
     * Determines whether this mode allows execution of a procedure with the parameter string array in its
     * procedure annotation.
     *
     * @deprecated use {@link #allowsExecuteProcedure} and {@link #shouldBoostProcedure} instead
     * @param allowed An array of strings that encodes permissions that allows the execution of a procedure
     * @return {@code true} if this mode allows the execution of a procedure with the given parameter string array
     * encoding permission
     */
    @Deprecated
    boolean shouldBoostAccessForProcedureWith( String[] allowed );

    /**
     * Check if execution of a procedure is allowed
     * @param procedureId id of the procedure
     * @return true if the procedure with this id is allowed to be executed
     */
    boolean allowsExecuteProcedure( int procedureId );

    /**
     * Check if execution of a procedure should be done with boosted privileges.
     * <p>
     * <strong>Note: this does not check if execution is allowed</strong>
     * @param procedureId id of the procedure
     * @return true if the procedure with this id should be executed with boosted privileges
     */
    boolean shouldBoostProcedure( int procedureId );

    /**
     * Check if execution of a user defined function is allowed
     * @param id id of the function
     * @return true if the function with this id is allowed to be executed
     */
    boolean allowsExecuteFunction( int id );

    /**
     * Check if execution of a user defined function should be done with boosted privileges.
     * <p>
     * <strong>Note: this does not check if execution is allowed</strong>
     * @param id id of the function
     * @return true if the function with this id should be executed with boosted privileges
     */
    boolean shouldBoostFunction( int id );

    /**
     * Check if execution of a aggregating user defined function is allowed
     * @param id id of the function
     * @return true if the function with this id is allowed to be executed
     */
    boolean allowsExecuteAggregatingFunction( int id );

    /**
     * Check if execution of a aggregating user defined function should be done with boosted privileges.
     * <p>
     * <strong>Note: this does not check if execution is allowed</strong>
     * @param id id of the function
     * @return true if the function with this id should be executed with boosted privileges
     */
    boolean shouldBoostAggregatingFunction( int id );

    boolean allowsSetLabel( long labelId );

    boolean allowsRemoveLabel( long labelId );

    boolean allowsCreateNode( int[] labelIds );

    boolean allowsDeleteNode( Supplier<TokenSet> labelSupplier );

    boolean allowsCreateRelationship( int relType );

    boolean allowsDeleteRelationship( int relType );

    boolean allowsSetProperty( Supplier<TokenSet> labels, int propertyKey );

    boolean allowsSetProperty( RelTypeSupplier relType, int propertyKey );

    AuthorizationViolationException onViolation( String msg );
    String name();

    default Set<String> roles()
    {
        return Collections.emptySet();
    }

    default boolean isOverridden()
    {
        return false;
    }
}
