/*
 * Copyright (c) "Neo4j"
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

import java.util.Arrays;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.kernel.api.TokenSet;

import static java.lang.String.format;

/**
 * can be used for authorization and security logging
 */
public class SecurityAuthorizationHandler
{
    AbstractSecurityLog securityLog;

    public SecurityAuthorizationHandler( AbstractSecurityLog securityLog )
    {
        this.securityLog = securityLog;
    }

    public void assertAllowsCreateNode( SecurityContext securityContext, Function<Integer,String> resolver, int[] labelIds )
    {
        AccessMode accessMode = securityContext.mode();
        if ( !accessMode.allowsCreateNode( labelIds ) )
        {
            String labels = null == labelIds ? "" : Arrays.stream( labelIds ).mapToObj( resolver::apply ).collect( Collectors.joining( "," ) );
            throw logAndGetAuthorizationException( securityContext,
                                                   format( "Create node with labels '%s' is not allowed for %s.", labels, securityContext.description() ) );
        }
    }

    public void assertAllowsDeleteNode( SecurityContext securityContext, Function<Integer,String> resolver, Supplier<TokenSet> labelSupplier )
    {
        AccessMode accessMode = securityContext.mode();
        if ( !accessMode.allowsDeleteNode( labelSupplier ) )
        {
            String labels = Arrays.stream( labelSupplier.get().all() ).mapToObj( id -> resolver.apply( (int) id ) ).collect( Collectors.joining( "," ) );
            throw logAndGetAuthorizationException( securityContext,
                                                   format( "Delete node with labels '%s' is not allowed for %s.", labels, securityContext.description() ) );
        }
    }

    public void assertAllowsCreateRelationship( SecurityContext securityContext, Function<Integer,String> resolver, int relType )
    {
        AccessMode accessMode = securityContext.mode();
        if ( !accessMode.allowsCreateRelationship( relType ) )
        {
            throw logAndGetAuthorizationException( securityContext,
                                                   format( "Create relationship with type '%s' is not allowed for %s.", resolver.apply( relType ),
                                                           securityContext.description() ) );
        }
    }

    public void assertAllowsDeleteRelationship( SecurityContext securityContext, Function<Integer,String> resolver, int relType )
    {
        AccessMode accessMode = securityContext.mode();
        if ( !accessMode.allowsDeleteRelationship( relType ) )
        {
            throw logAndGetAuthorizationException( securityContext,
                                                   format( "Delete relationship with type '%s' is not allowed for %s.", resolver.apply( relType ),
                                                           securityContext.description() ) );
        }
    }

    public void assertAllowsSetLabel( SecurityContext securityContext, Function<Integer,String> resolver, long labelId )
    {
        AccessMode accessMode = securityContext.mode();
        if ( !accessMode.allowsSetLabel( labelId ) )
        {
            throw logAndGetAuthorizationException( securityContext,
                                                   format( "Set label for label '%s' is not allowed for %s.", resolver.apply( (int) labelId ),
                                                           securityContext.description() ) );
        }
    }

    public void assertAllowsRemoveLabel( SecurityContext securityContext, Function<Integer,String> resolver, long labelId )
    {
        AccessMode accessMode = securityContext.mode();
        if ( !accessMode.allowsRemoveLabel( labelId ) )
        {
            throw logAndGetAuthorizationException( securityContext,
                                                   format( "Remove label for label '%s' is not allowed for %s.", resolver.apply( (int) labelId ),
                                                           securityContext.description() ) );
        }
    }

    public void assertAllowsSetProperty( SecurityContext securityContext, Function<Long,String> resolver, TokenSet labelIds, long propertyKey )
    {
        AccessMode accessMode = securityContext.mode();
        if ( !accessMode.allowsSetProperty( () -> labelIds, (int) propertyKey ) )
        {
            throw logAndGetAuthorizationException( securityContext,
                                                   format( "Set property for property '%s' is not allowed for %s.", resolver.apply( propertyKey ),
                                                           securityContext.description() ) );
        }
    }

    public void assertAllowsSetProperty( SecurityContext securityContext, Function<Long,String> resolver, long relType, long propertyKey )
    {
        AccessMode accessMode = securityContext.mode();
        if ( !accessMode.allowsSetProperty( () -> (int) relType, (int) propertyKey ) )
        {
            throw logAndGetAuthorizationException( securityContext,
                                                   format( "Set property for property '%s' is not allowed for %s.", resolver.apply( propertyKey ),
                                                           securityContext.description() ) );
        }
    }

    public void assertAllowsSchemaWrites( SecurityContext securityContext )
    {
        AccessMode accessMode = securityContext.mode();
        if ( !accessMode.allowsSchemaWrites() )
        {
            throw logAndGetAuthorizationException( securityContext, format( "Schema operations are not allowed for %s.", securityContext.description() ) );
        }
    }

    public void assertSchemaWrites( SecurityContext securityContext, PrivilegeAction action )
    {
        AccessMode accessMode = securityContext.mode();
        if ( !accessMode.allowsSchemaWrites( action ) )
        {
            throw logAndGetAuthorizationException( securityContext,
                                                   format( "Schema operation '%s' is not allowed for %s.", action, securityContext.description() ) );
        }
    }

    public void assertShowIndexAllowed( SecurityContext securityContext )
    {
        AccessMode accessMode = securityContext.mode();
        if ( !accessMode.allowsShowIndex() )
        {
            throw logAndGetAuthorizationException( securityContext, format( "Show indexes are not allowed for %s.", securityContext.description() ) );
        }
    }

    public void assertShowConstraintAllowed( SecurityContext securityContext )
    {
        AccessMode accessMode = securityContext.mode();
        if ( !accessMode.allowsShowConstraint() )
        {
            throw logAndGetAuthorizationException( securityContext, format( "Show constraints are not allowed for %s.", securityContext.description() ) );
        }
    }

    public final void assertAllowsTokenCreates( SecurityContext securityContext, PrivilegeAction action )
    {
        AccessMode accessMode = securityContext.mode();
        if ( !accessMode.allowsTokenCreates( action ) )
        {
            switch ( action )
            {
            case CREATE_LABEL:
                throw logAndGetAuthorizationException( securityContext,
                                                       format( "Creating new node label is not allowed for %s. See GRANT CREATE NEW NODE LABEL ON DATABASE...",
                                                               securityContext.description() ) );
            case CREATE_PROPERTYKEY:
                throw logAndGetAuthorizationException( securityContext,
                                                       format( "Creating new property name is not allowed for %s. See GRANT CREATE NEW PROPERTY NAME ON " +
                                                               "DATABASE...",
                                                               securityContext.description() ) );
            case CREATE_RELTYPE:
                throw logAndGetAuthorizationException( securityContext,
                                                       format( "Creating new relationship type is not allowed for %s. See GRANT CREATE NEW RELATIONSHIP TYPE " +
                                                               "ON DATABASE...",
                                                               securityContext.description() ) );
            default:
                throw logAndGetAuthorizationException( securityContext,
                                                       format( "'%s' operations are not allowed for %s.", action, securityContext.description() ) );
            }
        }
    }

    public AuthorizationViolationException logAndGetAuthorizationException( SecurityContext securityContext, String message )
    {
        securityLog.error( securityContext, message );
        return new AuthorizationViolationException( message );
    }

    public static String generateCredentialsExpiredMessage( String message )
    {
        return format(
                "%s%n%nThe credentials you provided were valid, but must be changed before you can use this instance. " +
                "If this is the first time you are using Neo4j, this is to ensure you are not using the default credentials in production. " +
                "If you are not using default credentials, you are getting this message because an administrator requires a password change.%n" +
                "Changing your password is easy to do via the Neo4j Browser.%n" +
                "If you are connecting via a shell or programmatically via a driver, " +
                "just issue a `ALTER CURRENT USER SET PASSWORD FROM 'current password' TO 'new password'` " +
                "statement against the system database in the current session, and then restart your driver with the new password configured.", message );
    }
}
