/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.security.enterprise.auth.plugin;

import java.util.Hashtable;
import java.util.LinkedHashSet;
import java.util.Set;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;

import org.neo4j.server.security.enterprise.auth.plugin.api.AuthToken;
import org.neo4j.server.security.enterprise.auth.plugin.api.AuthenticationException;
import org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthInfo;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthPlugin;

public class LdapGroupHasUsersAuthPlugin extends AuthPlugin.Adapter
{
    private static final String GROUP_SEARCH_BASE = "ou=groups,dc=example,dc=com";
    private static final String GROUP_SEARCH_FILTER = "(&(objectClass=posixGroup)(memberUid={0}))";
    public static final String GROUP_ID = "gidNumber";

    @Override
    public String name()
    {
        return "ldap-alternative-groups";
    }

    @Override
    public AuthInfo authenticateAndAuthorize( AuthToken authToken ) throws AuthenticationException
    {
        try
        {
            String username = authToken.principal();
            char[] password = authToken.credentials();

            LdapContext ctx = authenticate( username, password );
            Set<String> roles = authorize( ctx, username );

            return AuthInfo.of( username, roles );
        }
        catch ( NamingException e )
        {
            throw new AuthenticationException( e.getMessage() );
        }
    }

    private LdapContext authenticate( String username, char[] password ) throws NamingException
    {
        Hashtable<String,Object> env = new Hashtable<>();
        env.put( Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory" );
        env.put( Context.PROVIDER_URL, "ldap://0.0.0.0:10389" );

        env.put( Context.SECURITY_PRINCIPAL, String.format( "cn=%s,ou=users,dc=example,dc=com", username ) );
        env.put( Context.SECURITY_CREDENTIALS, password );

        return new InitialLdapContext( env, null );
    }

    private Set<String> authorize( LdapContext ctx, String username ) throws NamingException
    {
        Set<String> roleNames = new LinkedHashSet<>();

        // Setup our search controls
        SearchControls searchCtls = new SearchControls();
        searchCtls.setSearchScope( SearchControls.SUBTREE_SCOPE );
        searchCtls.setReturningAttributes( new String[]{GROUP_ID} );

        // Use a search argument to prevent potential code injection
        Object[] searchArguments = new Object[]{username};

        // Search for groups that has the user as a member
        NamingEnumeration result = ctx.search( GROUP_SEARCH_BASE, GROUP_SEARCH_FILTER, searchArguments, searchCtls );

        if ( result.hasMoreElements() )
        {
            SearchResult searchResult = (SearchResult) result.next();

            Attributes attributes = searchResult.getAttributes();
            if ( attributes != null )
            {
                NamingEnumeration attributeEnumeration = attributes.getAll();
                while ( attributeEnumeration.hasMore() )
                {
                    Attribute attribute = (Attribute) attributeEnumeration.next();
                    String attributeId = attribute.getID();
                    if ( attributeId.equalsIgnoreCase( GROUP_ID ) )
                    {
                        // We found a group that the user is a member of. See if it has a role mapped to it
                        String groupId = (String) attribute.get();
                        String neo4jGroup = getNeo4jRoleForGroupId( groupId );
                        if ( neo4jGroup != null )
                        {
                            // Yay! Add it to our set of roles
                            roleNames.add( neo4jGroup );
                        }
                    }
                }
            }
        }
        return roleNames;
    }

    private String getNeo4jRoleForGroupId( String groupId )
    {
        if ( "500".equals( groupId ) ) return PredefinedRoles.READER;
        if ( "501".equals( groupId ) ) return PredefinedRoles.PUBLISHER;
        if ( "502".equals( groupId ) ) return PredefinedRoles.ARCHITECT;
        if ( "503".equals( groupId ) ) return PredefinedRoles.ADMIN;
        return null;
    }
}
