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
package org.neo4j.server.security.enterprise.auth;

import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.SimpleAuthorizationInfo;
import org.apache.shiro.authz.SimpleRole;
import org.apache.shiro.authz.permission.RolePermissionResolver;
import org.apache.shiro.realm.ldap.JndiLdapContextFactory;
import org.apache.shiro.realm.ldap.JndiLdapRealm;
import org.apache.shiro.realm.ldap.LdapContextFactory;
import org.apache.shiro.realm.ldap.LdapUtils;
import org.apache.shiro.subject.PrincipalCollection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.naming.CommunicationException;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.LdapContext;

import org.neo4j.kernel.api.security.AuthenticationResult;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * Shiro realm for LDAP based on configuration settings
 */
public class LdapRealm extends JndiLdapRealm
{
    private static final String GROUP_DELIMITER = ";";
    private static final String KEY_VALUE_DELIMITER = "=";
    private static final String ROLE_DELIMITER = ",";

    private Boolean authenticationEnabled;
    private Boolean authorizationEnabled;
    private String userSearchBase;
    private String userSearchFilter;
    private List<String> membershipAttributeNames;
    private Boolean useSystemAccountForAuthorization;
    private Map<String,Collection<String>> groupToRoleMapping;
    private final Log log;

    private Map<String, SimpleAuthorizationInfo> authorizationInfoCache = new ConcurrentHashMap<>();

    // Parser regex for group-to-role-mapping
    private static final String KEY_GROUP = "\\s*('(.+)'|\"(.+)\"|(\\S)|(\\S.*\\S))\\s*";
    private static final String VALUE_GROUP = "\\s*(.*)";
    private Pattern keyValuePattern = Pattern.compile( KEY_GROUP + KEY_VALUE_DELIMITER + VALUE_GROUP );

    public LdapRealm( Config config, LogProvider logProvider )
    {
        super();
        log = logProvider.getLog( getClass() );
        setRolePermissionResolver( rolePermissionResolver );
        configureRealm( config );
    }

    @Override
    protected AuthenticationInfo queryForAuthenticationInfo( AuthenticationToken token,
            LdapContextFactory ldapContextFactory )
            throws NamingException
    {
        return authenticationEnabled ? super.queryForAuthenticationInfo( token, ldapContextFactory ) : null;
    }

    @Override
    protected AuthorizationInfo queryForAuthorizationInfo( PrincipalCollection principals,
            LdapContextFactory ldapContextFactory ) throws NamingException
    {
        if ( authorizationEnabled )
        {
            String username = (String) getAvailablePrincipal( principals );

            if ( useSystemAccountForAuthorization )
            {
                // Perform context search using the system context
                LdapContext ldapContext = ldapContextFactory.getSystemLdapContext();

                Set<String> roleNames;
                try
                {
                    roleNames = findRoleNamesForUser( username, ldapContext );
                }
                finally
                {
                    LdapUtils.closeContext( ldapContext );
                }

                return new SimpleAuthorizationInfo( roleNames );
            }
            else
            {
                // Authorization info is cached during authentication
                AuthorizationInfo authorizationInfo = authorizationInfoCache.get( username );
                if ( authorizationInfo == null )
                {
                    // TODO: Do a new LDAP search? But we need to cache the credentials for that...
                    // Or we need the resulting failure message to the client to contain some status
                    // so that the client can react by resending the auth token.
                }
                return authorizationInfo;
            }
        }
        return null;
    }

    @Override
    protected AuthenticationInfo createAuthenticationInfo( AuthenticationToken token, Object ldapPrincipal,
            Object ldapCredentials, LdapContext ldapContext )
            throws NamingException
    {
        // NOTE: This will be called only if authentication with the ldap context was successful

        // If authorization is enabled but useSystemAccountForAuthorization is disabled, we should perform
        // the search for groups directly here while the user's authenticated ldap context is open.
        if ( authorizationEnabled && !useSystemAccountForAuthorization )
        {
            String username = (String) token.getPrincipal();
            Set<String> roleNames = findRoleNamesForUser( username, ldapContext );
            cacheAuthorizationInfo( username, roleNames );
        }

        return new ShiroAuthenticationInfo( token.getPrincipal(), token.getCredentials(), getName(),
                AuthenticationResult.SUCCESS );
    }

    @Override
    protected void clearCachedAuthorizationInfo( PrincipalCollection principals )
    {
        super.clearCachedAuthorizationInfo( principals );

        String username = (String) getAvailablePrincipal( principals );

        authorizationInfoCache.remove( username );
    }

    private void cacheAuthorizationInfo( String username, Set<String> roleNames )
    {
        // Ideally we would like to use the existing authorizationCache in our base class,
        // but unfortunately it is private to AuthorizingRealm
        authorizationInfoCache.put( username, new SimpleAuthorizationInfo( roleNames ) );
    }

    private final RolePermissionResolver rolePermissionResolver = new RolePermissionResolver()
    {
        @Override
        public Collection<Permission> resolvePermissionsInRole( String roleString )
        {
            SimpleRole role = PredefinedRolesBuilder.roles.get( roleString );
            if ( role != null )
            {
                return role.getPermissions();
            }
            else
            {
                return Collections.emptyList();
            }
        }
    };

    private void configureRealm( Config config )
    {
        JndiLdapContextFactory contextFactory = new JndiLdapContextFactory();
        contextFactory.setUrl( "ldap://" + config.get( SecuritySettings.ldap_server ) );
        contextFactory.setAuthenticationMechanism( config.get( SecuritySettings.ldap_auth_mechanism ) );
        contextFactory.setReferral( config.get( SecuritySettings.ldap_referral ) );
        contextFactory.setSystemUsername( config.get( SecuritySettings.ldap_system_username ) );
        contextFactory.setSystemPassword( config.get( SecuritySettings.ldap_system_password ) );

        setContextFactory( contextFactory );

        String userDnTemplate = config.get( SecuritySettings.ldap_user_dn_template );
        if ( userDnTemplate != null )
        {
            setUserDnTemplate( userDnTemplate );
        }

        authenticationEnabled = config.get( SecuritySettings.ldap_authentication_enabled );
        authorizationEnabled = config.get( SecuritySettings.ldap_authorization_enabled );

        userSearchBase = config.get( SecuritySettings.ldap_authorization_user_search_base );
        userSearchFilter = config.get( SecuritySettings.ldap_authorization_user_search_filter );
        membershipAttributeNames = config.get( SecuritySettings.ldap_authorization_group_membership_attribute_names );
        useSystemAccountForAuthorization = config.get( SecuritySettings.ldap_authorization_use_system_account );
        groupToRoleMapping =
                parseGroupToRoleMapping( config.get( SecuritySettings.ldap_authorization_group_to_role_mapping ) );
    }

    Map<String,Collection<String>> parseGroupToRoleMapping( String groupToRoleMappingString )
    {
        Map<String,Collection<String>> map = new HashMap<>();

        if ( groupToRoleMappingString != null )
        {
            for ( String groupAndRoles : groupToRoleMappingString.split( GROUP_DELIMITER ) )
            {
                if ( !groupAndRoles.isEmpty() )
                {
                    Matcher matcher = keyValuePattern.matcher( groupAndRoles );
                    if ( !(matcher.find() && matcher.groupCount() == 6) )
                    {
                        String errorMessage = String.format( "Failed to parse setting %s: wrong number of fields",
                                SecuritySettings.ldap_authorization_group_to_role_mapping.name() );
                        log.error( errorMessage );
                        throw new IllegalArgumentException( errorMessage );
                    }

                    String group = matcher.group(2) != null ? matcher.group(2) :
                                   matcher.group(3) != null ? matcher.group(3) :
                                   matcher.group(4) != null ? matcher.group(4) :
                                   matcher.group(5) != null ? matcher.group(5) : "";

                    if ( group.isEmpty() )
                    {
                        String errorMessage = String.format( "Failed to parse setting %s: empty group name",
                                SecuritySettings.ldap_authorization_group_to_role_mapping.name() );
                        log.error( errorMessage );
                        throw new IllegalArgumentException( errorMessage );
                    }
                    Collection<String> roleList = new ArrayList<>();
                    for ( String role : matcher.group(6).trim().split( ROLE_DELIMITER ) )
                    {
                        if ( !role.isEmpty() )
                        {
                            roleList.add( role );
                        }
                    }
                    // We only support case-insensitive comparison of group DNs
                    map.put( group.toLowerCase(), roleList );
                }
            }
        }

        return map;
    }

    // TODO: Extract to an LdapAuthorizationStrategy ? This ("group by attribute") is one of multiple possible strategies
    Set<String> findRoleNamesForUser( String username, LdapContext ldapContext ) throws NamingException
    {
        Set<String> roleNames = new LinkedHashSet<String>();

        // For some combinations of settings we will never find anything
        if ( !validateUserSearchSettings() )
        {
            return roleNames;
        }

        SearchControls searchCtls = new SearchControls();
        searchCtls.setSearchScope( SearchControls.SUBTREE_SCOPE );
        searchCtls.setReturningAttributes( membershipAttributeNames.toArray( new String[1] ) );

        // Use search argument to prevent potential code injection
        Object[] searchArguments = new Object[]{username};

        NamingEnumeration result = ldapContext.search( userSearchBase, userSearchFilter, searchArguments, searchCtls );

        if ( result.hasMoreElements() )
        {
            SearchResult searchResult = (SearchResult) result.next();

            if ( result.hasMoreElements() )
            {
                log.warn( String.format(
                        "LDAP user search for user principal '%s' is ambiguous. The first match that will be checked " +
                        "for group membership is '%s' " +
                        "but the search also matches '%s'. Please check your LDAP realm configuration.",
                        username,
                        // TODO: Check if it is ok to write this potentially sensitive information to the log
                        searchResult.toString(),
                        ((SearchResult) result.next()).toString() ) );
            }

            Attributes attributes = searchResult.getAttributes();
            if ( attributes != null )
            {
                NamingEnumeration attributeEnumeration = attributes.getAll();
                while ( attributeEnumeration.hasMore() )
                {
                    Attribute attribute = (Attribute) attributeEnumeration.next();
                    String attributeId = attribute.getID();
                    if ( membershipAttributeNames.stream()
                            .anyMatch( searchAttribute -> attributeId.equalsIgnoreCase( searchAttribute ) ) )
                    {
                        Collection<String> groupNames = LdapUtils.getAllAttributeValues( attribute );
                        Collection<String> rolesForGroups = getRoleNamesForGroups( groupNames );
                        roleNames.addAll( rolesForGroups );
                    }
                }
            }
        }
        return roleNames;
    }

    private boolean validateUserSearchSettings()
    {
        boolean proceedWithSearch = true;

        if ( userSearchBase == null || userSearchBase.isEmpty() )
        {
            log.warn( "LDAP user search base is empty." );
            proceedWithSearch = false;
        }
        if ( userSearchFilter == null || !userSearchFilter.contains( "{0}" ) )
        {
            log.warn( "LDAP user search filter does not contain the argument placeholder {0}, so the search result " +
                      "will be independent of the user principal." );
        }
        if ( membershipAttributeNames == null || membershipAttributeNames.isEmpty() )
        {
            // If we don't have any attributes to look for we will never find anything
            log.warn( "LDAP group membership attribute names are empty. Authorization will not be possible." );
            proceedWithSearch = false;
        }

        return proceedWithSearch;
    }

    private Collection<String> getRoleNamesForGroups( Collection<String> groupNames )
    {
        Collection<String> roles = new ArrayList<>();
        for ( String group : groupNames )
        {
            Collection<String> rolesForGroup = groupToRoleMapping.get( group.toLowerCase() );
            if ( rolesForGroup != null )
            {
                roles.addAll( rolesForGroup );
            }
        }
        return roles;
    }

    // Exposed for testing
    Map<String,Collection<String>> getGroupToRoleMapping()
    {
        return groupToRoleMapping;
    }
}
