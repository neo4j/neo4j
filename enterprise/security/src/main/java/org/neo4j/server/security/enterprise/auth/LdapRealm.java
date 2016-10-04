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
import org.apache.shiro.authc.credential.HashedCredentialsMatcher;
import org.apache.shiro.authz.AuthorizationException;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.authz.SimpleAuthorizationInfo;
import org.apache.shiro.cache.Cache;
import org.apache.shiro.realm.ldap.JndiLdapContextFactory;
import org.apache.shiro.realm.ldap.JndiLdapRealm;
import org.apache.shiro.realm.ldap.LdapContextFactory;
import org.apache.shiro.realm.ldap.LdapUtils;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.util.CollectionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;
import javax.naming.ldap.StartTlsRequest;
import javax.naming.ldap.StartTlsResponse;

import org.neo4j.graphdb.security.AuthExpirationException;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.api.security.AuthenticationResult;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.server.security.enterprise.auth.plugin.api.RealmOperations;
import org.neo4j.server.security.enterprise.auth.plugin.spi.RealmLifecycle;
import org.neo4j.server.security.enterprise.configuration.SecuritySettings;
import org.neo4j.server.security.enterprise.log.SecurityLog;

import static java.lang.String.format;
import org.neo4j.server.security.auth.Credential;

/**
 * Shiro realm for LDAP based on configuration settings
 */
public class LdapRealm extends JndiLdapRealm implements RealmLifecycle
{
    private static final String GROUP_DELIMITER = ";";
    private static final String KEY_VALUE_DELIMITER = "=";
    private static final String ROLE_DELIMITER = ",";
    public static final String LDAP_REALM = "ldap";

    private Boolean authenticationEnabled;
    private Boolean authorizationEnabled;
    private Boolean useStartTls;
    private String userSearchBase;
    private String userSearchFilter;
    private List<String> membershipAttributeNames;
    private Boolean useSystemAccountForAuthorization;
    private Map<String,Collection<String>> groupToRoleMapping;
    private final SecurityLog securityLog;

    // Parser regex for group-to-role-mapping
    private static final String KEY_GROUP = "\\s*('(.+)'|\"(.+)\"|(\\S)|(\\S.*\\S))\\s*";
    private static final String VALUE_GROUP = "\\s*(.*)";
    private Pattern keyValuePattern = Pattern.compile( KEY_GROUP + KEY_VALUE_DELIMITER + VALUE_GROUP );

    public LdapRealm( Config config, SecurityLog securityLog )
    {
        super();
        setName( SecuritySettings.LDAP_REALM_NAME );
        this.securityLog = securityLog;
        setRolePermissionResolver( PredefinedRolesBuilder.rolePermissionResolver );
        configureRealm( config );
        setCredentialsMatcher( new HashedCredentialsMatcher( Credential.DIGEST_ALGO ) );
        setAuthorizationCachingEnabled( true );
        boolean authenticationCachingEnabled = config.get( SecuritySettings.ldap_authentication_cache_enabled );
        setAuthenticationCachingEnabled( authenticationCachingEnabled );
    }

    private String withRealm( String template, Object... args )
    {
        return "{LdapRealm}: " + format( template, args );
    }

    private String server( JndiLdapContextFactory jndiLdapContextFactory )
    {
        return "'" + jndiLdapContextFactory.getUrl() + "'" +
                ( useStartTls ? " using StartTLS" : "" );
    }

    @Override
    protected AuthenticationInfo queryForAuthenticationInfo( AuthenticationToken token,
            LdapContextFactory ldapContextFactory )
            throws NamingException
    {
        if ( authenticationEnabled )
        {
            String serverString = server( (JndiLdapContextFactory) ldapContextFactory );
            try
            {
                AuthenticationInfo info =
                        useStartTls ? queryForAuthenticationInfoUsingStartTls( token, ldapContextFactory ) :
                        super.queryForAuthenticationInfo( token, ldapContextFactory );
                securityLog.debug( withRealm( "Authenticated user '%s' against %s",
                        token.getPrincipal(), serverString ) );
                return info;
            }
            catch ( Exception e )
            {
                securityLog.error( withRealm( "Failed to authenticate user '%s' against %s: %s",
                        token.getPrincipal(), serverString, e.getMessage() ) );
                throw e;
            }
        }
        else
        {
            return null;
        }
    }

    protected AuthenticationInfo queryForAuthenticationInfoUsingStartTls( AuthenticationToken token,
            LdapContextFactory ldapContextFactory ) throws NamingException
    {
        Object principal = getLdapPrincipal(token);
        Object credentials = token.getCredentials();

        LdapContext ctx = null;

        try {
            ctx = getLdapContextUsingStartTls( ldapContextFactory, principal, credentials );

            return createAuthenticationInfo( token, principal, credentials, ctx );
        }
        finally
        {
            LdapUtils.closeContext( ctx );
        }
    }

    private LdapContext getLdapContextUsingStartTls( LdapContextFactory ldapContextFactory,
            Object principal, Object credentials ) throws NamingException
    {
        JndiLdapContextFactory jndiLdapContextFactory = (JndiLdapContextFactory) ldapContextFactory;
        Hashtable<String, Object> env = new Hashtable<>();
        env.put( Context.INITIAL_CONTEXT_FACTORY, jndiLdapContextFactory.getContextFactoryClassName() );
        env.put( Context.PROVIDER_URL, jndiLdapContextFactory.getUrl() );

        LdapContext ctx = null;

        try
        {
            ctx = new InitialLdapContext( env, null );

            StartTlsRequest startTlsRequest = new StartTlsRequest();
            StartTlsResponse tls = (StartTlsResponse) ctx.extendedOperation( startTlsRequest );

            tls.negotiate();

            ctx.addToEnvironment( Context.SECURITY_AUTHENTICATION,
                    jndiLdapContextFactory.getAuthenticationMechanism() );
            ctx.addToEnvironment( Context.SECURITY_PRINCIPAL, principal );
            ctx.addToEnvironment( Context.SECURITY_CREDENTIALS, credentials );

            ctx.reconnect( ctx.getConnectControls() );

            return ctx;
        }
        catch ( IOException e )
        {
            LdapUtils.closeContext( ctx );
            securityLog.error( withRealm( "Failed to negotiate TLS connection with '%s': ",
                    server( jndiLdapContextFactory ), e ) );
            throw new CommunicationException( e.getMessage() );
        }
        catch ( Throwable t )
        {
            LdapUtils.closeContext( ctx );
            securityLog.error( withRealm( "Unexpected failure to negotiate TLS connection with '%s': ",
                    server( jndiLdapContextFactory ), t ) );
            throw t;
        }
    }

    @Override
    protected AuthorizationInfo queryForAuthorizationInfo( PrincipalCollection principals,
            LdapContextFactory ldapContextFactory ) throws NamingException
    {
        if ( authorizationEnabled )
        {
            Collection realmPrincipals = principals.fromRealm( getName() );
            if (!CollectionUtils.isEmpty(realmPrincipals))
            {
                String username = (String) realmPrincipals.iterator().next();
                if ( useSystemAccountForAuthorization )
                {
                    // Perform context search using the system context
                    LdapContext ldapContext = useStartTls ? getSystemLdapContextUsingStartTls( ldapContextFactory ) :
                                              ldapContextFactory.getSystemLdapContext();

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
                    Cache<Object,AuthorizationInfo> authorizationCache = getAuthorizationCache();
                    AuthorizationInfo authorizationInfo = authorizationCache.get( username );
                    if ( authorizationInfo == null )
                    {
                        // The cached authorization info has expired.
                        // Since we do not have the subject's credentials we cannot perform a new LDAP search
                        // for authorization info. Instead we need to fail with a special status,
                        // so that the client can react by re-authenticating.
                        throw new AuthExpirationException( "LDAP authorization info expired." );
                    }
                    return authorizationInfo;
                }
            }
        }
        return null;
    }

    private LdapContext getSystemLdapContextUsingStartTls( LdapContextFactory ldapContextFactory )
            throws NamingException
    {
        JndiLdapContextFactory jndiLdapContextFactory = (JndiLdapContextFactory) ldapContextFactory;
        return getLdapContextUsingStartTls( ldapContextFactory, jndiLdapContextFactory.getSystemUsername(),
                jndiLdapContextFactory.getSystemPassword() );
    }

    @Override
    protected AuthenticationInfo createAuthenticationInfo( AuthenticationToken token, Object ldapPrincipal,
            Object ldapCredentials, LdapContext ldapContext )
            throws NamingException
    {
        // If authorization is enabled but useSystemAccountForAuthorization is disabled, we should perform
        // the search for groups directly here while the user's authenticated ldap context is open.
        if ( authorizationEnabled && !useSystemAccountForAuthorization )
        {
            String username = (String) token.getPrincipal();
            Set<String> roleNames = findRoleNamesForUser( username, ldapContext );
            cacheAuthorizationInfo( username, roleNames );
        }

        return new ShiroAuthenticationInfo( token.getPrincipal(), (String) token.getCredentials(), getName(),
                AuthenticationResult.SUCCESS );
    }

    @Override
    public boolean supports( AuthenticationToken token )
    {
        return supportsSchemeAndRealm( token );
    }

    private boolean supportsSchemeAndRealm( AuthenticationToken token )
    {
        try
        {
            if ( token instanceof ShiroAuthToken )
            {
                ShiroAuthToken shiroAuthToken = (ShiroAuthToken) token;
                return shiroAuthToken.getScheme().equals( AuthToken.BASIC_SCHEME ) &&
                       (shiroAuthToken.supportsRealm( LDAP_REALM ));
            }
            return false;
        }
        catch ( InvalidAuthTokenException e )
        {
            return false;
        }
    }

    @Override
    protected AuthorizationInfo doGetAuthorizationInfo( PrincipalCollection principals )
    {
        try
        {
            AuthorizationInfo info = super.doGetAuthorizationInfo( principals );
            securityLog.debug( withRealm( "Queried for authorization info for user '%s'",
                    principals.getPrimaryPrincipal() ) );
            return info;
        }
        catch ( AuthorizationException e )
        {
            securityLog.error( withRealm( "Failed to get authorization info: '%s' caused by '%s'",
                    e.getMessage(), e.getCause().getMessage() ) );
            throw e;
        }
    }

    private void cacheAuthorizationInfo( String username, Set<String> roleNames )
    {
        // Use the existing authorizationCache in our base class
        Cache<Object, AuthorizationInfo> authorizationCache = getAuthorizationCache();
        authorizationCache.put( username, new SimpleAuthorizationInfo( roleNames ) );
    }

    private void configureRealm( Config config )
    {
        JndiLdapContextFactory contextFactory = new JndiLdapContextFactory();
        contextFactory.setUrl( parseLdapServerUrl( config.get( SecuritySettings.ldap_server ) ) );
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
        useStartTls = config.get( SecuritySettings.ldap_use_starttls );

        userSearchBase = config.get( SecuritySettings.ldap_authorization_user_search_base );
        userSearchFilter = config.get( SecuritySettings.ldap_authorization_user_search_filter );
        membershipAttributeNames = config.get( SecuritySettings.ldap_authorization_group_membership_attribute_names );
        useSystemAccountForAuthorization = config.get( SecuritySettings.ldap_authorization_use_system_account );
        groupToRoleMapping =
                parseGroupToRoleMapping( config.get( SecuritySettings.ldap_authorization_group_to_role_mapping ) );
    }

    private String parseLdapServerUrl( String rawLdapServer )
    {
        return (rawLdapServer == null) ? null :
               rawLdapServer.contains( "://" ) ? rawLdapServer : "ldap://" + rawLdapServer;
    }

    private Map<String,Collection<String>> parseGroupToRoleMapping( String groupToRoleMappingString )
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
                        String errorMessage = format( "Failed to parse setting %s: wrong number of fields",
                                SecuritySettings.ldap_authorization_group_to_role_mapping.name() );
                        throw new IllegalArgumentException( errorMessage );
                    }

                    String group = matcher.group(2) != null ? matcher.group(2) :
                                   matcher.group(3) != null ? matcher.group(3) :
                                   matcher.group(4) != null ? matcher.group(4) :
                                   matcher.group(5) != null ? matcher.group(5) : "";

                    if ( group.isEmpty() )
                    {
                        String errorMessage = format( "Failed to parse setting %s: empty group name",
                                SecuritySettings.ldap_authorization_group_to_role_mapping.name() );
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
        Set<String> roleNames = new LinkedHashSet<>();

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
                securityLog.warn(
                        securityLog.isDebugEnabled() ?
                        withRealm(
                            "LDAP user search for user principal '%s' is ambiguous. The first match that will " +
                                            "be checked for group membership is '%s' but the search also matches '%s'. " +
                                            "Please check your LDAP realm configuration.",
                            username, searchResult.toString(), result.next().toString() )
                        :
                        withRealm(
                            "LDAP user search for user principal '%s' is ambiguous. The search matches more " +
                                            "than one entry. Please check your LDAP realm configuration.",
                            username )
                    );
            }

            Attributes attributes = searchResult.getAttributes();
            if ( attributes != null )
            {
                NamingEnumeration attributeEnumeration = attributes.getAll();
                while ( attributeEnumeration.hasMore() )
                {
                    Attribute attribute = (Attribute) attributeEnumeration.next();
                    String attributeId = attribute.getID();
                    if ( membershipAttributeNames.stream().anyMatch( attributeId::equalsIgnoreCase ) )
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

    private void assertValidUserSearchSettings()
    {
        boolean proceedWithSearch = true;

        if ( userSearchBase == null || userSearchBase.isEmpty() )
        {
            securityLog.error( "LDAP user search base is empty." );
            proceedWithSearch = false;
        }
        if ( userSearchFilter == null || !userSearchFilter.contains( "{0}" ) )
        {
            securityLog.warn( "LDAP user search filter does not contain the argument placeholder {0}, " +
                    "so the search result will be independent of the user principal." );
        }
        if ( membershipAttributeNames == null || membershipAttributeNames.isEmpty() )
        {
            // If we don't have any attributes to look for we will never find anything
            securityLog.error( "LDAP group membership attribute names are empty. Authorization will not be possible." );
            proceedWithSearch = false;
        }

        if ( !proceedWithSearch )
        {
            throw new IllegalArgumentException( "Illegal LDAP user search settings, see security log for details." );
        }
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

    @Override
    public void initialize( RealmOperations realmOperations ) throws Throwable
    {
        if ( authorizationEnabled )
        {
            // For some combinations of settings we will never find anything
            assertValidUserSearchSettings();
        }
    }

    @Override
    public void start() throws Throwable
    {
    }

    @Override
    public void stop() throws Throwable
    {
    }

    @Override
    public void shutdown() throws Throwable
    {
    }
}
