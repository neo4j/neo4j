/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.server.security.enterprise.auth;

import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.SimpleAuthenticationInfo;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.authz.SimpleAuthorizationInfo;
import org.apache.shiro.realm.ldap.JndiLdapContextFactory;
import org.apache.shiro.realm.ldap.LdapContextFactory;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.subject.SimplePrincipalCollection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.internal.matchers.Any;

import java.time.Duration;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.LdapContext;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.server.security.enterprise.configuration.SecuritySettings;
import org.neo4j.server.security.enterprise.log.SecurityLog;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.test.assertion.Assert.assertException;

public class LdapRealmTest
{
    Config config = mock( Config.class );
    private SecurityLog securityLog = mock( SecurityLog.class );
    private SecureHasher secureHasher = new SecureHasher();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp()
    {
        // Some dummy settings to pass validation
        when( config.get( SecuritySettings.ldap_authorization_user_search_base ) )
                .thenReturn( "dc=example,dc=com" );
        when( config.get( SecuritySettings.ldap_authorization_group_membership_attribute_names ) )
                .thenReturn( singletonList( "memberOf" ) );

        when( config.get( SecuritySettings.ldap_authentication_enabled ) ).thenReturn( true );
        when( config.get( SecuritySettings.ldap_authorization_enabled ) ).thenReturn( true );
        when( config.get( SecuritySettings.ldap_authentication_cache_enabled ) ).thenReturn( false );
        when( config.get( SecuritySettings.ldap_connection_timeout ) ).thenReturn( Duration.ofSeconds( 1 ) );
        when( config.get( SecuritySettings.ldap_read_timeout ) ).thenReturn( Duration.ofSeconds( 1 ) );
        when( config.get( SecuritySettings.ldap_authorization_connection_pooling ) ).thenReturn( true );
        when( config.get( SecuritySettings.ldap_authentication_use_samaccountname ) ).thenReturn( false );
    }

    @Test
    public void groupToRoleMappingShouldBeAbleToBeNull()
    {
        when( config.get( SecuritySettings.ldap_authorization_group_to_role_mapping ) ).thenReturn( null );

        new LdapRealm( config, securityLog, secureHasher );
    }

    @Test
    public void groupToRoleMappingShouldBeAbleToBeEmpty()
    {
        when( config.get( SecuritySettings.ldap_authorization_group_to_role_mapping ) ).thenReturn( "" );

        new LdapRealm( config, securityLog, secureHasher );
    }

    @Test
    public void groupToRoleMappingShouldBeAbleToHaveMultipleRoles()
    {
        when( config.get( SecuritySettings.ldap_authorization_group_to_role_mapping ) )
                .thenReturn( "group=role1,role2,role3" );

        LdapRealm realm = new LdapRealm( config, securityLog, secureHasher );

        assertThat( realm.getGroupToRoleMapping().get( "group" ),
                equalTo( asList( "role1", "role2", "role3" ) ) );
        assertThat( realm.getGroupToRoleMapping().size(), equalTo( 1 ) );
    }

    @Test
    public void groupToRoleMappingShouldBeAbleToHaveMultipleGroups()
    {
        when( config.get( SecuritySettings.ldap_authorization_group_to_role_mapping ) )
                .thenReturn( "group1=role1;group2=role2,role3;group3=role4" );

        LdapRealm realm = new LdapRealm( config, securityLog, secureHasher );

        assertThat( realm.getGroupToRoleMapping().keySet(),
                equalTo( new TreeSet<>( asList( "group1", "group2", "group3" ) ) ) );
        assertThat( realm.getGroupToRoleMapping().get( "group1" ), equalTo( singletonList( "role1" ) ) );
        assertThat( realm.getGroupToRoleMapping().get( "group2" ), equalTo( asList( "role2", "role3" ) ) );
        assertThat( realm.getGroupToRoleMapping().get( "group3" ), equalTo( singletonList( "role4" ) ) );
        assertThat( realm.getGroupToRoleMapping().size(), equalTo( 3 ) );
    }

    @Test
    public void groupToRoleMappingShouldBeAbleToHaveQuotedKeysAndWhitespaces()
    {
        when( config.get( SecuritySettings.ldap_authorization_group_to_role_mapping ) )
                .thenReturn( "'group1' = role1;\t \"group2\"\n=\t role2,role3 ;  gr oup3= role4\n ;'group4 '= ; g =r" );

        LdapRealm realm = new LdapRealm( config, securityLog, secureHasher );

        assertThat( realm.getGroupToRoleMapping().keySet(),
                equalTo( new TreeSet<>( asList( "group1", "group2", "gr oup3", "group4 ", "g" ) ) ) );
        assertThat( realm.getGroupToRoleMapping().get( "group1" ), equalTo( singletonList( "role1" ) ) );
        assertThat( realm.getGroupToRoleMapping().get( "group2" ), equalTo( asList( "role2", "role3" ) ) );
        assertThat( realm.getGroupToRoleMapping().get( "gr oup3" ), equalTo( singletonList( "role4" ) ) );
        assertThat( realm.getGroupToRoleMapping().get( "group4 " ), equalTo( Collections.emptyList() ) );
        assertThat( realm.getGroupToRoleMapping().get( "g" ), equalTo( singletonList( "r" ) ) );
        assertThat( realm.getGroupToRoleMapping().size(), equalTo( 5 ) );
    }

    @Test
    public void groupToRoleMappingShouldBeAbleToHaveTrailingSemicolons()
    {
        when( config.get( SecuritySettings.ldap_authorization_group_to_role_mapping ) ).thenReturn( "group=role;;" );

        LdapRealm realm = new LdapRealm( config, securityLog, secureHasher );

        assertThat( realm.getGroupToRoleMapping().get( "group" ), equalTo( singletonList( "role" ) ) );
        assertThat( realm.getGroupToRoleMapping().size(), equalTo( 1 ) );
    }

    @Test
    public void groupToRoleMappingShouldBeAbleToHaveTrailingCommas()
    {
        when( config.get( SecuritySettings.ldap_authorization_group_to_role_mapping ) )
                .thenReturn( "group=role1,role2,role3,,," );

        LdapRealm realm = new LdapRealm( config, securityLog, secureHasher );

        assertThat( realm.getGroupToRoleMapping().keySet(),
                equalTo( Stream.of( "group" ).collect( Collectors.toSet() ) ) );
        assertThat( realm.getGroupToRoleMapping().get( "group" ),
                equalTo( asList( "role1", "role2", "role3" ) ) );
        assertThat( realm.getGroupToRoleMapping().size(), equalTo( 1 ) );
    }

    @Test
    public void groupToRoleMappingShouldBeAbleToHaveNoRoles()
    {
        when( config.get( SecuritySettings.ldap_authorization_group_to_role_mapping ) ).thenReturn( "group=," );

        LdapRealm realm = new LdapRealm( config, securityLog, secureHasher );

        assertThat( realm.getGroupToRoleMapping().get( "group" ).size(), equalTo( 0 ) );
        assertThat( realm.getGroupToRoleMapping().size(), equalTo( 1 ) );
    }

    @Test
    public void groupToRoleMappingShouldNotBeAbleToHaveInvalidFormat()
    {
        when( config.get( SecuritySettings.ldap_authorization_group_to_role_mapping ) ).thenReturn( "group" );

        expectedException.expect( IllegalArgumentException.class );
        expectedException.expectMessage( "wrong number of fields" );

        new LdapRealm( config, securityLog, secureHasher );
    }

    @Test
    public void groupToRoleMappingShouldNotBeAbleToHaveEmptyGroupName()
    {
        when( config.get( SecuritySettings.ldap_authorization_group_to_role_mapping ) ).thenReturn( "=role" );

        expectedException.expect( IllegalArgumentException.class );
        expectedException.expectMessage( "wrong number of fields" );

        new LdapRealm( config, securityLog, secureHasher );
    }

    @Test
    public void groupComparisonShouldBeCaseInsensitive()
    {
        when( config.get( SecuritySettings.ldap_authorization_group_to_role_mapping ) )
                .thenReturn( "GrouP=role1,role2,role3" );

        LdapRealm realm = new LdapRealm( config, securityLog, secureHasher );

        assertThat( realm.getGroupToRoleMapping().get( "group" ),
                equalTo( asList( "role1", "role2", "role3" ) ) );
        assertThat( realm.getGroupToRoleMapping().size(), equalTo( 1 ) );
    }

    @Test
    public void shouldWarnAboutUserSearchFilterWithoutArgument() throws Exception
    {
        when( config.get( SecuritySettings.ldap_authorization_user_search_filter ) ).thenReturn( "" );

        LdapContext ldapContext = mock( LdapContext.class );
        NamingEnumeration result = mock( NamingEnumeration.class );
        when( ldapContext.search( anyString(), anyString(), any(), any() ) ).thenReturn( result );
        when( result.hasMoreElements() ).thenReturn( false );

        makeAndInit();

        verify( securityLog ).warn( contains( "LDAP user search filter does not contain the argument placeholder {0}" ) );
    }

    @Test
    public void shouldWarnAboutUserSearchBaseBeingEmpty() throws Exception
    {
        when( config.get( SecuritySettings.ldap_authorization_user_search_base ) ).thenReturn( "" );

        LdapContext ldapContext = mock( LdapContext.class );
        NamingEnumeration result = mock( NamingEnumeration.class );
        when( ldapContext.search( anyString(), anyString(), any(), any() ) ).thenReturn( result );
        when( result.hasMoreElements() ).thenReturn( false );

        assertException( this::makeAndInit, IllegalArgumentException.class,
                "Illegal LDAP user search settings, see security log for details." );

        verify( securityLog ).error( contains( "LDAP user search base is empty." ) );
    }

    @Test
    public void shouldWarnAboutGroupMembershipsBeingEmpty() throws Exception
    {
        when( config.get( SecuritySettings.ldap_authorization_group_membership_attribute_names ) )
                .thenReturn( Collections.emptyList() );

        LdapContext ldapContext = mock( LdapContext.class );
        NamingEnumeration result = mock( NamingEnumeration.class );
        when( ldapContext.search( anyString(), anyString(), any(), any() ) ).thenReturn( result );
        when( result.hasMoreElements() ).thenReturn( false );

        assertException( this::makeAndInit, IllegalArgumentException.class,
                "Illegal LDAP user search settings, see security log for details." );

        verify( securityLog ).error( contains( "LDAP group membership attribute names are empty. " +
                "Authorization will not be possible." ) );
    }

    @Test
    public void shouldWarnAboutAmbiguousUserSearch() throws NamingException
    {
        when( config.get( SecuritySettings.ldap_authorization_user_search_filter ) ).thenReturn( "{0}" );

        LdapContext ldapContext = mock( LdapContext.class );
        NamingEnumeration result = mock( NamingEnumeration.class );
        SearchResult searchResult = mock( SearchResult.class );
        when( ldapContext.search( anyString(), anyString(), any(), any() ) ).thenReturn( result );
        when( result.hasMoreElements() ).thenReturn( true );
        when( result.next() ).thenReturn( searchResult );
        when( searchResult.toString() ).thenReturn( "<ldap search result>" );

        LdapRealm realm = new LdapRealm( config, securityLog, secureHasher );
        realm.findRoleNamesForUser( "username", ldapContext );

        verify( securityLog ).warn( contains( "LDAP user search for user principal 'username' is ambiguous" ) );
    }

    @Test
    public void shouldAllowMultipleGroupMembershipAttributes() throws NamingException
    {
        when( config.get( SecuritySettings.ldap_authorization_user_search_filter ) ).thenReturn( "{0}" );
        when( config.get( SecuritySettings.ldap_authorization_group_membership_attribute_names ) )
                .thenReturn( asList( "attr0", "attr1", "attr2" ) );
        when( config.get( SecuritySettings.ldap_authorization_group_to_role_mapping ) )
                .thenReturn( "group1=role1;group2=role2,role3" );

        LdapContext ldapContext = mock( LdapContext.class );
        NamingEnumeration result = mock( NamingEnumeration.class );
        SearchResult searchResult = mock( SearchResult.class );
        Attributes attributes = mock( Attributes.class );
        Attribute attribute1 = mock( Attribute.class );
        Attribute attribute2 = mock( Attribute.class );
        Attribute attribute3 = mock( Attribute.class );
        NamingEnumeration attributeEnumeration = mock( NamingEnumeration.class );
        NamingEnumeration groupEnumeration1 = mock( NamingEnumeration.class );
        NamingEnumeration groupEnumeration2 = mock( NamingEnumeration.class );
        NamingEnumeration groupEnumeration3 = mock( NamingEnumeration.class );

        // Mock ldap search result "attr1" contains "group1" and "attr2" contains "group2" (a bit brittle...)
        // "attr0" is non-existing and should have no effect
        when( ldapContext.search( anyString(), anyString(), any(), any() ) ).thenReturn( result );
        when( result.hasMoreElements() ).thenReturn( true, false );
        when( result.next() ).thenReturn( searchResult );
        when( searchResult.getAttributes() ).thenReturn( attributes );
        when( attributes.getAll() ).thenReturn( attributeEnumeration );
        when( attributeEnumeration.hasMore() ).thenReturn( true, true, false );
        when( attributeEnumeration.next() ).thenReturn( attribute1, attribute2, attribute3 );

        when( attribute1.getID() ).thenReturn( "attr1" ); // This attribute should yield role1
        when( attribute1.getAll() ).thenReturn( groupEnumeration1 );
        when( groupEnumeration1.hasMore() ).thenReturn( true, false );
        when( groupEnumeration1.next() ).thenReturn( "group1" );

        when( attribute2.getID() ).thenReturn( "attr2" ); // This attribute should yield role2 and role3
        when( attribute2.getAll() ).thenReturn( groupEnumeration2 );
        when( groupEnumeration2.hasMore() ).thenReturn( true, false );
        when( groupEnumeration2.next() ).thenReturn( "group2" );

        when( attribute3.getID() ).thenReturn( "attr3" ); // This attribute should have no effect
        when( attribute3.getAll() ).thenReturn( groupEnumeration3 );
        when( groupEnumeration3.hasMore() ).thenReturn( true, false );
        when( groupEnumeration3.next() ).thenReturn( "groupWithNoRole" );

        // When
        LdapRealm realm = new LdapRealm( config, securityLog, secureHasher );
        Set<String> roles = realm.findRoleNamesForUser( "username", ldapContext );

        // Then
        assertThat( roles, hasItems( "role1", "role2", "role3" ) );
    }

    @Test
    public void shouldLogSuccessfulAuthenticationQueries() throws NamingException
    {
        // Given
        when( config.get( SecuritySettings.ldap_use_starttls ) ).thenReturn( false );
        when( config.get( SecuritySettings.ldap_authorization_use_system_account ) ).thenReturn( true );

        LdapRealm realm = new TestLdapRealm( config, securityLog, false );
        JndiLdapContextFactory jndiLdapContectFactory = mock( JndiLdapContextFactory.class );
        when( jndiLdapContectFactory.getUrl() ).thenReturn( "ldap://myserver.org:12345" );
        when( jndiLdapContectFactory.getLdapContext( Any.ANY, Any.ANY ) ).thenReturn( null );

        // When
        realm.queryForAuthenticationInfo( new ShiroAuthToken( map( "principal", "olivia", "credentials", "123" ) ),
                jndiLdapContectFactory );

        // Then
        verify( securityLog ).debug( contains( "{LdapRealm}: Authenticated user 'olivia' against 'ldap://myserver.org:12345'" ) );
    }

    @Test
    public void shouldLogSuccessfulAuthenticationQueriesUsingStartTLS() throws NamingException
    {
        // Given
        when( config.get( SecuritySettings.ldap_use_starttls ) ).thenReturn( true );

        LdapRealm realm = new TestLdapRealm( config, securityLog, false );
        JndiLdapContextFactory jndiLdapContectFactory = mock( JndiLdapContextFactory.class );
        when( jndiLdapContectFactory.getUrl() ).thenReturn( "ldap://myserver.org:12345" );

        // When
        realm.queryForAuthenticationInfo( new ShiroAuthToken( map( "principal", "olivia", "credentials", "123" ) ),
                jndiLdapContectFactory );

        // Then
        verify( securityLog ).debug( contains(
                "{LdapRealm}: Authenticated user 'olivia' against 'ldap://myserver.org:12345' using StartTLS" ) );
    }

    @Test
    public void shouldLogFailedAuthenticationQueries()
    {
        // Given
        when( config.get( SecuritySettings.ldap_use_starttls ) ).thenReturn( true );

        LdapRealm realm = new TestLdapRealm( config, securityLog, true );
        JndiLdapContextFactory jndiLdapContectFactory = mock( JndiLdapContextFactory.class );
        when( jndiLdapContectFactory.getUrl() ).thenReturn( "ldap://myserver.org:12345" );

        // When
        assertException( () -> realm.queryForAuthenticationInfo(
                new ShiroAuthToken( map( "principal", "olivia", "credentials", "123" ) ), jndiLdapContectFactory ),
                NamingException.class );

        // Then
        // Authentication failures are logged from MultiRealmAuthManager
        //verify( securityLog ).error( contains(
        //        "{LdapRealm}: Failed to authenticate user 'olivia' against 'ldap://myserver.org:12345' using StartTLS: " +
        //                "Simulated failure" ) );
    }

    @Test
    public void shouldLogSuccessfulAuthorizationQueries()
    {
        // Given
        when( config.get( SecuritySettings.ldap_use_starttls ) ).thenReturn( true );

        LdapRealm realm = new TestLdapRealm( config, securityLog, false );
        JndiLdapContextFactory jndiLdapContectFactory = mock( JndiLdapContextFactory.class );
        when( jndiLdapContectFactory.getUrl() ).thenReturn( "ldap://myserver.org:12345" );

        // When
        realm.doGetAuthorizationInfo( new SimplePrincipalCollection( "olivia", "LdapRealm" ) );

        // Then
        verify( securityLog ).debug( contains( "{LdapRealm}: Queried for authorization info for user 'olivia'" ) );
    }

    @Test
    public void shouldLogFailedAuthorizationQueries()
    {
        // Given
        when( config.get( SecuritySettings.ldap_use_starttls ) ).thenReturn( true );

        LdapRealm realm = new TestLdapRealm( config, securityLog, true );
        JndiLdapContextFactory jndiLdapContectFactory = mock( JndiLdapContextFactory.class );
        when( jndiLdapContectFactory.getUrl() ).thenReturn( "ldap://myserver.org:12345" );

        // When
        AuthorizationInfo info = realm.doGetAuthorizationInfo( new SimplePrincipalCollection( "olivia", "LdapRealm" ) );

        // Then
        assertNull( info );
        verify( securityLog ).warn( contains( "{LdapRealm}: Failed to get authorization info: " +
                "'LDAP naming error while attempting to retrieve authorization for user [olivia].'" +
                " caused by 'Simulated failure'"
        ) );
    }

    private class TestLdapRealm extends LdapRealm
    {

        private boolean failAuth;

        TestLdapRealm( Config config, SecurityLog securityLog, boolean failAuth )
        {
            super( config, securityLog, secureHasher );
            this.failAuth = failAuth;
        }

        @Override
        protected AuthenticationInfo queryForAuthenticationInfoUsingStartTls( AuthenticationToken token,
                LdapContextFactory ldapContextFactory ) throws NamingException
        {
            if ( failAuth )
            {
                throw new NamingException( "Simulated failure" );
            }
            return new SimpleAuthenticationInfo( "olivia", "123", "basic" );
        }

        @Override
        protected AuthorizationInfo queryForAuthorizationInfo( PrincipalCollection principals,
                LdapContextFactory ldapContextFactory ) throws NamingException
        {
            if ( failAuth )
            {
                throw new NamingException( "Simulated failure" );
            }
            return new SimpleAuthorizationInfo();
        }
    }

    private void makeAndInit()
    {
        try
        {
            LdapRealm realm = new LdapRealm( config, securityLog, secureHasher );
            realm.initialize();
        }
        catch ( Exception e )
        {
            throw e;
        }
        catch ( Throwable t )
        {
            throw new RuntimeException( t );
        }
    }
}
