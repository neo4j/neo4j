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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
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
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LdapRealmTest
{
    Config config = mock( Config.class );
    LogProvider logProvider = NullLogProvider.getInstance();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp()
    {
        // Some dummy settings to pass validation
        when( config.get( SecuritySettings.ldap_authorization_user_search_base ) )
                .thenReturn( "dc=example,dc=com" );
        when( config.get( SecuritySettings.ldap_authorization_group_membership_attribute_names ) )
                .thenReturn( Collections.singletonList( "memberOf" ) );
    }

    @Test
    public void groupToRoleMappingShouldBeAbleToBeNull()
    {
        when( config.get( SecuritySettings.ldap_authorization_group_to_role_mapping ) ).thenReturn( null );

        LdapRealm realm = new LdapRealm( config, logProvider );
    }

    @Test
    public void groupToRoleMappingShouldBeAbleToBeEmpty()
    {
        when( config.get( SecuritySettings.ldap_authorization_group_to_role_mapping ) ).thenReturn( "" );

        LdapRealm realm = new LdapRealm( config, logProvider );
    }

    @Test
    public void groupToRoleMappingShouldBeAbleToHaveMultipleRoles()
    {
        when( config.get( SecuritySettings.ldap_authorization_group_to_role_mapping ) )
                .thenReturn( "group=role1,role2,role3" );

        LdapRealm realm = new LdapRealm( config, logProvider );

        assertThat( realm.getGroupToRoleMapping().get( "group" ),
                equalTo( Arrays.asList( "role1", "role2", "role3" ) ) );
        assertThat( realm.getGroupToRoleMapping().size(), equalTo( 1 ) );
    }

    @Test
    public void groupToRoleMappingShouldBeAbleToHaveMultipleGroups()
    {
        when( config.get( SecuritySettings.ldap_authorization_group_to_role_mapping ) )
                .thenReturn( "group1=role1;group2=role2,role3;group3=role4" );

        LdapRealm realm = new LdapRealm( config, logProvider );

        assertThat( realm.getGroupToRoleMapping().keySet(),
                equalTo( new TreeSet<>( Arrays.asList( "group1", "group2", "group3" ) ) ) );
        assertThat( realm.getGroupToRoleMapping().get( "group1" ), equalTo( Arrays.asList( "role1" ) ) );
        assertThat( realm.getGroupToRoleMapping().get( "group2" ), equalTo( Arrays.asList( "role2", "role3" ) ) );
        assertThat( realm.getGroupToRoleMapping().get( "group3" ), equalTo( Arrays.asList( "role4" ) ) );
        assertThat( realm.getGroupToRoleMapping().size(), equalTo( 3 ) );
    }

    @Test
    public void groupToRoleMappingShouldBeAbleToHaveQuotedKeysAndWhitespaces()
    {
        when( config.get( SecuritySettings.ldap_authorization_group_to_role_mapping ) )
                .thenReturn( "'group1' = role1;\t \"group2\"\n=\t role2,role3 ;  gr oup3= role4\n ;'group4 '= ; g =r" );

        LdapRealm realm = new LdapRealm( config, logProvider );

        assertThat( realm.getGroupToRoleMapping().keySet(),
                equalTo( new TreeSet<>( Arrays.asList( "group1", "group2", "gr oup3", "group4 ", "g" ) ) ) );
        assertThat( realm.getGroupToRoleMapping().get( "group1" ), equalTo( Arrays.asList( "role1" ) ) );
        assertThat( realm.getGroupToRoleMapping().get( "group2" ), equalTo( Arrays.asList( "role2", "role3" ) ) );
        assertThat( realm.getGroupToRoleMapping().get( "gr oup3" ), equalTo( Arrays.asList( "role4" ) ) );
        assertThat( realm.getGroupToRoleMapping().get( "group4 " ), equalTo( Collections.emptyList() ) );
        assertThat( realm.getGroupToRoleMapping().get( "g" ), equalTo( Arrays.asList( "r" ) ) );
        assertThat( realm.getGroupToRoleMapping().size(), equalTo( 5 ) );
    }

    @Test
    public void groupToRoleMappingShouldBeAbleToHaveTrailingSemicolons()
    {
        when( config.get( SecuritySettings.ldap_authorization_group_to_role_mapping ) ).thenReturn( "group=role;;" );

        LdapRealm realm = new LdapRealm( config, logProvider );

        assertThat( realm.getGroupToRoleMapping().get( "group" ), equalTo( Collections.singletonList( "role" ) ) );
        assertThat( realm.getGroupToRoleMapping().size(), equalTo( 1 ) );
    }

    @Test
    public void groupToRoleMappingShouldBeAbleToHaveTrailingCommas()
    {
        when( config.get( SecuritySettings.ldap_authorization_group_to_role_mapping ) )
                .thenReturn( "group=role1,role2,role3,,," );

        LdapRealm realm = new LdapRealm( config, logProvider );

        assertThat( realm.getGroupToRoleMapping().keySet(),
                equalTo( Stream.of( "group" ).collect( Collectors.toSet() ) ) );
        assertThat( realm.getGroupToRoleMapping().get( "group" ),
                equalTo( Arrays.asList( "role1", "role2", "role3" ) ) );
        assertThat( realm.getGroupToRoleMapping().size(), equalTo( 1 ) );
    }

    @Test
    public void groupToRoleMappingShouldBeAbleToHaveNoRoles()
    {
        when( config.get( SecuritySettings.ldap_authorization_group_to_role_mapping ) ).thenReturn( "group=," );

        LdapRealm realm = new LdapRealm( config, logProvider );

        assertThat( realm.getGroupToRoleMapping().get( "group" ).size(), equalTo( 0 ) );
        assertThat( realm.getGroupToRoleMapping().size(), equalTo( 1 ) );
    }

    @Test
    public void groupToRoleMappingShouldNotBeAbleToHaveInvalidFormat()
    {
        when( config.get( SecuritySettings.ldap_authorization_group_to_role_mapping ) ).thenReturn( "group" );

        expectedException.expect( IllegalArgumentException.class );
        expectedException.expectMessage( "wrong number of fields" );

        LdapRealm realm = new LdapRealm( config, logProvider );
    }

    @Test
    public void groupToRoleMappingShouldNotBeAbleToHaveEmptyGroupName()
    {
        when( config.get( SecuritySettings.ldap_authorization_group_to_role_mapping ) ).thenReturn( "=role" );

        expectedException.expect( IllegalArgumentException.class );
        expectedException.expectMessage( "wrong number of fields" );

        LdapRealm realm = new LdapRealm( config, logProvider );
    }

    @Test
    public void groupComparisonShouldBeCaseInsensitive()
    {
        when( config.get( SecuritySettings.ldap_authorization_group_to_role_mapping ) )
                .thenReturn( "GrouP=role1,role2,role3" );

        LdapRealm realm = new LdapRealm( config, logProvider );

        assertThat( realm.getGroupToRoleMapping().get( "group" ),
                equalTo( Arrays.asList( "role1", "role2", "role3" ) ) );
        assertThat( realm.getGroupToRoleMapping().size(), equalTo( 1 ) );
    }

    @Test
    public void shouldWarnAboutUserSearchFilterWithoutArgument() throws NamingException
    {
        when( config.get( SecuritySettings.ldap_authorization_user_search_filter ) ).thenReturn( "" );

        LogProvider logProvider = mock( LogProvider.class );
        Log log = mock( Log.class );
        LdapContext ldapContext = mock( LdapContext.class );
        NamingEnumeration result = mock( NamingEnumeration.class );
        when( logProvider.getLog( LdapRealm.class ) ).thenReturn( log );
        when( ldapContext.search( anyString(), anyString(), anyObject(), anyObject() ) ).thenReturn( result );
        when( result.hasMoreElements() ).thenReturn( false );

        LdapRealm realm = new LdapRealm( config, logProvider );
        realm.findRoleNamesForUser( "username", ldapContext );

        verify( log ).warn( contains( "LDAP user search filter does not contain the argument placeholder {0}" ) );
    }

    @Test
    public void shouldWarnAboutAmbiguousUserSearch() throws NamingException
    {
        when( config.get( SecuritySettings.ldap_authorization_user_search_filter ) ).thenReturn( "{0}" );

        LogProvider logProvider = mock( LogProvider.class );
        Log log = mock( Log.class );
        LdapContext ldapContext = mock( LdapContext.class );
        NamingEnumeration result = mock( NamingEnumeration.class );
        SearchResult searchResult = mock( SearchResult.class );
        when( logProvider.getLog( LdapRealm.class ) ).thenReturn( log );
        when( ldapContext.search( anyString(), anyString(), anyObject(), anyObject() ) ).thenReturn( result );
        when( result.hasMoreElements() ).thenReturn( true );
        when( result.next() ).thenReturn( searchResult );
        when( searchResult.toString() ).thenReturn( "<ldap search result>" );

        LdapRealm realm = new LdapRealm( config, logProvider );
        realm.findRoleNamesForUser( "username", ldapContext );

        verify( log ).warn( contains( "LDAP user search for user principal 'username' is ambiguous" ) );
    }

    @Test
    public void shouldAllowMultipleGroupMembershipAttributes() throws NamingException
    {
        when( config.get( SecuritySettings.ldap_authorization_user_search_filter ) ).thenReturn( "{0}" );
        when( config.get( SecuritySettings.ldap_authorization_group_membership_attribute_names ) )
                .thenReturn( Arrays.asList( "attr0", "attr1", "attr2" ) );
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
        when( ldapContext.search( anyString(), anyString(), anyObject(), anyObject() ) ).thenReturn( result );
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
        LdapRealm realm = new LdapRealm( config, logProvider );
        Set<String> roles = realm.findRoleNamesForUser( "username", ldapContext );

        // Then
        assertThat( roles, hasItems( "role1", "role2", "role3" ) );
    }
}
