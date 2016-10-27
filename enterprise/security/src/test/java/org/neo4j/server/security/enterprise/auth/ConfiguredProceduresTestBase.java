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

import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.api.proc.ProcedureSignature;
import org.neo4j.kernel.api.proc.QualifiedName;
import org.neo4j.kernel.api.proc.UserFunctionSignature;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.server.security.enterprise.configuration.SecuritySettings;

import static org.hamcrest.CoreMatchers.not;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertFalse;
import static org.mockito.internal.util.collections.Sets.newSet;
import static org.neo4j.helpers.collection.MapUtil.genericMap;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ADMIN;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ARCHITECT;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLISHER;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.READER;

public abstract class ConfiguredProceduresTestBase<S> extends ProcedureInteractionTestBase<S>
{

    @Override
    public void setUp() throws Throwable
    {
        // tests are required to setup database with specific configs
    }

    @Test
    public void shouldTerminateLongRunningProcedureThatChecksTheGuardRegularlyOnTimeout() throws Throwable
    {
        configuredSetup( stringMap( GraphDatabaseSettings.transaction_timeout.name(), "2s" ) );

        assertFail( adminSubject, "CALL test.loop", "Transaction guard check failed" );

        Result result = neo.getLocalGraph().execute(
                "CALL dbms.listQueries() YIELD query WITH * WHERE NOT query CONTAINS 'listQueries' RETURN *" );

        assertFalse( result.hasNext() );
        result.close();
    }

    @Test
    public void shouldSetAllowedToConfigSetting() throws Throwable
    {
        configuredSetup( stringMap( SecuritySettings.default_allowed.name(), "nonEmpty" ) );
        Procedures procedures = neo.getLocalGraph().getDependencyResolver().resolveDependency( Procedures.class );

        ProcedureSignature numNodes = procedures.procedure( new QualifiedName( new String[]{"test"}, "numNodes" ) );
        assertThat( Arrays.asList( numNodes.allowed() ), containsInAnyOrder( "nonEmpty" ) );
    }

    @Test
    public void shouldSetAllowedToDefaultValueAndRunningWorks() throws Throwable
    {
        configuredSetup( stringMap( SecuritySettings.default_allowed.name(), "role1" ) );

        userManager.newRole( "role1", "noneSubject" );
        assertSuccess( noneSubject, "CALL test.numNodes", itr -> assertKeyIs( itr, "count", "3" ) );
    }

    @Test
    public void shouldRunProcedureWithMatchingWildcardAllowed() throws Throwable
    {
        configuredSetup( stringMap( SecuritySettings.procedure_roles.name(), "test.*:role1" ) );

        userManager.newRole( "role1", "noneSubject" );
        assertSuccess( noneSubject, "CALL test.numNodes", itr -> assertKeyIs( itr, "count", "3" ) );
    }

    @Test
    public void shouldNotRunProcedureWithMismatchingWildCardAllowed() throws Throwable
    {
        configuredSetup( stringMap( SecuritySettings.procedure_roles.name(), "tes.*:role1" ) );

        userManager.newRole( "role1", "noneSubject" );
        Procedures procedures = neo.getLocalGraph().getDependencyResolver().resolveDependency( Procedures.class );

        ProcedureSignature numNodes = procedures.procedure( new QualifiedName( new String[]{"test"}, "numNodes" ) );
        assertThat( Arrays.asList( numNodes.allowed() ), empty() );
        assertFail( noneSubject, "CALL test.numNodes", "Read operations are not allowed" );
    }

    @Test
    public void shouldNotSetProcedureAllowedIfSettingNotSet() throws Throwable
    {
        configuredSetup( defaultConfiguration() );
        Procedures procedures = neo.getLocalGraph().getDependencyResolver().resolveDependency( Procedures.class );

        ProcedureSignature numNodes = procedures.procedure( new QualifiedName( new String[]{"test"}, "numNodes" ) );
        assertThat( Arrays.asList( numNodes.allowed() ), empty() );
    }

    @SuppressWarnings( "OptionalGetWithoutIsPresent" )
    @Test
    public void shouldSetAllowedToConfigSettingForUDF() throws Throwable
    {
        configuredSetup( stringMap( SecuritySettings.default_allowed.name(), "nonEmpty" ) );
        Procedures procedures = neo.getLocalGraph().getDependencyResolver().resolveDependency( Procedures.class );

        UserFunctionSignature funcSig = procedures.function(
                new QualifiedName( new String[]{"test"}, "nonAllowedFunc" ) ).get();
        assertThat( Arrays.asList( funcSig.allowed() ), containsInAnyOrder( "nonEmpty" ) );
    }

    @Test
    public void shouldSetAllowedToDefaultValueAndRunningWorksForUDF() throws Throwable
    {
        configuredSetup( stringMap( SecuritySettings.default_allowed.name(), "role1" ) );

        userManager.newRole( "role1", "noneSubject" );
        assertSuccess( neo.login( "noneSubject", "abc" ), "RETURN test.allowedFunc() AS c",
                itr -> assertKeyIs( itr, "c", "success for role1" ) );
    }

    @SuppressWarnings( "OptionalGetWithoutIsPresent" )
    @Test
    public void shouldNotSetProcedureAllowedIfSettingNotSetForUDF() throws Throwable
    {
        configuredSetup( defaultConfiguration() );
        Procedures procedures = neo.getLocalGraph().getDependencyResolver().resolveDependency( Procedures.class );

        UserFunctionSignature funcSig = procedures.function(
                new QualifiedName( new String[]{"test"}, "nonAllowedFunc" ) ).get();
        assertThat( Arrays.asList( funcSig.allowed() ), empty() );
    }

    @Test
    public void shouldSetWildcardRoleConfigOnlyIfNotAnnotated() throws Throwable
    {
        configuredSetup( stringMap( SecuritySettings.procedure_roles.name(), "test.*:tester" ) );

        userManager.newRole( "tester", "noneSubject" );

        assertSuccess( noneSubject, "CALL test.numNodes", itr -> assertKeyIs( itr, "count", "3" ) );
    }

    @Test
    public void shouldSetAllMatchingWildcardRoleConfigs() throws Throwable
    {
        configuredSetup( stringMap( SecuritySettings.procedure_roles.name(), "test.*:tester;test.create*:other" ) );

        userManager.newRole( "tester", "noneSubject" );
        userManager.newRole( "other", "readSubject" );

        assertSuccess( readSubject, "CALL test.allowedReadProcedure", itr -> assertKeyIs( itr, "value", "foo" ) );
        assertSuccess( noneSubject, "CALL test.createNode", ResourceIterator::close );
        assertSuccess( readSubject, "CALL test.createNode", ResourceIterator::close );
        assertSuccess( noneSubject, "CALL test.numNodes", itr -> assertKeyIs( itr, "count", "5" ) );
    }

    @Test
    public void shouldSetAllMatchingWildcardRoleConfigsWithDefaultForUDFs() throws Throwable
    {
        configuredSetup( stringMap( SecuritySettings.procedure_roles.name(), "test.*:tester;test.create*:other",
                                    SecuritySettings.default_allowed.name(), "default" ) );

        userManager.newRole( "tester", "noneSubject" );
        userManager.newRole( "default", "noneSubject" );
        userManager.newRole( "other", "readSubject" );

        assertSuccess( noneSubject, "RETURN test.nonAllowedFunc() AS f", itr -> assertKeyIs( itr, "f", "success" ) );
        assertSuccess( readSubject, "RETURN test.allowedFunction1() AS f", itr -> assertKeyIs( itr, "f", "foo" ) );
        assertSuccess( readSubject, "RETURN test.nonAllowedFunc() AS f", itr -> assertKeyIs( itr, "f", "success" ) );
    }

    @Test
    public void shouldHandleWriteAfterAllowedReadProcedureWithAuthDisabled() throws Throwable
    {
        neo = setUpNeoServer( stringMap( GraphDatabaseSettings.auth_enabled.name(), "false" ) );

        neo.getLocalGraph().getDependencyResolver().resolveDependency( Procedures.class )
                .registerProcedure( ClassWithProcedures.class );

        S subject = neo.login( "no_auth", "" );
        assertEmpty( subject, "CALL test.allowedReadProcedure() YIELD value CREATE (:NewNode {name: value})" );
    }

    @Test
    public void shouldHandleMultipleRolesSpecifiedForMapping() throws Throwable
    {
        // Given
        configuredSetup( stringMap( SecuritySettings.procedure_roles.name(), "test.*:tester, other" ) );

        // When
        userManager.newRole( "tester", "noneSubject" );
        userManager.newRole( "other", "readSubject" );

        // Then
        assertSuccess( readSubject, "CALL test.createNode", ResourceIterator::close );
        assertSuccess( noneSubject, "CALL test.numNodes", itr -> assertKeyIs( itr, "count", "4" ) );
    }

    @Test
    public void shouldShowAllowedRolesWhenListingProcedures() throws Throwable
    {
        configuredSetup( stringMap(
                SecuritySettings.procedure_roles.name(), "test.numNodes:counter,user",
                SecuritySettings.default_allowed.name(), "default" ) );

        Map<String,Set<String>> expected = genericMap(
                "test.staticReadProcedure", newSet( "default", READER, PUBLISHER, ARCHITECT, ADMIN ),
                "test.staticWriteProcedure", newSet( "default", PUBLISHER, ARCHITECT, ADMIN ),
                "test.staticSchemaProcedure", newSet( "default", ARCHITECT, ADMIN ),
                "test.annotatedProcedure", newSet( "annotated", READER, PUBLISHER, ARCHITECT, ADMIN ),
                "test.numNodes", newSet( "counter", "user", READER, PUBLISHER, ARCHITECT, ADMIN ),
                "db.labels", newSet( "default", READER, PUBLISHER, ARCHITECT, ADMIN ),
                "dbms.security.changePassword", newSet( ADMIN ),
                "dbms.procedures", newSet( ADMIN ),
                "dbms.listQueries", newSet( ADMIN ),
                "dbms.security.createUser", newSet( ADMIN ) );

        String call = "CALL dbms.procedures";
        assertListProceduresHasRoles( adminSubject, expected, call );
        assertListProceduresHasRoles( schemaSubject, expected, call );
        assertListProceduresHasRoles( writeSubject, expected, call );
        assertListProceduresHasRoles( readSubject, expected, call );
    }

    @Test
    public void shouldShowAllowedRolesWhenListingFunctions() throws Throwable
    {
        configuredSetup( stringMap(
                SecuritySettings.procedure_roles.name(), "test.allowedFunc:counter,user",
                SecuritySettings.default_allowed.name(), "default" ) );

        Map<String,Set<String>> expected = genericMap(
                "test.annotatedFunction", newSet( "annotated", READER, PUBLISHER, ARCHITECT, ADMIN ),
                "test.allowedFunc", newSet( "counter", "user", READER, PUBLISHER, ARCHITECT, ADMIN ),
                "test.nonAllowedFunc", newSet( "default", READER, PUBLISHER, ARCHITECT, ADMIN ) );

        String call = "CALL dbms.functions";
        assertListProceduresHasRoles( adminSubject, expected, call );
        assertListProceduresHasRoles( schemaSubject, expected, call );
        assertListProceduresHasRoles( writeSubject, expected, call );
        assertListProceduresHasRoles( readSubject, expected, call );
    }

    private void assertListProceduresHasRoles( S subject, Map<String,Set<String>> expected, String call )
    {
        assertSuccess( subject, call, itr ->
        {
            List<String> failures = itr.stream().filter( record ->
            {
                String name = record.get( "name" ).toString();
                List<?> roles = (List<?>) record.get( "roles" );
                return expected.containsKey( name ) && !expected.get( name ).equals( new HashSet<>( roles ) );
            } ).map( record ->
            {
                String name = record.get( "name" ).toString();
                return name + ": expected '" + expected.get( name ) + "' but was '" + record.get( "roles" ) + "'";
            } ).collect( toList() );

            assertThat( "Expectations violated: " + failures.toString(), failures.isEmpty() );
        } );
    }
}
