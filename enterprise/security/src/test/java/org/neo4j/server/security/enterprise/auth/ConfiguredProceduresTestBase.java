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

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.api.proc.ProcedureSignature;
import org.neo4j.kernel.api.proc.QualifiedName;
import org.neo4j.kernel.api.proc.UserFunctionSignature;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.server.security.enterprise.configuration.SecuritySettings;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertFalse;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

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

        ProcedureSignature allowedRead =
                procedures.procedure( new QualifiedName( new String[]{"test"}, "allowedReadProcedure" ) );
        assertThat( Arrays.asList( allowedRead.allowed() ), containsInAnyOrder( "role1" ) );
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

        UserFunctionSignature f2 =
                procedures.function( new QualifiedName( new String[]{"test"}, "allowedFunc" ) ).get();
        assertThat( Arrays.asList( f2.allowed() ), containsInAnyOrder( "role1" ) );
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

        assertFail( noneSubject, "CALL test.allowedReadProcedure", READ_OPS_NOT_ALLOWED );
        assertSuccess( noneSubject, "CALL test.numNodes", itr -> assertKeyIs( itr, "count", "3" ) );
    }

    @Test
    public void shouldSetAllMatchingWildcardRoleConfigs() throws Throwable
    {
        configuredSetup( stringMap( SecuritySettings.procedure_roles.name(), "test.*:tester;test.create*:other" ) );

        userManager.newRole( "tester", "noneSubject" );
        userManager.newRole( "other", "readSubject" );

        assertFail( noneSubject, "CALL test.allowedReadProcedure", READ_OPS_NOT_ALLOWED );
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

        assertFail( noneSubject, "RETURN test.allowedFunction1()", READ_OPS_NOT_ALLOWED );
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
        assertEmpty( subject, "CALL test.allowedReadProcedure() YIELD value CREATE (:NEWNODE {name:value})" );
    }
}
