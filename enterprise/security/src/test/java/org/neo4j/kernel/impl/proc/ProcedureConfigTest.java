/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.proc;

import org.junit.Test;

import org.neo4j.kernel.configuration.Config;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.procedure_unrestricted;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.procedure_whitelist;
import static org.neo4j.helpers.collection.MapUtil.genericMap;
import static org.neo4j.kernel.impl.proc.ProcedureConfig.PROC_ALLOWED_SETTING_DEFAULT_NAME;
import static org.neo4j.kernel.impl.proc.ProcedureConfig.PROC_ALLOWED_SETTING_ROLES;

public class ProcedureConfigTest
{
    private static final String[] EMPTY = new String[]{};

    private static String[] arrayOf( String... values )
    {
        return values;
    }

    @Test
    public void shouldHaveEmptyDefaultConfigs()
    {
        Config config = Config.defaults();
        ProcedureConfig procConfig = new ProcedureConfig( config );
        assertThat( procConfig.rolesFor( "x" ), equalTo( EMPTY ) );
    }

    @Test
    public void shouldHaveConfigsWithDefaultProcedureAllowed()
    {
        Config config = Config.defaults().with( genericMap( PROC_ALLOWED_SETTING_DEFAULT_NAME, "role1" ) );
        ProcedureConfig procConfig = new ProcedureConfig( config );
        assertThat( procConfig.rolesFor( "x" ), equalTo( arrayOf( "role1" ) ) );
    }

    @Test
    public void shouldHaveConfigsWithExactMatchProcedureAllowed()
    {
        Config config = Config.defaults()
                .with( genericMap( PROC_ALLOWED_SETTING_DEFAULT_NAME, "role1", PROC_ALLOWED_SETTING_ROLES,
                        "xyz:anotherRole" ) );
        ProcedureConfig procConfig = new ProcedureConfig( config );
        assertThat( procConfig.rolesFor( "xyz" ), equalTo( arrayOf( "anotherRole" ) ) );
        assertThat( procConfig.rolesFor( "abc" ), equalTo( arrayOf( "role1" ) ) );
    }

    @Test
    public void shouldNotFailOnEmptyStringDefaultName()
    {
        Config config = Config.defaults()
                .with( genericMap( PROC_ALLOWED_SETTING_DEFAULT_NAME, ""));
        new ProcedureConfig( config );
    }

    @Test
    public void shouldNotFailOnEmptyStringRoles()
    {
        Config config = Config.defaults()
                .with( genericMap( PROC_ALLOWED_SETTING_ROLES, "" ) );
        new ProcedureConfig( config );
    }

    @Test
    public void shouldNotFailOnBadStringRoles()
    {
        Config config = Config.defaults()
                .with( genericMap( PROC_ALLOWED_SETTING_ROLES, "matrix" ) );
        new ProcedureConfig( config );
    }

    @Test
    public void shouldNotFailOnEmptyStringBoth()
    {
        Config config = Config.defaults()
                .with( genericMap( PROC_ALLOWED_SETTING_DEFAULT_NAME, "",
                        PROC_ALLOWED_SETTING_ROLES, "" ) );
        new ProcedureConfig( config );
    }

    @Test
    public void shouldHaveConfigsWithWildcardProcedureAllowed()
    {
        Config config = Config.defaults()
                .with( genericMap( PROC_ALLOWED_SETTING_DEFAULT_NAME, "role1", PROC_ALLOWED_SETTING_ROLES,
                        "xyz*:anotherRole" ) );
        ProcedureConfig procConfig = new ProcedureConfig( config );
        assertThat( procConfig.rolesFor( "xyzabc" ), equalTo( arrayOf( "anotherRole" ) ) );
        assertThat( procConfig.rolesFor( "abcxyz" ), equalTo( arrayOf( "role1" ) ) );
    }

    @Test
    public void shouldHaveConfigsWithWildcardProcedureAllowedAndNoDefault()
    {
        Config config = Config.defaults().with( genericMap( PROC_ALLOWED_SETTING_ROLES, "xyz*:anotherRole" ) );
        ProcedureConfig procConfig = new ProcedureConfig( config );
        assertThat( procConfig.rolesFor( "xyzabc" ), equalTo( arrayOf( "anotherRole" ) ) );
        assertThat( procConfig.rolesFor( "abcxyz" ), equalTo( EMPTY ) );
    }

    @Test
    public void shouldHaveConfigsWithMultipleWildcardProcedureAllowedAndNoDefault()
    {
        Config config = Config.defaults().with( genericMap( PROC_ALLOWED_SETTING_ROLES,
                "apoc.convert.*:apoc_reader;apoc.load.json:apoc_writer;apoc.trigger.add:TriggerHappy" ) );
        ProcedureConfig procConfig = new ProcedureConfig( config );
        assertThat( procConfig.rolesFor( "xyz" ), equalTo( EMPTY ) );
        assertThat( procConfig.rolesFor( "apoc.convert.xml" ), equalTo( arrayOf( "apoc_reader" ) ) );
        assertThat( procConfig.rolesFor( "apoc.convert.json" ), equalTo( arrayOf( "apoc_reader" ) ) );
        assertThat( procConfig.rolesFor( "apoc.load.xml" ), equalTo( EMPTY ) );
        assertThat( procConfig.rolesFor( "apoc.load.json" ), equalTo( arrayOf( "apoc_writer" ) ) );
        assertThat( procConfig.rolesFor( "apoc.trigger.add" ), equalTo( arrayOf( "TriggerHappy" ) ) );
        assertThat( procConfig.rolesFor( "apoc.convert-json" ), equalTo( EMPTY ) );
        assertThat( procConfig.rolesFor( "apoc.load-xml" ), equalTo( EMPTY ) );
        assertThat( procConfig.rolesFor( "apoc.load-json" ), equalTo( EMPTY ) );
        assertThat( procConfig.rolesFor( "apoc.trigger-add" ), equalTo( EMPTY ) );
    }

    @Test
    public void shouldHaveConfigsWithOverlappingMatchingWildcards()
    {
        Config config = Config.defaults()
                .with( genericMap( PROC_ALLOWED_SETTING_DEFAULT_NAME, "default", PROC_ALLOWED_SETTING_ROLES,
                        "apoc.*:apoc;apoc.load.*:loader;apoc.trigger.*:trigger;apoc.trigger.add:TriggerHappy" ) );
        ProcedureConfig procConfig = new ProcedureConfig( config );
        assertThat( procConfig.rolesFor( "xyz" ), equalTo( arrayOf( "default" ) ) );
        assertThat( procConfig.rolesFor( "apoc.convert.xml" ), equalTo( arrayOf( "apoc" ) ) );
        assertThat( procConfig.rolesFor( "apoc.load.xml" ), equalTo( arrayOf( "apoc", "loader" ) ) );
        assertThat( procConfig.rolesFor( "apoc.trigger.add" ), equalTo( arrayOf( "apoc", "trigger", "TriggerHappy" ) ) );
        assertThat( procConfig.rolesFor( "apoc.trigger.remove" ), equalTo( arrayOf( "apoc", "trigger" ) ) );
        assertThat( procConfig.rolesFor( "apoc.load-xml" ), equalTo( arrayOf( "apoc" ) ) );
    }

    @Test
    public void shouldSupportSeveralRolesPerPattern()
    {
        Config config = Config.defaults().with( genericMap( PROC_ALLOWED_SETTING_ROLES,
                "xyz*:role1,role2,  role3  ,    role4   ;    abc:  role3   ,role1" ) );
        ProcedureConfig procConfig = new ProcedureConfig( config );
        assertThat( procConfig.rolesFor( "xyzabc" ), equalTo( arrayOf( "role1", "role2", "role3", "role4" ) ) );
        assertThat( procConfig.rolesFor( "abc" ), equalTo( arrayOf( "role3", "role1" ) ) );
        assertThat( procConfig.rolesFor( "abcxyz" ), equalTo( EMPTY ) );
    }

    @Test
    public void shouldNotAllowFullAccessDefault()
    {
        Config config = Config.defaults();
        ProcedureConfig procConfig = new ProcedureConfig( config );
        assertThat( procConfig.fullAccessFor( "x" ), equalTo( false ) );
    }

    @Test
    public void shouldAllowFullAccessForProcedures()
    {
        Config config = Config.defaults().with( genericMap( procedure_unrestricted.name(),
                "test.procedure.name" ) );
        ProcedureConfig procConfig = new ProcedureConfig( config );

        assertThat( procConfig.fullAccessFor( "xyzabc" ), equalTo( false ) );
        assertThat( procConfig.fullAccessFor( "test.procedure.name" ), equalTo( true ) );
    }

    @Test
    public void shouldAllowFullAccessForSeveralProcedures()
    {
        Config config = Config.defaults().with( genericMap( procedure_unrestricted.name(),
                "test.procedure.name, test.procedure.otherName" ) );
        ProcedureConfig procConfig = new ProcedureConfig( config );

        assertThat( procConfig.fullAccessFor( "xyzabc" ), equalTo( false ) );
        assertThat( procConfig.fullAccessFor( "test.procedure.name" ), equalTo( true ) );
        assertThat( procConfig.fullAccessFor( "test.procedure.otherName" ), equalTo( true ) );
    }

    @Test
    public void shouldAllowFullAcsessForSeveralProceduresOddNames()
    {
        Config config = Config.defaults().with( genericMap( procedure_unrestricted.name(),
                "test\\.procedure.name, test*rocedure.otherName" ) );
        ProcedureConfig procConfig = new ProcedureConfig( config );

        assertThat( procConfig.fullAccessFor( "xyzabc" ), equalTo( false ) );
        assertThat( procConfig.fullAccessFor( "test\\.procedure.name" ), equalTo( true ) );
        assertThat( procConfig.fullAccessFor( "test*procedure.otherName" ), equalTo( true ) );
    }

    @Test
    public void shouldAllowFullAccessWildcardProceduresNames()
    {
        Config config = Config.defaults().with( genericMap( procedure_unrestricted.name(),
                " test.procedure.*  ,     test.*.otherName" ) );
        ProcedureConfig procConfig = new ProcedureConfig( config );

        assertThat( procConfig.fullAccessFor( "xyzabc" ), equalTo( false ) );
        assertThat( procConfig.fullAccessFor( "test.procedure.name" ), equalTo( true ) );
        assertThat( procConfig.fullAccessFor( "test.procedure.otherName" ), equalTo( true ) );
        assertThat( procConfig.fullAccessFor( "test.other.otherName" ), equalTo( true ) );
        assertThat( procConfig.fullAccessFor( "test.other.cool.otherName" ), equalTo( true ) );
        assertThat( procConfig.fullAccessFor( "test.other.name" ), equalTo( false ) );
    }

    @Test
    public void shouldBlockWithWhiteListingForProcedures()
    {
        Config config = Config.defaults().with( genericMap( procedure_unrestricted.name(),
                "test.procedure.name, test.procedure.name2",  procedure_whitelist.name(),
                "test.procedure.name") );
        ProcedureConfig procConfig = new ProcedureConfig( config );

        assertThat( procConfig.isWhitelisted( "xyzabc" ), equalTo( false ) );
        assertThat( procConfig.isWhitelisted( "test.procedure.name" ), equalTo( true ) );
        assertThat( procConfig.isWhitelisted( "test.procedure.name2" ), equalTo( false ) );
    }

    @Test
    public void shouldAllowWhiteListsWildcardProceduresNames()
    {
        Config config = Config.defaults().with( genericMap( procedure_whitelist.name(),
                " test.procedure.* ,  test.*.otherName"  ));
        ProcedureConfig procConfig = new ProcedureConfig( config );

        assertThat( procConfig.isWhitelisted( "xyzabc" ), equalTo( false ) );
        assertThat( procConfig.isWhitelisted( "test.procedure.name" ), equalTo( true ) );
        assertThat( procConfig.isWhitelisted( "test.procedure.otherName" ), equalTo( true ) );
        assertThat( procConfig.isWhitelisted( "test.other.otherName" ), equalTo( true ) );
        assertThat( procConfig.isWhitelisted( "test.other.cool.otherName" ), equalTo( true ) );
        assertThat( procConfig.isWhitelisted( "test.other.name" ), equalTo( false ) );
    }

    @Test
    public void shouldIgnoreOddRegex()
    {
        Config config = Config.defaults().with( genericMap( procedure_whitelist.name(), "[\\db^a]*" ) );
        ProcedureConfig procConfig = new ProcedureConfig( config );
        assertThat( procConfig.isWhitelisted( "123" ), equalTo( false ) );
        assertThat( procConfig.isWhitelisted( "b" ), equalTo( false ) );
        assertThat( procConfig.isWhitelisted( "a" ), equalTo( false ) );

        config = Config.defaults().with( genericMap( procedure_whitelist.name(), "(abc)" ) );
        procConfig = new ProcedureConfig( config );
        assertThat( procConfig.isWhitelisted( "(abc)" ), equalTo( true ) );

        config = Config.defaults().with( genericMap( procedure_whitelist.name(), "^$" ) );
        procConfig = new ProcedureConfig( config );
        assertThat( procConfig.isWhitelisted( "^$" ), equalTo( true ) );

        config = Config.defaults().with( genericMap( procedure_whitelist.name(), "\\" ) );
        procConfig = new ProcedureConfig( config );
        assertThat( procConfig.isWhitelisted( "\\" ), equalTo( true ) );

        config = Config.defaults().with( genericMap( procedure_whitelist.name(), "&&" ) );
        procConfig = new ProcedureConfig( config );
        assertThat( procConfig.isWhitelisted( "&&" ), equalTo( true ) );

        config = Config.defaults().with( genericMap( procedure_whitelist.name(), "\\p{Lower}" ) );
        procConfig = new ProcedureConfig( config );
        assertThat( procConfig.isWhitelisted( "a" ), equalTo( false ) );
        assertThat( procConfig.isWhitelisted( "\\p{Lower}" ), equalTo( true ) );

        config = Config.defaults().with( genericMap( procedure_whitelist.name(), "a+" ) );
        procConfig = new ProcedureConfig( config );
        assertThat( procConfig.isWhitelisted( "aaaaaa" ), equalTo( false ) );
        assertThat( procConfig.isWhitelisted( "a+" ), equalTo( true ) );

        config = Config.defaults().with( genericMap( procedure_whitelist.name(), "a|b" ) );
        procConfig = new ProcedureConfig( config );
        assertThat( procConfig.isWhitelisted( "a" ), equalTo( false ) );
        assertThat( procConfig.isWhitelisted( "b" ), equalTo( false ) );
        assertThat( procConfig.isWhitelisted( "|" ), equalTo( false ) );
        assertThat( procConfig.isWhitelisted( "a|b" ), equalTo( true ) );

        config = Config.defaults().with( genericMap( procedure_whitelist.name(), "[a-c]" ) );
        procConfig = new ProcedureConfig( config );
        assertThat( procConfig.isWhitelisted( "a" ), equalTo( false ) );
        assertThat( procConfig.isWhitelisted( "b" ), equalTo( false ) );
        assertThat( procConfig.isWhitelisted( "c" ), equalTo( false ) );
        assertThat( procConfig.isWhitelisted( "-" ), equalTo( false ) );
        assertThat( procConfig.isWhitelisted( "[a-c]" ), equalTo( true ) );

        config = Config.defaults().with( genericMap( procedure_whitelist.name(), "a\tb" ) );
        procConfig = new ProcedureConfig( config );
        assertThat( procConfig.isWhitelisted( "a    b" ), equalTo( false ) );
        assertThat( procConfig.isWhitelisted( "a\tb" ), equalTo( true ) );
    }
}
