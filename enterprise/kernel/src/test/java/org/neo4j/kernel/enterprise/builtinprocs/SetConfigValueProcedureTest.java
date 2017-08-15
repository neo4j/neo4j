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
package org.neo4j.kernel.enterprise.builtinprocs;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.ImpermanentEnterpriseDatabaseRule;
import org.neo4j.test.rule.concurrent.ThreadingRule;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.cypher_hints_error;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.log_queries;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.plugin_dir;

public class SetConfigValueProcedureTest
{
    @Rule
    public final DatabaseRule db = new ImpermanentEnterpriseDatabaseRule()
    {
        @Override
        protected void configure( GraphDatabaseBuilder builder )
        {
            builder.setConfig( cypher_hints_error, "true" );
        }
    };
    @Rule
    public final ThreadingRule threads = new ThreadingRule();

    @Test
    public void configShouldBeAffected() throws Exception
    {
        Config config = db.resolveDependency( Config.class );

        db.execute( "CALL dbms.setConfigValue('" + log_queries.name() + "', 'false')" );
        assertFalse( config.get( log_queries ) );

        db.execute( "CALL dbms.setConfigValue('" + log_queries.name() + "', 'true')" );
        assertTrue( config.get( log_queries ) );
    }

    @Test
    public void failIfUnknownSetting() throws Exception
    {
        try
        {
            db.execute( "CALL dbms.setConfigValue('unknown.setting.indeed', 'foo')" );
        }
        catch ( Exception e )
        {
            Throwable cause = Exceptions.rootCause( e );
            assertThat( cause, instanceOf( IllegalArgumentException.class ) );
            assertThat( cause.getMessage(), equalTo( "Unknown setting: unknown.setting.indeed" ) );
            return;
        }
        fail( "Should throw IllegalArgumentException" );
    }

    @Test
    public void failIfStaticSetting() throws Exception
    {
        try
        {
            // Static setting, at least for now
            db.execute( "CALL dbms.setConfigValue('" + plugin_dir.name() + "', 'path/to/dir')" );
        }
        catch ( Exception e )
        {
            Throwable cause = Exceptions.rootCause( e );
            assertThat( cause, instanceOf( IllegalArgumentException.class ) );
            assertThat( cause.getMessage(), equalTo( "Setting is not dynamic and can not be changed at runtime" ) );
            return;
        }
        fail( "Should throw IllegalArgumentException" );
    }

    @Test
    public void failIfInvalidValue() throws Exception
    {
        try
        {
            db.execute( "CALL dbms.setConfigValue('" + log_queries.name() + "', 'invalid')" );
        }
        catch ( Exception e )
        {
            Throwable cause = Exceptions.rootCause( e );
            assertThat( cause, instanceOf( InvalidSettingException.class ) );
            assertThat( cause.getMessage(),
                    equalTo( "Bad value 'invalid' for setting 'dbms.logs.query.enabled': must be 'true' or 'false'" ) );
            return;
        }
        fail( "Should throw InvalidSettingException" );
    }
}
