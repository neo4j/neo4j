/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.builtinprocs;

import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.collection.RawIterator;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.StubResourceManager;
import org.neo4j.kernel.impl.api.integrationtest.KernelIntegrationTest;

import static org.apache.commons.lang3.ArrayUtils.toArray;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureName;
import static org.neo4j.internal.kernel.api.security.SecurityContext.AUTH_DISABLED;

public class BuiltInDbmsProceduresIT extends KernelIntegrationTest
{
    private final ResourceTracker resourceTracker = new StubResourceManager();

    @Test
    public void listConfig() throws Exception
    {
        // When
        List<Object[]> config = callListConfig( "" );
        List<String> names = config.stream()
                .map( o -> o[0].toString() )
                .collect( Collectors.toList() );

        // The size of the config is not fixed so just make sure it's the right magnitude
        assertTrue( names.size() > 10 );

        assertThat( names, hasItem( GraphDatabaseSettings.record_format.name() ) );

        // Should not contain "unsupported.*" configs
        assertEquals( names.stream()
                .filter( n -> n.startsWith( "unsupported" ) )
                .count(), 0 );
    }

    @Test
    public void listConfigWithASpecificConfigName() throws Exception
    {
        // When
        List<Object[]> config = callListConfig( GraphDatabaseSettings.strict_config_validation.name() );

        assertEquals( 1, config.size() );
        assertArrayEquals( new Object[]{ "dbms.config.strict_validation",
                "A strict configuration validation will prevent the database from starting up if unknown " +
                        "configuration options are specified in the neo4j settings namespace (such as dbms., ha., " +
                        "cypher., etc). This is currently false by default but will be true by default in 4.0.",
                "false", false }, config.get( 0 ) );
    }

    @Test
    public void durationAlwaysListedWithUnit() throws Exception
    {
        // When
        List<Object[]> config = callListConfig( GraphDatabaseSettings.transaction_timeout.name() );

        assertEquals( 1, config.size() );
        assertArrayEquals( new Object[]{ "dbms.transaction.timeout",
                "The maximum time interval of a transaction within which it should be completed.",
                "0ms", true }, config.get( 0 ) );
    }

    @Test
    public void listDynamicSetting() throws ProcedureException
    {
        List<Object[]> config = callListConfig( GraphDatabaseSettings.check_point_iops_limit.name() );

        assertEquals( 1, config.size() );
        assertTrue( (Boolean) config.get(0)[3] );
    }

    @Test
    public void listNotDynamicSetting() throws ProcedureException
    {
        List<Object[]> config = callListConfig( GraphDatabaseSettings.data_directory.name() );

        assertEquals( 1, config.size() );
        assertFalse( (Boolean) config.get(0)[3] );
    }

    private List<Object[]> callListConfig( String seatchString ) throws ProcedureException
    {
        QualifiedName procedureName = procedureName( "dbms", "listConfig" );
        RawIterator<Object[],ProcedureException> callResult =
                dbmsOperations().procedureCallDbms( procedureName, toArray( seatchString ), dependencyResolver, AUTH_DISABLED, resourceTracker );
        return asList( callResult );
    }
}
