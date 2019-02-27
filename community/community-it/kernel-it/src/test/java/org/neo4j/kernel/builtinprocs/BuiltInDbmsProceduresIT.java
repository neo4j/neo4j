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
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.impl.api.integrationtest.KernelIntegrationTest;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;

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
import static org.neo4j.kernel.api.ResourceManager.EMPTY_RESOURCE_MANAGER;
import static org.neo4j.values.storable.Values.stringValue;

public class BuiltInDbmsProceduresIT extends KernelIntegrationTest
{
    @Test
    public void listConfig() throws Exception
    {
        // When
        List<AnyValue[]> config = callListConfig( "" );
        List<String> names = config.stream()
                .map( o -> ((TextValue) o[0]).stringValue() )
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
        List<AnyValue[]> config = callListConfig( GraphDatabaseSettings.strict_config_validation.name() );

        assertEquals( 1, config.size() );
        assertArrayEquals( new AnyValue[]{stringValue( "dbms.config.strict_validation" ),
                stringValue(
                        "A strict configuration validation will prevent the database from starting up if unknown " +
                        "configuration options are specified in the neo4j settings namespace (such as dbms., " +
                        "cypher., etc). This is currently false by default but will be true by default in 4.0." ),
                stringValue( "false" ), Values.FALSE}, config.get( 0 ) );
    }

    @Test
    public void durationAlwaysListedWithUnit() throws Exception
    {
        // When
        List<AnyValue[]> config = callListConfig( GraphDatabaseSettings.transaction_timeout.name() );

        assertEquals( 1, config.size() );
        assertArrayEquals( new AnyValue[]{ stringValue( "dbms.transaction.timeout" ),
                stringValue( "The maximum time interval of a transaction within which it should be completed." ),
                stringValue( "0ms" ), Values.TRUE }, config.get( 0 ) );
    }

    @Test
    public void listDynamicSetting() throws KernelException
    {
        List<AnyValue[]> config = callListConfig( GraphDatabaseSettings.check_point_iops_limit.name() );

        assertEquals( 1, config.size() );
        assertTrue( ((BooleanValue) config.get(0)[3]).booleanValue() );
    }

    @Test
    public void listNotDynamicSetting() throws KernelException
    {
        List<AnyValue[]> config = callListConfig( GraphDatabaseSettings.data_directory.name() );

        assertEquals( 1, config.size() );
        assertFalse(((BooleanValue) config.get(0)[3]).booleanValue() );
    }

    private List<AnyValue[]> callListConfig( String searchString ) throws KernelException
    {
        QualifiedName procedureName = procedureName( "dbms", "listConfig" );
        int procedureId = procs().procedureGet( procedureName ).id();
        RawIterator<AnyValue[],ProcedureException> callResult =
                dbmsOperations()
                        .procedureCallDbms( procedureId, toArray( stringValue( searchString ) ), dependencyResolver,
                                AUTH_DISABLED, EMPTY_RESOURCE_MANAGER, valueMapper );
        return asList( callResult );
    }
}
