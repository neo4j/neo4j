/**
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.core;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.util.CappedOperation;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.ConsoleLogger;

public class NoDuplicatesPropertyChainVerifierTest
{
    private StringBuffer buffer;
    private StringLogger logger;
    private Primitive entity = new NodeImpl( 42 );

    private PropertyChainVerifier chainVerifier;

    @Before
    public void setUp()
    {
        buffer = new StringBuffer();
        logger = StringLogger.wrap( buffer );
        chainVerifier = new NoDuplicatesPropertyChainVerifier();
        chainVerifier.addObserver( new NodeManager.CappedLoggingDuplicatePropertyObserver(
            new ConsoleLogger( StringLogger.cappedLogger( logger, CappedOperation.<String>time( 2, TimeUnit.HOURS ) ) )
        ) );
    }

    @Test
    public void shouldLogNothingWhenChainHasNoDuplicates() throws Exception
    {
        DefinedProperty[] propertyChain = new DefinedProperty[] {
                Property.charProperty( 1, 'a' ),
                Property.charProperty( 2, 'b' )
        };
        chainVerifier.verifySortedPropertyChain( propertyChain, entity );

        assertThat( buffer.length(), is( 0 ) );
    }

    @Test
    public void shouldLogNothingWhenTheChainIsEmpty()
    {
        chainVerifier.verifySortedPropertyChain( new DefinedProperty[0], entity );

        assertThat( buffer.length(), is( 0 ) );
    }

    @Test
    public void shouldLogWhenMoreThanOnePropertyHasTheSameKeyId()
    {
        DefinedProperty[] propertyChain = new DefinedProperty[] {
                Property.charProperty( 13, 'x' ),
                Property.charProperty( 13, 'y' )
        };
        chainVerifier.verifySortedPropertyChain( propertyChain, entity );

        String output = buffer.toString();
        assertThat( output, containsString(
                NodeManager.CappedLoggingDuplicatePropertyObserver.DUPLICATE_WARNING_MESSAGE ) );
    }
}
