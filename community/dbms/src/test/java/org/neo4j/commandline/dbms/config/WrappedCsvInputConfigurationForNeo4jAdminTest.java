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
package org.neo4j.commandline.dbms.config;

import org.junit.Test;

import java.util.function.Function;

import org.neo4j.unsafe.impl.batchimport.input.csv.Configuration;

import static org.junit.Assert.assertEquals;
import static org.neo4j.unsafe.impl.batchimport.input.csv.Configuration.COMMAS;

public class WrappedCsvInputConfigurationForNeo4jAdminTest
{
    @Test
    public void shouldDelegateArrayDelimiter()
    {
        shouldDelegate( expected -> new Configuration.Overridden( COMMAS )
        {
            @Override
            public char arrayDelimiter()
            {
                return expected;
            }
        }, Configuration::arrayDelimiter, 'a', 'b' );
    }

    @Test
    public void shouldDelegateDelimiter()
    {
        shouldDelegate( expected -> new Configuration.Overridden( COMMAS )
        {
            @Override
            public char delimiter()
            {
                return expected;
            }
        }, Configuration::delimiter, 'a', 'b' );
    }

    @Test
    public void shouldDelegateQuoteCharacter()
    {
        shouldDelegate( expected -> new Configuration.Overridden( COMMAS )
        {
            @Override
            public char quotationCharacter()
            {
                return expected;
            }
        }, Configuration::quotationCharacter, 'a', 'b' );
    }

    @Test
    public void shouldOverrideTrimStrings()
    {
        shouldOverride( expected -> new Configuration.Overridden( COMMAS )
        {
            @Override
            public boolean trimStrings()
            {
                return expected;
            }
        }, Configuration::trimStrings, true, false );
    }

    @Test
    public void shouldOverrideBufferSize()
    {
        shouldOverride( expected -> new Configuration.Overridden( COMMAS )
        {
            @Override
            public int bufferSize()
            {
                return expected;
            }
        }, Configuration::bufferSize, 100, 200 );
    }

    @Test
    public void shouldDelegateMultiLineFields()
    {
        shouldDelegate( expected -> new Configuration.Overridden( COMMAS )
        {
            @Override
            public boolean multilineFields()
            {
                return expected;
            }
        }, Configuration::multilineFields, true, false );
    }

    @Test
    public void shouldOverrideEmptyQuotedStringsAsNull()
    {
        shouldOverride( expected -> new Configuration.Overridden( COMMAS )
        {
            @Override
            public boolean emptyQuotedStringsAsNull()
            {
                return expected;
            }
        }, Configuration::emptyQuotedStringsAsNull, true, false );
    }

    @Test
    public void shouldOverrideLegacyStyleQuoting()
    {
        shouldOverride( expected -> new Configuration.Overridden( COMMAS )
        {
            @Override
            public boolean legacyStyleQuoting()
            {
                return expected;
            }
        }, Configuration::legacyStyleQuoting, true, false );
    }

    @SafeVarargs
    private final <T> void shouldDelegate( Function<T,Configuration> configFactory, Function<Configuration,T> getter, T... expectedValues )
    {
        for ( T expectedValue : expectedValues )
        {
            // given
            Configuration configuration = configFactory.apply( expectedValue );

            // when
            WrappedCsvInputConfigurationForNeo4jAdmin wrapped = new WrappedCsvInputConfigurationForNeo4jAdmin( configuration );

            // then
            assertEquals( expectedValue, getter.apply( wrapped ) );
        }

        // then
        assertEquals( getter.apply( COMMAS ), getter.apply( new WrappedCsvInputConfigurationForNeo4jAdmin( COMMAS ) ) );
    }

    @SafeVarargs
    private final <T> void shouldOverride( Function<T,Configuration> configFactory, Function<Configuration,T> getter, T... values )
    {
        for ( T value : values )
        {
            // given
            Configuration configuration = configFactory.apply( value );
            WrappedCsvInputConfigurationForNeo4jAdmin vanilla = new WrappedCsvInputConfigurationForNeo4jAdmin( COMMAS );

            // when
            WrappedCsvInputConfigurationForNeo4jAdmin wrapped = new WrappedCsvInputConfigurationForNeo4jAdmin( configuration );

            // then
            assertEquals( getter.apply( vanilla ), getter.apply( wrapped ) );
        }
    }
}
