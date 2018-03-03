/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.configuration.ssl;

import net.jcip.annotations.NotThreadSafe;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;

@NotThreadSafe
public class SslSystemSettingsTest
{
    private String original;

    @Before
    public void setup()
    {
        original = System.getProperty( SslSystemSettings.TLS_PROTOCOL_DEFAULT_KEY );
    }

    @After
    public void cleanup()
    {
        if ( original == null )
        {
            System.clearProperty( SslSystemSettings.TLS_PROTOCOL_DEFAULT_KEY );
        }
        else
        {
            System.setProperty( SslSystemSettings.TLS_PROTOCOL_DEFAULT_KEY, original );
        }
    }

    @Test
    public void defaultShouldBeTLSv12() throws Exception
    {
        // when
        List<String> tlsDefault = SslSystemSettings.getTlsDefault();

        // then
        assertThat( tlsDefault, contains( "TLSv1.2" ) );
    }

    @Test
    public void shouldSupportSingleEntry() throws Exception
    {
        // given
        System.setProperty( SslSystemSettings.TLS_PROTOCOL_DEFAULT_KEY, "TLSv1" );

        // when
        List<String> tlsDefault = SslSystemSettings.getTlsDefault();

        // then
        assertThat( tlsDefault, contains( "TLSv1" ) );
    }

    @Test
    public void shouldSupportMultipleEntries() throws Exception
    {
        // given
        System.setProperty( SslSystemSettings.TLS_PROTOCOL_DEFAULT_KEY, "TLSv1,TLSv1.1,TLSv1.2" );

        // when
        List<String> tlsDefault = SslSystemSettings.getTlsDefault();

        // then
        assertThat( tlsDefault, contains( "TLSv1", "TLSv1.1", "TLSv1.2" ) );
    }

    @Test
    public void shouldIgnoreSpaces() throws Exception
    {
        // given
        System.setProperty( SslSystemSettings.TLS_PROTOCOL_DEFAULT_KEY, "  TLSv1  ,  TLSv1.1  ,  TLSv1.2  " );

        // when
        List<String> tlsDefault = SslSystemSettings.getTlsDefault();

        // then
        assertThat( tlsDefault, contains( "TLSv1", "TLSv1.1", "TLSv1.2" ) );
    }
}
