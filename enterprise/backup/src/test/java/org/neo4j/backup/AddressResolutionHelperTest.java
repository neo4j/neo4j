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
package org.neo4j.backup;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.OptionalHostnamePort;
import org.neo4j.kernel.configuration.Config;

import static org.junit.Assert.assertEquals;

public class AddressResolutionHelperTest
{

    AddressResolutionHelper subject;

    // Parameters
    Config defaultConfig = Config.defaults();

    @Before
    public void setup()
    {
        subject = new AddressResolutionHelper();
    }

    @Test
    public void noPortResolvesToDefault_ha()
    {
        Integer portIsNotSupplied = null;
        // when
        HostnamePort resolved = subject.resolveCorrectHAAddress( defaultConfig, new OptionalHostnamePort( "localhost", portIsNotSupplied, null ) );

        // then
        assertEquals( resolved.getPort(), 6362 );
    }

    @Test
    public void suppliedPortResolvesToGiven_ha()
    {
        // given
        Integer portIsSupplied = 1234;

        // when
        HostnamePort resolved = subject.resolveCorrectHAAddress( defaultConfig, new OptionalHostnamePort( "localhost", portIsSupplied, null ) );

        // then
        assertEquals( resolved.getPort(), portIsSupplied.intValue() );
    }
}
