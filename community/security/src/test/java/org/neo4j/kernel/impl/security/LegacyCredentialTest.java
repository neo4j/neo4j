/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.security;

import org.junit.Test;

import org.neo4j.server.security.auth.LegacyCredential;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.server.security.auth.LegacyCredential.INACCESSIBLE;

public class LegacyCredentialTest
{
    @Test
    public void testMatchesPassword()
    {
        LegacyCredential credential = LegacyCredential.forPassword( "foo" );
        assertTrue( credential.matchesPassword( "foo" ) );
        assertFalse( credential.matchesPassword( "fooo" ) );
        assertFalse( credential.matchesPassword( "fo" ) );
        assertFalse( credential.matchesPassword( "bar" ) );
    }

    @Test
    public void testEquals()
    {
        LegacyCredential credential = LegacyCredential.forPassword( "foo" );
        LegacyCredential sameCredential = new LegacyCredential( credential.salt(), credential.passwordHash() );
        assertEquals( credential, sameCredential );
    }

    @Test
    public void testInaccessibleCredentials()
    {
        LegacyCredential credential = new LegacyCredential( INACCESSIBLE.salt(), INACCESSIBLE.passwordHash() );

        //equals
        assertEquals( INACCESSIBLE, credential );
        assertEquals( credential, INACCESSIBLE );
        assertEquals( INACCESSIBLE, INACCESSIBLE );
        assertNotEquals( INACCESSIBLE, LegacyCredential.forPassword( "" ) );
        assertNotEquals( LegacyCredential.forPassword( "" ), INACCESSIBLE );

        //matchesPassword
        assertFalse( INACCESSIBLE.matchesPassword( new String( new byte[]{} )) );
        assertFalse( INACCESSIBLE.matchesPassword( "foo" ) );
        assertFalse( INACCESSIBLE.matchesPassword( "" ) );
    }
}
