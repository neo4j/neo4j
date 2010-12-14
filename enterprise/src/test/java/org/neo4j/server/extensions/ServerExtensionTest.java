/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.server.extensions;

import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Collection;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;

public class ServerExtensionTest
{
    @Test
    public void canLoadServerExtensions() throws Exception
    {
        boolean foundExtensions = false;
        for ( ServerExtension extension : ServerExtension.load() )
        {
            foundExtensions = true;
        }
        assertTrue( "could not find any extensions", foundExtensions );
    }

    @Test
    public void canGetExtensionMethods() throws Exception
    {
        ServerExtension extension = new ExtensionWithAnnotatedMethods();
        Collection<MediaExtender> extenders = extension.getServerMediaExtenders( null );
        assertNotNull( extenders );
        assertThat( extenders, hasItem( extenderWithName( "expected name" ) ) );
        assertThat( extenders, hasItem( extenderWithName( "methodWithoutNameAnnotation" ) ) );
    }

    private TypeSafeMatcher<MediaExtender> extenderWithName( final String expected )
    {
        return new TypeSafeMatcher<MediaExtender>()
        {
            private String name;

            @Override
            public boolean matchesSafely( MediaExtender extender )
            {
                name = extender.name();
                return expected.equals( name );
            }

            @Override
            public void describeTo( Description descr )
            {
                descr.appendText( String.format( "expected \"%s\", but got \"%s\"", expected, name ) );
            }
        };
    }
}
