/*
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
package org.neo4j.kernel.impl.query;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.kernel.api.security.AccessMode.Static.WRITE_ONLY;

public class EmbeddedQuerySessionTest
{

    @Test
    public void shouldIncludeUserNameInToString()
    {
        TransactionalContext mock = mock( TransactionalContext.class );
        when( mock.accessMode() ).thenReturn( WRITE_ONLY );
        QuerySession session = QueryEngineProvider.embeddedSession( mock );

        assertThat( session.toString(),
                equalTo( String.format( "embedded-session\tthread\t%s\t%s",
                        Thread.currentThread().getName(), WRITE_ONLY.name() ) ) );
    }
}
