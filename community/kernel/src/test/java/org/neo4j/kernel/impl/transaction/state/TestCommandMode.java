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
package org.neo4j.kernel.impl.transaction.state;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

import static org.neo4j.kernel.impl.transaction.command.Command.Mode.fromRecordState;

import org.junit.Test;

public class TestCommandMode
{

    @Test
    public void shouldInferCorrectModes() throws Exception
    {
        assertThat( fromRecordState( /* create */true, /* inUse */true ), equalTo( org.neo4j.kernel.impl.transaction.command.Command.Mode.CREATE));
        assertThat( fromRecordState( /* create */false, /* inUse */true ), equalTo( org.neo4j.kernel.impl.transaction.command.Command.Mode.UPDATE));

        assertThat( fromRecordState( /* create */false, /* inUse */false ), equalTo( org.neo4j.kernel.impl.transaction.command.Command.Mode.DELETE));
        assertThat( fromRecordState( /* create */true, /* inUse */false ), equalTo( org.neo4j.kernel.impl.transaction.command.Command.Mode.DELETE));
    }

}
