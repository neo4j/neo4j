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
package org.neo4j.kernel.impl.transaction.command;

import org.junit.Test;

import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.index.IndexCommand;
import org.neo4j.kernel.impl.index.IndexCommand.RemoveCommand;
import org.neo4j.kernel.impl.index.IndexDefineCommand;
import org.neo4j.kernel.impl.index.IndexEntityType;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class PhysicalLogCommandReaderV2_2_4Test
{
    @Test
    public void shouldReadNoKeyIdAsMinusOne() throws Exception
    {
        // GIVEN
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        IndexDefineCommand definitions = new IndexDefineCommand();
        int indexNameId = 10;
        definitions.init(
                MapUtil.genericMap( "myindex", indexNameId ),
                MapUtil.genericMap() );
        definitions.serialize( channel );
        RemoveCommand removeCommand = new IndexCommand.RemoveCommand();
        removeCommand.init( indexNameId, IndexEntityType.Node.id(), 1234, -1, null );
        removeCommand.serialize( channel );

        // WHEN
        PhysicalLogCommandReaderV2_2_4 reader = new PhysicalLogCommandReaderV2_2_4();
        assertTrue( reader.read( channel ) instanceof IndexDefineCommand );
        RemoveCommand readRemoveCommand = (RemoveCommand) reader.read( channel );

        // THEN
        assertEquals( removeCommand.getIndexNameId(), readRemoveCommand.getIndexNameId() );
        assertEquals( removeCommand.getEntityType(), readRemoveCommand.getEntityType() );
        assertEquals( removeCommand.getEntityId(), readRemoveCommand.getEntityId() );
        assertEquals( removeCommand.getKeyId(), readRemoveCommand.getKeyId() );
        assertNull( removeCommand.getValue() );
    }
}
