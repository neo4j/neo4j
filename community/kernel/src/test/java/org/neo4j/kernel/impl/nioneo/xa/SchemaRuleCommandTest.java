/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.xa;

import static java.nio.ByteBuffer.allocate;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.IteratorUtil.first;
import static org.neo4j.kernel.impl.nioneo.xa.Command.readCommand;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.RecordSerializer;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule;
import org.neo4j.kernel.impl.nioneo.store.SchemaStore;
import org.neo4j.kernel.impl.nioneo.xa.Command.SchemaRuleCommand;
import org.neo4j.kernel.impl.transaction.xaframework.InMemoryLogBuffer;

public class SchemaRuleCommandTest
{
    @Test
    public void shouldWriteCreatedSchemaRuleToStore() throws Exception
    {
        // GIVEN
        Collection<DynamicRecord> records = serialize( rule, id, true );
        SchemaRuleCommand command = new SchemaRuleCommand( neoStore, store, indexes, records, rule );

        // WHEN
        command.execute();

        // THEN
        verify( store ).updateRecord( first( records ) );
        verify( indexes ).createIndex( rule );
    }

    @Test
    public void shouldDropSchemaRuleFromStore() throws Exception
    {
        // GIVEN
        Collection<DynamicRecord> records = serialize( rule, id, false );
        SchemaRuleCommand command = new SchemaRuleCommand( neoStore, store, indexes, records, rule );

        // WHEN
        command.execute();

        // THEN
        verify( store ).updateRecord( first( records ) );
        verify( indexes ).dropIndex( rule );
    }
    
    @Test
    public void shouldWriteSchemaRuleToLog() throws Exception
    {
        // GIVEN
        Collection<DynamicRecord> records = serialize( rule, id, true );
        SchemaRuleCommand command = new SchemaRuleCommand( neoStore, store, indexes, records, rule );
        InMemoryLogBuffer buffer = new InMemoryLogBuffer();
        NeoStore neoStore = mock( NeoStore.class );
        when( neoStore.getSchemaStore() ).thenReturn( store );

        // WHEN
        command.writeToFile( buffer );
        Command readCommand = readCommand( neoStore, indexes, buffer, allocate( 1000 ) );

        // THEN
        assertThat( readCommand, instanceOf( SchemaRuleCommand.class ) );
    }

    private final NeoStore neoStore = mock( NeoStore.class );
    private final SchemaStore store = mock( SchemaStore.class );
    private final IndexingService indexes = mock( IndexingService.class );
    private final int labelId = 2;
    private final long propertyKey = 8;
    private final long id = 0;
    private final IndexRule rule = new IndexRule( id, labelId, propertyKey );

    private Collection<DynamicRecord> serialize( SchemaRule rule, long id, boolean inUse )
    {
        RecordSerializer serializer = new RecordSerializer();
        serializer = serializer.append( rule );
        DynamicRecord record = new DynamicRecord( id );
        record.setData( serializer.serialize() );
        if ( inUse )
        {
            record.setCreated();
            record.setInUse( true );
        }
        return Arrays.asList( record );
    }
}
