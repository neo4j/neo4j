/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;

import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.RecordSerializer;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule;
import org.neo4j.kernel.impl.nioneo.store.SchemaStore;
import org.neo4j.kernel.impl.nioneo.xa.command.Command;
import org.neo4j.kernel.impl.nioneo.xa.command.Command.SchemaRuleCommand;
import org.neo4j.kernel.impl.nioneo.xa.command.NeoTransactionStoreApplier;
import org.neo4j.kernel.impl.nioneo.xa.command.PhysicalLogNeoCommandReaderV1;
import org.neo4j.kernel.impl.transaction.xaframework.CommandWriter;
import org.neo4j.kernel.impl.transaction.xaframework.InMemoryLogChannel;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.neo4j.helpers.collection.IteratorUtil.first;
import static org.neo4j.kernel.impl.api.TransactionRepresentationStoreApplier.DEFAULT_HIGH_ID_TRACKING;
import static org.neo4j.kernel.impl.api.index.TestSchemaIndexProviderDescriptor.PROVIDER_DESCRIPTOR;
import static org.neo4j.kernel.impl.nioneo.store.UniquenessConstraintRule.uniquenessConstraintRule;

public class SchemaRuleCommandTest
{
    @Test
    public void shouldWriteCreatedSchemaRuleToStore() throws Exception
    {
        // GIVEN
        Collection<DynamicRecord> beforeRecords = serialize( rule, id, false, false);
        Collection<DynamicRecord> afterRecords = serialize( rule, id, true, true);

        when( neoStore.getSchemaStore() ).thenReturn( store );

        SchemaRuleCommand command = new SchemaRuleCommand();
        command.init( beforeRecords, afterRecords, rule );

        // WHEN
        executor.visitSchemaRuleCommand( command );

        // THEN
        verify( store ).updateRecord( first( afterRecords ) );
        verify( indexes ).createIndex( rule );
    }

    @Test
    public void shouldSetLatestConstraintRule() throws Exception
    {
        // Given
        Collection<DynamicRecord> beforeRecords = serialize( rule, id, true, true);
        Collection<DynamicRecord> afterRecords = serialize( rule, id, true, false);

        when( neoStore.getSchemaStore() ).thenReturn( store );

        SchemaRuleCommand command = new SchemaRuleCommand();
        command.init( beforeRecords, afterRecords, uniquenessConstraintRule( id, labelId, propertyKey, 0 )  );

        // WHEN
        executor.visitSchemaRuleCommand( command );

        // THEN
        verify( store ).updateRecord( first( afterRecords ) );
        verify( neoStore ).setLatestConstraintIntroducingTx( txId );
    }

    @Test
    public void shouldDropSchemaRuleFromStore() throws Exception
    {
        // GIVEN
        Collection<DynamicRecord> beforeRecords = serialize( rule, id, true, true);
        Collection<DynamicRecord> afterRecords = serialize( rule, id, false, false);

        when( neoStore.getSchemaStore() ).thenReturn( store );

        SchemaRuleCommand command = new SchemaRuleCommand();
        command.init( beforeRecords, afterRecords, rule );

        // WHEN
        executor.visitSchemaRuleCommand( command );

        // THEN
        verify( store ).updateRecord( first( afterRecords ) );
        verify( indexes ).dropIndex( rule );
    }

    @Test
    public void shouldWriteSchemaRuleToLog() throws Exception
    {
        // GIVEN
        Collection<DynamicRecord> beforeRecords = serialize( rule, id, false, false);
        Collection<DynamicRecord> afterRecords = serialize( rule, id, true, true);

        SchemaRuleCommand command = new SchemaRuleCommand();
        command.init( beforeRecords, afterRecords, rule );
        InMemoryLogChannel buffer = new InMemoryLogChannel();

        when( neoStore.getSchemaStore() ).thenReturn( store );

        // WHEN
        new CommandWriter( buffer ).visitSchemaRuleCommand( command );
        Command readCommand = reader.read( buffer );

        // THEN
        assertThat( readCommand, instanceOf( SchemaRuleCommand.class ) );

        assertSchemaRule( (SchemaRuleCommand)readCommand );
    }

    @Test
    public void shouldRecreateSchemaRuleWhenDeleteCommandReadFromDisk() throws Exception
    {
        // GIVEN
        Collection<DynamicRecord> beforeRecords = serialize( rule, id, true, true);
        Collection<DynamicRecord> afterRecords = serialize( rule, id, false, false);

        SchemaRuleCommand command = new SchemaRuleCommand();
        command.init( beforeRecords, afterRecords, rule );
        InMemoryLogChannel buffer = new InMemoryLogChannel();
        when( neoStore.getSchemaStore() ).thenReturn( store );

        // WHEN
        new CommandWriter( buffer ).visitSchemaRuleCommand( command );
        Command readCommand = reader.read( buffer );

        // THEN
        assertThat( readCommand, instanceOf( SchemaRuleCommand.class ) );

        assertSchemaRule( (SchemaRuleCommand)readCommand );
    }

    private final int labelId = 2;
    private final int propertyKey = 8;
    private final long id = 0;
    private final long txId = 1337l;
    private final NeoStore neoStore = mock( NeoStore.class );
    private final SchemaStore store = mock( SchemaStore.class );
    private final IndexingService indexes = mock( IndexingService.class );
    private final NeoTransactionStoreApplier executor = new NeoTransactionStoreApplier( neoStore, indexes,
            mock( CacheAccessBackDoor.class ), LockService.NO_LOCK_SERVICE, txId, DEFAULT_HIGH_ID_TRACKING, false );
    private final PhysicalLogNeoCommandReaderV1 reader = new PhysicalLogNeoCommandReaderV1();
    private final IndexRule rule = IndexRule.indexRule( id, labelId, propertyKey, PROVIDER_DESCRIPTOR );

    private Collection<DynamicRecord> serialize( SchemaRule rule, long id, boolean inUse, boolean created )
    {
        RecordSerializer serializer = new RecordSerializer();
        serializer = serializer.append( rule );
        DynamicRecord record = new DynamicRecord( id );
        record.setData( serializer.serialize() );
        if ( created )
        {
            record.setCreated();
        }
        if ( inUse )
        {
            record.setInUse( true );
        }
        return Arrays.asList( record );
    }

    private void assertSchemaRule( SchemaRuleCommand readSchemaCommand )
    {
        assertEquals( id, readSchemaCommand.getKey() );
        assertEquals( labelId, ((IndexRule)readSchemaCommand.getSchemaRule()).getLabel() );
        assertEquals( propertyKey, ((IndexRule)readSchemaCommand.getSchemaRule()).getPropertyKey() );
    }

}
