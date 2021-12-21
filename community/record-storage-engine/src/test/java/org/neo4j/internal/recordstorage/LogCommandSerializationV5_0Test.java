/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.internal.recordstorage;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;

import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;
import org.neo4j.storageengine.api.CommandReader;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.values.storable.Values;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.io.pagecache.context.CursorContext.NULL;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@ExtendWith( RandomExtension.class )
public class LogCommandSerializationV5_0Test extends LogCommandSerializationV4_3D_3Test
{

    @Inject
    private RandomSupport random;

    @RepeatedTest( 10 )
    void propertyKeyCommand() throws Exception
    {
        testDoubleSerialization( Command.PropertyKeyTokenCommand.class, createRandomPropertyKeyTokenCommand() );
    }

    Command.PropertyKeyTokenCommand createRandomPropertyKeyTokenCommand()
    {
        var id = random.nextInt();
        var before = createRandomPropertyKeyTokenRecord( id );
        var after = createRandomPropertyKeyTokenRecord( id );
        return new Command.PropertyKeyTokenCommand( writer(), before, after );
    }

    PropertyKeyTokenRecord createRandomPropertyKeyTokenRecord( int id )
    {
        var record = new PropertyKeyTokenRecord( id );
        record.initialize( random.nextBoolean(), random.nextInt(), random.nextInt() );
        record.setInternal( random.nextBoolean() );
        if ( random.nextBoolean() )
        {
            record.setCreated();
        }
        return record;
    }

    @RepeatedTest( 10 )
    void schemaCommandSerialization() throws IOException
    {
        testDoubleSerialization( Command.SchemaRuleCommand.class, createRandomSchemaCommand() );
    }

    Command.SchemaRuleCommand createRandomSchemaCommand()
    {
        var id = Math.abs( random.nextLong() );
        var before = createRandomSchemaRecord( id );
        var after = createRandomSchemaRecord( id );
        SchemaRule rule = IndexPrototype.forSchema( SchemaDescriptors.forLabel( 1, 2, 3 ) ).withName( "index_" + id ).materialise( id );
        return new Command.SchemaRuleCommand( writer(), before, after, rule );
    }

    SchemaRecord createRandomSchemaRecord( long id )
    {
        var record = new SchemaRecord( id );
        var inUse = random.nextBoolean();
        record.initialize( inUse, inUse ? random.nextLong() : -1 );
        if ( random.nextBoolean() )
        {
            record.setCreated();
        }
        if ( inUse )
        {
            record.setConstraint( random.nextBoolean() );
        }
        return record;
    }

    @RepeatedTest( 10 )
    void propertyCommandSerialization() throws IOException
    {
        testDoubleSerialization( Command.PropertyCommand.class, createRandomProperty() );
    }

    Command.PropertyCommand createRandomProperty()
    {
        var id = Math.abs( random.nextLong() );
        var before = createRandomPropertyRecord( id );
        var after = createRandomPropertyRecord( id );
        return new Command.PropertyCommand( writer(), before, after );
    }

    PropertyRecord createRandomPropertyRecord( long id )
    {
        var record = new PropertyRecord( id );
        record.initialize( random.nextBoolean(), random.nextLong(), random.nextLong() );
        if ( random.nextBoolean() )
        {
            record.setCreated();
        }
        if ( record.inUse() )
        {
            PropertyBlock block = new PropertyBlock();
            PropertyStore.encodeValue( block, random.nextInt( 1000 ), Values.of( 123 ), null, null, NULL, INSTANCE );
            record.addPropertyBlock( block );
        }
        if ( random.nextBoolean() )
        {
            record.addDeletedRecord( new DynamicRecord( random.nextLong( 1000 ) ) );
        }
        if ( random.nextBoolean() )
        {
            if ( random.nextBoolean() )
            {
                record.setSecondaryUnitIdOnCreate( random.nextLong( 1000 ) );
            }
            else
            {
                record.setSecondaryUnitIdOnLoad( random.nextLong( 1000 ) );
            }
        }
        switch ( random.nextInt( 3 ) )
        {
        case 0 -> record.setNodeId( 44 );
        case 1 -> record.setRelId( 88 );
        default -> record.setSchemaRuleId( 11 );
        }

        return record;
    }

    /**
     * The purpose of this test is to verify that serialization of deserialized command produces the same checksum. This test doesn't assert equality of
     * original and deserialized commands, as input supposed to be randomly generated and can produce records that contain information that will not be
     * serialized. I.e. serialization of record not in use can skip irrelevant information. On the other side, if something is written into the tx log, it must
     * be read during deserialization
     * <p>
     * Possible improvement: validate that original record and deserialized record are applied to store they produce equal data.
     */
    private void testDoubleSerialization( Class<? extends Command.BaseCommand<?>> type, Command.BaseCommand<?> original ) throws IOException
    {
        InMemoryClosableChannel originalChannel = new InMemoryClosableChannel();

        originalChannel.beginChecksum();
        original.serialize( originalChannel );
        var originalChecksum = originalChannel.putChecksum();

        // When
        CommandReader reader = createReader();
        var readOnce = (Command.BaseCommand<?>) reader.read( originalChannel );
        assertThat( readOnce ).isInstanceOf( type );

        var anotherChannel = new InMemoryClosableChannel();
        anotherChannel.beginChecksum();
        readOnce.serialize( anotherChannel );
        var anotherChecksum = anotherChannel.putChecksum();

        var readTwice = (Command.BaseCommand<?>) reader.read( anotherChannel );
        assertThat( readTwice ).isInstanceOf( type );

        assertCommandsEqual( original, readOnce );
        assertCommandsEqual( readOnce, readTwice );
        assertThat( originalChecksum ).as( "Checksums must be equal after double serialization \n" +
                                           "Original: " + original + "\n" +
                                           "Read once: " + readOnce + "\n" +
                                           "Read twice: " + readTwice )
                                      .isEqualTo( anotherChecksum );
    }

    static void assertCommandsEqual( Command.BaseCommand<?> left, Command.BaseCommand<?> right )
    {
        assertEqualsIncludingFlags( left.getBefore(), right.getBefore() );
        assertEqualsIncludingFlags( left.getAfter(), right.getAfter() );
    }

    private static void assertEqualsIncludingFlags( AbstractBaseRecord expected, AbstractBaseRecord record )
    {
        assertThat( expected ).isEqualTo( record );
        assertThat( expected.isCreated() ).as( "Created flag mismatch" ).isEqualTo( record.isCreated() );
        assertThat( expected.isUseFixedReferences() ).as( "Fixed references flag mismatch" ).isEqualTo( record.isUseFixedReferences() );
        assertThat( expected.isSecondaryUnitCreated() ).as( "Secondary unit created flag mismatch" ).isEqualTo( record.isSecondaryUnitCreated() );
    }

    @Override
    protected CommandReader createReader()
    {
        return LogCommandSerializationV5_0.INSTANCE;
    }

    @Override
    protected LogCommandSerialization writer()
    {
        return LogCommandSerializationV5_0.INSTANCE;
    }
}
