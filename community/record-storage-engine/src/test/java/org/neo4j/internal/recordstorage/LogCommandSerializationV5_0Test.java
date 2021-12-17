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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;

import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;
import org.neo4j.storageengine.api.CommandReader;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith( RandomExtension.class )
public class LogCommandSerializationV5_0Test extends LogCommandSerializationV4_3D_3Test
{

    @Inject
    private RandomSupport random;

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
        record.initialize( random.nextBoolean(), random.nextLong() );
        if ( random.nextBoolean() )
        {
            record.setCreated();
        }
        record.setConstraint( random.nextBoolean() );
        return record;
    }

    /**
     * The purpose of this test is to verify that serialization of deserialized command produces the same checksum. This test doesn't assert equality of
     * original and deserialized commands, as input supposed to be randomly generated and can produce records that contain information that will not be
     * serialized. I.e. serialization of record not in use can skip irrelevant information.
     * On the other side, if something is written into the tx log, it must be read during deserialization
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
        assertEquals( expected, record );
        assertEquals( expected.isCreated(), record.isCreated() );
        assertEquals( expected.isUseFixedReferences(), record.isUseFixedReferences() );
        assertEquals( expected.isSecondaryUnitCreated(), record.isSecondaryUnitCreated() );
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
