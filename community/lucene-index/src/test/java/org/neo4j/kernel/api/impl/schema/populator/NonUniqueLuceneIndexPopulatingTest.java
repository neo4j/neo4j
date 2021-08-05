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
package org.neo4j.kernel.api.impl.schema.populator;

import org.apache.lucene.index.Term;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.impl.schema.writer.LuceneIndexWriter;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.kernel.api.impl.LuceneTestUtil.documentRepresentingProperties;
import static org.neo4j.kernel.api.impl.schema.LuceneDocumentStructure.newTermForChangeOrRemove;
import static org.neo4j.kernel.api.index.IndexQueryHelper.add;
import static org.neo4j.kernel.api.index.IndexQueryHelper.change;
import static org.neo4j.kernel.api.index.IndexQueryHelper.remove;

@TestDirectoryExtension
class NonUniqueLuceneIndexPopulatingTest
{
    private static final SchemaDescriptor SCHEMA_DESCRIPTOR = SchemaDescriptor.forLabel( 1, 42 );

    @Test
    void additionsDeliveredToIndexWriter() throws Exception
    {
        LuceneIndexWriter writer = mock( LuceneIndexWriter.class );
        NonUniqueLuceneIndexPopulatingUpdater updater = newUpdater( writer );
        String expectedString1 = documentRepresentingProperties( 1, "foo" ).toString();
        String expectedString2 = documentRepresentingProperties( 2, "bar" ).toString();
        String expectedString3 = documentRepresentingProperties( 3, "qux" ).toString();

        updater.process( add( 1, SCHEMA_DESCRIPTOR, "foo" ) );
        verifyDocument( writer, newTermForChangeOrRemove( 1 ), expectedString1 );

        updater.process( add( 2, SCHEMA_DESCRIPTOR, "bar" ) );
        verifyDocument( writer, newTermForChangeOrRemove( 2 ), expectedString2 );

        updater.process( add( 3, SCHEMA_DESCRIPTOR, "qux" ) );
        verifyDocument( writer, newTermForChangeOrRemove( 3 ), expectedString3 );
    }

    @Test
    void changesDeliveredToIndexWriter() throws Exception
    {
        LuceneIndexWriter writer = mock( LuceneIndexWriter.class );
        NonUniqueLuceneIndexPopulatingUpdater updater = newUpdater( writer );
        String expectedString1 = documentRepresentingProperties( 1, "after1" ).toString();
        String expectedString2 = documentRepresentingProperties( 2, "after2" ).toString();

        updater.process( change( 1, SCHEMA_DESCRIPTOR, "before1", "after1" ) );
        verifyDocument( writer, newTermForChangeOrRemove( 1 ), expectedString1 );

        updater.process( change( 2, SCHEMA_DESCRIPTOR, "before2", "after2" ) );
        verifyDocument( writer, newTermForChangeOrRemove( 2 ), expectedString2 );
    }

    @Test
    void removalsDeliveredToIndexWriter() throws Exception
    {
        LuceneIndexWriter writer = mock( LuceneIndexWriter.class );
        NonUniqueLuceneIndexPopulatingUpdater updater = newUpdater( writer );

        updater.process( remove( 1, SCHEMA_DESCRIPTOR, "foo" ) );
        verify( writer ).deleteDocuments( newTermForChangeOrRemove( 1 ) );

        updater.process( remove( 2, SCHEMA_DESCRIPTOR, "bar" ) );
        verify( writer ).deleteDocuments( newTermForChangeOrRemove( 2 ) );

        updater.process( remove( 3, SCHEMA_DESCRIPTOR, "baz" ) );
        verify( writer ).deleteDocuments( newTermForChangeOrRemove( 3 ) );
    }

    private static void verifyDocument( LuceneIndexWriter writer, Term eq, String documentString ) throws IOException
    {
        verify( writer ).updateDocument( eq( eq ), argThat( doc -> documentString.equals( doc.toString() ) ) );
    }

    private static NonUniqueLuceneIndexPopulatingUpdater newUpdater( LuceneIndexWriter writer )
    {
        return new NonUniqueLuceneIndexPopulatingUpdater( writer );
    }
}
