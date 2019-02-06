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
package org.neo4j.kernel.api.impl.schema.reader;

import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Function;

import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.values.storable.Value;

class LuceneDistinctValuesProgressor implements IndexProgressor
{
    private final TermsEnum terms;
    private final NodeValueClient client;
    private final Function<BytesRef,Value> valueMaterializer;

    LuceneDistinctValuesProgressor( TermsEnum terms, NodeValueClient client, Function<BytesRef,Value> valueMaterializer ) throws IOException
    {
        this.terms = terms;
        this.client = client;
        this.valueMaterializer = valueMaterializer;
    }

    @Override
    public boolean next()
    {
        try
        {
            while ( (terms.next()) != null )
            {
                if ( client.acceptNode( terms.docFreq(), valueMaterializer.apply( terms.term() ) ) )
                {
                    return true;
                }
            }
            return false;
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public void close()
    {
    }
}
