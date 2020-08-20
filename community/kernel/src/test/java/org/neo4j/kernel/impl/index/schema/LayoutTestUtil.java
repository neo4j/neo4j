/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.index.schema;

import org.apache.commons.lang3.StringUtils;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.index.schema.config.ConfiguredSpaceFillingCurveSettingsCache;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettingsCache;
import org.neo4j.values.storable.Value;

import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.neo4j.values.storable.Values.stringValue;

public class LayoutTestUtil
{
    public static String generateStringResultingInSizeForIndexProvider( int size, GraphDatabaseSettings.SchemaIndex schemaIndex )
    {
        switch ( schemaIndex )
        {
        case NATIVE_BTREE10:
            Config config = Config.defaults();
            ConfiguredSpaceFillingCurveSettingsCache globalConfigCache = new ConfiguredSpaceFillingCurveSettingsCache( config );
            IndexSpecificSpaceFillingCurveSettingsCache spatialSettings = new IndexSpecificSpaceFillingCurveSettingsCache( globalConfigCache, emptyMap() );
            return generateStringResultingInSize( new GenericLayout( 1, spatialSettings ), size );
        case NATIVE20:
            return generateStringResultingInSize( new StringLayout(), size );
        default:
            throw new UnsupportedOperationException( "SchemaIndex " + schemaIndex + " is not supported." );
        }
    }

    static <KEY extends NativeIndexKey<KEY>> Value generateStringValueResultingInSize( Layout<KEY,?> layout, int size )
    {
        return stringValue( generateStringResultingInSize( layout, size ) );
    }

    static <KEY extends NativeIndexKey<KEY>> String generateStringResultingInSize( Layout<KEY,?> layout, int size )
    {
        String string;
        KEY key = layout.newKey();
        key.initialize( 0 );
        int stringLength = size;
        do
        {
            string = StringUtils.repeat( 'A', stringLength-- );
            Value value = stringValue( string );
            key.initFromValue( 0, value, NativeIndexKey.Inclusion.NEUTRAL );
        }
        while ( layout.keySize( key ) > size );
        assertEquals( size, layout.keySize( key ) );
        return string;
    }
}
