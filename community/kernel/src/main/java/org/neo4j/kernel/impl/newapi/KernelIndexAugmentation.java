/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.impl.newapi;

import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.kernel.api.IndexLimitation;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.IndexValueCapability;
import org.neo4j.internal.kernel.api.schema.IndexProviderDescriptor;
import org.neo4j.values.storable.ValueCategory;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10;

/**
 * Gathers the business logic on what indexes to augment with node property value injection.
 *
 * Currently, the new generic native index provider can provide values directly from the index for
 * all value types except points, which are stored in a lossy fashion in the index. Even thought
 * that does not initially seems like a big deal, this stops Cypher completely from being able to
 * rely on index-backed property values from index scans, even though the index does not contain any
 * points - the reason being that it might get points later, and in that case the plan must still
 * hold. Therefore augmenting the index with property lookup backed values makes sense.
 */
public class KernelIndexAugmentation
{
    private static boolean isNativeBTree( String key, String version )
    {
        return NATIVE_BTREE10.providerName().equals( key ) &&
               NATIVE_BTREE10.providerVersion().equals( version );
    }

    public static IndexCapability augmentIndexCapability( IndexCapability capability,
                                                          IndexProviderDescriptor providerDescriptor )
    {
        if ( isNativeBTree( providerDescriptor.getKey(), providerDescriptor.getVersion() ) )
        {
            return new IndexCapability()
            {
                @Override
                public IndexOrder[] orderCapability( ValueCategory... valueCategories )
                {
                    return capability.orderCapability( valueCategories );
                }

                @Override
                public IndexValueCapability valueCapability( ValueCategory... valueCategories )
                {
                    return IndexValueCapability.YES;
                }

                @Override
                public IndexLimitation[] limitations()
                {
                    return capability.limitations();
                }
            };
        }
        else
        {
            return capability;
        }
    }

    public static boolean shouldInjectValues( IndexReference index, boolean needsValues )
    {
        return needsValues && isNativeBTree( index.providerKey(), index.providerVersion() );
    }
}
