/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.counts;

import org.neo4j.kernel.impl.api.CountsVisitor;
import org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory;
import org.neo4j.kernel.impl.store.counts.keys.IndexSampleKey;
import org.neo4j.kernel.impl.store.counts.keys.IndexStatisticsKey;
import org.neo4j.kernel.impl.store.counts.keys.NodeKey;
import org.neo4j.kernel.impl.store.counts.keys.RelationshipKey;
import org.neo4j.register.Register;

public class CountsStorageServiceImpl implements CountsStorageService
{
    private CountsStore countsStore;
    private IndexStatsUpdater indexStatsUpdater;
    private final UpdaterFactory updaterFactory;

    public CountsStorageServiceImpl( CountsStore countsStore )
    {
        this.countsStore = countsStore;
        indexStatsUpdater = new IndexStatsUpdaterFactory().indexStatsUpdater( countsStore );
        updaterFactory = new UpdaterFactory();
    }

    public CountsSnapshot snapshot( long txId )
    {
        return countsStore.snapshot( txId );
    }

    public void setInitializer(IndexStatsUpdater indexStatsUpdater)
    {
        this.indexStatsUpdater = indexStatsUpdater;
    }

    @Override
    public Updater updaterFor( long txId )
    {
        return updaterFactory.getUpdater( countsStore, txId );
    }

    public IndexStatsUpdater indexStatsUpdater()
    {
        return indexStatsUpdater;
    }

    @Override
    public Updater apply(long txId)
    {
        return updaterFor( txId );
    }

    @Override
    public Register.DoubleLongRegister nodeCount( int labelId, Register.DoubleLongRegister target )
    {
        long value = countsStore.get( CountsKeyFactory.nodeKey( labelId ) )[0];
        target.write( 0, value );
        return target;
    }

    @Override
    public Register.DoubleLongRegister relationshipCount( int startLabelId, int typeId, int endLabelId,
            Register.DoubleLongRegister target )
    {
        long value = countsStore.get( CountsKeyFactory.relationshipKey( startLabelId, typeId, endLabelId ) )[0];
        target.write( 0, value );
        return target;
    }

    @Override
    public Register.DoubleLongRegister indexUpdatesAndSize( int labelId, int propertyKeyId,
            Register.DoubleLongRegister target )
    {
        long[] values = countsStore.get( CountsKeyFactory.indexStatisticsKey( labelId, propertyKeyId ) );
        target.write( values[0], values[1] );
        return target;
    }

    @Override
    public Register.DoubleLongRegister indexSample( int labelId, int propertyKeyId, Register.DoubleLongRegister target )
    {
        long[] values = countsStore.get( CountsKeyFactory.indexSampleKey( labelId, propertyKeyId ) );
        target.write( values[0], values[1] );
        return target;
    }

    @Override
    public void accept( CountsVisitor visitor )
    {
        countsStore.forEach( ( countsKey, values ) -> {
            switch ( countsKey.recordType() )
            {
            case ENTITY_NODE:
            {
                visitor.visitNodeCount( ((NodeKey) countsKey).getLabelId(), values[0] );
                break;
            }
            case ENTITY_RELATIONSHIP:
            {
                RelationshipKey k = (RelationshipKey) countsKey;
                visitor.visitRelationshipCount( k.getStartLabelId(), k.getTypeId(), k.getEndLabelId(), values[0] );
                break;
            }
            case INDEX_STATISTICS:
            {
                IndexStatisticsKey key = (IndexStatisticsKey) countsKey;
                visitor.visitIndexStatistics( key.labelId(), key.propertyKeyId(), values[0], values[1] );
                break;
            }
            case INDEX_SAMPLE:
            {
                IndexSampleKey key = (IndexSampleKey) countsKey;
                visitor.visitIndexSample( key.labelId(), key.propertyKeyId(), values[0], values[1] );
                break;
            }
            default:
                throw new IllegalStateException( "unexpected counts key " + countsKey );
            }
        } );
    }

    @Override
    public void initialize( CountsSnapshot snapshot )
    {
        countsStore = new InMemoryCountsStore( snapshot );
    }
}
