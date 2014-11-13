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

/**
 * This tool helps to remove the node&relationship auto indexes from database if already exists.
 * This tool has to run offline, that is when the database is closed. No need to do any changes in database configuration.
 *
 */
package org.neo4j.index.impl.lucene;

import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;

public class AutoIndexDeleter
{
    static final String NODE_AUTO_INDEX = "node_auto_index";
    static final String RELATIONSHIP_AUTO_INDEX = "relationship_auto_index";
    private final String dbLocation;

    public AutoIndexDeleter( String dbLocation )
    {
        this.dbLocation = dbLocation;
    }

    public void deleteAutoIndex()
    {
        EmbeddedGraphDatabase db = (EmbeddedGraphDatabase) new GraphDatabaseFactory().newEmbeddedDatabase( dbLocation );
        XaDataSourceManager xaDsMgr = db.getDependencyResolver().resolveDependency( XaDataSourceManager.class );

        LuceneDataSource luceneDs = (LuceneDataSource) xaDsMgr.getXaDataSource( LuceneDataSource.DEFAULT_NAME );

        Transaction tx = db.beginTx();
        deleteAutoIndexIfFound( db.index(), luceneDs, new IndexIdentifier( LuceneCommand.NODE, luceneDs.nodeEntityType,
                NODE_AUTO_INDEX ) );
        deleteAutoIndexIfFound( db.index(), luceneDs, new IndexIdentifier( LuceneCommand.RELATIONSHIP,
                luceneDs.relationshipEntityType, RELATIONSHIP_AUTO_INDEX ) );
        tx.finish();

        db.shutdown();
        System.out.println( "Removal of auto indexes completed successfully." );
    }

    private void deleteAutoIndexIfFound( IndexManager manager, LuceneDataSource luceneDs, IndexIdentifier indexIdentifier )
    {
        String autoIndexName = indexIdentifier.indexName;
        boolean found = false;

        switch( indexIdentifier.entityTypeByte )
        {
        case LuceneCommand.NODE:
            found =  manager.existsForNodes( autoIndexName );
            break;
        case LuceneCommand.RELATIONSHIP:
            found = manager.existsForRelationships( autoIndexName );
            break;
        default:
            throw new IllegalArgumentException( "Cannot understand the index entity type " + indexIdentifier.toString() );
        }

        if ( found )
        {
            luceneDs.deleteIndex( indexIdentifier, false );
            System.out.println( "index " + autoIndexName + " is deleted successfully." );
        }
        else
        {
            System.out.println( "No auto index found for " + autoIndexName +"." );
        }
    }
}
