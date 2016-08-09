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
package org.neo4j.index;

import java.io.File;
import java.io.IOException;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.EmbeddedDatabaseRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.graphdb.schema.Schema.IndexState.FAILED;

public class TransientIndexFailureOnStartupTest
{
    @Rule
    public final DatabaseRule db = new EmbeddedDatabaseRule();

    @Test
    public void failedIndexShouldNotBecomeOperationalAgain() throws Exception
    {
        // given
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( label( "Person" ) ).on( "name" ).create();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( label( "Person" ) ).setProperty( "name", "Johan" );
            tx.success();
        }
        // when - we restart the database in a state where the index is not operational
        db.restartDatabase( new DatabaseRule.RestartAction()
        {
            @Override
            public void run( FileSystemAbstraction fs, File path ) throws IOException
            {
                fs.renameFile(
                        new File( path, "schema/index/lucene/1/_0.cfs" ),
                        new File( path, "schema/index/lucene/1/sneaky" ) );
            }
        } );
        // then - the database should still be operational
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( label( "Person" ) ).setProperty( "name", "Lars" );
            tx.success();
        }
        // when - we restart the database in a state where the index is operational again
        db.restartDatabase( new DatabaseRule.RestartAction()
        {
            @Override
            public void run( FileSystemAbstraction fs, File path ) throws IOException
            {
                fs.renameFile(
                        new File( path, "schema/index/lucene/1/sneaky" ),
                        new File( path, "schema/index/lucene/1/_0.cfs" ) );
            }
        } );
        // then - an index once failed must remain failed
        try ( Transaction tx = db.beginTx() )
        {
            assertNotNull(
                    "Must be able to find node created while index was offline",
                    db.findNode( label( "Person" ), "name", "Lars" ) );
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            for ( IndexDefinition index : db.schema().getIndexes() )
            {
                assertEquals( FAILED, db.schema().getIndexState( index ) );
            }
            tx.success();
        }
    }

    @Test
    public void shouldNotBeAbleToViolateConstraintWhenBackingIndexIsFailed() throws Exception
    {
        // given
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().constraintFor( label( "Person" ) ).assertPropertyIsUnique( "name" ).create();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( label( "Person" ) ).setProperty( "name", "Lars" );
            tx.success();
        }
        // when - we restart the database in a state where the index is not operational
        db.restartDatabase( new DatabaseRule.RestartAction()
        {
            @Override
            public void run( FileSystemAbstraction fs, File path ) throws IOException
            {
                fs.renameFile(
                        new File( path, "schema/index/lucene/1/_0.cfs" ),
                        new File( path, "schema/index/lucene/1/sneaky" ) );
            }
        } );
        // then - we must not be able to violate the constraint
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( label( "Person" ) ).setProperty( "name", "Johan" );
            tx.success();
        }
        catch ( Throwable e )
        {
            // we accept failure here, but we don't require it
        }
        Throwable failure = null;
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( label( "Person" ) ).setProperty( "name", "Lars" );
            tx.success();
        }
        catch ( Throwable e )
        {
            // this must fail, otherwise we have violated the constraint
            failure = e;
        }
        assertNotNull( failure );
        // when - we restart the database in a state where the index is operational again
        db.restartDatabase( new DatabaseRule.RestartAction()
        {
            @Override
            public void run( FileSystemAbstraction fs, File path ) throws IOException
            {
                fs.renameFile(
                        new File( path, "schema/index/lucene/1/sneaky" ),
                        new File( path, "schema/index/lucene/1/_0.cfs" ) );
            }
        } );
        // then - an index once failed must remain failed
        try ( Transaction tx = db.beginTx() )
        {
            for ( IndexDefinition index : db.schema().getIndexes() )
            {
                assertEquals( FAILED, db.schema().getIndexState( index ) );
            }
            tx.success();
        }
    }
}
