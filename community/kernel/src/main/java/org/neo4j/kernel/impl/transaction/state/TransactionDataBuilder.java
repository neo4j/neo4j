/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.state;


public class TransactionDataBuilder
{
//    private final TransactionWriter writer;
//
//    public TransactionDataBuilder( TransactionWriter writer )
//    {
//        this.writer = writer;
//    }
//
//    public void createSchema( Collection<DynamicRecord> beforeRecords, Collection<DynamicRecord> afterRecords )
//    {
//        try
//        {
//            writer.createSchema( beforeRecords, afterRecords );
//        }
//        catch ( IOException e )
//        {
//            throw ioError( e );
//        }
//    }
//
//    public void propertyKey( int id, String key )
//    {
//        try
//        {
//            writer.propertyKey( id, key, id );
//        }
//        catch ( IOException e )
//        {
//            throw ioError( e );
//        }
//    }
//
//    public void nodeLabel( int id, String name )
//    {
//        try
//        {
//            writer.label( id, name, id );
//        }
//        catch ( IOException e )
//        {
//            throw ioError( e );
//        }
//    }
//
//    public void relationshipType( int id, String relationshipType )
//    {
//        try
//        {
//            writer.relationshipType( id, relationshipType, id );
//        }
//        catch ( IOException e )
//        {
//            throw ioError( e );
//        }
//    }
//
//    public void create( NodeRecord node )
//    {
//        try
//        {
//            writer.create( node );
//        }
//        catch ( IOException e )
//        {
//            throw ioError( e );
//        }
//    }
//
//    public void create( LabelTokenRecord labelToken )
//    {
//        try
//        {
//            writer.create( labelToken );
//        }
//        catch ( IOException e )
//        {
//            throw ioError( e );
//        }
//    }
//
//    public void create( PropertyKeyTokenRecord token )
//    {
//        try
//        {
//            writer.create( token );
//        }
//        catch ( IOException e )
//        {
//            throw ioError( e );
//
//        }
//    }
//
//    public void create( RelationshipGroupRecord group )
//    {
//        try
//        {
//            writer.create( group );
//        }
//        catch ( IOException e )
//        {
//            throw ioError( e );
//        }
//    }
//
//    public void update( Command.SchemaRuleCommand command )
//    {
//        try
//        {
//            writer.updateSchema( command.getRecordsBefore(), command.getRecordsAfter() );
//        }
//        catch ( IOException e )
//        {
//            throw ioError( e );
//        }
//    }
//
//    public void create( Command.RelationshipTypeTokenCommand command )
//    {
//        try
//        {
//            writer.add( command.fillRecord() );
//        }
//        catch ( IOException e )
//        {
//            throw ioError( e );
//        }
//    }
//
//    public void update( RelationshipGroupRecord group )
//    {
//        try
//        {
//            writer.update( group );
//        }
//        catch ( IOException e )
//        {
//            throw ioError( e );
//        }
//    }
//
//    public void delete( RelationshipGroupRecord group )
//    {
//        try
//        {
//            writer.delete( group );
//        }
//        catch ( IOException e )
//        {
//            throw ioError( e );
//        }
//    }
//
//    public void update( NeoStoreRecord record )
//    {
//        try
//        {
//            writer.update( record );
//        }
//        catch ( IOException e )
//        {
//            throw ioError( e );
//        }
//    }
//
//    public void update( NodeRecord before, NodeRecord after )
//    {
//        try
//        {
//            writer.update( before, after );
//        }
//        catch ( IOException e )
//        {
//            throw ioError( e );
//        }
//    }
//
//    public void delete( NodeRecord node )
//    {
//        try
//        {
//            writer.delete( node );
//        }
//        catch ( IOException e )
//        {
//            throw ioError( e );
//        }
//    }
//
//    public void create( RelationshipRecord relationship )
//    {
//        try
//        {
//            writer.create( relationship );
//        }
//        catch ( IOException e )
//        {
//            throw ioError( e );
//        }
//    }
//
//    public void update( RelationshipRecord relationship )
//    {
//        try
//        {
//            writer.update( relationship );
//        }
//        catch ( IOException e )
//        {
//            throw ioError( e );
//        }
//    }
//
//    public void delete( RelationshipRecord relationship )
//    {
//        try
//        {
//            writer.delete( relationship );
//        }
//        catch ( IOException e )
//        {
//            throw ioError( e );
//        }
//    }
//
//    public void create( PropertyRecord property )
//    {
//        try
//        {
//            writer.create( property );
//        }
//        catch ( IOException e )
//        {
//            throw ioError( e );
//        }
//    }
//
//    public void update( PropertyRecord before, PropertyRecord property )
//    {
//        try
//        {
//            writer.update( before, property );
//        }
//        catch ( IOException e )
//        {
//            throw ioError( e );
//        }
//    }
//
//    public void delete( PropertyRecord before, PropertyRecord property )
//    {
//        try
//        {
//            writer.delete( before, property );
//        }
//        catch ( IOException e )
//        {
//            throw ioError( e );
//        }
//    }
//
//    private Error ioError( IOException e )
//    {
//        return new ThisShouldNotHappenError( "Mattias", "In-memory stuff should not throw IOException", e );
//    }
}
