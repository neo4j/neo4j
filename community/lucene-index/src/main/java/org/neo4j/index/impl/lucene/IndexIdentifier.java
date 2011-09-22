/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.index.impl.lucene;

class IndexIdentifier
{
    final String indexName;
    final EntityType entityType;
    final byte entityTypeByte;
    private final int hashCode;
    
    public IndexIdentifier( byte entityTypeByte, EntityType entityType, String indexName )
    {
        this.entityTypeByte = entityTypeByte;
        this.entityType = entityType;
        this.indexName = indexName;
        this.hashCode = calculateHashCode();
    }
    
    @Override
    public boolean equals( Object o )
    {
        if ( o == null || !getClass().equals( o.getClass() ) )
        {
            return false;
        }
        IndexIdentifier i = (IndexIdentifier) o;
        return entityTypeByte == i.entityTypeByte && indexName.equals( i.indexName );
    }
    
    private int calculateHashCode()
    {
        int code = 17;
        code += 7*entityTypeByte;
        code += 7*indexName.hashCode();
        return code;
    }
    
    @Override
    public int hashCode()
    {
        return this.hashCode;
    }
    
    @Override
    public String toString()
    {
        return "Index[" + indexName + "," + (entityTypeByte==LuceneCommand.NODE?"Node":"Relationship") + "]";
    }
}
