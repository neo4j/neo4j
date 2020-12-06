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
package org.neo4j.shell.test.bolt;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.value.Uncoercible;
import org.neo4j.driver.v1.types.Entity;
import org.neo4j.driver.v1.types.IsoDuration;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Path;
import org.neo4j.driver.v1.types.Point;
import org.neo4j.driver.v1.types.Relationship;
import org.neo4j.driver.v1.types.Type;
import org.neo4j.driver.v1.util.Function;

/**
 * A fake value
 */
class FakeValue implements Value
{

    @Override
    public String toString()
    {
        return asString();
    }

    @Override
    public int size()
    {
        return 0;
    }

    @Override
    public Iterable<Value> values()
    {
        return null;
    }

    @Override
    public <T> Iterable<T> values( Function<Value, T> mapFunction )
    {
        return null;
    }

    @Override
    public Map<String, Object> asMap()
    {
        return null;
    }

    @Override
    public <T> Map<String, T> asMap( Function<Value, T> mapFunction )
    {
        throw new Uncoercible( getClass().getSimpleName(), "Map" );
    }

    @Override
    public boolean isEmpty()
    {
        return false;
    }

    @Override
    public Iterable<String> keys()
    {
        return null;
    }

    @Override
    public boolean containsKey( String key )
    {
        return false;
    }

    @Override
    public Value get( String key )
    {
        return null;
    }

    @Override
    public Value get( int index )
    {
        return null;
    }

    @Override
    public Type type()
    {
        return null;
    }

    @Override
    public boolean hasType( Type type )
    {
        return false;
    }

    @Override
    public boolean isTrue()
    {
        return false;
    }

    @Override
    public boolean isFalse()
    {
        return false;
    }

    @Override
    public boolean isNull()
    {
        return false;
    }

    @Override
    public Object asObject()
    {
        throw new Uncoercible( getClass().getSimpleName(), "Object" );
    }

    @Override
    public <T> T computeOrDefault( Function<Value, T> mapper, T defaultValue )
    {
        throw new UnsupportedOperationException( "No implementation" );
    }

    @Override
    public boolean asBoolean()
    {
        throw new Uncoercible( getClass().getSimpleName(), "Bool" );
    }

    @Override
    public boolean asBoolean( boolean defaultValue )
    {
        throw new Uncoercible( getClass().getSimpleName(), "Bool" );
    }

    @Override
    public byte[] asByteArray()
    {
        throw new Uncoercible( getClass().getSimpleName(), "Byte[]" );
    }

    @Override
    public byte[] asByteArray( byte[] defaultValue )
    {
        throw new Uncoercible( getClass().getSimpleName(), "Byte[]" );
    }

    @Override
    public String asString()
    {
        throw new Uncoercible( getClass().getSimpleName(), "String" );
    }

    @Override
    public String asString( String defaultValue )
    {
        throw new Uncoercible( getClass().getSimpleName(), "String" );
    }

    @Override
    public Number asNumber()
    {
        throw new Uncoercible( getClass().getSimpleName(), "Number" );
    }

    @Override
    public long asLong()
    {
        throw new Uncoercible( getClass().getSimpleName(), "Long" );
    }

    @Override
    public long asLong( long defaultValue )
    {
        throw new Uncoercible( getClass().getSimpleName(), "Long" );
    }

    @Override
    public int asInt()
    {
        throw new Uncoercible( getClass().getSimpleName(), "Int" );
    }

    @Override
    public int asInt( int defaultValue )
    {
        throw new Uncoercible( getClass().getSimpleName(), "Int" );
    }

    @Override
    public double asDouble()
    {
        throw new Uncoercible( getClass().getSimpleName(), "Double" );
    }

    @Override
    public double asDouble( double defaultValue )
    {
        throw new Uncoercible( getClass().getSimpleName(), "Double" );
    }

    @Override
    public float asFloat()
    {
        throw new Uncoercible( getClass().getSimpleName(), "Float" );
    }

    @Override
    public float asFloat( float defaultValue )
    {
        throw new Uncoercible( getClass().getSimpleName(), "Float" );
    }

    @Override
    public List<Object> asList()
    {
        throw new Uncoercible( getClass().getSimpleName(), "List" );
    }

    @Override
    public List<Object> asList( List<Object> defaultValue )
    {
        throw new Uncoercible( getClass().getSimpleName(), "List" );
    }

    @Override
    public <T> List<T> asList( Function<Value, T> mapFunction )
    {
        throw new Uncoercible( getClass().getSimpleName(), "List" );
    }

    @Override
    public <T> List<T> asList( Function<Value, T> mapFunction, List<T> defaultValue )
    {
        throw new Uncoercible( getClass().getSimpleName(), "List" );
    }

    @Override
    public Entity asEntity()
    {
        throw new Uncoercible( getClass().getSimpleName(), "Entity" );
    }

    @Override
    public Node asNode()
    {
        throw new Uncoercible( getClass().getSimpleName(), "Node" );
    }

    @Override
    public Relationship asRelationship()
    {
        throw new Uncoercible( getClass().getSimpleName(), "Relationship" );
    }

    @Override
    public Path asPath()
    {
        throw new Uncoercible( getClass().getSimpleName(), "Path" );
    }

    @Override
    public LocalDate asLocalDate()
    {
        throw new Uncoercible( getClass().getSimpleName(), "LocalDate" );
    }

    @Override
    public OffsetTime asOffsetTime()
    {
        throw new Uncoercible( getClass().getSimpleName(), "OffsetTime" );
    }

    @Override
    public LocalTime asLocalTime()
    {
        throw new Uncoercible( getClass().getSimpleName(), "LocalTime" );
    }

    @Override
    public LocalDateTime asLocalDateTime()
    {
        throw new Uncoercible( getClass().getSimpleName(), "LocalDateTime" );
    }

    @Override
    public OffsetDateTime asOffsetDateTime()
    {
        throw new Uncoercible( getClass().getSimpleName(), "OffsetDateTime" );
    }

    @Override
    public ZonedDateTime asZonedDateTime()
    {
        throw new Uncoercible( getClass().getSimpleName(), "ZonedDateTime" );
    }

    @Override
    public IsoDuration asIsoDuration()
    {
        throw new Uncoercible( getClass().getSimpleName(), "IsoDuration" );
    }

    @Override
    public Point asPoint()
    {
        throw new Uncoercible( getClass().getSimpleName(), "Point" );
    }

    @Override
    public LocalDate asLocalDate( LocalDate defaultValue )
    {
        throw new Uncoercible( getClass().getSimpleName(), "LocalDate" );
    }

    @Override
    public OffsetTime asOffsetTime( OffsetTime defaultValue )
    {
        throw new Uncoercible( getClass().getSimpleName(), "OffsetTime" );
    }

    @Override
    public LocalTime asLocalTime( LocalTime defaultValue )
    {
        throw new Uncoercible( getClass().getSimpleName(), "LocalTime" );
    }

    @Override
    public LocalDateTime asLocalDateTime( LocalDateTime defaultValue )
    {
        throw new Uncoercible( getClass().getSimpleName(), "LocalDateTime" );
    }

    @Override
    public OffsetDateTime asOffsetDateTime( OffsetDateTime defaultValue )
    {
        throw new Uncoercible( getClass().getSimpleName(), "OffsetDateTime" );
    }

    @Override
    public ZonedDateTime asZonedDateTime( ZonedDateTime defaultValue )
    {
        throw new Uncoercible( getClass().getSimpleName(), "ZonedDateTime" );
    }

    @Override
    public IsoDuration asIsoDuration( IsoDuration defaultValue )
    {
        throw new Uncoercible( getClass().getSimpleName(), "IsoDuration" );
    }

    @Override
    public Point asPoint( Point defaultValue )
    {
        throw new Uncoercible( getClass().getSimpleName(), "Point" );
    }

    @Override
    public Map<String, Object> asMap( Map<String, Object> defaultValue )
    {
        throw new Uncoercible( getClass().getSimpleName(), "Map" );
    }

    @Override
    public <T> Map<String, T> asMap( Function<Value, T> mapFunction, Map<String, T> defaultValue )
    {
        throw new Uncoercible( getClass().getSimpleName(), "Map" );
    }

    @Override
    public Value get( String key, Value defaultValue )
    {
        return null;
    }

    @Override
    public Object get( String key, Object defaultValue )
    {
        return null;
    }

    @Override
    public Number get( String key, Number defaultValue )
    {
        return null;
    }

    @Override
    public Entity get( String key, Entity defaultValue )
    {
        return null;
    }

    @Override
    public Node get( String key, Node defaultValue )
    {
        return null;
    }

    @Override
    public Path get( String key, Path defaultValue )
    {
        return null;
    }

    @Override
    public Relationship get( String key, Relationship defaultValue )
    {
        return null;
    }

    @Override
    public List<Object> get( String key, List<Object> defaultValue )
    {
        return null;
    }

    @Override
    public <T> List<T> get( String key, List<T> defaultValue, Function<Value, T> mapFunc )
    {
        return null;
    }

    @Override
    public Map<String, Object> get( String key, Map<String, Object> defaultValue )
    {
        return null;
    }

    @Override
    public <T> Map<String, T> get( String key, Map<String, T> defaultValue, Function<Value, T> mapFunc )
    {
        return null;
    }

    @Override
    public int get( String key, int defaultValue )
    {
        return 0;
    }

    @Override
    public long get( String key, long defaultValue )
    {
        return 0;
    }

    @Override
    public boolean get( String key, boolean defaultValue )
    {
        return false;
    }

    @Override
    public String get( String key, String defaultValue )
    {
        return null;
    }

    @Override
    public float get( String key, float defaultValue )
    {
        return 0;
    }

    @Override
    public double get( String key, double defaultValue )
    {
        return 0;
    }
}
