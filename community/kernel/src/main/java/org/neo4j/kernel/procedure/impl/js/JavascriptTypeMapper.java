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
package org.neo4j.kernel.procedure.impl.js;

import jdk.nashorn.internal.runtime.ScriptObject;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.api.Neo4jTypes;
import org.neo4j.kernel.api.Neo4jTypes.AnyType;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.procedures.ProcedureSignature;

import static org.neo4j.kernel.api.Neo4jTypes.NTAny;
import static org.neo4j.kernel.api.Neo4jTypes.NTBoolean;
import static org.neo4j.kernel.api.Neo4jTypes.NTFloat;
import static org.neo4j.kernel.api.Neo4jTypes.NTInteger;
import static org.neo4j.kernel.api.Neo4jTypes.NTList;
import static org.neo4j.kernel.api.Neo4jTypes.NTMap;
import static org.neo4j.kernel.api.Neo4jTypes.NTNode;
import static org.neo4j.kernel.api.Neo4jTypes.NTNumber;
import static org.neo4j.kernel.api.Neo4jTypes.NTPath;
import static org.neo4j.kernel.api.Neo4jTypes.NTRelationship;
import static org.neo4j.kernel.api.Neo4jTypes.NTText;

public class JavascriptTypeMapper
{
    /* Future optimization;
     JMH benchmarks show that we can get roughly one order-of-magnitude higher performance from procedures if we used Nashorns primitive accessors
     (ScriptObject#getInt and friends) rather than use boxed primitives. However, this requires that we then not box them further up the stack.
     */

    /** Array of javascript->java converters, one converter per field in the output records */
    private final FromJS[] fromJS;

    /** Signature of the procedure this mapper is built for */
    private ProcedureSignature signature;

    public JavascriptTypeMapper( ProcedureSignature signature ) throws ProcedureException
    {
        this.signature = signature;
        this.fromJS = buildFromJSConverters( signature.outputSignature() );
    }

    /** Translate a yielded record array into a regular java object array */
    public void translateRecord( ScriptObject rec, List<Object> targetRecord ) throws ProcedureException
    {
        if(((Number)rec.getLength()).intValue() != signature.outputSignature().size() )
        {
            throw new ProcedureException( Status.Statement.ProcedureError,
                    "Procedure output does not match declared signature. `%s` should yield %d fields per record, but %d fields were returned.",
                    signature.toString(), signature.outputSignature().size(), rec.getLength() );
        }

        for ( int i = 0; i < signature.outputSignature().size(); i++ )
        {
            targetRecord.set(i, fromJS[i].apply( rec.get( i ) ));
        }
    }

    private abstract static class FromJS<OUT>
    {
        private final AnyType type;

        FromJS( AnyType type ) { this.type = type; }

        /** Given a JS-provided record object, convert and return the value at specified record index */
        abstract OUT apply( Object jsValue ) throws ProcedureException;

        /**
         * @return true if the given class would be a valid output for this converter, used to avoid conversion for primitive arrays and collection types
         *         that already are the right type */
        abstract boolean convertsTo( Class<?> target );

        /** The Neo4j type this converts to */
        AnyType type() { return type; }
    }

    private FromJS[] buildFromJSConverters( List<ProcedureSignature.ArgumentSignature> signature ) throws ProcedureException
    {
        FromJS[] converters = new FromJS[signature.size()];
        // For each field in the output records
        for ( int i = 0; i < signature.size(); i++ )
        {
            AnyType type = signature.get( i ).neo4jType();
            converters[i] = converterFor( type );
        }
        return converters;
    }

    private FromJS<?> converterFor( AnyType type ) throws ProcedureException
    {
        if( type == NTInteger )
        {
            return integerFromJS;
        }
        else if( type == NTFloat )
        {
            return floatFromJS;
        }
        else if( type == NTNumber )
        {
            return numberFromJS;
        }
        else if( type == NTBoolean )
        {
            return boolFromJS;
        }
        else if( type == NTText )
        {
            return textFromJS;
        }
        else if( type == NTNode )
        {
            return nodeFromJS;
        }
        else if( type == NTRelationship )
        {
            return relFromJS;
        }
        else if( type == NTPath )
        {
            return pathFromJS;
        }
        else if( type == NTMap )
        {
            return mapFromJS;
        }
        else if( type == NTAny )
        {
            return anyFromJS;
        }
        else if( type instanceof Neo4jTypes.ListType )
        {
            return new ListFromJS( converterFor( ((Neo4jTypes.ListType) type).innerType() ) );
        }
        else
        {
            throw new ProcedureException( Status.Statement.ProcedureError, "Unknown type: `%s`", type );
        }
    }

    private static final AnyFromJS anyFromJS = new AnyFromJS();
    private static final ListFromJS anyList = new ListFromJS(anyFromJS);
    private static final TextFromJS textFromJS = new TextFromJS();
    private static final BooleanFromJS boolFromJS = new BooleanFromJS();
    private static final IntegerFromJS integerFromJS = new IntegerFromJS();
    private static final FloatFromJS floatFromJS = new FloatFromJS();
    private static final NumberFromJS numberFromJS = new NumberFromJS();
    private static final NodeFromJS nodeFromJS = new NodeFromJS();
    private static final RelationshipFromJS relFromJS = new RelationshipFromJS();
    private static final PathFromJS pathFromJS = new PathFromJS();
    private static final MapFromJS mapFromJS = new MapFromJS();


    private static class IntegerFromJS extends FromJS<Long>
    {
        IntegerFromJS() { super( NTInteger ); }

        @Override
        public Long apply( Object val ) throws ProcedureException
        {
            if(!(val instanceof Number))
            {
                throw coercionException( NTInteger, val );
            }
            return ((Number)val).longValue();
        }

        @Override
        boolean convertsTo( Class<?> target )
        {
            return target == int.class || target == short.class || target == byte.class || target == long.class ||
                   target == Integer.class || target == Short.class || target == Byte.class || target == Long.class;
        }
    }

    private static class TextFromJS extends FromJS<String>
    {
        TextFromJS() { super( NTText ); }

        @Override
        public String apply( Object val ) throws ProcedureException
        {
            return val.toString();
        }

        @Override
        boolean convertsTo( Class<?> target )
        {
            return target == String.class;
        }
    }

    private static class FloatFromJS extends FromJS<Double>
    {
        FloatFromJS() { super( NTFloat ); }

        @Override
        public Double apply( Object val ) throws ProcedureException
        {
            if(!(val instanceof Number))
            {
                throw coercionException( NTFloat, val );
            }
            return ((Number)val).doubleValue();
        }

        @Override
        boolean convertsTo( Class<?> target )
        {
            return target == double.class || target == float.class || target == Double.class || target == Float.class;
        }
    }

    private static class NumberFromJS extends FromJS<Number>
    {
        NumberFromJS() { super( NTNumber ); }

        @Override
        public Number apply( Object val ) throws ProcedureException
        {
            if(!(val instanceof Number))
            {
                throw coercionException( NTNumber, val );
            }
            return (Number) val;
        }

        @Override
        boolean convertsTo( Class<?> target )
        {
            return floatFromJS.convertsTo( target ) || integerFromJS.convertsTo( target );
        }
    }

    private static class BooleanFromJS extends FromJS<Boolean>
    {
        BooleanFromJS() { super( NTBoolean ); }

        @Override
        public Boolean apply( Object val ) throws ProcedureException
        {
            if(!(val instanceof Boolean))
            {
                throw coercionException( NTBoolean, val );
            }
            return (Boolean) val;
        }

        @Override
        boolean convertsTo( Class<?> target )
        {
            return target == boolean.class || target == Boolean.class;
        }
    }

    private static class NodeFromJS extends FromJS<Node>
    {
        NodeFromJS() { super( NTNode ); }

        @Override
        public Node apply( Object val ) throws ProcedureException
        {
            if(!(val instanceof Node))
            {
                throw coercionException( NTNode, val );
            }
            return (Node) val;
        }

        @Override
        boolean convertsTo( Class<?> target )
        {
            return target == Node.class;
        }
    }

    private static class RelationshipFromJS extends FromJS<Relationship>
    {
        RelationshipFromJS() { super( NTRelationship ); }

        @Override
        public Relationship apply( Object val ) throws ProcedureException
        {
            if(!(val instanceof Relationship))
            {
                throw coercionException( NTRelationship, val );
            }
            return (Relationship) val;
        }

        @Override
        boolean convertsTo( Class<?> target )
        {
            return target == Relationship.class;
        }
    }

    private static class PathFromJS extends FromJS<Path>
    {
        PathFromJS() { super( NTPath ); }

        @Override
        public Path apply( Object val ) throws ProcedureException
        {
            if(!(val instanceof Path))
            {
                throw coercionException( NTPath, val );
            }
            return (Path) val;
        }

        @Override
        boolean convertsTo( Class<?> target )
        {
            return target == Path.class;
        }
    }

    private static class ListFromJS extends FromJS<Object>
    {
        private final FromJS<?> innerType;

        private ListFromJS( FromJS<?> innerType )
        {
            super( NTList( innerType.type ) );
            this.innerType = innerType;
        }

        @Override
        public Object apply( Object val ) throws ProcedureException
        {
            if ( val instanceof Collection )
            {
                // TODO: Sync about the .isArray above - is it ok that procedures with List signatures return primitive arrays?
                // TODO: Type check the generic type of the collection, ensure it matches innerType
                return val;
            }

            Class<?> cls = val.getClass();
            // If this is a primitive array, and the primitive array is of the type our signature signals
            if ( cls.isArray() && innerType.convertsTo( cls.getComponentType() ) )
            {
                return val;
            }


            if(val instanceof ScriptObject)
            {
                return toList( (ScriptObject) val, innerType );
            }

            throw coercionException( type(), val );
        }

        @Override
        boolean convertsTo( Class<?> target )
        {
            return Collection.class.isAssignableFrom( target ) || (target.isArray() && innerType.convertsTo( target.getComponentType() ));
        }
    }

    /** Converts any JS value to an appropriate Neo4j type based on JS type information available at runtime. */
    private static class AnyFromJS extends FromJS<Object>
    {
        AnyFromJS() { super( NTAny ); }

        @Override
        public Object apply( Object js ) throws ProcedureException
        {
            // Types that require no conversion
            if( js instanceof String || js instanceof Boolean || js instanceof Node || js instanceof Relationship || js instanceof Path
                || js instanceof Map || js instanceof Collection || js.getClass().isArray() || js instanceof Double || js instanceof Long )
            {
                // TODO: Collection types need deep-copy-and-map - or will it always be the case that regular java collections will have come from java?
                return js;
            }

            if( js instanceof Integer || js instanceof Short || js instanceof Byte )
            {
                return ((Number)js).longValue();
            }
            if( js instanceof Float )
            {
                return ((Number)js).doubleValue();
            }
            if( js instanceof ScriptObject )
            {
                ScriptObject sobj = (ScriptObject) js;
                if( sobj.isArray() )
                {
                    return toList( sobj, this );
                }
                else
                {
                    return toMap( sobj );
                }
            }

            throw new ProcedureException( Status.Statement.InvalidType, "Unknown type: `%s`.", js.getClass().getSimpleName() );
        }

        @Override
        boolean convertsTo( Class<?> target )
        {
            return numberFromJS.convertsTo( target ) || mapFromJS.convertsTo( target ) || pathFromJS.convertsTo( target ) || boolFromJS.convertsTo( target ) || anyList.convertsTo( target );
        }
    }

    /**
     * This is a bit tricky. In the Cypher type system, PropertyContainer and Map are the same type, meaning Relationship and Node both inherit from
     * Map. Because of that, we allow both Map objects and Node/Relationship objects to go through this converter without wrapping, trusting cypher to
     * recognize the Node/Rel objects and act appropriately. This should change as we shift away from internal use of the embedded API and consolidate the
     * type system.
     */
    private static class MapFromJS extends FromJS<Object>
    {
        MapFromJS() { super( NTMap ); }

        @Override
        public Object apply( Object val ) throws ProcedureException
        {
            if(val instanceof PropertyContainer || val instanceof Map )
            {
                return val;
            }

            if(val instanceof ScriptObject)
            {
                return toMap( (ScriptObject) val );
            }

            throw coercionException( NTMap, val );
        }

        @Override
        boolean convertsTo( Class<?> target )
        {
            return Map.class.isAssignableFrom( target ) || nodeFromJS.convertsTo( target ) || relFromJS.convertsTo( target );
        }
    }

    /** Deep copy of maps, since script context is thread-bound and may have changed by the time this gets read. */
    private static Map<String, Object> toMap( ScriptObject js ) throws ProcedureException
    {
        Map<String, Object> mapped = new HashMap<>( js.size(), 1.0f );
        for ( Map.Entry<Object,Object> entry : js.entrySet() )
        {
            mapped.put( entry.getKey().toString(), anyFromJS.apply( entry.getValue() ) );
        }
        return mapped;
    }

    /** Deep copy of lists, since script context is thread-bound and may have changed by the time this gets read. */
    private static List<Object> toList( ScriptObject js, FromJS<?> innerType ) throws ProcedureException
    {
        // TODO: Nashorn has optimized data structures that are backed by primitive arrays, which we could pull out directly here (eg. int[] and so on)
        List<Object> mapped = new LinkedList<>();
        Iterator<Object> iter = js.valueIterator();
        while(iter.hasNext())
        {
            mapped.add( innerType.apply(iter.next()) );
        }
        return mapped;
    }

    private static ProcedureException coercionException( AnyType target, Object uncoercible )
    {
        return new ProcedureException( Status.Statement.InvalidType, "Cannot coerce `%s` to `%s`.", uncoercible.getClass().getSimpleName(), target );
    }

}
