package org.neo4j.server.webadmin.rest.representations;

import java.util.ArrayList;

import javax.management.openmbean.CompositeData;

import org.neo4j.server.helpers.PropertyTypeDispatcher;
import org.neo4j.server.rest.repr.ListRepresentation;
import org.neo4j.server.rest.repr.Representation;
import org.neo4j.server.rest.repr.ValueRepresentation;

/**
 * Converts common primitive and basic objects and arrays of the same into a representation. Has
 * additional understanding of CompositeData, to allow representations of JMX beans.
 */
public class JmxAttributeRepresentationDispatcher extends PropertyTypeDispatcher<String, Representation>
{

    @Override
    protected Representation dispatchBooleanProperty( boolean property, String param )
    {
        return ValueRepresentation.bool( property );
    }

    @Override
    protected Representation dispatchDoubleProperty( double property, String param )
    {
        return ValueRepresentation.number( property );
    }

    @Override
    protected Representation dispatchFloatProperty( float property, String param )
    {
        return ValueRepresentation.number( property );
    }

    @Override
    protected Representation dispatchIntegerProperty( int property, String param )
    {
        return ValueRepresentation.number( property );
    }

    @Override
    protected Representation dispatchLongProperty( long property, String param )
    {
        return ValueRepresentation.number( property );
    }

    @Override
    protected Representation dispatchShortProperty( short property, String param )
    {
        return ValueRepresentation.number( property );
    }

    @Override
    protected Representation dispatchStringProperty( String property, String param )
    {
        return ValueRepresentation.string( property );
    }
    
    @Override
    protected Representation dispatchOtherProperty( Object property, String param ) {
        if( property instanceof CompositeData) {
            return new JmxCompositeDataRepresentation( (CompositeData) property );
        } else {
            return ValueRepresentation.string( property.toString() );
        }
    }

    @Override
    protected Representation dispatchOtherArray( Object[] property, String param )
    {
        if(property instanceof CompositeData[]) {
            ArrayList<Representation> values = new ArrayList<Representation>();
            for(CompositeData value : (CompositeData[]) property) {
                values.add( new JmxCompositeDataRepresentation( value ) );
            }
            return new ListRepresentation( "", values);
        } else {
            return super.dispatchOtherArray(property, param);
        }
    }
    
    @Override
    protected Representation dispatchStringArrayProperty( String[] array, String param )
    {
        ArrayList<Representation> values = new ArrayList<Representation>();
        for ( String z : array )
        {
            values.add( ValueRepresentation.string( z ) );
        }
        return new ListRepresentation( "", values);
    }

    @Override
    @SuppressWarnings( "boxing" )
    protected Representation dispatchShortArrayProperty( PropertyArray<short[], Short> array,
            String param )
    {
        ArrayList<Representation> values = new ArrayList<Representation>();
        for ( Short z : array )
        {
            values.add( ValueRepresentation.number( z ) );
        }
        return new ListRepresentation( "", values);
    }

    @Override
    @SuppressWarnings( "boxing" )
    protected Representation dispatchIntegerArrayProperty( PropertyArray<int[], Integer> array,
            String param )
    {
        ArrayList<Representation> values = new ArrayList<Representation>();
        for ( Integer z : array )
        {
            values.add( ValueRepresentation.number( z ) );
        }
        return new ListRepresentation( "", values);
    }

    @Override
    @SuppressWarnings( "boxing" )
    protected Representation dispatchLongArrayProperty( PropertyArray<long[], Long> array, String param )
    {
        ArrayList<Representation> values = new ArrayList<Representation>();
        for ( Long z : array )
        {
            values.add( ValueRepresentation.number( z ) );
        }
        return new ListRepresentation( "", values);
    }

    @Override
    @SuppressWarnings( "boxing" )
    protected Representation dispatchFloatArrayProperty( PropertyArray<float[], Float> array, String param )
    {

        ArrayList<Representation> values = new ArrayList<Representation>();
        for ( Float z : array )
        {
            values.add( ValueRepresentation.number( z ) );
        }
        return new ListRepresentation( "", values);
    }

    @Override
    @SuppressWarnings( "boxing" )
    protected Representation dispatchDoubleArrayProperty( PropertyArray<double[], Double> array,
            String param )
    {
        ArrayList<Representation> values = new ArrayList<Representation>();
        for ( Double z : array )
        {
            values.add( ValueRepresentation.number( z ) );
        }
        return new ListRepresentation( "", values);
    }

    @Override
    @SuppressWarnings( "boxing" )
    protected Representation dispatchBooleanArrayProperty( PropertyArray<boolean[], Boolean> array,
            String param )
    {
        ArrayList<Representation> values = new ArrayList<Representation>();
        for ( Boolean z : array )
        {
            values.add( ValueRepresentation.bool( z ) );
        }
        return new ListRepresentation( "", values);
    }

    @Override
    protected Representation dispatchByteProperty( byte property,
            String param )
    {
        throw new UnsupportedOperationException("Representing bytes not implemented.");
    }

    @Override
    protected Representation dispatchCharacterProperty( char property,
            String param )
    {
        return ValueRepresentation.number( property );
    }
}
