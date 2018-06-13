package org.neo4j.kernel.impl.index.schema;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.values.storable.ValueGroup;

import static org.neo4j.kernel.impl.index.schema.StringIndexKey.ENTITY_ID_SIZE;
import static org.neo4j.kernel.impl.index.schema.ZonedDateTimeLayout.ZONE_ID_FLAG;
import static org.neo4j.kernel.impl.index.schema.ZonedDateTimeLayout.ZONE_ID_MASK;
import static org.neo4j.kernel.impl.index.schema.ZonedDateTimeLayout.asZoneId;
import static org.neo4j.kernel.impl.index.schema.ZonedDateTimeLayout.asZoneOffset;
import static org.neo4j.kernel.impl.index.schema.ZonedDateTimeLayout.isZoneId;

public class GenericLayout extends IndexLayout<GenericKey>
{
    private static final int KEY_HEADER_SIZE = Byte.BYTES;

    enum Type
    {
        ZONED_DATE_TIME( ValueGroup.ZONED_DATE_TIME, (byte) 0 ),
        LOCAL_DATE_TIME( ValueGroup.LOCAL_DATE_TIME, (byte) 1 ),
        DATE( ValueGroup.DATE, (byte) 2 ),
        ZONED_TIME( ValueGroup.ZONED_TIME, (byte) 3 ),
        LOCAL_TIME( ValueGroup.LOCAL_TIME, (byte) 4 ),
        DURATION( ValueGroup.DURATION, (byte) 5 ),
        TEXT( ValueGroup.TEXT, (byte) 6 ),
        BOOLEAN( ValueGroup.BOOLEAN, (byte) 7 ),
        NUMBER( ValueGroup.NUMBER, (byte) 8 );

        private final ValueGroup valueGroup;
        private final byte typeId;

        Type( ValueGroup valueGroup, byte typeId )
        {
            this.valueGroup = valueGroup;
            this.typeId = typeId;
        }
    }

    private static final Type[] TYPES = Type.values();
    private static final Type[] TYPE_BY_ID = new Type[TYPES.length];
    static final Type[] TYPE_BY_GROUP = new Type[ValueGroup.values().length];
    static
    {
        for ( Type type : TYPES )
        {
            TYPE_BY_ID[type.typeId] = type;
        }
        for ( Type type : TYPES )
        {
            TYPE_BY_GROUP[type.valueGroup.ordinal()] = type;
        }
    }

    public GenericLayout()
    {
        super( "NSIL", 0, 1 );
    }

    @Override
    public GenericKey newKey()
    {
        return new GenericKey();
    }

    @Override
    public GenericKey copyKey( GenericKey key, GenericKey into )
    {
        into.setEntityId( key.getEntityId() );
        into.setCompareId( key.getCompareId() );
        into.type = key.type;
        into.long0 = key.long0;
        into.long1 = key.long1;
        into.long2 = key.long2;
        into.long3 = key.long3;
        into.copyByteArray( key, (int) key.long0 );
        return into;
    }

    @Override
    public int keySize( GenericKey key )
    {
        return KEY_HEADER_SIZE + actualKeySize( key );
    }

    private int actualKeySize( GenericKey key )
    {
        // TODO instead of having entityId part of each individual size, own that here and each type add their data size
        switch ( key.type )
        {
        case ZONED_DATE_TIME:
            return ZonedDateTimeIndexKey.SIZE;
        case LOCAL_DATE_TIME:
            return LocalDateTimeIndexKey.SIZE;
        case DATE:
            return DateIndexKey.SIZE;
        case ZONED_TIME:
            return ZonedTimeIndexKey.SIZE;
        case LOCAL_TIME:
            return LocalTimeIndexKey.SIZE;
        case DURATION:
            return DurationIndexKey.SIZE;
        case TEXT:
            return ENTITY_ID_SIZE + (int) key.long0;
        case BOOLEAN:
            return ENTITY_ID_SIZE + Byte.BYTES;
        case NUMBER:
            return NumberIndexKey.SIZE;
        default:
            throw new IllegalArgumentException( "Unknown type " + key.type );
        }
    }

    @Override
    public void writeKey( PageCursor cursor, GenericKey key )
    {
        cursor.putLong( key.getEntityId() );
        cursor.putByte( key.type.typeId );
        switch ( key.type )
        {
        case ZONED_DATE_TIME:
            writeZonedDateTime( cursor, key );
            break;
        case LOCAL_DATE_TIME:
            writeLocalDateTime( cursor, key );
            break;
        case DATE:
            writeDate( cursor, key );
            break;
        case ZONED_TIME:
            writeZonedTime( cursor, key );
            break;
        case LOCAL_TIME:
            writeLocalTime( cursor, key );
            break;
        case DURATION:
            writeDuration( cursor, key );
            break;
        case TEXT:
            writeText( cursor, key );
            break;
        case BOOLEAN:
            writeBoolean( cursor, key );
            break;
        case NUMBER:
            writeNumber( cursor, key );
            break;
        default:
            throw new IllegalArgumentException( "Unknown type " + key.type );
        }
    }

    private void writeNumber( PageCursor cursor, GenericKey key )
    {
        cursor.putByte( (byte) key.long1 );
        cursor.putLong( key.long0 );
    }

    private void writeBoolean( PageCursor cursor, GenericKey key )
    {
        cursor.putByte( (byte) key.long0 );
    }

    private void writeText( PageCursor cursor, GenericKey key )
    {
        cursor.putBytes( key.byteArray, 0, (int) key.long0 );
    }

    private void writeDuration( PageCursor cursor, GenericKey key )
    {
        cursor.putLong( key.long0 );
        cursor.putInt( (int) key.long1 );
        cursor.putLong( key.long2 );
        cursor.putLong( key.long3 );
    }

    private void writeLocalTime( PageCursor cursor, GenericKey key )
    {
        cursor.putLong( key.long0 );
    }

    private void writeZonedTime( PageCursor cursor, GenericKey key )
    {
        cursor.putLong( key.long0 );
        cursor.putInt( (int) key.long1 );
    }

    private void writeDate( PageCursor cursor, GenericKey key )
    {
        cursor.putLong( key.long0 );
    }

    private void writeLocalDateTime( PageCursor cursor, GenericKey key )
    {
        cursor.putLong( key.long1 );
        cursor.putInt( (int) key.long0 );
    }

    private void writeZonedDateTime( PageCursor cursor, GenericKey key )
    {
        cursor.putLong( key.long0 );
        cursor.putInt( (int) key.long1 );
        if ( key.long2 >= 0 )
        {
            cursor.putInt( (int) key.long2 | ZONE_ID_FLAG );
        }
        else
        {
            cursor.putInt( (int) key.long3 & ZONE_ID_MASK );
        }
    }

    @Override
    public void readKey( PageCursor cursor, GenericKey into, int keySize )
    {
        into.setEntityId( cursor.getLong() );
        byte typeId = cursor.getByte();
        keySize -= KEY_HEADER_SIZE;
        if ( typeId < 0 || typeId >= TYPES.length )
        {
            into.type = Type.NUMBER;
            into.long0 = 0;
            into.long1 = 0;
            return;
        }

        switch ( into.type )
        {
        case ZONED_DATE_TIME:
            readZonedDateTime( cursor, into );
            break;
        case LOCAL_DATE_TIME:
            readLocalDateTime( cursor, into );
            break;
        case DATE:
            readDate( cursor, into );
            break;
        case ZONED_TIME:
            readZonedTime( cursor, into );
            break;
        case LOCAL_TIME:
            readLocalTime( cursor, into );
            break;
        case DURATION:
            readDuration( cursor, into );
            break;
        case TEXT:
            readText( cursor, into, keySize );
            break;
        case BOOLEAN:
            readBoolean( cursor, into );
            break;
        case NUMBER:
            readNumber( cursor, into );
            break;
        default:
            throw new IllegalArgumentException( "Unknown type " + into.type );
        }
    }

    private void readNumber( PageCursor cursor, GenericKey into )
    {
        into.long1 = cursor.getByte();
        into.long0 = cursor.getLong();
    }

    private void readBoolean( PageCursor cursor, GenericKey into )
    {
        into.long0 = cursor.getByte();
    }

    private void readText( PageCursor cursor, GenericKey into, int keySize )
    {
        if ( keySize < ENTITY_ID_SIZE )
        {
            into.setEntityId( Long.MIN_VALUE );
            into.setBytesLength( 0 );
            return;
        }

        int bytesLength = keySize - ENTITY_ID_SIZE;
        into.setBytesLength( bytesLength );
        cursor.getBytes( into.byteArray, 0, bytesLength );
    }

    private void readDuration( PageCursor cursor, GenericKey into )
    {
        into.long0 = cursor.getLong();
        into.long1 = cursor.getInt();
        into.long2 = cursor.getLong();
        into.long3 = cursor.getLong();
    }

    private void readLocalTime( PageCursor cursor, GenericKey into )
    {
        into.long0 = cursor.getLong();
    }

    private void readZonedTime( PageCursor cursor, GenericKey into )
    {
        into.long0 = cursor.getLong();
        into.long1 = cursor.getInt();
    }

    private void readDate( PageCursor cursor, GenericKey into )
    {
        into.long0 = cursor.getLong();
    }

    private void readLocalDateTime( PageCursor cursor, GenericKey into )
    {
        into.long1 = cursor.getLong();
        into.long0 = cursor.getInt();
    }

    private void readZonedDateTime( PageCursor cursor, GenericKey into )
    {
        into.long0 = cursor.getLong();
        into.long1 = cursor.getInt();
        int encodedZone = cursor.getInt();
        if ( isZoneId( encodedZone ) )
        {
            into.long2 = asZoneId( encodedZone );
            into.long3 = 0;
        }
        else
        {
            into.long2 = -1;
            into.long3 = asZoneOffset( encodedZone );
        }
    }

    @Override
    public boolean fixedSize()
    {
        return false;
    }
}
