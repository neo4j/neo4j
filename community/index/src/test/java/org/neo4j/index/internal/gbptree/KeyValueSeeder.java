package org.neo4j.index.internal.gbptree;

interface KeyValueSeeder<KEY, VALUE>
{
    KEY key( long seed );

    VALUE value( long seed );

    long getSeed( KEY key );
}
