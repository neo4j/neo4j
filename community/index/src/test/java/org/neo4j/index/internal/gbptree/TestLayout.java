package org.neo4j.index.internal.gbptree;

abstract class TestLayout<KEY,VALUE> extends Layout.Adapter<KEY,VALUE> implements KeyValueSeeder<KEY,VALUE>
{
    abstract int compareValue( VALUE v1, VALUE v2 );
}
