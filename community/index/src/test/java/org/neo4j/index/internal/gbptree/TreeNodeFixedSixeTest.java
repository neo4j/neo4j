package org.neo4j.index.internal.gbptree;

import org.apache.commons.lang3.mutable.MutableLong;

public class TreeNodeFixedSixeTest extends TreeNodeTestBase<MutableLong,MutableLong>
{
    private SimpleLongLayout layout = new SimpleLongLayout();

    @Override
    protected Layout<MutableLong,MutableLong> getLayout()
    {
        return layout;
    }

    @Override
    protected TreeNode<MutableLong,MutableLong> getNode( int pageSize, Layout<MutableLong,MutableLong> layout )
    {
        return new TreeNodeFixedSize<>( pageSize, layout );
    }

    @Override
    protected MutableLong key( long sortOrder )
    {
        MutableLong key = layout.newKey();
        key.setValue( sortOrder );
        return key;
    }

    @Override
    protected MutableLong value( long someValue )
    {
        MutableLong value = layout.newValue();
        value.setValue( someValue );
        return value;
    }

    @Override
    boolean valuesEqual( MutableLong firstValue, MutableLong secondValue )
    {
        return firstValue.equals( secondValue );
    }
}
