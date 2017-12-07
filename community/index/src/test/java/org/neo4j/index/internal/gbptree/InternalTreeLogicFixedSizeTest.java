package org.neo4j.index.internal.gbptree;

import org.apache.commons.lang3.mutable.MutableLong;

public class InternalTreeLogicFixedSizeTest extends InternalTreeLogicTestBase<MutableLong,MutableLong>
{
    SimpleLongLayout layout = new SimpleLongLayout();

    @Override
    protected ValueMerger<MutableLong,MutableLong> getAdder()
    {
        return (( existingKey, newKey, base, add ) ->
        {
            base.add( add.longValue() );
            return base;
        } );
    }

    @Override
    protected TreeNode<MutableLong,MutableLong> getTreeNode( int pageSize, Layout<MutableLong,MutableLong> layout )
    {
        return new TreeNodeFixedSize<>( pageSize, layout );
    }

    @Override
    protected TestLayout<MutableLong,MutableLong> getLayout()
    {
        return layout;
    }
}
