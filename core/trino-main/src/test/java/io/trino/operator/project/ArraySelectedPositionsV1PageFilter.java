package io.trino.operator.project;

import com.google.common.collect.ImmutableList;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;

public final class ArraySelectedPositionsV1PageFilter
        implements PageFilter
{
    private int[] selectedPositions = new int[0];

    public SelectedPositions filter(ConnectorSession session, Page page)
    {
        int positionCount = page.getPositionCount();
        if (this.selectedPositions.length < positionCount) {
            this.selectedPositions = new int[positionCount];
        }

        Block block_0 = page.getBlock(0);
        int selectedPositionsCount = 0;
        if (block_0.mayHaveNull()) {
            for (int position = 0; position < positionCount; position++) {
                selectedPositions[selectedPositionsCount] = position;
                selectedPositionsCount += !block_0.isNull(position) && 64992484L < block_0.getInt(position, 0) ? 1 : 0;
            }
        }
        else {
            for (int position = 0; position < positionCount; position++) {
                selectedPositions[selectedPositionsCount] = position;
                selectedPositionsCount += 64992484L < block_0.getInt(position, 0) ? 1 : 0;
            }
        }

        return PageFilter.positionsArrayToSelectedPositions(this.selectedPositions, selectedPositionsCount, positionCount);
    }

    public boolean isDeterministic()
    {
        return true;
    }

    public InputChannels getInputChannels()
    {
        return new InputChannels(ImmutableList.of(0), ImmutableList.of(0));
    }

    public ArraySelectedPositionsV1PageFilter()
    {
    }
}
