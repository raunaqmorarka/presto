package io.trino.operator.project;

import com.google.common.collect.ImmutableList;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;

public final class BooleanSelectedPositionsV2PageFilter
        implements PageFilter
{
    private boolean[] selectedPositions = new boolean[0];

    public SelectedPositions filter(ConnectorSession session, Page page)
    {
        int positionCount = page.getPositionCount();
        if (this.selectedPositions.length < positionCount) {
            this.selectedPositions = new boolean[positionCount];
        }

        Block block_0 = page.getBlock(0);
        if (block_0.mayHaveNull()) {
            for (int position = 0; position < positionCount; position++) {
                selectedPositions[position] = !block_0.isNull(position);
            }

            for (int position = 0; position < positionCount; position++) {
                selectedPositions[position] = selectedPositions[position] && 64992484L < block_0.getInt(position, 0);
            }
        }
        else {
            for (int position = 0; position < positionCount; position++) {
                selectedPositions[position] = 64992484L < block_0.getInt(position, 0);
            }
        }

        return PageFilter.positionsArrayToSelectedPositions(this.selectedPositions, positionCount);
    }

    public boolean isDeterministic()
    {
        return true;
    }

    public InputChannels getInputChannels()
    {
        return new InputChannels(ImmutableList.of(0), ImmutableList.of(0));
    }

    public BooleanSelectedPositionsV2PageFilter()
    {
    }
}
