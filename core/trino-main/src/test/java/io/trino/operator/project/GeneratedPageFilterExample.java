package io.trino.operator.project;

import com.google.common.collect.ImmutableList;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;

public final class GeneratedPageFilterExample
        implements PageFilter
{
    private boolean[] selectedPositions = new boolean[0];

    public boolean filter(ConnectorSession session, Page page, int position)
    {
        Block block_0 = page.getBlock(0);
        boolean wasNull = false;
        boolean var10000;
        if (wasNull) {
            var10000 = false;
        }
        else {
            wasNull = block_0.isNull(position);
            var10000 = wasNull ? false : 64992484L < block_0.getInt(position, 0);
        }

        boolean result = var10000;
        return !wasNull && result;
    }

    public SelectedPositions filter(ConnectorSession session, Page page)
    {
        int positionCount = page.getPositionCount();
        if (this.selectedPositions.length < positionCount) {
            this.selectedPositions = new boolean[positionCount];
        }

        boolean[] selectedPositions = this.selectedPositions;

        for (int position = 0; position < positionCount; ++position) {
            selectedPositions[position] = this.filter(session, page, position);
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

    public GeneratedPageFilterExample()
    {
    }
}
