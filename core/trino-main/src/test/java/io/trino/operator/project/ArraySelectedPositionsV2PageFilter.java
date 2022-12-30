/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.operator.project;

import com.google.common.collect.ImmutableList;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;

public final class ArraySelectedPositionsV2PageFilter
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
            int nonNullPositionsCount = 0;
            for (int position = 0; position < positionCount; position++) {
                selectedPositions[nonNullPositionsCount] = position;
                nonNullPositionsCount += !block_0.isNull(position) ? 1 : 0;
            }
            for (int position = 0; position < nonNullPositionsCount; position++) {
                int nonNullPosition = selectedPositions[position];
                selectedPositions[selectedPositionsCount] = nonNullPosition;
                selectedPositionsCount += 64992484L < block_0.getInt(position, 0) ? 1 : 0;
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

    public ArraySelectedPositionsV2PageFilter()
    {
    }
}
