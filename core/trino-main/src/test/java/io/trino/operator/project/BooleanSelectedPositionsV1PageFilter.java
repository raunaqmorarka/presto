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

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

public final class BooleanSelectedPositionsV1PageFilter
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
                selectedPositions[position] = !block_0.isNull(position) && 64992484L < block_0.getInt(position, 0);
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

    public BooleanSelectedPositionsV1PageFilter()
    {
    }
}
