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
package io.trino.operator.aggregation.state;

import com.google.common.collect.ImmutableList;
import io.trino.operator.aggregation.LongLongState;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import static io.trino.spi.type.BigintType.BIGINT;

public class LongLongStateSerializer
        implements AccumulatorStateSerializer<LongLongState>
{
    private final Type serializedType = RowType.anonymous(ImmutableList.of(BIGINT, BIGINT));

    @Override
    public Type getSerializedType()
    {
        return serializedType;
    }

    @Override
    public void serialize(LongLongState state, BlockBuilder out)
    {
        BlockBuilder rowBuilder = out.beginBlockEntry();
        long[] longs = state.getLongArray();
        int offset = state.getLongArrayOffset();
        BIGINT.writeLong(rowBuilder, longs[offset]);
        BIGINT.writeLong(rowBuilder, longs[offset + 1]);
        out.closeEntry();
    }

    @Override
    public void deserialize(Block block, int index, LongLongState state)
    {
        long[] longs = state.getLongArray();
        int offset = state.getLongArrayOffset();

        Block rowBlock = block.getObject(index, Block.class);
        longs[offset] = BIGINT.getLong(rowBlock, 0);
        longs[offset + 1] = BIGINT.getLong(rowBlock, 1);
    }
}
