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

import io.trino.array.LongBigArray;
import io.trino.operator.aggregation.LongLongState;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateFactory;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.System.arraycopy;

public class LongLongStateFactory
        implements AccumulatorStateFactory<LongLongState>
{
    @Override
    public LongLongState createSingleState()
    {
        return new SingleLongLongState();
    }

    @Override
    public LongLongState createGroupedState()
    {
        return new GroupedLongLongState();
    }

    private static final class GroupedLongLongState
            extends AbstractGroupedAccumulatorState
            implements LongLongState
    {
        private static final int INSTANCE_SIZE = instanceSize(GroupedLongLongState.class);

        private final LongBigArray longs = new LongBigArray();

        @Override
        public void ensureCapacity(long size)
        {
            longs.ensureCapacity(size * 2);
        }

        @Override
        public long[] getLongArray()
        {
            return longs.getSegment(getGroupId() * 2);
        }

        @Override
        public int getLongArrayOffset()
        {
            return longs.getOffset(getGroupId() * 2);
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + longs.sizeOf();
        }
    }

    private static final class SingleLongLongState
            implements LongLongState
    {
        private static final int INSTANCE_SIZE = instanceSize(SingleLongLongState.class);
        private static final int SIZE = (int) sizeOf(new long[2]);

        private final long[] longs = new long[2];

        public SingleLongLongState() {}

        // for copying
        private SingleLongLongState(long[] longs)
        {
            arraycopy(longs, 0, this.longs, 0, 2);
        }

        @Override
        public long[] getLongArray()
        {
            return longs;
        }

        @Override
        public int getLongArrayOffset()
        {
            return 0;
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + SIZE;
        }

        @Override
        public AccumulatorState copy()
        {
            return new SingleLongLongState(longs);
        }
    }
}
