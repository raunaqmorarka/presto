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
package io.trino.filesystem.alluxio;

import alluxio.conf.AlluxioConfiguration;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.cache.CacheFileSystem;
import io.trino.filesystem.cache.DefaultCacheKeyProvider;
import io.trino.filesystem.local.LocalFileSystem;
import io.trino.filesystem.memory.MemoryFileSystemFactory;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import static java.lang.Math.min;
import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(Lifecycle.PER_METHOD)
public class TestFuzzAlluxioCacheFileSystem
{
    @Test
    public void testFuzzTrinoInputReadFully()
            throws IOException
    {
        fuzzTrinoInputOperation((fs, l) -> fs.newInputFile(l).newInput(), TrinoInput::readFully);
    }

    @Test
    public void testFuzzTrinoInputReadTail()
            throws IOException
    {
        fuzzTrinoInputOperation((fs, l) -> fs.newInputFile(l).newInput(), (input, position, buffer, bufferOffset, bufferLength) -> input.readTail(buffer, bufferOffset, bufferLength));
    }

    @Test
    public void testFuzzTrinoInputStreamRead()
            throws IOException
    {
        fuzzTrinoInputOperation((fs, l) -> fs.newInputFile(l).newStream(), (input, position, buffer, bufferOffset, bufferLength) -> {
            input.seek(position);
            input.read(buffer, bufferOffset, bufferLength);
        });
    }

    public <T> void fuzzTrinoInputOperation(CreateTrinoInput<T> f, TrinoInputOperation<T> operation)
            throws IOException
    {
        Random random = new Random();
        for (int i = 0; i < 200; i++) {
            int fileSize = random.nextInt(10 * 100 * 100 * 2);
            try (TestFileSystem expectedFileSystemState = new TestMemoryFileSystem()) {
                try (TestFileSystem testFileSystemState = new TestAlluxioFileSystem()) {
                    TrinoFileSystem expectedFileSystem = expectedFileSystemState.create(random);
                    TrinoFileSystem testFileSystem = testFileSystemState.create(random);

                    Location expectedLocation = expectedFileSystemState.tempLocation();
                    Location testLocation = testFileSystemState.tempLocation();

                    createTestFile(expectedFileSystem, expectedLocation, fileSize);
                    createTestFile(testFileSystem, testLocation, fileSize);

                    T expected = f.apply(expectedFileSystem, expectedLocation);
                    T test = f.apply(testFileSystem, testLocation);

                    for (int j = 0; j < 1000; j++) {
                        applyOperation(random, fileSize, expected, test, operation);
                    }
                }
            }
        }
    }

    public <T> void applyOperation(Random random, int fileSize, T expectedInput, T actualInput, TrinoInputOperation<T> operation)
    {
        long position = random.nextLong(-100, 2L * fileSize);
        byte[] buffer = new byte[random.nextInt(2 * fileSize)];
        int bufferOffset = random.nextInt(-100, buffer.length + 100);
        int length = random.nextInt(-100, buffer.length + 100);

        Map.Entry<Slice, Optional<Exception>> expected = applyCatching(operation, expectedInput, position, buffer, bufferOffset, length);
        Map.Entry<Slice, Optional<Exception>> actual = applyCatching(operation, actualInput, position, buffer, bufferOffset, length);
        assertEquals(expected.getValue().map(Exception::getMessage), actual.getValue().map(Exception::getMessage));
        assertEquals(expected.getKey(), actual.getKey());
    }

    private static void createTestFile(TrinoFileSystem fileSystem, Location location, int fileSize)
            throws IOException
    {
        try (OutputStream output = fileSystem.newOutputFile(location).create()) {
            byte[] bytes = new byte[4];
            Slice slice = Slices.wrappedBuffer(bytes);
            for (int i = 0; i < fileSize; i++) {
                slice.setInt(0, i);
                output.write(bytes, 0, min(fileSize - i, 4));
            }
        }
    }

    private interface TestFileSystem
            extends Closeable
    {
        TrinoFileSystem create(Random random)
                throws IOException;

        Location tempLocation();
    }

    private static class TestMemoryFileSystem
            implements TestFileSystem
    {
        @Override
        public TrinoFileSystem create(Random random)
        {
            return new MemoryFileSystemFactory().create(ConnectorIdentity.ofUser(""));
        }

        @Override
        public Location tempLocation()
        {
            return Location.of("memory:///fuzz");
        }

        @Override
        public void close()
        {
        }
    }

    private static class TestLocalFileSystem
            implements TestFileSystem
    {
        private Path tempDirectory;

        @Override
        public TrinoFileSystem create(Random random)
                throws IOException
        {
            tempDirectory = Files.createTempDirectory("test");
            return new LocalFileSystem(tempDirectory);
        }

        @Override
        public Location tempLocation()
        {
            return Location.of("local:///fuzz");
        }

        @Override
        public void close()
        {
            tempDirectory.toFile().delete();
        }
    }

    private static class TestAlluxioFileSystem
            implements TestFileSystem
    {
        private Path tempDirectory;
        private Path cacheDirectory;

        @Override
        public TrinoFileSystem create(Random random)
                throws IOException
        {
            int cacheSize = random.nextInt(2 * 100, 10 * 100 * 100);
            int cachePageSize = random.nextInt(100, cacheSize);

            tempDirectory = Files.createTempDirectory("test");
            cacheDirectory = Files.createDirectory(tempDirectory.resolve("cache"));

            AlluxioFileSystemCacheConfig configuration = new AlluxioFileSystemCacheConfig()
                    .setCacheDirectories(cacheDirectory.toAbsolutePath().toString())
                    .setCachePageSize(DataSize.ofBytes(cachePageSize))
                    .setCacheTTL(null)
                    .setMaxCacheSizes(cacheSize + "B");
            AlluxioConfiguration alluxioConfiguration = AlluxioFileSystemCacheModule.getAlluxioConfiguration(configuration);

            MemoryFileSystemFactory fileSystemFactory = new MemoryFileSystemFactory();
            TestingAlluxioFileSystemCache alluxioCache = new TestingAlluxioFileSystemCache(alluxioConfiguration, new DefaultCacheKeyProvider());
            return new CacheFileSystem(fileSystemFactory.create(ConnectorIdentity.ofUser("hello")),
                    alluxioCache, alluxioCache.getCacheKeyProvider());
        }

        @Override
        public Location tempLocation()
        {
            return Location.of("memory:///fuzz");
        }

        @Override
        public void close()
        {
            tempDirectory.toFile().delete();
        }
    }

    private interface TrinoInputOperation<T>
    {
        void apply(T input, long position, byte[] buffer, int bufferOffset, int bufferLength)
                throws IOException;
    }

    private interface CreateTrinoInput<T>
    {
        T apply(TrinoFileSystem fileSystem, Location location)
                throws IOException;
    }

    // Poor man's result type
    private static <T> Map.Entry<Slice, Optional<Exception>> applyCatching(TrinoInputOperation<T> testOperation, T input, long position, byte[] buffer, int offset, int length)
    {
        try {
            testOperation.apply(input, position, buffer, offset, length);
            return Map.entry(Slices.wrappedBuffer(buffer), Optional.empty());
        }
        catch (IOException | IndexOutOfBoundsException | IllegalArgumentException e) {
            return Map.entry(Slices.EMPTY_SLICE, Optional.of(e));
        }
    }
}
