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
package io.trino.filesystem.memory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.Weigher;
import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.filesystem.cache.TrinoFileSystemCache;
import org.weakref.jmx.Managed;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_LOCATION;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_READ_SIZE;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_KEY;
import static io.trino.filesystem.tracing.Tracing.withTracing;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public final class MemoryFileSystemCache
        implements TrinoFileSystemCache
{
    private final Tracer tracer;
    private final int maxContentLengthBytes;
    private final Cache<CacheKey, Slice> cache;

    @Inject
    public MemoryFileSystemCache(Tracer tracer, MemoryFileSystemCacheConfig config)
    {
        this(tracer, config.getCacheTTL(), config.getMaxSize(), config.getMaxContentLength());
    }

    public MemoryFileSystemCache(Tracer tracer, Duration expireAfterWrite, DataSize maxSize, DataSize maxContentLength)
    {
        this.tracer = requireNonNull(tracer, "tracer is null");
        checkArgument(maxContentLength.compareTo(DataSize.of(1, GIGABYTE)) <= 0, "maxContentLength must be less than or equal to 1GB");
        this.cache = EvictableCacheBuilder.newBuilder()
                .maximumWeight(maxSize.toBytes())
                .weigher((Weigher<CacheKey, Slice>) (key, value) -> toIntExact(estimatedSizeOf(key.providedKey()) + Integer.BYTES + value.getRetainedSize()))
                .expireAfterWrite(expireAfterWrite.toMillis(), TimeUnit.MILLISECONDS)
                .shareNothingWhenDisabled()
                .recordStats()
                .build();
        this.maxContentLengthBytes = toIntExact(maxContentLength.toBytes());
    }

    @Override
    public TrinoInput cacheInput(TrinoInputFile delegate, String key)
            throws IOException
    {
        long fileSize = delegate.length();
        if (fileSize > maxContentLengthBytes) {
            return delegate.newInput();
        }

        Span span = createSpan(key, delegate.location(), fileSize, "cacheInput");
        return withTracing(span, () -> new MemoryInput(delegate.location(), getFromCache(key, delegate)));
    }

    @Override
    public TrinoInputStream cacheStream(TrinoInputFile delegate, String key)
            throws IOException
    {
        long fileSize = delegate.length();
        if (fileSize > maxContentLengthBytes) {
            return delegate.newStream();
        }

        Span span = createSpan(key, delegate.location(), fileSize, "cacheInputStream");
        return withTracing(span, () -> new MemoryInputStream(delegate.location(), getFromCache(key, delegate)));
    }

    @Override
    public void expire(Location location)
            throws IOException
    {
        List<CacheKey> expired = cache.asMap().keySet().stream()
                .filter(key -> key.providedKey().contains(location.path()))
                .collect(toImmutableList());
        cache.invalidateAll(expired);
    }

    @Managed
    public void flushCache()
    {
        cache.invalidateAll();
    }

    @Managed
    public long getHitCount()
    {
        return cache.stats().hitCount();
    }

    @Managed
    public long getRequestCount()
    {
        return cache.stats().requestCount();
    }

    @VisibleForTesting
    boolean isCached(String key, long fileSize)
    {
        return cache.getIfPresent(new CacheKey(key, toIntExact(fileSize))) != null;
    }

    private Slice getFromCache(String key, TrinoInputFile delegate)
            throws IOException
    {
        try {
            // File length is available for free at this point, we add it to cache key to be resilient to manual modifications to file
            return cache.get(new CacheKey(key, toIntExact(delegate.length())), () -> load(key, delegate));
        }
        catch (ExecutionException e) {
            throw handleException(delegate.location(), e.getCause());
        }
    }

    private Slice load(String key, TrinoInputFile delegate)
            throws IOException
    {
        long fileSize = delegate.length();
        try (TrinoInput trinoInput = delegate.newInput()) {
            Span span = createSpan(key, delegate.location(), fileSize, "loadCache");
            return withTracing(span, () -> trinoInput.readTail(toIntExact(fileSize)));
        }
    }

    private Span createSpan(String key, Location location, long fileSize, String name)
    {
        return tracer.spanBuilder("MemoryFileSystemCache." + name)
                .setAttribute(CACHE_KEY, key)
                .setAttribute(CACHE_FILE_LOCATION, location.toString())
                .setAttribute(CACHE_FILE_READ_SIZE, fileSize)
                .startSpan();
    }

    private static IOException handleException(Location location, Throwable cause)
            throws IOException
    {
        if (cause instanceof FileNotFoundException || cause instanceof NoSuchFileException) {
            throw withCause(new FileNotFoundException(location.toString()), cause);
        }
        if (cause instanceof FileAlreadyExistsException) {
            throw withCause(new FileAlreadyExistsException(location.toString()), cause);
        }
        throw new IOException(cause.getMessage() + ": " + location, cause);
    }

    private static <T extends Throwable> T withCause(T throwable, Throwable cause)
    {
        throwable.initCause(cause);
        return throwable;
    }

    private record CacheKey(String providedKey, int fileSize) {}
}
