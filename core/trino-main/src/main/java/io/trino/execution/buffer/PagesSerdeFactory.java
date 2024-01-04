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
package io.trino.execution.buffer;

import io.airlift.compress.Compressor;
import io.airlift.compress.Decompressor;
import io.airlift.compress.lz4.Lz4Compressor;
import io.airlift.compress.lz4.Lz4Decompressor;
import io.airlift.compress.lzo.LzoCompressor;
import io.airlift.compress.lzo.LzoDecompressor;
import io.airlift.compress.snappy.SnappyCompressor;
import io.airlift.compress.snappy.SnappyDecompressor;
import io.airlift.compress.zstd.ZstdCompressor;
import io.airlift.compress.zstd.ZstdDecompressor;
import io.trino.spi.block.BlockEncodingSerde;

import javax.crypto.SecretKey;

import java.util.Optional;

import static io.trino.execution.buffer.CompressionKind.LZ4;
import static java.util.Objects.requireNonNull;

public class PagesSerdeFactory
{
    private static final int SERIALIZED_PAGE_DEFAULT_BLOCK_SIZE_IN_BYTES = 64 * 1024;

    private final BlockEncodingSerde blockEncodingSerde;
    private final boolean compressionEnabled;
    private final CompressionKind compressionKind;

    public PagesSerdeFactory(BlockEncodingSerde blockEncodingSerde, boolean compressionEnabled)
    {
        this(blockEncodingSerde, compressionEnabled, LZ4);
    }

    public PagesSerdeFactory(BlockEncodingSerde blockEncodingSerde, boolean compressionEnabled, CompressionKind compressionKind)
    {
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        this.compressionEnabled = compressionEnabled;
        this.compressionKind = compressionKind;
    }

    public PageSerializer createSerializer(Optional<SecretKey> encryptionKey)
    {
        return new PageSerializer(blockEncodingSerde, compressionEnabled, createCompressor(compressionKind), encryptionKey, SERIALIZED_PAGE_DEFAULT_BLOCK_SIZE_IN_BYTES);
    }

    public PageDeserializer createDeserializer(Optional<SecretKey> encryptionKey)
    {
        return new PageDeserializer(blockEncodingSerde, compressionEnabled, createDecompressor(compressionKind), encryptionKey, SERIALIZED_PAGE_DEFAULT_BLOCK_SIZE_IN_BYTES);
    }

    private Compressor createCompressor(CompressionKind compressionKind)
    {
        switch (compressionKind) {
            case NONE:
                return null;
            case SNAPPY:
                return new SnappyCompressor();
            case LZO:
                return new LzoCompressor();
            case LZ4:
                return new Lz4Compressor();
            case ZSTD:
                return new ZstdCompressor();
        }
        throw new IllegalArgumentException("Unsupported compression kind" + compressionKind);
    }

    private Decompressor createDecompressor(CompressionKind compressionKind)
    {
        switch (compressionKind) {
            case NONE:
                return null;
            case SNAPPY:
                return new SnappyDecompressor();
            case LZO:
                return new LzoDecompressor();
            case LZ4:
                return new Lz4Decompressor();
            case ZSTD:
                return new ZstdDecompressor();
        }
        throw new IllegalArgumentException("Unsupported decompression kind" + compressionKind);
    }
}
