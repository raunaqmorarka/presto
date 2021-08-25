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

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.Random;

import static io.trino.jmh.Benchmarks.benchmark;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

@State(Scope.Thread)
@OutputTimeUnit(MICROSECONDS)
@Fork(2)
@Warmup(iterations = 15)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkTernary
{
    private boolean[] test;

    @Benchmark
    public int ifNotCount()
    {
        int c = 0;
        for (boolean b : test) {
            if (!b) {
                c += 1;
            }
        }
        return c;
    }

    @Benchmark
    public int ternaryNotCount()
    {
        int c = 0;
        for (boolean b : test) {
            c += b ? 0 : 1;
        }
        return c;
    }

    @Benchmark
    public int ternaryCount()
    {
        int c = 0;
        for (boolean b : test) {
            c += b ? 1 : 0;
        }
        return c;
    }

    @Benchmark
    public int ternaryXorCount()
    {
        int c = 0;
        for (boolean b : test) {
            c += (b ? 1 : 0) ^ 1;
        }
        return c;
    }

    @Benchmark
    public int ifCount()
    {
        int c = 0;
        for (boolean b : test) {
            if (b) {
                c += 1;
            }
        }
        return c;
    }

    @Setup(Level.Invocation)
    public void setup()
    {
        Random random = new Random(42);
        test = new boolean[100_000];
        for (int i = 0; i < 100_000; i++) {
            test[i] = random.nextBoolean();
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        benchmark(BenchmarkDictionaryBlock.class).run();
    }
}
