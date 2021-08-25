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

import java.util.Random;

public class BenchmarkTernarySimple
{
    private boolean[] test;

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

    public int ternaryNotCount()
    {
        int c = 0;
        for (boolean b : test) {
            c += b ? 0 : 1;
        }
        return c;
    }

    public int ternaryCount()
    {
        int c = 0;
        for (boolean b : test) {
            c += b ? 1 : 0;
        }
        return c;
    }

    public int ternaryXorCount()
    {
        int c = 0;
        for (boolean b : test) {
            c += (b ? 1 : 0) ^ 1;
        }
        return c;
    }

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
        BenchmarkTernarySimple block = new BenchmarkTernarySimple();
        block.setup();
        int k = 0;
        long n = System.nanoTime();
        for (int i = 0; i < 100_000; i++) {
            k += block.ifCount();
        }
        System.out.println(k);
        System.out.println(System.nanoTime() - n);
    }
}
