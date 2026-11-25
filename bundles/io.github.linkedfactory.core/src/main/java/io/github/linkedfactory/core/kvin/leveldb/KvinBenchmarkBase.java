/*
 * Copyright (c) 2022 Fraunhofer IWU.
 *
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
package io.github.linkedfactory.core.kvin.leveldb;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.KvinTuple;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Simple benchmark for the LevelDB-based time series store.
 */
public abstract class KvinBenchmarkBase {
	protected final URI valueProperty = URIs.createURI("property:value");
	protected final int seed = 200;
	protected final int writeValues = 20000000;
	protected final int readValues = 2000;
	protected final long startTimeValues = 1478252048736L;

	protected abstract Kvin createStore();

	public void run() throws InterruptedException {
		Kvin store = createStore();
		long benchmarkStart = System.currentTimeMillis();
		try {
			Random random = new Random();
			int[] nrs = new int[1000];
			for (int i = 0; i < nrs.length; i++) {
				nrs[i] = random.nextInt(Integer.MAX_VALUE);
			}
			Random rand = new Random(seed);

			long currentTime = startTimeValues;
			List<KvinTuple> batch = new ArrayList<>();
			for (int i = 0; i <= writeValues; i++) {
				int randomNr = nrs[rand.nextInt(nrs.length)];
				URI uri = URIs.createURI("http://linkedfactory.github.io/" + randomNr + "/e3fabrik/rollex/" + randomNr + "/measured-point-1");
				URI ctx = URIs.createURI("ctx:" + nrs[rand.nextInt(nrs.length)]);

				Object value = randomNr % 2 == 0 ? (Object) rand.nextGaussian() : (Object) rand.nextLong(100000);
				batch.add(new KvinTuple(ctx, uri, valueProperty, currentTime, value));

				// insert data via batch
				if (i % 100000 == 0 || i == writeValues) {
					System.out.println("  at: " + i);
					store.put(batch);
					batch.clear();
				}

				currentTime += rand.nextInt(1000);
			}

			double seconds = (System.currentTimeMillis() - benchmarkStart) / 1000.0;
			System.out.println(String.format("Wrote " + writeValues + " in %1$,.2f seconds: %2$,.2f ops per second",
					seconds, writeValues / seconds));

			rand = new Random(seed);
			benchmarkStart = System.currentTimeMillis();
			long startTimeFetch = currentTime;
			for (int i = 0; i <= readValues; i++) {
				int randomNr = nrs[rand.nextInt(nrs.length)];
				URI uri = URIs.createURI("http://linkedfactory.github.io/" + randomNr + "/e3fabrik/rollex/" + randomNr + "/measured-point-1");
				URI ctx = URIs.createURI("ctx:" + nrs[rand.nextInt(nrs.length)]);

				List<KvinTuple> r = store.fetch(uri, valueProperty, ctx, startTimeFetch, startTimeValues, 2, 0, null).toList();
				if (i % 1000 == 0) {
					System.out.println(r);
				}
			}

			System.out.println("Reading " + readValues + " values took: " +
					String.format("%1$,.2f seconds", (System.currentTimeMillis() - benchmarkStart) / 1000.0));
		} finally {
			Thread.sleep(5000);
			store.close();
		}
	}
}
