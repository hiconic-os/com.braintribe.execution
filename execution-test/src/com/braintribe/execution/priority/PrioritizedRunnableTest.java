// ============================================================================
// Copyright BRAINTRIBE TECHNOLOGY GMBH, Austria, 2002-2022
// 
// This library is free software; you can redistribute it and/or modify it under the terms of the GNU Lesser General Public
// License as published by the Free Software Foundation; either version 3 of the License, or (at your option) any later version.
// 
// This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public License along with this library; See http://www.gnu.org/licenses/.
// ============================================================================
package com.braintribe.execution.priority;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.braintribe.utils.lcd.CollectionTools2;

public class PrioritizedRunnableTest {

	private static AtomicInteger insertionCounter = new AtomicInteger(0);

	@Test
	public void testOrderingRunnables() throws Exception {

		PrioritizedRunnable high = new PrioritizedRunnable(null, 1, insertionCounter.incrementAndGet());
		PrioritizedRunnable normal1 = new PrioritizedRunnable(null, 0, insertionCounter.incrementAndGet());
		PrioritizedRunnable normal2 = new PrioritizedRunnable(null, 0, insertionCounter.incrementAndGet());
		PrioritizedRunnable normal3 = new PrioritizedRunnable(null, 0, insertionCounter.incrementAndGet());
		PrioritizedRunnable normal4 = new PrioritizedRunnable(null, 0, insertionCounter.incrementAndGet());
		PrioritizedRunnable low1 = new PrioritizedRunnable(null, -1, insertionCounter.incrementAndGet());
		List<PrioritizedRunnable> all = CollectionTools2.asList(high, normal1, normal2, normal3, normal4, low1);

		int iterations = 10;

		for (int i=0; i<iterations; ++i) {
			
			Collections.shuffle(all);
			
			PriorityBlockingQueue<PrioritizedRunnable> queue = new PriorityBlockingQueue<>();
			queue.addAll(all);
			
			assertThat(queue.size()).isEqualTo(all.size());
			
			PrioritizedRunnable[] array = new PrioritizedRunnable[all.size()];
			for (int k=0; k<all.size(); ++k) {
				array[k] = queue.take();
			}
			
			assertThat(array[0].getPriority()).isEqualTo(1);
			for (int k=1; k<(all.size()-2); ++k) {
				assertThat(array[k].getPriority()).isEqualTo(0);
			}
			assertThat(array[all.size()-1].getPriority()).isEqualTo(-1);
		}

	}
	@Test
	public void testOrderingCallables() throws Exception {

		PrioritizedCallable<String> high = new PrioritizedCallable<>(null, 1, insertionCounter.incrementAndGet());
		PrioritizedCallable<String> normal1 = new PrioritizedCallable<>(null, 0, insertionCounter.incrementAndGet());
		PrioritizedCallable<String> normal2 = new PrioritizedCallable<>(null, 0, insertionCounter.incrementAndGet());
		PrioritizedCallable<String> normal3 = new PrioritizedCallable<>(null, 0, insertionCounter.incrementAndGet());
		PrioritizedCallable<String> normal4 = new PrioritizedCallable<>(null, 0, insertionCounter.incrementAndGet());
		PrioritizedCallable<String> low1 = new PrioritizedCallable<>(null, -1, insertionCounter.incrementAndGet());
		List<PrioritizedCallable<String>> all = CollectionTools2.asList(high, normal1, normal2, normal3, normal4, low1);

		int iterations = 10;

		for (int i=0; i<iterations; ++i) {
			
			Collections.shuffle(all);
			
			PriorityBlockingQueue<PrioritizedCallable<String>> queue = new PriorityBlockingQueue<>();
			queue.addAll(all);
			
			assertThat(queue.size()).isEqualTo(all.size());
			
			PrioritizedCallable<String>[] array = new PrioritizedCallable[all.size()];
			for (int k=0; k<all.size(); ++k) {
				array[k] = queue.take();
			}
			
			assertThat(array[0].getPriority()).isEqualTo(1);
			for (int k=1; k<(all.size()-2); ++k) {
				assertThat(array[k].getPriority()).isEqualTo(0);
			}
			assertThat(array[all.size()-1].getPriority()).isEqualTo(-1);
		}

	}
}
