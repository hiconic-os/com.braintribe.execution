// ============================================================================
// Copyright BRAINTRIBE TECHNOLOGY GMBH, Austria, 2002-2022
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// ============================================================================
package com.braintribe.execution.priority;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.braintribe.common.lcd.Numbers;
import com.braintribe.execution.ThreadPoolBuilder;
import com.braintribe.processing.async.api.Promise;
import com.braintribe.processing.async.api.PromiseState;
import com.braintribe.processing.async.impl.HubPromise;

public class PrioritizedThreadPoolExecutorTest {

	@Test
	public void testPrioritizedThreadPoolExecutor() throws Exception {

		PrioritizedThreadPoolExecutor pool = new PrioritizedThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS);

		List<Integer> results = Collections.synchronizedList(new ArrayList<>());

		int iterations = 1;
		int tasks = 100;
		try {
			List<Future<?>> futures = new ArrayList<>();
			for (int i=0; i<iterations; ++i) {

				for (int w=0; w<tasks; ++w) {
					final int workerId = w;
					futures.add(pool.submit(() -> {
						try {
							Thread.sleep(50L);
						} catch(Exception e) {
							throw new RuntimeException("got interrupted", e);
						}
						System.out.println(""+workerId+": done");
						results.add(workerId);
					}, 0d));
					System.out.println(""+workerId+": submitted");

				}
				futures.add(pool.submit(() -> {
					try {
						Thread.sleep(100L);
					} catch(Exception e) {
						throw new RuntimeException("got interrupted", e);
					}
					System.out.println("HIGH: done");
					results.add(Numbers.MILLION);
				}, 1d));
				System.out.println("HIGH: submitted");

				for (Future<?> f : futures) {
					f.get();
				}
				assertThat(results).hasSize(tasks+1);
				int highIndex = -1;
				int lastNumber = -1;
				for (int w=0; w<=tasks; ++w) {
					int currentNumber = results.get(w);

					System.out.println("Current number: "+currentNumber);
					if (currentNumber == Numbers.MILLION) {
						highIndex = w;
						System.out.println("High index: "+w);
					} else {
						assertThat(currentNumber).isGreaterThan(lastNumber);
						lastNumber = currentNumber;
					}
				}
				assertThat(highIndex).isLessThan(tasks);
			}
		} finally {
			pool.shutdown();
		}

	}

	@Test
	public void testPrioritizedThreadPoolExecutorWithBuilder() throws Exception {

		PrioritizedThreadPoolExecutor pool = ThreadPoolBuilder.newPool().poolSize(1, 1).threadNamePrefix("test##").buildWithPriority();
		
		List<Integer> results = Collections.synchronizedList(new ArrayList<>());

		int tasks = 100;
		try {
			List<Future<?>> futures = new ArrayList<>();

			for (int w=0; w<tasks; ++w) {
				final int workerId = w;
				futures.add(pool.submit(() -> {
					try {
						Thread.sleep(50L);
					} catch(Exception e) {
						throw new RuntimeException("got interrupted", e);
					}
					System.out.println(""+workerId+": done");
					results.add(workerId);
				}, 0d));
				System.out.println(""+workerId+": submitted");

			}
			futures.add(pool.submit(() -> {
				try {
					Thread.sleep(100L);
				} catch(Exception e) {
					throw new RuntimeException("got interrupted", e);
				}
				System.out.println("HIGH: done");
				results.add(Numbers.MILLION);
			}, 1d));
			System.out.println("HIGH: submitted");

			for (Future<?> f : futures) {
				f.get();
			}
			assertThat(results).hasSize(tasks+1);
			int highIndex = -1;
			int lastNumber = -1;
			for (int w=0; w<=tasks; ++w) {
				int currentNumber = results.get(w);

				System.out.println("Current number: "+currentNumber);
				if (currentNumber == Numbers.MILLION) {
					highIndex = w;
					System.out.println("High index: "+w);
				} else {
					assertThat(currentNumber).isGreaterThan(lastNumber);
					lastNumber = currentNumber;
				}
			}
			assertThat(highIndex).isLessThan(tasks);
		} finally {
			pool.shutdown();
		}

	}
	
	@Test
	public void testSimpleComparableInPrioritizedThreadPoolExecutorWithSubmit() throws Exception {
		
		PrioritizedThreadPoolExecutor pool = ThreadPoolBuilder.newPool().poolSize(1, 1).threadNamePrefix("test##").buildWithPriority();
		
		List<Promise<String>> promises = new ArrayList<Promise<String>>(); 
		
		for (int i = 0; i < 10; i ++) {
			TestRunnable runnable = new TestRunnable(i);
			pool.submit(runnable);
			promises.add(runnable.getPromise());
		}
		
		
		for (Promise<String> promise: promises) {
			PromiseState state = promise.waitFor(10, TimeUnit.SECONDS);
			assertThat(state).isSameAs(PromiseState.done);
			System.out.println(promise.get());
		}
	}
	
	@Test
	public void testSimpleComparableInPrioritizedThreadPoolExecutorWithExecute() throws Exception {
		
		PrioritizedThreadPoolExecutor pool = ThreadPoolBuilder.newPool().poolSize(1, 1).threadNamePrefix("test##").buildWithPriority();
		
		List<Promise<String>> promises = new ArrayList<Promise<String>>(); 
		
		for (int i = 0; i < 10; i ++) {
			TestRunnable runnable = new TestRunnable(i);
			pool.execute(runnable);
			promises.add(runnable.getPromise());
		}
		
		
		for (Promise<String> promise: promises) {
			PromiseState state = promise.waitFor(10, TimeUnit.SECONDS);
			assertThat(state).isSameAs(PromiseState.done);
			System.out.println(promise.get());
		}
	}
	
	@Test
	public void testSimpleComparableThreadPoolExecutorWithExecute() throws Exception {
		PriorityBlockingQueue<Runnable> queue = new PriorityBlockingQueue<>();
		
		ThreadPoolExecutor pool = ThreadPoolBuilder.newPool().poolSize(1, 1).workQueue(queue).threadNamePrefix("test##").build();
		
		List<Promise<String>> promises = new ArrayList<Promise<String>>(); 
		
		for (int i = 0; i < 10; i ++) {
			TestRunnable runnable = new TestRunnable(i);
			pool.execute(runnable);
			promises.add(runnable.getPromise());
		}
		
		
		for (Promise<String> promise: promises) {
			PromiseState state = promise.waitFor(10, TimeUnit.SECONDS);
			assertThat(state).isSameAs(PromiseState.done);
			System.out.println(promise.get());
		}
	}
	
	@Test
	public void testSimpleComparableThreadPoolExecutorWithSubmit() throws Exception {
		PriorityBlockingQueue<Runnable> queue = new PriorityBlockingQueue<>();
		
		ThreadPoolExecutor pool = ThreadPoolBuilder.newPool().poolSize(1, 1).workQueue(queue).threadNamePrefix("test##").build();
		
		List<Promise<String>> promises = new ArrayList<Promise<String>>(); 
		
		for (int i = 0; i < 10; i ++) {
			TestRunnable runnable = new TestRunnable(i);
			pool.submit(runnable);
			promises.add(runnable.getPromise());
		}
		
		
		for (Promise<String> promise: promises) {
			PromiseState state = promise.waitFor(10, TimeUnit.SECONDS);
			assertThat(state).isSameAs(PromiseState.done);
			System.out.println(promise.get());
		}
	}
	
	private static class TestRunnable implements Comparable<TestRunnable>, Runnable {
		private int prio;
		private HubPromise<String> promise = new HubPromise<String>();

		public TestRunnable(int prio) {
			super();
			this.prio = prio;
		}

		@Override
		public int compareTo(TestRunnable o) {
			return prio - o.prio;
		}
		
		@Override
		public void run() {
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				// noop
			}
			promise.accept("Runnable with prio " + prio);
		}
		
		public Promise<String> getPromise() {
			return promise;
		}
	}
	


}
