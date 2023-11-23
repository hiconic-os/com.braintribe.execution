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
