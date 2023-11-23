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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.braintribe.execution.ExtendedThreadPoolExecutor;

public class PrioritizedThreadPoolExecutor extends ExtendedThreadPoolExecutor {

	private AtomicInteger insertionCounter = new AtomicInteger(0);

	public PrioritizedThreadPoolExecutor(final int corePoolSize, final int maximumPoolSize, final long keepAliveTime, final TimeUnit unit) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit,
				((BlockingQueue<Runnable>) (BlockingQueue<?>) new PriorityBlockingQueue<PrioritizedRunnable>()));
	}

	@Override
	protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
		RunnableFuture<T> newTaskFor = super.newTaskFor(callable);

		double priority = 0.0d;

		PrioritizedCallable<T> pr = null;
		if (callable instanceof PrioritizedCallable) {
			pr = (PrioritizedCallable<T>) callable;
		}
		if (pr != null) {
			priority = pr.getPriority();
		}

		return new PriorityFuture<T>(newTaskFor, priority, insertionCounter.incrementAndGet());
	}
	@Override
	protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
		RunnableFuture<T> newTaskFor = super.newTaskFor(runnable, value);

		double priority = 0.0d;

		PrioritizedRunnable pr = null;
		if (runnable instanceof PrioritizedRunnable) {
			pr = (PrioritizedRunnable) runnable;
		}
		if (pr != null) {
			priority = pr.getPriority();
		}

		return new PriorityFuture<T>(newTaskFor, priority, insertionCounter.incrementAndGet());
	}

	@Override
	public <T> Future<T> submit(Callable<T> task) {
		return super.submit(new PrioritizedCallable<T>(task, 0d, insertionCounter.incrementAndGet()));
	}
	@Override
	public Future<?> submit(Runnable task) {
		return super.submit(new PrioritizedRunnable(task, 0d, insertionCounter.incrementAndGet()));
	}
	@Override
	public <T> Future<T> submit(Runnable task, T result) {
		return super.submit(new PrioritizedRunnable(task, 0d, insertionCounter.incrementAndGet()), result);
	}

	public <T> Future<T> submit(Callable<T> task, double priority) {
		return super.submit(new PrioritizedCallable<T>(task, priority, insertionCounter.incrementAndGet()));
	}
	public Future<?> submit(Runnable task, double priority) {
		return super.submit(new PrioritizedRunnable(task, priority, insertionCounter.incrementAndGet()));
	}
	public <T> Future<T> submit(Runnable task, T result, double priority) {
		return super.submit(new PrioritizedRunnable(task, priority, insertionCounter.incrementAndGet()), result);
	}
}
