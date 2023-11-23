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
package com.braintribe.execution.virtual;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.braintribe.cfg.Configurable;
import com.braintribe.cfg.LifecycleAware;
import com.braintribe.execution.context.AttributeContextTransferCallable;
import com.braintribe.execution.context.AttributeContextTransferRunnable;
import com.braintribe.execution.monitoring.MonitoredThreadPool;
import com.braintribe.execution.monitoring.ThreadPoolMonitoring;
import com.braintribe.logging.Logger;
import com.braintribe.utils.StringTools;
import com.braintribe.utils.date.NanoClock;

public class VirtualThreadExecutor implements ExecutorService, LifecycleAware, MonitoredThreadPool {

	private static Logger logger = Logger.getLogger(VirtualThreadExecutor.class);

	private boolean addThreadContextToNdc = true;
	private boolean enableMonitoring = true;
	private String threadNamePrefix = null;
	private boolean waitForTasksToCompleteOnShutdown = true;
	private boolean constructed = false;
	private String description = null;

	private final String threadPoolId = UUID.randomUUID().toString();
	private static AtomicLong threadIdCounter = new AtomicLong(0);

	private int concurrency = 4;

	private AtomicInteger tasksPending = new AtomicInteger(0);
	private ExecutorService executor;

	private Semaphore semaphore;

	public VirtualThreadExecutor(final int concurrency) {
		this.concurrency = concurrency;
	}

	protected String beforeExecute() {

		if (enableMonitoring) {
			tasksPending.decrementAndGet();

			Long execId = threadIdCounter.incrementAndGet();
			String execIdString = Long.toString(execId.longValue(), 36);

			if (addThreadContextToNdc) {
				Thread currentThread = Thread.currentThread();
				logger.pushContext("executionId={" + execIdString + "}");
				logger.pushContext("threadId={" + currentThread.getName() + "}");
			}

			ThreadPoolMonitoring.beforeExecution(threadPoolId, execIdString);

			return execIdString;
		}
		return null;
	}

	protected void afterExecute(String execIdString) {

		if (enableMonitoring) {
			if (execIdString != null) {
				ThreadPoolMonitoring.afterExecution(threadPoolId, execIdString);
			}

			if (addThreadContextToNdc) {
				logger.popContext();
				logger.popContext();
			}
		}
	}

	@Override
	public void postConstruct() {
		if (constructed) {
			return;
		}
		constructed = true;

		semaphore = new Semaphore(concurrency);
		executor = Executors.newThreadPerTaskExecutor(new CountingVirtualThreadFactory(threadNamePrefix));

		if (description == null) {
			if (threadNamePrefix != null) {
				description = threadNamePrefix;
			} else {
				description = "anonymous-" + threadPoolId;
			}
		}

		ThreadPoolMonitoring.registerThreadPool(threadPoolId, this);

		logger.debug(() -> "Constructed thread pool " + getIdentification());
	}

	@Override
	public void preDestroy() {
		String identification = getIdentification();

		logger.debug(() -> "Shutting down thread pool " + identification + " (waitForTasksToCompleteOnShutdown: " + waitForTasksToCompleteOnShutdown
				+ ")");
		Instant start = NanoClock.INSTANCE.instant();

		try {
			if (waitForTasksToCompleteOnShutdown) {
				executor.shutdown();
			} else {
				executor.shutdownNow();
			}
		} finally {
			ThreadPoolMonitoring.unregisterThreadPool(threadPoolId);

			logger.debug(() -> "Shutting down thread pool " + identification + " took " + StringTools.prettyPrintDuration(start, true, null));
		}
	}

	private String getIdentification() {
		if (description == null && threadNamePrefix == null) {
			return threadPoolId;
		}
		if (description == null) {
			return threadPoolId.concat(" (").concat(threadNamePrefix).concat(")");
		}
		if (threadNamePrefix == null) {
			return threadPoolId.concat(" (").concat(description).concat(")");
		}
		return threadPoolId.concat(" (").concat(threadNamePrefix).concat(" / ").concat(description).concat(")");
	}

	@Configurable
	public void setAddThreadContextToNdc(boolean addThreadContextToNdc) {
		this.addThreadContextToNdc = addThreadContextToNdc;
	}
	@Configurable
	public void setEnableMonitoring(boolean enableMonitoring) {
		this.enableMonitoring = enableMonitoring;
	}
	@Configurable
	public void setThreadNamePrefix(String threadNamePrefix) {
		this.threadNamePrefix = threadNamePrefix;
	}
	@Configurable
	public void setWaitForTasksToCompleteOnShutdown(boolean waitForTasksToCompleteOnShutdown) {
		this.waitForTasksToCompleteOnShutdown = waitForTasksToCompleteOnShutdown;
	}
	@Configurable
	public void setDescription(String description) {
		this.description = description;
	}

	@Override
	public String getDescription() {
		return description;
	}

	@Override
	public int getPendingTasksInQueue() {
		return tasksPending.get();
	}

	@Override
	public int getThreadPoolSize() {
		return concurrency;
	}

	@Override
	public int getCoreThreadPoolSize() {
		return concurrency;
	}

	@Override
	public int getMaximumThreadPoolSize() {
		return concurrency;
	}

	protected void executionFinished(Instant created, Instant executed, Instant finished) {
		if (created == null || executed == null || finished == null) {
			return;
		}
		Duration enqueued = Duration.between(created, executed);
		Duration executionTime = Duration.between(executed, finished);
		ThreadPoolMonitoring.registerThreadPoolExecution(threadPoolId, enqueued, executionTime);
	}

	@Override
	public <T> Future<T> submit(Callable<T> task) {
		return executor.submit(enrich(task));
	}

	@Override
	public Future<?> submit(Runnable task) {
		return executor.submit(enrich(task));
	}

	private Runnable enrich(Runnable runnable) {
		Runnable effectiveRunnable = runnable;

		if (enableMonitoring) {
			tasksPending.incrementAndGet();
			effectiveRunnable = new VirtualRunnable(runnable, this, semaphore);
		}

		effectiveRunnable = new AttributeContextTransferRunnable(effectiveRunnable);

		return effectiveRunnable;
	}

	private <S> Callable<S> enrich(Callable<S> callable) {
		Callable<S> effectiveCallable = callable;

		if (enableMonitoring) {
			tasksPending.incrementAndGet();
			effectiveCallable = new VirtualCallable<S>(callable, this, semaphore);
		}

		effectiveCallable = new AttributeContextTransferCallable<S>(effectiveCallable);

		return effectiveCallable;
	}

	// Standard Delegating methods start here

	@Override
	public void execute(Runnable command) {
		executor.execute(command);
	}

	@Override
	public void shutdown() {
		executor.shutdown();
	}

	@Override
	public List<Runnable> shutdownNow() {
		return executor.shutdownNow();
	}

	@Override
	public boolean isShutdown() {
		return executor.isShutdown();
	}

	@Override
	public boolean isTerminated() {
		return executor.isTerminated();
	}

	@Override
	public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
		return executor.awaitTermination(timeout, unit);
	}

	@Override
	public <T> Future<T> submit(Runnable task, T result) {
		return executor.submit(task, result);
	}

	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
		return executor.invokeAll(tasks);
	}

	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
		return executor.invokeAll(tasks, timeout, unit);
	}

	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
		return executor.invokeAny(tasks);
	}

	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
			throws InterruptedException, ExecutionException, TimeoutException {
		return executor.invokeAny(tasks, timeout, unit);
	}

	@Override
	public void close() {
		executor.close();
	}
}
