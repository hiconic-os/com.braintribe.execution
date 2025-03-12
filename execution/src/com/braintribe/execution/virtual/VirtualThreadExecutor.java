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
	private boolean interruptThreadsOnShutdown = false;
	private Duration terminationTimeout;
	private boolean constructed = false;
	private String description = null;

	private final String threadPoolId = UUID.randomUUID().toString();
	private static AtomicLong threadIdCounter = new AtomicLong(0);

	private int concurrency = 4;

	private final AtomicInteger tasksPending = new AtomicInteger(0);
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
		close();
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

	/**
	 * @deprecated use {@link #setInterruptThreadsOnShutdown(boolean)} (but with the opposite logical value)
	 */
	@Deprecated
	@Configurable
	public void setWaitForTasksToCompleteOnShutdown(boolean waitForTasksToCompleteOnShutdown) {
		setInterruptThreadsOnShutdown(!waitForTasksToCompleteOnShutdown);
	}
	@Configurable
	public void setInterruptThreadsOnShutdown(boolean interruptThreadsOnShutdown) {
		this.interruptThreadsOnShutdown = interruptThreadsOnShutdown;
	}
	@Configurable
	public void setTerminationTimeout(Duration terminationTimeout) {
		this.terminationTimeout = terminationTimeout;
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
		if (executor.isShutdown())
			return;

		String identification = getIdentification();

		logger.debug(() -> "Shutting down thread pool " + identification + " (interruptThreadsOnShutdown: " + interruptThreadsOnShutdown + ")");
		Instant start = NanoClock.INSTANCE.instant();

		try {
			if (interruptThreadsOnShutdown) {
				executor.shutdownNow();
			} else {
				executor.shutdown();
			}

			if (terminationTimeout != null)
				try {
					executor.awaitTermination(terminationTimeout.toMillis(), TimeUnit.MILLISECONDS);
				} catch (InterruptedException e) {
					logger.warn(this + " interrupted while waiting for its executor termination", e);
					throw new RuntimeException("", e);
				}

		} finally {
			ThreadPoolMonitoring.unregisterThreadPool(threadPoolId);

			logger.debug(() -> "Shutting down thread pool " + identification + " took " + StringTools.prettyPrintDuration(start, true, null));
		}
	}

	@Override
	public String toString() {
		String d = description == null ? "no-description" : description;
		return getClass().getSimpleName() + "(" + d + ")";
	}
}
