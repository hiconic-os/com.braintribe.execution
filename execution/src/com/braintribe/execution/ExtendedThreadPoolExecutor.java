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
package com.braintribe.execution;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.braintribe.cfg.Configurable;
import com.braintribe.cfg.LifecycleAware;
import com.braintribe.execution.context.AttributeContextTransferRunnable;
import com.braintribe.execution.monitoring.ExecutionMonitoring;
import com.braintribe.execution.monitoring.MonitoredThreadPool;
import com.braintribe.execution.monitoring.MonitoringRunnable;
import com.braintribe.execution.monitoring.ThreadPoolMonitoring;
import com.braintribe.execution.priority.ComparableDelegationRunnable;
import com.braintribe.execution.priority.ComparableDelegationRunnableFuture;
import com.braintribe.execution.priority.PrioritizedCallable;
import com.braintribe.execution.priority.PrioritizedRunnable;
import com.braintribe.execution.priority.PriorityFuture;
import com.braintribe.logging.Logger;
import com.braintribe.utils.StringTools;
import com.braintribe.utils.date.NanoClock;

/**
 * Extension of the ThreadPoolExecutor class that implements the beforeExecute and afterExecute methods. These methods
 * are used to add context information to the Logger.
 * 
 * Note: Not putting this into the utils project so that it can be extended by more CSP internals in the future.
 */
public class ExtendedThreadPoolExecutor extends ThreadPoolExecutor implements LifecycleAware, MonitoredThreadPool, ExecutionMonitoring {

	private static Logger logger = Logger.getLogger(ExtendedThreadPoolExecutor.class);

	private boolean addThreadContextToNdc = true;
	private boolean enableMonitoring = true;
	private boolean customThreadFactory = false;
	private String threadNamePrefix = null;
	private boolean waitForTasksToCompleteOnShutdown = true;
	private boolean constructed = false;
	private String description = null;

	private final String threadPoolId = UUID.randomUUID().toString();

	private static AtomicLong threadIdCounter = new AtomicLong(0);

	private final ThreadLocal<String> currentExecutionId = new ThreadLocal<>();

	public ExtendedThreadPoolExecutor(final int corePoolSize, final int maximumPoolSize, final long keepAliveTime, final TimeUnit unit,
			final BlockingQueue<Runnable> workQueue, final ThreadFactory threadFactory) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
		this.customThreadFactory = true;
	}

	public ExtendedThreadPoolExecutor(final int corePoolSize, final int maximumPoolSize, final long keepAliveTime, final TimeUnit unit,
			final BlockingQueue<Runnable> workQueue, final ThreadFactory threadFactory, final RejectedExecutionHandler handler) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
		this.customThreadFactory = true;
	}

	public ExtendedThreadPoolExecutor(final int corePoolSize, final int maximumPoolSize, final long keepAliveTime, final TimeUnit unit,
			final BlockingQueue<Runnable> workQueue) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
	}

	public ExtendedThreadPoolExecutor(int corePoolSize, int maximumPoolSize, int keepAliveTime, TimeUnit seconds, BlockingQueue<Runnable> workQueue,
			RejectedExecutionHandler rejectedExecutionHandler) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, seconds, workQueue, rejectedExecutionHandler);
	}

	@Override
	protected void beforeExecute(final Thread t, final Runnable r) {
		super.beforeExecute(t, r);

		if (enableMonitoring) {
			Long execId = threadIdCounter.incrementAndGet();
			String execIdString = Long.toString(execId.longValue(), 36);

			if (addThreadContextToNdc) {
				logger.pushContext("executionId={" + execIdString + "}");
				logger.pushContext("threadId={" + t.getName() + "}");
			}

			currentExecutionId.set(execIdString);

			ThreadPoolMonitoring.beforeExecution(threadPoolId, execIdString);
		}
	}

	@Override
	protected void afterExecute(final Runnable r, final Throwable t) {
		super.afterExecute(r, t);

		if (enableMonitoring) {
			String execIdString = currentExecutionId.get();
			currentExecutionId.remove();
			ThreadPoolMonitoring.afterExecution(threadPoolId, execIdString);

			if (addThreadContextToNdc) {
				logger.popContext();
				logger.popContext();
			}
		}
	}

	@Override
	public void allowCoreThreadTimeOut(boolean value) {
		if (value) {
			long aliveTime = super.getKeepAliveTime(TimeUnit.NANOSECONDS);
			if (aliveTime <= 0) {
				super.setKeepAliveTime(1L, TimeUnit.MINUTES);
			}
		}
		super.allowCoreThreadTimeOut(value);
	}

	@Override
	public void postConstruct() {
		if (constructed) {
			return;
		}
		constructed = true;

		if (customThreadFactory) {
			if (threadNamePrefix != null) {
				logger.debug(() -> "Since a custom ThreadFactory has been specified, the threadNamePrefix will not be taken into account.");
			}
		} else {
			ThreadFactory tf = new CountingThreadFactory(threadNamePrefix);
			super.setThreadFactory(tf);
		}

		if (description == null) {
			logger.trace("A thread-pool without description has been created. This is not advisable.");
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
				super.shutdown();
			} else {
				super.shutdownNow();
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

	@Override
	public void setThreadFactory(ThreadFactory threadFactory) {
		super.setThreadFactory(threadFactory);
		if (threadFactory != null) {
			customThreadFactory = true;
		}
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
		BlockingQueue<Runnable> queue = super.getQueue();
		if (queue == null) {
			return 0;
		}
		return queue.size();
	}

	@Override
	public int getThreadPoolSize() {
		return super.getPoolSize();
	}

	@Override
	public int getCoreThreadPoolSize() {
		return super.getCorePoolSize();
	}

	@Override
	public int getMaximumThreadPoolSize() {
		return super.getMaximumPoolSize();
	}

	@Override
	public void execute(Runnable command) {
		super.execute(enrich(command));
	}
	
	private Runnable enrich(Runnable runnable) {
		
		Runnable effectiveRunnable = runnable;
		
		if (enableMonitoring)
			effectiveRunnable = new MonitoringRunnable(runnable, this);
			
		effectiveRunnable = new AttributeContextTransferRunnable(runnable);
		
		if (runnable instanceof Comparable) {
			ComparableDelegationRunnable comparableDelegationRunnable = new ComparableDelegationRunnable((Comparable<Object>)runnable, effectiveRunnable);
			effectiveRunnable = comparableDelegationRunnable;
		}
		
		return effectiveRunnable;
	}
	
	@Override
	public void accept(Instant created, Instant executed, Instant finished) {
		if (created == null || executed == null || finished == null) {
			return;
		}
		Duration enqueued = Duration.between(created, executed);
		Duration executionTime = Duration.between(executed, finished);
		ThreadPoolMonitoring.registerThreadPoolExecution(threadPoolId, enqueued, executionTime);
	}

	
	@Override
	protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
		return preserveComparability(super.newTaskFor(callable), callable);
	}
	
	@Override
	protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
		return preserveComparability(super.newTaskFor(runnable, value), runnable);
	}
	
	private <T> RunnableFuture<T> preserveComparability(RunnableFuture<T> runnableFuture, Object comparableCandidate) {
		if (comparableCandidate instanceof Comparable) {
			return new ComparableDelegationRunnableFuture<T>(runnableFuture, (Comparable<Object>) comparableCandidate);
		}
		
		return runnableFuture;
	}
}
