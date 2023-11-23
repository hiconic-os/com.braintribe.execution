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
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
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
import com.braintribe.logging.Logger;
import com.braintribe.utils.StringTools;
import com.braintribe.utils.date.NanoClock;

public class ExtendedScheduledThreadPoolExecutor extends ScheduledThreadPoolExecutor
		implements LifecycleAware, MonitoredThreadPool, ExecutionMonitoring {

	private static Logger logger = Logger.getLogger(ExtendedScheduledThreadPoolExecutor.class);

	private boolean addThreadContextToNdc = true;
	private boolean enableMonitoring = true;

	private final String threadPoolId = UUID.randomUUID().toString();
	private String description;

	private static AtomicLong threadIdCounter = new AtomicLong(0);
	private final ThreadLocal<String> currentExecutionId = new ThreadLocal<>();

	private boolean waitForTasksToCompleteOnShutdown = true;

	public ExtendedScheduledThreadPoolExecutor(int corePoolSize, ThreadFactory threadFactory, RejectedExecutionHandler handler) {
		super(corePoolSize, threadFactory, handler);
	}
	public ExtendedScheduledThreadPoolExecutor(int corePoolSize, ThreadFactory threadFactory) {
		super(corePoolSize, threadFactory);
	}
	public ExtendedScheduledThreadPoolExecutor(int corePoolSize) {
		super(corePoolSize, Thread.ofVirtual().factory());
	}

	@Override
	public void postConstruct() {
		if (description == null) {
			logger.trace("A scheduled thread-pool without description has been created. This is not advisable.");
			description = "anonymous-" + threadPoolId;
		}
		ThreadPoolMonitoring.registerThreadPool(threadPoolId, this);

		logger.debug(() -> "Constructed thread pool " + getIdentification());
	}

	@Override
	public void preDestroy() {
		String identification = getIdentification();

		logger.debug(() -> "Shutting down schedules thread pool " + identification + " (waitForTasksToCompleteOnShutdown: "
				+ waitForTasksToCompleteOnShutdown + ")");
		Instant start = NanoClock.INSTANCE.instant();

		try {
			if (waitForTasksToCompleteOnShutdown) {
				super.shutdown();
			} else {
				super.shutdownNow();
			}
		} finally {
			ThreadPoolMonitoring.unregisterThreadPool(threadPoolId);

			logger.debug(
					() -> "Shutting down scheduled thread pool " + identification + " took " + StringTools.prettyPrintDuration(start, true, null));
		}
	}

	private String getIdentification() {
		if (description == null) {
			return threadPoolId;
		}
		return threadPoolId.concat(" (").concat(description).concat(")");
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

	@Configurable
	public void setAddThreadContextToNdc(boolean addThreadContextToNdc) {
		this.addThreadContextToNdc = addThreadContextToNdc;
	}
	@Configurable
	public void setEnableMonitoring(boolean enableMonitoring) {
		this.enableMonitoring = enableMonitoring;
	}
	@Configurable
	public void setWaitForTasksToCompleteOnShutdown(boolean waitForTasksToCompleteOnShutdown) {
		this.waitForTasksToCompleteOnShutdown = waitForTasksToCompleteOnShutdown;
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
			ComparableDelegationRunnable comparableDelegationRunnable = new ComparableDelegationRunnable((Comparable<Object>) runnable,
					effectiveRunnable);
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

}
