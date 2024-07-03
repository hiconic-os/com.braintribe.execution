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
package com.braintribe.execution.monitoring;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import com.braintribe.common.lcd.Numbers;
import com.braintribe.logging.Logger;

public class StandardThreadPoolStatistics implements ThreadPoolStatistics {

	private static Logger logger = Logger.getLogger(StandardThreadPoolStatistics.class);

	private String threadPoolId;
	private MonitoredThreadPool extendedThreadPoolExecutor;
	private ConcurrentHashMap<String, ThreadPoolExecution> currentExecutions = new ConcurrentHashMap<>();
	private AtomicLong executionCount = new AtomicLong(0);

	private AtomicLong executedCount = new AtomicLong(0);
	private AtomicLong totalExecutionTimeInMs = new AtomicLong(0);

	private long latestExecution = -1L;
	private boolean scheduledThreadPool = false;

	private ReentrantLock durationLock = new ReentrantLock();
	private Duration enqueuedDuration = Duration.of(0, ChronoUnit.MILLIS);
	private Duration executionDuration = Duration.of(0, ChronoUnit.MILLIS);
	private Duration maxEnqueuedDuration = null;
	private Duration minEnqueuedDuration = null;
	private long durationCount = 0l;

	public StandardThreadPoolStatistics(String threadPoolId, MonitoredThreadPool extendedThreadPoolExecutor) {
		this.threadPoolId = threadPoolId;
		this.extendedThreadPoolExecutor = extendedThreadPoolExecutor;

	}

	protected void beforeExecution(String execIdString) {
		currentExecutions.put(execIdString, new ThreadPoolExecution());
		executionCount.incrementAndGet();
		latestExecution = System.currentTimeMillis();
	}

	protected void afterExecution(String execIdString) {
		ThreadPoolExecution execution = currentExecutions.remove(execIdString);
		if (execution != null) {
			executedCount.incrementAndGet();
			execution.stopped();
			Duration executionTime = execution.getExecutionTime();
			totalExecutionTimeInMs.addAndGet(executionTime.toMillis());
		}
	}

	public void registerThreadPoolExecution(Duration enqueued, Duration execution) {
		durationLock.lock();
		try {
			durationCount++;
			enqueuedDuration = enqueuedDuration.plus(enqueued);
			executionDuration = executionDuration.plus(execution);

			if (maxEnqueuedDuration == null || maxEnqueuedDuration.compareTo(enqueued) < 0) {
				maxEnqueuedDuration = enqueued;
			}
			if (minEnqueuedDuration == null || minEnqueuedDuration.compareTo(enqueued) > 0) {
				minEnqueuedDuration = enqueued;
			}
		} catch (Exception e) {
			logger.warn(() -> "Error while trying to monitor thread pool " + threadPoolId, e);
		} finally {
			durationLock.unlock();
		}
	}

	@Override
	public int currentlyRunning() {
		return currentExecutions.size();
	}

	@Override
	public long totalExecutions() {
		return executionCount.get();
	}

	@Override
	public long averageRunningTimeInMs() {
		long totalTime = totalExecutionTimeInMs.get();
		long amount = executedCount.get();
		if (amount <= 0) {
			// Division by Zero or overflow
			return 0;
		}
		return totalTime / amount;
	}

	@Override
	public String getThreadPoolId() {
		return threadPoolId;
	}

	@Override
	public int getPendingTasksInQueue() {
		return extendedThreadPoolExecutor.getPendingTasksInQueue();
	}

	@Override
	public long timeSinceLastExecutionInMs() {
		if (latestExecution == -1L) {
			return -1L;
		}
		return System.currentTimeMillis() - latestExecution;
	}

	@Override
	public int getPoolSize() {
		return extendedThreadPoolExecutor.getThreadPoolSize();
	}

	@Override
	public int getCorePoolSize() {
		return extendedThreadPoolExecutor.getCoreThreadPoolSize();
	}

	@Override
	public int getMaximumPoolSize() {
		return extendedThreadPoolExecutor.getMaximumThreadPoolSize();
	}

	@Override
	public String getDescription() {
		return extendedThreadPoolExecutor.getDescription();
	}

	@Override
	public boolean isScheduledThreadPool() {
		return scheduledThreadPool;
	}

	public void setScheduledThreadPool(boolean scheduledThreadPool) {
		this.scheduledThreadPool = scheduledThreadPool;
	}

	@Override
	public Long getMaximumEnqueuedTimeInMs() {
		if (maxEnqueuedDuration == null) {
			return null;
		}
		return maxEnqueuedDuration.toMillis();
	}

	@Override
	public Long getMinimumEnqueuedTimeInMs() {
		if (minEnqueuedDuration == null) {
			return null;
		}
		return minEnqueuedDuration.toMillis();
	}

	@Override
	public Double getAverageEnqueuedTimeInMs() {
		if (durationCount == 0) {
			return -1d;
		}
		try {
			double totalNanos = enqueuedDuration.toNanos();
			double avgNanos = totalNanos / durationCount;
			double avgMs = avgNanos / Numbers.NANOSECONDS_PER_MILLISECOND;
			return avgMs;
		} catch (ArithmeticException ae) {
			try {
				double totalMs = enqueuedDuration.toMillis();
				double avg = totalMs / durationCount;
				return avg;
			} catch (ArithmeticException ae2) {
				// too large for ms
				double totalMinutes = enqueuedDuration.toMinutes();
				double avgMin = totalMinutes / durationCount;
				double avgMs = avgMin * Numbers.MILLISECONDS_PER_MINUTE;
				return avgMs;
			}
		}
	}

	/* enqueuedDuration = enqueuedDuration.plus(enqueued); executionDuration = executionDuration.plus(execution);
	 * 
	 * if (maxEnqueuedDuration == null || maxEnqueuedDuration.compareTo(enqueued) < 0) { maxEnqueuedDuration = enqueued;
	 * } if (minEnqueuedDuration == null || minEnqueuedDuration.compareTo(enqueued) > 0) { minEnqueuedDuration =
	 * enqueued; } */
}
