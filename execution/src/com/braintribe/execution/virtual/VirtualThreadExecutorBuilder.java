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

public class VirtualThreadExecutorBuilder {

	private Integer concurrency = null;
	private Boolean waitForTasksToCompleteOnShutdown = null;
	private String threadNamePrefix = null;
	private Boolean addThreadContextToNdc = null;
	private String description = null;
	private boolean monitoring;

	private VirtualThreadExecutorBuilder() {
		//
	}

	public static VirtualThreadExecutorBuilder newPool() {
		return new VirtualThreadExecutorBuilder();
	}

	public VirtualThreadExecutorBuilder concurrency(int concurrency) {
		this.concurrency = concurrency;
		return this;
	}

	public VirtualThreadExecutorBuilder waitForTasksToCompleteOnShutdown(boolean waitForTasksToCompleteOnShutdown) {
		this.waitForTasksToCompleteOnShutdown = waitForTasksToCompleteOnShutdown;
		return this;
	}

	public VirtualThreadExecutorBuilder threadNamePrefix(String threadNamePrefix) {
		this.threadNamePrefix = threadNamePrefix;
		return this;
	}

	public VirtualThreadExecutorBuilder setAddThreadContextToNdc(boolean addThreadContextToNdc) {
		this.addThreadContextToNdc = addThreadContextToNdc;
		return this;
	}

	public VirtualThreadExecutorBuilder description(String desc) {
		this.description = desc;
		return this;
	}

	public VirtualThreadExecutorBuilder monitoring(boolean monitoring) {
		this.monitoring = monitoring;
		return this;
	}

	public VirtualThreadExecutor build() {
		if (concurrency == null) {
			throw new IllegalArgumentException("Concurrency " + concurrency + " is not set");
		}

		VirtualThreadExecutor result = new VirtualThreadExecutor(concurrency);

		applyConfiguration(result);

		return result;
	}

	private void applyConfiguration(VirtualThreadExecutor result) {
		if (waitForTasksToCompleteOnShutdown != null) {
			result.setWaitForTasksToCompleteOnShutdown(waitForTasksToCompleteOnShutdown);
		}
		if (threadNamePrefix != null) {
			result.setThreadNamePrefix(threadNamePrefix);
		}
		if (addThreadContextToNdc != null) {
			result.setAddThreadContextToNdc(addThreadContextToNdc);
		}
		result.setDescription(description);
		result.setEnableMonitoring(monitoring);

		result.postConstruct();
	}
}
