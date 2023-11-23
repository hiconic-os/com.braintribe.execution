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
package com.braintribe.execution.monitoring;

import java.time.Instant;

import com.braintribe.utils.date.NanoClock;

public class MonitoringRunnable implements Runnable {

	private Runnable delegate;
	private Instant creationInstant;
	private ExecutionMonitoring monitoring;

	public MonitoringRunnable(Runnable delegate, ExecutionMonitoring monitoring) {
		this.monitoring = monitoring;
		this.creationInstant = NanoClock.INSTANCE.instant();
		this.delegate = delegate;
	}

	@Override
	public void run() {
		Instant executionInstant = NanoClock.INSTANCE.instant();
		try {
			this.delegate.run();
		} finally {
			Instant finishedInstant = NanoClock.INSTANCE.instant();
			monitoring.accept(creationInstant, executionInstant, finishedInstant);
		}
	}
}
