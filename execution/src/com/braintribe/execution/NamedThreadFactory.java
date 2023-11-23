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

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This minor variation of the {@link Executors.DefaultThreadFactory} does exactly the same but tries - in addition - to
 * find a meaningful name for the thread (for improved debugging and analytics).
 * 
 * @author roman.kurmanowytsch
 */
public class NamedThreadFactory implements ThreadFactory {

	protected boolean doLogging = false;

	static final AtomicInteger poolNumber = new AtomicInteger(1);
	final AtomicInteger threadNumber = new AtomicInteger(1);
	private String namePrefix;

	private int priority = Thread.NORM_PRIORITY;

	public NamedThreadFactory() {
		this.namePrefix = "pool-" + poolNumber.getAndIncrement() + "-thread-";
	}

	@Override
	public Thread newThread(final Runnable r) {

		final Thread t = Thread.ofVirtual().name(this.namePrefix + this.threadNumber.getAndIncrement()).unstarted(r);
		if (t.getPriority() != this.priority) {
			t.setPriority(this.priority);
		}
		return t;
	}

	public boolean isDoLogging() {
		return this.doLogging;
	}

	public void setDoLogging(final boolean doLogging) {
		this.doLogging = doLogging;
	}
	public void setNamePrefix(String namePrefix) {
		if (namePrefix == null || namePrefix.isEmpty()) {
			return;
		}
		if (!namePrefix.endsWith("-")) {
			namePrefix = namePrefix + "-";
		}
		this.namePrefix = namePrefix;
	}
	public void setPriority(int priority) {
		this.priority = priority;
	}

}
