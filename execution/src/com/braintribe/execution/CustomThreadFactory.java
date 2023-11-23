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

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>
 * A simple fluently-configurable {@link ThreadFactory}.
 * 
 * <p>
 * Example:
 * 
 * <pre>
 * {@code
 * ThreadFactory factory = CustomThreadFactory.create().namePrefix("myThread-").daemon(true).priority(Thread.MIN_PRIORITY);
 * }
 * </pre>
 * 
 */
public class CustomThreadFactory implements ThreadFactory {

	private final AtomicInteger threadNumber = new AtomicInteger(1);
	private String namePrefix;
	private boolean daemon;
	private int priority = Thread.NORM_PRIORITY;

	public static CustomThreadFactory create() {
		return new CustomThreadFactory();
	}

	public CustomThreadFactory group(ThreadGroup group) {
		return this;
	}

	public CustomThreadFactory namePrefix(String namePrefix) {
		this.namePrefix = namePrefix;
		return this;
	}

	public CustomThreadFactory daemon(boolean daemon) {
		this.daemon = daemon;
		return this;
	}

	public CustomThreadFactory priority(int priority) {
		this.priority = priority;
		return this;
	}

	@Override
	public Thread newThread(Runnable r) {

		Thread t = Thread.ofVirtual().unstarted(r);
		t.setName(namePrefix + threadNumber.getAndIncrement());

		if (t.getPriority() != priority) {
			t.setPriority(priority);
		}

		return t;

	}

}
