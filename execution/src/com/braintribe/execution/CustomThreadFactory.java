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
