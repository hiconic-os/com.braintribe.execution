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

import com.braintribe.cfg.Configurable;

public class CountingThreadFactory implements ThreadFactory {

	private static final AtomicInteger poolNumber = new AtomicInteger(1);

	private final ThreadGroup group;
	private final String namePrefix;
	private final AtomicInteger threadNumber = new AtomicInteger(1);

	private ExtendedThreadFactory factory = (group, runnable, name) -> {
		Thread t = Thread.ofVirtual().unstarted(runnable);
		t.setName(name);
		return t;
	};

	public CountingThreadFactory(String prefix) {
		this.group = defaultGroup();
		this.namePrefix = namePrefix(prefix);
	}

	private static String namePrefix(String prefix) {
		if (prefix == null || prefix.trim().length() == 0)
			prefix = "pool";
		return prefix + "-" + poolNumber.getAndIncrement() + "-thread-";
	}

	private static ThreadGroup defaultGroup() {
		return Thread.currentThread().getThreadGroup();
	}

	@Configurable
	public void setExtendedThreadFactory(ExtendedThreadFactory factory) {
		this.factory = factory;
	}

	@Override
	public Thread newThread(Runnable r) {
		Thread t = factory.newThread(group, r, namePrefix + threadNumber.getAndIncrement());

		if (t.getPriority() != Thread.NORM_PRIORITY)
			t.setPriority(Thread.NORM_PRIORITY);

		return t;
	}
}
