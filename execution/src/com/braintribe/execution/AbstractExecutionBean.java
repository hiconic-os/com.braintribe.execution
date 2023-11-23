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

import com.braintribe.cfg.DestructionAware;
import com.braintribe.logging.Logger;

public abstract class AbstractExecutionBean implements NamedCallable<Void>, DestructionAware {

	private static Logger logger = Logger.getLogger(AbstractExecutionBean.class);

	private long interval = 86400000L; // every day
	private boolean stop = false;

	// **************************************************************************
	// Constructor.
	// **************************************************************************

	/**
	 * Default constructor.
	 */
	public AbstractExecutionBean() {

	}

	// **************************************************************************
	// Getter/Setter.
	// **************************************************************************

	public void setInterval(final long interval) {
		this.interval = interval;
	}

	public long getInterval() {
		return this.interval;
	}

	public void setStop(final boolean stop) {
		this.stop = stop;
	}

	public boolean isStop() {
		return this.stop;
	}

	// **************************************************************************
	// Interface Methods.
	// **************************************************************************
	/**
	 * @see java.util.concurrent.Callable#call()
	 */
	@Override
	public Void call() throws Exception {

		while (!this.stop) {

			try {
				execute();
			} catch (final Throwable e) {
				logger.warn(String.format("error while doing conversion: %s", e.getMessage()), e);
			}

			this.waitInterval();
		}

		return null;
	}

	public abstract void execute() throws Exception;

	/**
	 * Waits for the configured hartbeat.
	 */
	protected void waitInterval() {
		final long intervalPerLoop = 1000L;
		final long loops = this.interval / intervalPerLoop;
		try {
			for (long i = 0; i < loops && !this.stop; ++i) {
				synchronized (this) {
					wait(intervalPerLoop);
				}
			}
		} catch (final Exception ignore) {
			//ignored
		}
	}

	/**
	 * Stops the infinite loop.
	 */
	@Override
	public void preDestroy() {
		this.stop = true;
	}

}
