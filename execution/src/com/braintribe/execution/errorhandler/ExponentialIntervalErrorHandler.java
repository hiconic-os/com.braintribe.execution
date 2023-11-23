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
package com.braintribe.execution.errorhandler;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.braintribe.cfg.Configurable;
import com.braintribe.logging.Logger;

public abstract class ExponentialIntervalErrorHandler <T> implements ErrorHandler<T> {

	protected static Logger logger = Logger.getLogger(ExponentialIntervalErrorHandler.class);

	protected long initialWaitTime = 5000L;
	protected long multiplier = 2L;
	protected long maximumWaitTime = 3600000L; // 1 hour
	protected boolean executeDelay = true;

	protected Map<String,ContextSettings> contextSettings = new HashMap<String,ContextSettings>();
	public final static String DEFAULT_CONTEXT = "";

	@Override
	public void reset(T context) {

		ContextSettings contextSettings = this.getContextSettings(context);

		if (contextSettings.currentWaitTime !=  this.initialWaitTime) {
			String loggingInformation = this.getLoggingInformation(context);
			logger.debug(loggingInformation+": Resetting the current wait time from "+contextSettings.currentWaitTime+" ms to the initial wait time "+this.initialWaitTime+" ms.");
			contextSettings.currentWaitTime = this.initialWaitTime;
			contextSettings.nextExecutionAfter = -1;
		}
	}

	@Override
	public void handleError(Throwable t, T context) {

		ContextSettings contextSettings = this.getContextSettings(context);

		String loggingInformation = this.getLoggingInformation(context);
		if (contextSettings.currentWaitTime <= 0) {
			logger.debug(loggingInformation+": We have a negative or zero wait time ("+contextSettings.currentWaitTime+"). Not waiting.");
			return;
		}
		if (this.executeDelay) {
			try {
				logger.debug(loggingInformation+": Because of an error, we wait "+contextSettings.currentWaitTime+" ms before retrying.");
				synchronized (this) {
					wait(contextSettings.currentWaitTime);
				}
			} catch(Exception e) {
				logger.error(loggingInformation+": Error while waiting for the next retry.", e);
			}
		} else {
			logger.debug(loggingInformation+": Not executing delay of "+contextSettings.currentWaitTime+" ms");
		}
		contextSettings.currentWaitTime *= this.multiplier;

		//Check whether we had an overflow or reached the maximum wait time
		if ((contextSettings.currentWaitTime < 0) || (contextSettings.currentWaitTime > this.maximumWaitTime)) {
			contextSettings.currentWaitTime = this.maximumWaitTime;
		}

		contextSettings.nextExecutionAfter = System.currentTimeMillis() + contextSettings.currentWaitTime;

		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd-HHmmssSSS");
		Date execDate = new Date(contextSettings.nextExecutionAfter);
		String execDateString = sdf.format(execDate);

		logger.debug(loggingInformation+": Increased the wait time to "+contextSettings.currentWaitTime+" ms; next execution will be after "+execDateString);
	}

	@Override
	public boolean shouldExecute(T context) {

		ContextSettings contextSettings = this.getContextSettings(context);

		long now = System.currentTimeMillis();

		if ((contextSettings.nextExecutionAfter <= 0) || (contextSettings.nextExecutionAfter > now)) {
			return true;
		}
		return false;
	}

	protected abstract String getContextKey(T context);
	protected abstract String getLoggingInformation(T context);

	protected ContextSettings getContextSettings(T context) {
		String key = this.getContextKey(context);
		if (key == null) {
			key = DEFAULT_CONTEXT;
		}
		ContextSettings contextSettings = this.contextSettings.get(key);
		if (contextSettings == null) {
			contextSettings = new ContextSettings();
			contextSettings.name = key;
			contextSettings.currentWaitTime = this.initialWaitTime;
			this.contextSettings.put(key, contextSettings);
		}
		return contextSettings;
	}

	public long getInitialWaitTime() {
		return initialWaitTime;
	}
	@Configurable
	public void setInitialWaitTime(long initialWaitTime) {
		this.initialWaitTime = initialWaitTime;
	}

	public long getMultiplier() {
		return multiplier;
	}
	@Configurable
	public void setMultiplier(long multiplier) {
		this.multiplier = multiplier;
	}

	public long getMaximumWaitTime() {
		return maximumWaitTime;
	}
	@Configurable
	public void setMaximumWaitTime(long maximumWaitTime) {
		this.maximumWaitTime = maximumWaitTime;
	}

	public boolean isExecuteDelay() {
		return executeDelay;
	}
	@Configurable
	public void setExecuteDelay(boolean executeDelay) {
		this.executeDelay = executeDelay;
	}

}
