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

import com.braintribe.cfg.Configurable;
import com.braintribe.logging.Logger;

public abstract class FixedIntervalErrorHandler<T> implements ErrorHandler<T> {

	protected static Logger logger = Logger.getLogger(FixedIntervalErrorHandler.class);

	protected long timeout = 5000L;

	@Override
	public void reset(T context) {
		//Nothing to do here
	}

	@Override
	public void handleError(Throwable t, T context) {
		String contextInformation = this.getContextInformation(context);
		try {
			logger.debug(contextInformation+": Because of an error, we wait "+this.timeout+" ms before retrying.");
			synchronized (this) {
				wait(this.timeout);
			}
		} catch(Exception e) {
			logger.error(contextInformation+": Error while waiting for the next retry.", e);
		}
	}

	@Override
	public boolean shouldExecute(T context) {
		return true;
	}

	protected abstract String getContextInformation(T context);
	
	public long getTimeout() {
		return timeout;
	}
	@Configurable
	public void setTimeout(long timeout) {
		this.timeout = timeout;
	}

}
