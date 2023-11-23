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
package com.braintribe.utils.cron;

import static com.cronutils.model.field.expression.FieldExpressionFactory.always;
import static com.cronutils.model.field.expression.FieldExpressionFactory.every;
import static com.cronutils.model.field.expression.FieldExpressionFactory.on;
import static com.cronutils.model.field.expression.FieldExpressionFactory.questionMark;

import com.braintribe.common.lcd.Numbers;
import com.braintribe.utils.DateTools;
import com.cronutils.builder.CronBuilder;
import com.cronutils.model.Cron;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.field.expression.FieldExpression;

public class CronTools {

	/**
	 * Converts a time span (as specified in a time span specification) to a cron expression (compatible with Quartz).
	 * Note that an interval will not be reflected exactly in the cron expression as it is very complex to do. Simple
	 * timespans like full seconds, full minutes, and so will be covered.
	 * 
	 * What will not be covered are, for example, 90s as this will be translated to "0 * * * * ? *" (i.e., every
	 * minute).
	 * 
	 * @param timeSpanSpecification
	 *            The time span to be converted (e.g., 1s, 3min, 2h, 1d,...)
	 * @return A cron expression, compatible with Quartz.
	 * @throws IllegalArgumentException
	 *             Thrown when the interval is a negative number.
	 */
	public static String createCronExpressionFromTimeSpan(String timeSpanSpecification) {
		return createCronExpressionFromTimeSpan(timeSpanSpecification, CronType.QUARTZ);
	}

	/**
	 * Converts a time span (as specified in a time span specification) to a cron expression (compatible with Quartz).
	 * Note that an interval will not be reflected exactly in the cron expression as it is very complex to do. Simple
	 * timespans like full seconds, full minutes, and so will be covered.
	 * 
	 * What will not be covered are, for example, 90s as this will be translated to "0 * * * * ? *" (i.e., every
	 * minute).
	 * 
	 * @param timeSpanSpecification
	 *            The time span to be converted (e.g., 1s, 3min, 2h, 1d,...)
	 * @param cronType
	 *            The type of the cron expression to be returned.
	 * @return A cron expression, compatible with the type specified in the cronType parameter.
	 * @throws IllegalArgumentException
	 *             Thrown when the cronType is null or the interval is a negative number.
	 */
	public static String createCronExpressionFromTimeSpan(String timeSpanSpecification, CronType cronType) {
		long timeSpan = DateTools.parseTimeSpan(timeSpanSpecification, null);
		return createCronExpressionFromTimeSpan(timeSpan, cronType);
	}

	/**
	 * This method converts a time interval (in milliseconds) into cron expression. Note that an interval will not be
	 * reflected exactly in the cron expression as it is very complex to do. Simple timespans like full seconds, full
	 * minutes, and so will be covered.
	 * 
	 * What will not be covered are, for example, 90000 as this will be translated to "0 * * * * ? *" (i.e., every
	 * minute).
	 * 
	 * @param interval
	 *            The interval in milliseconds to bo converted.
	 * @return A cron expression, compatible with Quartz.
	 * @throws IllegalArgumentException
	 *             Thrown when the interval is a negative number.
	 */
	public static String createCronExpressionFromTimeSpan(long interval) {
		return createCronExpressionFromTimeSpan(interval, CronType.QUARTZ);
	}

	/**
	 * This method converts a time interval (in milliseconds) into cron expression. Note that an interval will not be
	 * reflected exactly in the cron expression as it is very complex to do. Simple timespans like full seconds, full
	 * minutes, and so will be covered.
	 * 
	 * What will not be covered are, for example, 90000 as this will be translated to "0 * * * * ? *" (i.e., every
	 * minute).
	 * 
	 * @param interval
	 *            The interval in milliseconds to bo converted.
	 * @param cronType
	 *            The type of the cron expression to be returned.
	 * @return A cron expression, compatible with the type specified in the cronType parameter.
	 * @throws IllegalArgumentException
	 *             Thrown when the cronType is null or the interval is a negative number.
	 */
	public static String createCronExpressionFromTimeSpan(long interval, CronType cronType) {

		if (cronType == null) {
			throw new IllegalArgumentException("The CronType must not be null.");
		}
		if (interval < 0) {
			throw new IllegalArgumentException("The interval " + interval + " must be a positive number or 0.");
		}

		switch (cronType) {
			case CRON4J:
				return createCron4jExpression(interval);
			case QUARTZ:
				return createQuartzExpression(interval);
			default:
				throw new IllegalStateException("Unsupported cron type " + cronType);

		}
	}

	private static String createQuartzExpression(long interval) {

		FieldExpression year = always();
		FieldExpression month = always();
		FieldExpression day = always();
		FieldExpression hour = always();
		FieldExpression minute = always();
		FieldExpression second = always();

		long msPerMonth = Numbers.MILLISECONDS_PER_DAY * 30l;

		if (interval >= Numbers.MILLISECONDS_PER_YEAR) {
			year = every((int) (interval / Numbers.MILLISECONDS_PER_YEAR));
			month = on(1);
			day = on(1);
			hour = on(0);
			minute = on(0);
			second = on(0);
		} else if (interval >= msPerMonth) {
			month = every((int) (interval / msPerMonth));
			day = on(1);
			hour = on(0);
			minute = on(0);
			second = on(0);
		} else if (interval >= Numbers.MILLISECONDS_PER_DAY) {
			day = every((int) (interval / Numbers.MILLISECONDS_PER_DAY));
			hour = on(0);
			minute = on(0);
			second = on(0);
		} else if (interval >= Numbers.MILLISECONDS_PER_HOUR) {
			hour = every((int) (interval / Numbers.MILLISECONDS_PER_HOUR));
			minute = on(0);
			second = on(0);
		} else if (interval >= Numbers.MILLISECONDS_PER_MINUTE) {
			minute = every((int) (interval / Numbers.MILLISECONDS_PER_MINUTE));
			second = on(0);
		} else if (interval >= Numbers.MILLISECONDS_PER_SECOND) {
			second = every((int) (interval / Numbers.MILLISECONDS_PER_SECOND));
		}

		//@formatter:off
		Cron cron = CronBuilder.cron(CronDefinitionBuilder.instanceDefinitionFor(com.cronutils.model.CronType.QUARTZ))
			    .withYear(year)
			    .withDoM(day)
			    .withMonth(month)
			    .withDoW(questionMark())
			    .withHour(hour)
			    .withMinute(minute)
			    .withSecond(second)
			    .instance();
		//@formatter:on

		String cronAsString = cron.asString();
		return cronAsString;
	}

	private static String createCron4jExpression(long interval) {

		FieldExpression month = always();
		FieldExpression day = always();
		FieldExpression hour = always();
		FieldExpression minute = always();

		long msPerMonth = Numbers.MILLISECONDS_PER_DAY * 30l;

		if (interval >= msPerMonth) {
			int months = (int) (interval / msPerMonth);
			if (months >= 12) {
				months = 1;
			}
			month = every(months);
			day = on(1);
			hour = on(0);
			minute = on(0);
		} else if (interval >= Numbers.MILLISECONDS_PER_DAY) {
			day = every((int) (interval / Numbers.MILLISECONDS_PER_DAY));
			hour = on(0);
			minute = on(0);
		} else if (interval >= Numbers.MILLISECONDS_PER_HOUR) {
			hour = every((int) (interval / Numbers.MILLISECONDS_PER_HOUR));
			minute = on(0);
		} else if (interval >= Numbers.MILLISECONDS_PER_MINUTE) {
			minute = every((int) (interval / Numbers.MILLISECONDS_PER_MINUTE));
		} else if (interval >= Numbers.MILLISECONDS_PER_SECOND) {
			minute = every(1);
		}

		//@formatter:off
		Cron cron = CronBuilder.cron(CronDefinitionBuilder.instanceDefinitionFor(com.cronutils.model.CronType.CRON4J))
			    .withDoM(day)
			    .withMonth(month)
			    .withHour(hour)
			    .withMinute(minute)
			    .withDoW(always())
			    .instance();
		//@formatter:on

		String cronAsString = cron.asString();
		return cronAsString;
	}

}
