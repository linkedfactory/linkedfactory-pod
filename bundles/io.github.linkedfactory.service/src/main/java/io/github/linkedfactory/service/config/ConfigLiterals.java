package io.github.linkedfactory.service.config;

import net.enilink.komma.core.ILiteral;

import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.format.DateTimeParseException;

import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

/**
 * Parses the values of settings in the bundle configuration.
 */
final class ConfigLiterals {
	private ConfigLiterals() {
	}

	/**
	 * Parses an ISO 8601 duration such as {@code P7D}, or a number of milliseconds.
	 *
	 * @return the duration, or {@code null} if the literal is missing or invalid
	 */
	static Duration duration(ILiteral literal) {
		if (literal == null) {
			return null;
		}
		try {
			return Duration.ofMillis(Long.parseLong(literal.getLabel()));
		} catch (NumberFormatException nfe) {
			try {
				return Duration.parse(literal.getLabel());
			} catch (DateTimeParseException dtpe) {
				return null;
			}
		}
	}

	/**
	 * Like {@link #duration(ILiteral)}, but rejects an invalid literal.
	 *
	 * @param setting the name of the setting, for the error message
	 */
	static Duration requireDuration(ILiteral literal, String setting) {
		Duration duration = duration(literal);
		if (literal != null && duration == null) {
			throw new IllegalArgumentException("Invalid duration for " + setting + ": " + literal.getLabel());
		}
		return duration;
	}

	/**
	 * Parses a number of bytes that must not be negative.
	 *
	 * @param setting the name of the setting, for the error message
	 * @return the number, or {@code defaultValue} if the literal is missing
	 */
	static long bytes(ILiteral literal, String setting, long defaultValue) {
		if (literal == null) {
			return defaultValue;
		}
		try {
			long bytes = Long.parseLong(literal.getLabel().trim());
			if (bytes >= 0) {
				return bytes;
			}
		} catch (NumberFormatException e) {
			// reported below
		}
		throw new IllegalArgumentException("Invalid number of bytes for " + setting + ": " + literal.getLabel());
	}

	/**
	 * Parses settings in the format of Java properties files, one {@code key=value} per line.
	 * Trailing whitespace of values is removed.
	 *
	 * @return the settings sorted by key, empty if the text is {@code null}
	 */
	static Map<String, String> properties(String text) {
		Map<String, String> result = new TreeMap<>();
		if (text == null) {
			return result;
		}
		Properties properties = new Properties();
		try {
			properties.load(new StringReader(text));
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
		properties.stringPropertyNames().forEach(key -> result.put(key, properties.getProperty(key).trim()));
		return result;
	}
}
