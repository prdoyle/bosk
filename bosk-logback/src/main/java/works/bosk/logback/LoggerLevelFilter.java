package works.bosk.logback;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import static ch.qos.logback.core.spi.FilterReply.DENY;
import static ch.qos.logback.core.spi.FilterReply.NEUTRAL;

/**
 * Simple name-and-level filter configured by XML {@code <logger name="..." level="..."/>}
 * elements that deliberately have the same appearance and meaning as the actual
 * logback logger configs.
 * <p>
 * Events to be filtered out return {@code DENY}; otherwise {@code NEUTRAL}.
 * <p>
 * This class intentionally does not call {@link ILoggingEvent#getMDCPropertyMap()}
 * to avoid the overhead in the case that replay never happens.
 * <p>
 * It seems tragic that we need to implement all this just to offer the same
 * log filtering that Logback already has built-in, but if there's a better alternative,
 * we haven't found it yet.
 */
public class LoggerLevelFilter extends Filter<ILoggingEvent> {
	private final List<LoggerConfig> loggerConfigs = new ArrayList<>();

	/**
	 * Logger level overrides keyed by logger name prefix
	 */
	private NavigableMap<String, Level> loggerLevels;

	/**
	 * Called by Joran when a nested &lt;logger/&gt; element is encountered.
	 * Can't do validation yet because Joran uses old-fashioned
	 * bean-style initialization where objects are initially wrong
	 * and are mutated until fully formed.
	 */
	public void addLogger(LoggerConfig loggerConfig) {
		if (loggerConfig == null) {
			return;
		}
		loggerConfigs.add(loggerConfig);
	}

	@Override
	public void start() {
		// "Compile" the config into immutable spec objects for runtime use.
		// At this stage, Joran configuration is done, so we can validate the config.
		NavigableMap<String, Level> map = new java.util.TreeMap<>();
		for (LoggerConfig r : loggerConfigs) {
			map.put(r.getLoggerNamePrefix(), r.getLevel());
		}
		this.loggerLevels = Collections.unmodifiableNavigableMap(map);
		super.start();
	}

	@Override
	public FilterReply decide(ILoggingEvent event) {
		if (event == null) {
			return NEUTRAL;
		}
		String loggerName = event.getLoggerName();
		if (loggerName == null) {
			return NEUTRAL;
		}
		NavigableMap<String, Level> rules = this.loggerLevels;
		if (rules == null || rules.isEmpty()) {
			return NEUTRAL;
		}

		String candidate = rules.floorKey(loggerName);
		if (candidate == null) {
			// No rules could possibly match our logger
			return NEUTRAL;
		}

		Level matched;
		if (loggerName.equals(candidate) || loggerName.startsWith(candidate + '.')) {
			matched = rules.get(candidate);
		} else {
			// This can happen if we have a confounding logger override between loggerName and its parent,
			// like if there's an override for "abc" and "abc.def", and we're looking up "abc.ghi".
			// In that case, the map lookup lands on "abc.def" which is unrelated to loggerName and
			// we must explicitly try prefixes.
			//
			// This is not very fast. If it happens often enough to be a problem, we may need to
			// consider an alternative algorithm/data structure like a trie or just memoizing
			// the results by loggerName.
			matched = getLevelByPrefix(loggerName, rules);
		}

		if (matched == null) {
			return NEUTRAL;
		}

		return event.getLevel().isGreaterOrEqual(matched) ? NEUTRAL : DENY;
	}

	private static @Nullable Level getLevelByPrefix(String loggerName, NavigableMap<String, Level> rules) {
		for (
			int idx = loggerName.lastIndexOf('.');
			idx != -1;
			idx = loggerName.lastIndexOf('.', idx - 1)
		) {
			Level candidate = rules.get(loggerName.substring(0, idx));
			if (candidate != null) {
				return candidate;
			}
		}
		return null;
	}

	/**
	 * Represents one {@code <logger>} element from the logback config.
	 */
	public static class LoggerConfig {
		private String name;
		private String level;

		// Called by Joran
		public void setName(String name) { this.name = name; }
		public void setLevel(String level) { this.level = level; }

		/**
		 * @return validated logger name prefix
		 * @throws IllegalArgumentException if the name field is invalid
		 */
		@NonNull String getLoggerNamePrefix() {
			String name = this.name;
			if (name == null) {
				throw new IllegalArgumentException("LoggerConfig name must not be null");
			}
			String loggerNamePrefix = name.trim();
			if (loggerNamePrefix.isEmpty()) {
				throw new IllegalArgumentException("LoggerConfig name must not be empty");
			}

			// Basic format validation enforced here so decide() can rely on
			// simple lexicographic reasoning. Allowed characters are letters,
			// digits, underscore and dot. Names must not start/end with '.',
			// and must not contain empty segments (".."). Invalid entries
			// are ignored.
			if (loggerNamePrefix.startsWith(".") || loggerNamePrefix.endsWith(".")) {
				throw new IllegalArgumentException("LoggerConfig name must not start or end with '.'");
			}
			if (loggerNamePrefix.contains("..")) {
				throw new IllegalArgumentException("LoggerConfig name must not contain empty segments (i.e. '..')");
			}
			if (!loggerNamePrefix.matches("[A-Za-z0-9_.]+")) {
				// Reject names containing characters (for example '$') that
				// would break the simple lexicographic assumptions used in
				// decide().
				throw new IllegalArgumentException("LoggerConfig name contains invalid characters. Allowed characters are letters, digits, underscore and dot.");
			}
			return loggerNamePrefix;
		}

		/**
		 * @return validated level
		 * @throws IllegalArgumentException if the level field is not valid
		 */
		@NonNull Level getLevel() {
			String levelName = this.level;
			if (levelName == null || levelName.isBlank()) {
				throw new IllegalArgumentException("LoggerConfig level must not be null or blank");
			}
			Level level = Level.toLevel(levelName, null);
			if (level == null) {
				throw new IllegalArgumentException("LoggerConfig level must be a valid logback level name");
			}
			return level;
		}
	}

}
