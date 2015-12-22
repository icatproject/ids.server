package org.icatproject.ids;

import java.io.File;

import javax.annotation.PostConstruct;
import javax.ejb.Singleton;
import javax.ejb.Startup;

import org.icatproject.utils.CheckedProperties;
import org.icatproject.utils.CheckedProperties.CheckedPropertyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.Context;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.util.StatusPrinter;

@Singleton
@Startup
public class LoggingConfigurator {

	@PostConstruct
	private void init() {
		try {
			CheckedProperties props = new CheckedProperties();
			props.loadFromFile("ids.properties");

			File f = null;
			if (props.has("logback.xml")) {
				f = props.getFile("logback.xml");
				if (!f.exists()) {
					String msg = "logback.xml file " + f.getAbsolutePath() + " specified in " + f + " not found";
					throw new IllegalStateException(msg);
				}
				LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
				try {
					JoranConfigurator configurator = new JoranConfigurator();
					configurator.setContext((Context) LoggerFactory.getILoggerFactory());
					context.reset();
					configurator.doConfigure(f);
				} catch (JoranException je) {
					// StatusPrinter will handle this
				}
				StatusPrinter.printInCaseOfErrorsOrWarnings(context);
			}

			Logger logger = LoggerFactory.getLogger(LoggingConfigurator.class);
			if (f != null) {
				logger.info("Logging configuration read from " + f);
			} else {
				logger.info("Using logback default configuration");
			}
		} catch (CheckedPropertyException e) {
			throw new IllegalStateException(e.getMessage());
		}
	}
}
